package streaming

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming/options"
)

var (
	// maxJitterMs is the maximum retry backoff jitter in milliseconds.
	maxJitterMs = 5000
	// checkStalePeriod is the period at which stale sinks are checked.
	checkStalePeriod = 500 * time.Millisecond
	// staleLeaseDuration is the duration for which the lease is valid
	staleLeaseDuration = 30 * time.Second
)

// acquireLeaseScript is the script used to acquire the stale check lease.
var acquireLeaseScript = redis.NewScript(`
    local key = KEYS[1]
    local new_value = ARGV[1]
    local current_time = ARGV[2]

    local current_value = redis.call("GET", key)
    if current_value == false or tonumber(current_value) < tonumber(current_time) then
        redis.call("SET", key, new_value, "PX", ARGV[3])
        return 1
    end
    return 0
`)

type (
	// Sink represents a stream sink.
	Sink struct {
		// Name is the sink name.
		Name string
		// closed is true if Close completed.
		closed bool
		// consumer is the sink consumer name.
		consumer string
		// staleLockKeyName is the stale check lock key name.
		staleLockKeyName string
		// startID is the sink start event ID.
		startID string
		// noAck is true if there is no need to acknowledge events.
		noAck bool
		// lock is the sink mutex.
		lock sync.Mutex
		// streams are the streams the sink consumes events from.
		streams []*Stream
		// streamCursors is the stream cursors used to read events in
		// the form [stream1, ">", stream2, ">", ..."]
		streamCursors []string
		// blockDuration is the XREADGROUP timeout.
		blockDuration time.Duration
		// maxPolled is the maximum number of events to read in one
		// XREADGROUP call.
		maxPolled int64
		// bufferSize is the sink channel buffer size.
		bufferSize int
		// chans are the sink event channels.
		chans []chan *Event
		// donechan is the sink done channel.
		donechan chan struct{}
		// streamschan notifies the reader when streams are added or
		// removed.
		streamschan chan struct{}
		// wait is the sink cleanup wait group.
		wait sync.WaitGroup
		// closing is true if Close was called.
		closing bool
		// eventFilter is the event filter if any.
		eventFilter eventFilterFunc
		// consumersMap are the replicated maps used to track sink
		// consumers.  Each map key is the sink name and the value is a list
		// of consumer names.  consumersMap is indexed by stream name.
		consumersMap map[string]*rmap.Map
		// consumersKeepAliveMap is the replicated map used to track sink
		// keep-alives.  Each map key is the sink name and the value is the
		// last sink keep-alive timestamp.
		consumersKeepAliveMap *rmap.Map
		// ackGracePeriod is the grace period after which an event is
		// considered unacknowledged.
		ackGracePeriod time.Duration
		// logger is the logger used by the sink.
		logger pulse.Logger
		// rdb is the redis connection.
		rdb *redis.Client
	}

	// eventFilterFunc is the function used to filter events.
	eventFilterFunc func(*Event) bool
)

// newSink creates a new sink.
// Sinks use one Redis consumer per stream they are consuming from.
// Pulse maintains a pool of consumers per stream and reuses them when possible.
// This is because deleting a consumer causes Redis to drop its pending messages
// which is not the semantics Pulse wants to enforce.
func newSink(ctx context.Context, name string, stream *Stream, opts ...options.Sink) (*Sink, error) {
	o := options.ParseSinkOptions(opts...)
	var eventMatcher eventFilterFunc
	if o.Topic != "" {
		eventMatcher = func(e *Event) bool { return e.Topic == o.Topic }
	} else if o.TopicPattern != "" {
		topicPatternRegexp := regexp.MustCompile(o.TopicPattern)
		eventMatcher = func(e *Event) bool { return topicPatternRegexp.MatchString(e.Topic) }
	}

	cm, err := rmap.Join(ctx, consumersMapName(stream), stream.rdb, rmap.WithLogger(stream.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink %s: %w", name, err)
	}
	km, err := rmap.Join(ctx, sinkKeepAliveMapName(name), stream.rdb, rmap.WithLogger(stream.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink keep-alives %s: %w", name, err)
	}

	if err := stream.rdb.XGroupCreateMkStream(ctx, stream.key, name, o.LastEventID).Err(); err != nil && !isBusyGroupErr(err) {
		return nil, fmt.Errorf("failed to create Redis consumer group %s for stream %s: %w", name, stream.Name, err)
	}

	sink := &Sink{
		Name:                  name,
		staleLockKeyName:      staleLockName(name),
		startID:               o.LastEventID,
		noAck:                 o.NoAck,
		streams:               []*Stream{stream},
		streamCursors:         []string{stream.key, ">"},
		blockDuration:         o.BlockDuration,
		maxPolled:             o.MaxPolled,
		bufferSize:            o.BufferSize,
		streamschan:           make(chan struct{}),
		donechan:              make(chan struct{}),
		eventFilter:           eventMatcher,
		consumersMap:          map[string]*rmap.Map{stream.Name: cm},
		consumersKeepAliveMap: km,
		ackGracePeriod:        o.AckGracePeriod,
		logger:                stream.rootLogger.WithPrefix("sink", name),
		rdb:                   stream.rdb,
	}
	consumer, err := sink.getOrCreateConsumer(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create consumer: %w", err)
	}
	sink.consumer = consumer
	sink.logger = sink.logger.WithPrefix("consumer", consumer)

	if _, err := km.Set(ctx, consumer, strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
		return nil, fmt.Errorf("failed to update sink keep-alive: %v", err)
	}

	sink.wait.Add(2)
	go sink.read()
	go sink.manageStaleMessages()

	return sink, nil
}

// getOrCreateConsumer leverages the consumersMap replicated map to atomically get
// an unused consumer or create a new one.
func (s *Sink) getOrCreateConsumer(ctx context.Context, stream *Stream) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for {
		cm := s.consumersMap[stream.Name]
		unusedKey := unusedConsumersKey(s.Name)
		list, ok := cm.Get(unusedKey)

		if !ok {
			consumer := ulid.Make().String()
			if err := stream.rdb.XGroupCreateConsumer(ctx, stream.key, s.Name, consumer).Err(); err != nil {
				return "", fmt.Errorf("failed to create Redis consumer %s for consumer group %s: %w", consumer, s.Name, err)
			}
			if _, err := s.consumersMap[stream.Name].AppendValues(ctx, s.Name, consumer); err != nil {
				s.markConsumerAsUnused(ctx, consumer)
				return "", fmt.Errorf("failed to append store consumer %s for sink %s: %w", consumer, s.Name, err)
			}
			s.logger.Debug("created new consumer", "consumer", consumer)
			return consumer, nil
		}

		unusedConsumers := strings.Split(list, ",")
		consumer := unusedConsumers[0]
		updatedConsumers := strings.Join(unusedConsumers[1:], ",")
		var prev any
		var err error
		if updatedConsumers == "" {
			prev, err = cm.TestAndDelete(ctx, unusedKey, list)
		} else {
			prev, err = cm.TestAndSet(ctx, unusedKey, list, updatedConsumers)
		}
		if err != nil {
			return "", fmt.Errorf("failed to remove consumer from unused pool: %w", err)
		}
		if prev != list {
			// Another goroutine beat us, try again.
			continue
		}

		// Update the consumer's keep-alive timestamp
		if _, err := s.consumersKeepAliveMap.Set(ctx, consumer, strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
			s.logger.Error(fmt.Errorf("failed to update sink keep-alive for reused consumer: %v", err))
			return "", err
		}
		s.logger.Debug("using existing consumer", "consumer", consumer)
		return consumer, nil
	}
}

// Subscribe returns a channel that receives events from the sink.
func (s *Sink) Subscribe() <-chan *Event {
	c := make(chan *Event, s.bufferSize)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.chans = append(s.chans, c)
	return c
}

// Unsubscribe removes the channel from the sink and closes it.
func (s *Sink) Unsubscribe(c <-chan *Event) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, ch := range s.chans {
		if ch == c {
			close(ch)
			s.chans = append(s.chans[:i], s.chans[i+1:]...)
			return
		}
	}
}

// Ack acknowledges the event.
func (s *Sink) Ack(ctx context.Context, e *Event) error {
	err := e.rdb.XAck(ctx, e.streamKey, e.SinkName, e.ID).Err()
	if err != nil {
		s.logger.Error(err, "ack", e.ID, "stream", e.StreamName)
		return err
	}
	s.logger.Debug("acked", "event", e.ID, "stream", e.StreamName, "from-sink", e.SinkName)
	return nil
}

// AddStream adds the stream to the sink. By default the stream cursor starts at
// the same timestamp as the sink main stream cursor.  This can be overridden
// with opts. AddStream does nothing if the stream is already part of the sink.
func (s *Sink) AddStream(ctx context.Context, stream *Stream, opts ...options.AddStream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, s := range s.streams {
		if s.Name == stream.Name {
			return nil
		}
	}
	startID := s.startID
	options := options.ParseAddStreamOptions(opts...)
	for _, option := range opts {
		option(&options)
	}
	if options.LastEventID != "" {
		startID = options.LastEventID
	}

	cm, err := rmap.Join(ctx, consumersMapName(stream), stream.rdb, rmap.WithLogger(stream.logger))
	if err != nil {
		return fmt.Errorf("failed to join consumer replicated map for stream %s: %w", stream.Name, err)
	}
	if _, err := cm.AppendValues(ctx, s.Name, s.consumer); err != nil {
		return fmt.Errorf("failed to append consumer %s to replicated map for stream %s: %w", s.consumer, stream.Name, err)
	}
	if err := stream.rdb.XGroupCreateMkStream(ctx, stream.key, s.Name, startID).Err(); err != nil && !isBusyGroupErr(err) {
		return fmt.Errorf("failed to create Redis consumer group %s for stream %s: %w", s.Name, stream.Name, err)
	}
	s.streams = append(s.streams, stream)
	s.streamCursors = make([]string, len(s.streams)*2)
	for i, stream := range s.streams {
		s.streamCursors[i] = stream.key
		s.streamCursors[len(s.streams)+i] = ">"
	}
	s.consumersMap[stream.Name] = cm
	s.notifyStreamChange()
	s.logger.Info("added", "stream", stream.Name)
	return nil
}

// RemoveStream removes the stream from the sink, it is idempotent.
func (s *Sink) RemoveStream(ctx context.Context, stream *Stream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	found := false
	for i, st := range s.streams {
		if st == stream {
			s.streams = append(s.streams[:i], s.streams[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	s.streamCursors = make([]string, len(s.streams)*2)
	for i, stream := range s.streams {
		s.streamCursors[i] = stream.key
		s.streamCursors[len(s.streams)+i] = ">"
	}
	s.markConsumerAsUnused(ctx, s.consumer)
	remains, err := s.consumersMap[stream.Name].RemoveValues(ctx, s.Name, s.consumer)
	if err != nil {
		return fmt.Errorf("failed to remove consumer %s from replicated map for stream %s: %w", s.consumer, stream.Name, err)
	}
	if len(remains) == 0 {
		if err := s.deleteConsumerGroup(ctx, stream); err != nil {
			return err
		}
	}
	s.notifyStreamChange()
	s.logger.Info("removed", "stream", stream.Name)
	return nil
}

// Close stops event polling and closes the sink channel. It is safe to call
// Close multiple times.
func (s *Sink) Close() {
	s.lock.Lock()
	if s.closing {
		s.lock.Unlock()
		return
	}
	s.closing = true
	close(s.donechan)
	s.lock.Unlock()
	s.wait.Wait()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.markConsumerAsUnused(context.Background(), s.consumer)
	for _, c := range s.chans {
		close(c)
	}
	s.consumersKeepAliveMap.Close()
	for _, cm := range s.consumersMap {
		cm.Close()
	}
	s.closed = true
	s.logger.Info("closed")
}

// IsClosed returns true if the sink was closed.
func (s *Sink) IsClosed() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.closed
}

// read reads events from the streams and sends them to the sink channel.
func (s *Sink) read() {
	defer s.wait.Done()
	ctx := context.Background()
	for {
		streamsEvents, err := readOnce(ctx, s.readGroup, s.streamschan, s.donechan, s.logger)
		s.lock.Lock()
		if s.closing {
			s.lock.Unlock()
			// Honor the Close contract and do not forward any more events.
			// Any events in the PEL will be claimed by another consumer.
			return
		}
		if err != nil {
			if err := handleReadError(err, s.logger); err != nil {
				s.logger.Error(fmt.Errorf("error reading events: %w", err))
			}
			s.lock.Unlock()
			continue
		}
		for _, events := range streamsEvents {
			streamName := events.Stream[len(streamKeyPrefix):]
			streamEvents(streamName, events.Stream, s.Name, events.Messages, s.eventFilter, s.chans, s.rdb, s.logger)
		}
		s.lock.Unlock()
	}
}

// manageStaleMessages does 3 things:
//  1. It refreshes this consumer keep-alive every half ack grace period
//  2. It claims any stale message every check stale period (a stale message is
//     one that has been read but not acked for more than the ack grace period)
//  3. It leverages the consumers keep-alive map to detect when a consumer is
//     no longer active and removes it from the consumer group.
func (s *Sink) manageStaleMessages() {
	keepAliveTicker := time.NewTicker(s.ackGracePeriod / 2)
	checkStaleTicker := time.NewTicker(checkStalePeriod)
	defer func() {
		checkStaleTicker.Stop()
		keepAliveTicker.Stop()
		s.wait.Done()
	}()
	ctx := context.Background()
	for {
		select {
		case <-keepAliveTicker.C:
			// Update this consumer keep-alive
			t := strconv.FormatInt(time.Now().Unix(), 10)
			if _, err := s.consumersKeepAliveMap.Set(ctx, s.consumer, t); err != nil {
				s.logger.Error(fmt.Errorf("failed to update sink keep-alive: %v", err))
			}

		case <-checkStaleTicker.C:
			acquired, err := s.acquireStaleCheckLease(ctx)
			if err != nil {
				s.logger.Error(fmt.Errorf("failed to acquire stale check lease: %v", err))
				continue
			}
			if !acquired {
				continue
			}
			s.removeStaleConsumers(ctx)
			s.releaseStaleCheckLease(ctx)

		case <-s.donechan:
			return
		}
	}
}

// acquireStaleCheckLease attempts to acquire the lease for stale check.
func (s *Sink) acquireStaleCheckLease(ctx context.Context) (bool, error) {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	newExpiration := now + int64(staleLeaseDuration.Milliseconds())

	result, err := acquireLeaseScript.Run(ctx, s.rdb, []string{s.staleLockKeyName},
		newExpiration, now, staleLeaseDuration.Milliseconds()).Result()
	if err != nil {
		return false, err
	}

	return result == int64(1), nil
}

// releaseStaleCheckLease releases the lease for stale check.
func (s *Sink) releaseStaleCheckLease(ctx context.Context) {
	s.rdb.Del(ctx, s.staleLockKeyName)
}

// removeStaleConsumers removes consumers that have not been active for more
// than twice the ack grace period.
func (s *Sink) removeStaleConsumers(ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closing {
		return
	}
	consumers := s.consumersKeepAliveMap.Map()
	// 1. Claim stale messages
	for _, stream := range s.streams {
		args := redis.XAutoClaimArgs{
			Stream:   stream.key,
			Group:    s.Name,
			MinIdle:  s.ackGracePeriod,
			Start:    "0-0",
			Consumer: s.consumer,
		}
		start, err := s.claim(ctx, stream.Name, args)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to claim stale messages for stream %s: %w", stream.Name, err))
			continue
		}
		for start != "0-0" {
			args.Start = start
			start, err = s.claim(ctx, stream.Name, args)
			if err != nil {
				s.logger.Error(fmt.Errorf("failed to claim stale messages for stream %s: %w", stream.Name, err))
				break
			}
		}
	}
	// 2. Mark stale consumers as unused
	var unusedConsumers []string
	for _, cm := range s.consumersMap {
		if unused, ok := cm.GetValues(unusedConsumersKey(s.Name)); ok {
			unusedConsumers = append(unusedConsumers, unused...)
		}
	}
	for consumer, lastKeepAlive := range consumers {
		if slices.Contains(unusedConsumers, consumer) {
			continue
		}
		lastKeepAliveTime, err := strconv.ParseInt(lastKeepAlive, 10, 64)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to parse sink keep-alive: %v", err))
			continue
		}
		if time.Since(time.Unix(lastKeepAliveTime, 0)) > 2*s.ackGracePeriod {
			s.markConsumerAsUnused(ctx, consumer)
		}
	}
}

// Helper function to claim messages from a stream used by removeStaleConsumers.
func (s *Sink) claim(ctx context.Context, streamName string, args redis.XAutoClaimArgs) (string, error) {
	messages, start, err := s.rdb.XAutoClaim(ctx, &args).Result()
	if len(messages) > 0 {
		s.logger.Info("claimed", "stream", streamName, "messages", len(messages))
		streamEvents(streamName, args.Stream, s.Name, messages, s.eventFilter, s.chans, s.rdb, s.logger)
	}
	return start, err
}

// readGroup reads events from the streams using the sink consumer group.
func (s *Sink) readGroup(ctx context.Context) ([]redis.XStream, error) {
	s.lock.Lock()
	readStreams := make([]string, len(s.streamCursors))
	copy(readStreams, s.streamCursors)
	s.lock.Unlock()
	if s.blockDuration > 4*time.Second {
		s.logger.Debug("reading", "streams", readStreams, "max", s.maxPolled, "block", s.blockDuration)
	}
	return s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.Name,
		Consumer: s.consumer,
		Streams:  readStreams,
		Count:    s.maxPolled,
		Block:    s.blockDuration,
		NoAck:    s.noAck,
	}).Result()
}

// notifyStreamChange notifies the reader that the streams have changed.
func (s *Sink) notifyStreamChange() {
	select {
	case s.streamschan <- struct{}{}:
	default:
	}
}

// deleteConsumerGroup deletes the consumer group.
func (s *Sink) deleteConsumerGroup(ctx context.Context, stream *Stream) error {
	if err := s.rdb.XGroupDestroy(ctx, stream.key, s.Name).Err(); err != nil {
		return fmt.Errorf("failed to destroy Redis consumer group %q for stream %q: %w", s.Name, stream.Name, err)
	}
	delete(s.consumersMap, stream.Name)
	return nil
}

// markConsumerAsUnused marks the consumer as unused.
// s.lock must be held when calling this function.
func (s *Sink) markConsumerAsUnused(ctx context.Context, consumer string) {
	s.logger.Debug("marking consumer unused", "sink", s.Name, "consumer", consumer)
	key := unusedConsumersKey(s.Name)
	for _, cm := range s.consumersMap {
		if _, err := cm.AppendUniqueValues(ctx, key, consumer); err != nil {
			s.logger.Error(fmt.Errorf("failed to mark consumer as unused: %w", err))
		}
	}
}

func unusedConsumersKey(sink string) string {
	return fmt.Sprintf("%s:unused", sink)
}

// isBusyGroupErr returns true if the error is a busy group error.
func isBusyGroupErr(err error) bool {
	return strings.Contains(err.Error(), "BUSYGROUP")
}

// consumersMapName is the name of the replicated map that backs a sink.
func consumersMapName(stream *Stream) string {
	return fmt.Sprintf("stream:%s:sinks", stream.Name)
}

// sinkKeepAliveMapName is the name of the replicated map that backs a sink keep-alives.
func sinkKeepAliveMapName(sink string) string {
	return fmt.Sprintf("sink:%s:keepalive", sink)
}

// staleLockName is the name of the lock used to check for stale messages.
func staleLockName(sink string) string {
	return fmt.Sprintf("sink:%s:stalelease", sink)
}
