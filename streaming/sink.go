package streaming

import (
	"context"
	"fmt"
	"regexp"
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
	// checkIdlePeriod is the period at which idle messages are checked.
	checkIdlePeriod = 500 * time.Millisecond
)

// acquireLeaseScript is the script used to acquire the stale check lease.
var acquireLeaseScript = `
    local key = KEYS[1]
    local new_value = ARGV[1]
    local current_time = ARGV[2]

    local current_value = redis.call("GET", key)
    if current_value == false or tonumber(current_value) < tonumber(current_time) then
        redis.call("SET", key, new_value, "PX", ARGV[3])
        return 1
    end
    return 0
`

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
		staleLockKeyName []string
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
		// consumer names are unique for each in-process sink instance.
		consumersMap map[string]*rmap.Map
		// consumersKeepAliveMap records consumer keep-alives for this
		// sink (i.e. for all in-process instances of the sink).
		consumersKeepAliveMap *rmap.Map
		// ackGracePeriod is the grace period after which an event is
		// considered unacknowledged.
		ackGracePeriod time.Duration
		// lastKeepAlive is the last keep-alive timestamp for this consumer.
		lastKeepAlive int64
		// logger is the logger used by the sink.
		logger pulse.Logger
		// acquireLease is the acquire lease script.
		acquireLease *redis.Script
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
		staleLockKeyName:      []string{staleLockName(name)},
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
		acquireLease:          redis.NewScript(acquireLeaseScript),
		rdb:                   stream.rdb,
	}
	consumer, err := sink.reuseOrCreateConsumer(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create consumer: %w", err)
	}
	sink.consumer = consumer
	sink.logger = sink.logger.WithPrefix("consumer", consumer)

	sink.wait.Add(3)
	go sink.read()
	go sink.periodicKeepAlive()
	go sink.periodicIdleMessageCheck()

	return sink, nil
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
	s.recordUnusedConsumer(context.Background())
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

// reuseOrCreateConsumer leverages the consumersKeepAliveMap replicated map to
// atomically reuse an unused consumer or create a new one.
func (s *Sink) reuseOrCreateConsumer(ctx context.Context, stream *Stream) (string, error) {
	km := s.consumersKeepAliveMap.Map()
	for consumer, ka := range km {
		keepAlive, err := strconv.ParseInt(ka, 10, 64)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to parse keep-alive timestamp for consumer %s: %w", consumer, err))
			continue
		}
		if time.Since(time.Unix(0, keepAlive)) < s.ackGracePeriod {
			continue
		}
		newKeepAlive := time.Now().UnixNano()
		prev, err := s.consumersKeepAliveMap.TestAndSet(ctx, consumer, ka, strconv.FormatInt(newKeepAlive, 10))
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to test and set keep-alive for reused consumer: %v", err))
			continue
		}
		if prev != ka {
			continue
		}
		s.lastKeepAlive = newKeepAlive
		s.logger.Debug("reusing stale consumer", "consumer", consumer)
		return consumer, nil
	}

	consumer := ulid.Make().String()
	if err := stream.rdb.XGroupCreateConsumer(ctx, stream.key, s.Name, consumer).Err(); err != nil {
		return "", fmt.Errorf("failed to create Redis consumer %s for consumer group %s: %w", consumer, s.Name, err)
	}
	if _, err := s.consumersMap[stream.Name].AppendValues(ctx, s.Name, consumer); err != nil {
		s.recordUnusedConsumer(ctx)
		return "", fmt.Errorf("failed to append store consumer %s for sink %s: %w", consumer, s.Name, err)
	}
	s.lastKeepAlive = time.Now().UnixNano()
	if _, err := s.consumersKeepAliveMap.Set(ctx, consumer, strconv.FormatInt(s.lastKeepAlive, 10)); err != nil {
		s.recordUnusedConsumer(ctx)
		return "", fmt.Errorf("failed to set sink keep-alive for new consumer %s: %w", consumer, err)
	}
	s.logger.Debug("created new consumer", "consumer", consumer)
	return consumer, nil
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

// periodicKeepAlive updates this consumer keep-alive every half ack grace period
func (s *Sink) periodicKeepAlive() {
	defer s.wait.Done()
	ticker := time.NewTicker(s.ackGracePeriod / 2)
	defer ticker.Stop()

	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano()
			if _, err := s.consumersKeepAliveMap.Set(ctx, s.consumer, strconv.FormatInt(now, 10)); err != nil {
				s.logger.Error(fmt.Errorf("failed to update sink keep-alive: %v", err))
				continue
			}
			s.lock.Lock()
			s.lastKeepAlive = now
			s.lock.Unlock()

		case <-s.donechan:
			return
		}
	}
}

// periodicIdleMessageCheck claims any idle message every check stale period.
// An idle message is one that has not been acked for more than the ack grace period.
func (s *Sink) periodicIdleMessageCheck() {
	defer s.wait.Done()
	ticker := time.NewTicker(checkIdlePeriod)
	defer ticker.Stop()

	staleLeaseDuration := checkIdlePeriod.Milliseconds() - 5
	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano() / int64(time.Millisecond)
			newExpiration := now + staleLeaseDuration
			result, err := s.acquireLease.Run(ctx, s.rdb, s.staleLockKeyName, newExpiration, now, staleLeaseDuration).Result()
			if err != nil {
				s.logger.Error(fmt.Errorf("failed to acquire stale check lease: %v", err))
				continue
			}
			if result != int64(1) {
				// Another sink instance claimed the lease.
				continue
			}
			s.claimIdleMessages(ctx)

		case <-s.donechan:
			return
		}
	}
}

// claimIdleMessages claims idle messages from the streams.
func (s *Sink) claimIdleMessages(ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closing {
		return
	}
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
}

// Helper function to claim messages from a stream used by claimIdleMessages.
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

	// Make sure this consumer is still alive.
	if time.Since(time.Unix(0, s.lastKeepAlive)) > s.ackGracePeriod {
		s.logger.Debug("consumer stale, creating new one", "consumer", s.consumer)
		var err error
		s.consumer, err = s.reuseOrCreateConsumer(ctx, s.streams[0])
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to create new consumer: %w", err))
			s.lock.Unlock()
			return nil, err
		}
	}

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

// recordUnusedConsumer marks the consumer as unused by setting its keep-alive
// timestamp to 0. It deletes the keep alive map if all consumers are unused.
// s.lock must be held when calling this function.
// TBD: also delete the redis consumer
func (s *Sink) recordUnusedConsumer(ctx context.Context) {
	s.logger.Debug("unused", "sink", s.Name, "consumer", s.consumer)
	s.lastKeepAlive = 0
	keepAlives := s.consumersKeepAliveMap.Map()
	if len(keepAlives) == 0 {
		return
	}

	// TBD: also delete the redis consumer
	// allStale := true
	// keys := make([]string, 0, len(keepAlives))
	// vals := make([]string, 0, len(keepAlives))
	// for k, ka := range keepAlives {
	// 	keys = append(keys, k)
	// 	vals = append(vals, ka)
	// 	t, err := strconv.ParseInt(ka, 10, 64)
	// 	if err != nil {
	// 		s.logger.Error(fmt.Errorf("failed to parse keep-alive timestamp for consumer %s: %w", k, err))
	// 		allStale = false
	// 		break
	// 	}
	// 	if time.Since(time.Unix(0, t)) < s.ackGracePeriod {
	// 		allStale = false
	// 		break
	// 	}
	// }
	// if allStale {
	// 	reset, err := s.consumersKeepAliveMap.TestAndReset(ctx, keys, vals)
	// 	if err != nil {
	// 		s.logger.Error(fmt.Errorf("failed to test and reset unused consumer keep-alives: %w", err))
	// 	}
	// 	if reset {
	// 		s.logger.Debug("deleted unused keep-alive map")
	// 		return
	// 	}
	// }

	// There are non-stale entries, mark this consumer as unused.

	if _, err := s.consumersKeepAliveMap.Set(ctx, s.consumer, "0"); err != nil {
		s.logger.Error(fmt.Errorf("failed to set unused consumer keep-alive: %w", err))
	}
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
