package streaming

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"

	"goa.design/clue/log"
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

// acquireLeaseScript is the script used to acquire the idle message check lease.
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
		// leaseKeyName is the stale check lock key name.
		leaseKeyName []string
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

	if err := acquireLeaseScript.Load(ctx, stream.rdb).Err(); err != nil {
		return nil, fmt.Errorf("failed to load stale check lease script: %w", err)
	}

	logger := stream.rootLogger.WithPrefix("sink", name)
	cm, err := rmap.Join(ctx, consumersMapName(stream), stream.rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink %s: %w", name, err)
	}
	km, err := rmap.Join(ctx, sinkKeepAliveMapName(name), stream.rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink keep-alives %s: %w", name, err)
	}

	if err := stream.rdb.XGroupCreateMkStream(ctx, stream.key, name, o.LastEventID).Err(); err != nil && !isBusyGroupErr(err) {
		return nil, fmt.Errorf("failed to create Redis consumer group %s for stream %s: %w", name, stream.Name, err)
	}

	sink := &Sink{
		Name:                  name,
		leaseKeyName:          []string{staleLockName(name)},
		startID:               o.LastEventID,
		noAck:                 o.NoAck,
		streams:               []*Stream{stream},
		streamCursors:         []string{stream.key, ">"},
		blockDuration:         o.BlockDuration,
		maxPolled:             o.MaxPolled,
		bufferSize:            o.BufferSize,
		donechan:              make(chan struct{}),
		eventFilter:           eventMatcher,
		consumersMap:          map[string]*rmap.Map{stream.Name: cm},
		consumersKeepAliveMap: km,
		ackGracePeriod:        o.AckGracePeriod,
		acquireLease:          acquireLeaseScript,
		logger:                logger,
		rdb:                   stream.rdb,
	}
	consumer, err := sink.newConsumer(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	sink.consumer = consumer
	sink.logger = sink.logger.WithPrefix("consumer", consumer)

	// create new logger context for goroutines.
	logCtx := context.Background()
	logCtx = log.WithContext(logCtx, ctx)

	sink.wait.Add(3)
	pulse.Go(logger, func() { sink.read(logCtx) })
	pulse.Go(logger, sink.periodicKeepAlive)
	pulse.Go(logger, sink.periodicIdleMessageCheck)

	sink.logger.Info("created", "start", sink.startID, "stream", stream.Name, "max_polled", sink.maxPolled, "block_duration", sink.blockDuration, "buffer_size", sink.bufferSize, "no_ack", sink.noAck, "ack_grace_period", sink.ackGracePeriod)

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
	err := e.Acker.XAck(ctx, e.streamKey, e.SinkName, e.ID).Err()
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
	if err := s.removeStreamConsumer(ctx, stream); err != nil {
		return err
	}
	s.logger.Info("removed", "stream", stream.Name)
	return nil
}

// removeStreamConsumer removes the stream consumer from the sink.
func (s *Sink) removeStreamConsumer(ctx context.Context, stream *Stream) error {
	remains, _, err := s.consumersMap[stream.Name].RemoveValues(ctx, s.Name, s.consumer)
	if err != nil {
		return fmt.Errorf("failed to remove consumer %s from replicated map for stream %s: %w", s.consumer, stream.Name, err)
	}
	if len(remains) == 0 {
		if err := s.deleteConsumerGroup(ctx, stream); err != nil {
			return err
		}
	}
	return nil
}

// Close stops event polling, waits for all events to be processed, and closes the sink channel.
// It is safe to call Close multiple times.
func (s *Sink) Close(ctx context.Context) {
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
	for _, c := range s.chans {
		close(c)
	}
	// Note: we do not delete the consumer from the keep-alive and consumer maps
	// so that another instance may claim any pending messages.
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

// newConsumer creates a new consumer and registers it in the consumers and
// keep-alive maps.
func (s *Sink) newConsumer(ctx context.Context, stream *Stream) (string, error) {
	consumer := ulid.Make().String()
	if err := stream.rdb.XGroupCreateConsumer(ctx, stream.key, s.Name, consumer).Err(); err != nil {
		return "", fmt.Errorf("failed to create Redis consumer %s for consumer group %s: %w", consumer, s.Name, err)
	}
	if _, err := s.consumersMap[stream.Name].AppendValues(ctx, s.Name, consumer); err != nil {
		if err := stream.rdb.XGroupDelConsumer(ctx, stream.key, s.Name, consumer).Err(); err != nil {
			s.logger.Error(fmt.Errorf("failed to delete consumer %s after failed append: %w", consumer, err))
		}
		return "", fmt.Errorf("failed to append store consumer %s for sink %s: %w", consumer, s.Name, err)
	}
	s.lastKeepAlive = time.Now().UnixNano()
	if _, err := s.consumersKeepAliveMap.Set(ctx, consumer, strconv.FormatInt(s.lastKeepAlive, 10)); err != nil {
		if err := stream.rdb.XGroupDelConsumer(ctx, stream.key, s.Name, consumer).Err(); err != nil {
			s.logger.Error(fmt.Errorf("failed to delete consumer %s after failed keep-alive set: %w", consumer, err))
		}
		return "", fmt.Errorf("failed to set sink keep-alive for new consumer %s: %w", consumer, err)
	}
	return consumer, nil
}

// read reads events from the streams and sends them to the sink channel.
func (s *Sink) read(ctx context.Context) {
	defer s.logger.Debug("read: exiting")
	defer s.wait.Done()
	for {
		if err := s.ensureConsumer(ctx); err != nil {
			time.Sleep(time.Duration(rand.Int63n(int64(s.blockDuration))))
			continue
		}
		s.lock.Lock()
		readStreams := make([]string, len(s.streamCursors))
		copy(readStreams, s.streamCursors)
		s.lock.Unlock()

		s.logger.Debug("reading", "streams", readStreams, "max", s.maxPolled, "block", s.blockDuration)
		streams, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.Name,
			Consumer: s.consumer,
			Streams:  readStreams,
			Count:    s.maxPolled,
			Block:    s.blockDuration,
			NoAck:    s.noAck,
		}).Result()

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
		for _, events := range streams {
			streamName := events.Stream[len(streamKeyPrefix):]
			streamEvents(streamName, events.Stream, s.Name, events.Messages, s.eventFilter, s.chans, s.rdb, s.logger)
		}
		s.lock.Unlock()
	}
}

// ensureConsumer ensures that the consumer is still alive.
func (s *Sink) ensureConsumer(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if time.Since(time.Unix(0, s.lastKeepAlive)) > 2*s.ackGracePeriod {
		s.logger.Debug("consumer stale, creating new one")
		var err error
		s.consumer, err = s.newConsumer(ctx, s.streams[0])
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to create new consumer: %w", err))
			return err
		}
	}
	return nil
}

// periodicKeepAlive updates this consumer keep-alive every half ack grace period.
func (s *Sink) periodicKeepAlive() {
	defer s.wait.Done()
	defer s.logger.Debug("periodicKeepAlive: exiting")
	ticker := time.NewTicker(s.ackGracePeriod)
	defer ticker.Stop()

	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			s.lock.Lock()
			now := time.Now().UnixNano()
			if _, err := s.consumersKeepAliveMap.Set(ctx, s.consumer, strconv.FormatInt(now, 10)); err != nil {
				s.logger.Error(fmt.Errorf("failed to update sink keep-alive: %v", err))
				s.lock.Unlock()
				continue
			}
			s.lastKeepAlive = now
			s.lock.Unlock()

		case <-s.donechan:
			return
		}
	}
}

// periodicIdleMessageCheck claims any idle message every check stale period.
// An idle message is one that has not been acked for more than the ack grace period.
// Once all idle messages are claimed, any stale consumer is deleted.
func (s *Sink) periodicIdleMessageCheck() {
	defer s.wait.Done()
	defer s.logger.Debug("periodicIdleMessageCheck: exiting")
	ticker := time.NewTicker(checkIdlePeriod)
	defer ticker.Stop()

	leaseDuration := checkIdlePeriod.Milliseconds() - 5
	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano() / int64(time.Millisecond)
			newExpiration := now + leaseDuration
			result, err := s.acquireLease.EvalSha(ctx, s.rdb, s.leaseKeyName, newExpiration, now, leaseDuration).Result()
			if err != nil {
				s.logger.Error(fmt.Errorf("failed to acquire idle message check lease: %v", err))
				continue
			}
			if result != int64(1) {
				// Another sink instance claimed the lease.
				continue
			}
			s.lock.Lock()
			s.claimIdleMessages(ctx)
			s.deleteStaleConsumers(ctx)
			s.lock.Unlock()

		case <-s.donechan:
			return
		}
	}
}

// claimIdleMessages claims idle messages from the streams.
// s.lock must be held.
func (s *Sink) claimIdleMessages(ctx context.Context) {
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
			s.logger.Error(fmt.Errorf("failed to claim idle messages for stream %s: %w", stream.Name, err))
			continue
		}
		for start != "0-0" {
			args.Start = start
			start, err = s.claim(ctx, stream.Name, args)
			if err != nil {
				s.logger.Error(fmt.Errorf("failed to claim idle messages for stream %s: %w", stream.Name, err))
				break
			}
		}
	}
}

// deleteStaleConsumers deletes stale consumers.
// s.lock must be held.
func (s *Sink) deleteStaleConsumers(ctx context.Context) {
	keepAlives := s.consumersKeepAliveMap.Map()
	staleConsumers := make(map[string]struct{})
	for consumer, timestamp := range keepAlives {
		ts, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to parse timestamp for consumers: %w", err), "consumer", consumer)
			continue
		}
		if time.Since(time.Unix(0, ts)) > 2*s.ackGracePeriod {
			staleConsumers[consumer] = struct{}{}
			s.logger.Debug("stale consumer", "consumer", consumer, "since", time.Since(time.Unix(0, ts)), "ttl", 2*s.ackGracePeriod)
		}
	}
	// Delete any stale consumers from the consumer group.
	for _, stream := range s.streams {
		sinks := s.consumersMap[stream.Name]
		for _, sink := range sinks.Keys() {
			consumers, _ := sinks.GetValues(sink)
			for _, consumer := range consumers {
				if _, ok := staleConsumers[consumer]; !ok {
					continue
				}
				if err := s.rdb.XGroupDelConsumer(ctx, stream.key, s.Name, consumer).Err(); err != nil {
					s.logger.Error(fmt.Errorf("failed to delete stale consumer %s: %w", consumer, err))
				}
				if _, err := s.consumersKeepAliveMap.Delete(ctx, consumer); err != nil {
					s.logger.Error(fmt.Errorf("failed to delete sink keep-alive for stale consumer %s: %w", consumer, err))
				}
				if _, _, err := sinks.RemoveValues(ctx, s.Name, consumer); err != nil {
					s.logger.Error(fmt.Errorf("failed to remove consumer from map: %w", err), "stream", stream.Name, "consumer", consumer)
				}
				delete(staleConsumers, consumer)
				s.logger.Debug("deleted stale consumer", "consumer", consumer)
			}
		}
	}
	// Delete any remaining stale consumers from the keep-alive map.
	for consumer := range staleConsumers {
		if _, err := s.consumersKeepAliveMap.Delete(ctx, consumer); err != nil {
			s.logger.Error(fmt.Errorf("failed to delete sink keep-alive for orphaned consumer %s: %w", consumer, err))
		}
		s.logger.Debug("deleted orphaned consumer", "consumer", consumer)
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

// deleteConsumerGroup deletes the consumer group.
func (s *Sink) deleteConsumerGroup(ctx context.Context, stream *Stream) error {
	if err := s.rdb.XGroupDestroy(ctx, stream.key, s.Name).Err(); err != nil {
		return fmt.Errorf("failed to destroy Redis consumer group %q for stream %q: %w", s.Name, stream.Name, err)
	}
	delete(s.consumersMap, stream.Name)
	return nil
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
