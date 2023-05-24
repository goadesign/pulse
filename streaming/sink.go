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

	"goa.design/ponos/ponos"
	"goa.design/ponos/rmap"
)

var (
	// maxJitterMs is the maximum retry backoff jitter in milliseconds.
	maxJitterMs = 5000
	// checkStalePeriod is the period at which stale sinks are checked.
	checkStalePeriod = 500 * time.Millisecond
)

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
		// eventMatcher is the event matcher if any.
		eventMatcher EventMatcherFunc
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
		logger ponos.Logger
		// rdb is the redis connection.
		rdb *redis.Client
	}
)

// newSink creates a new sink.
func newSink(ctx context.Context, name string, stream *Stream, opts ...SinkOption) (*Sink, error) {
	options := defaultSinkOptions()
	for _, option := range opts {
		option(&options)
	}
	eventMatcher := options.EventMatcher
	if eventMatcher == nil {
		if options.Topic != "" {
			eventMatcher = func(e *Event) bool { return e.Topic == options.Topic }
		} else if options.TopicPattern != "" {
			topicPatternRegexp := regexp.MustCompile(options.TopicPattern)
			eventMatcher = func(e *Event) bool { return topicPatternRegexp.MatchString(e.Topic) }
		}
	}

	// Record consumer so we can destroy the sink when no longer needed.
	// Note: we fail if we cannot record the consumer so we are guaranteed that
	// the consumer group won't get destroyed prematurely. When closing the sink
	// we do the reverse: we destroy the consumer group first and then remove
	// the consumer from the replicated map.
	cm, err := rmap.Join(ctx, consumersMapName(stream), stream.rdb, rmap.WithLogger(stream.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink %s: %w", name, err)
	}
	km, err := rmap.Join(ctx, sinkKeepAliveMapName(name), stream.rdb, rmap.WithLogger(stream.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink keep-alives %s: %w", name, err)
	}
	consumer := ulid.Make().String()
	if _, err := cm.AppendValues(ctx, name, consumer); err != nil {
		return nil, fmt.Errorf("failed to append store consumer %s for sink %s: %w", consumer, name, err)
	}
	if _, err := km.Set(ctx, consumer, strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
		return nil, fmt.Errorf("failed to update sink keep-alive: %v", err)
	}

	// Create the consumer group, ignore error if it already exists.
	if err := stream.rdb.XGroupCreateMkStream(ctx, stream.key, name, options.LastEventID).Err(); err != nil && !isBusyGroupErr(err) {
		return nil, fmt.Errorf("failed to create Redis consumer group %s for stream %s: %w", name, stream.Name, err)
	}

	// Create consumer and sink
	if err := stream.rdb.XGroupCreateConsumer(ctx, stream.key, name, consumer).Err(); err != nil {
		return nil, fmt.Errorf("failed to create Redis consumer %s for consumer group %s: %w", consumer, name, err)
	}
	sink := &Sink{
		Name:                  name,
		consumer:              consumer,
		staleLockKeyName:      staleLockName(name),
		startID:               options.LastEventID,
		noAck:                 options.NoAck,
		streams:               []*Stream{stream},
		streamCursors:         []string{stream.key, ">"},
		blockDuration:         options.BlockDuration,
		maxPolled:             options.MaxPolled,
		bufferSize:            options.BufferSize,
		streamschan:           make(chan struct{}),
		donechan:              make(chan struct{}),
		eventMatcher:          eventMatcher,
		consumersMap:          map[string]*rmap.Map{stream.Name: cm},
		consumersKeepAliveMap: km,
		ackGracePeriod:        options.AckGracePeriod,
		logger:                stream.rootLogger.WithPrefix("sink", name, "consumer", consumer),
		rdb:                   stream.rdb,
	}

	sink.wait.Add(2)
	go sink.read()
	go sink.manageStaleMessages()
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

// Ack acknowledges the event.
func (s *Sink) Ack(ctx context.Context, e *Event) error {
	err := e.rdb.XAck(ctx, e.streamKey, e.SinkName, e.ID).Err()
	if err != nil {
		s.logger.Error(err, "ack", e.ID, "stream", e.StreamName)
		return err
	}
	s.logger.Debug("acked", "event", e.ID, "stream", e.StreamName)
	return nil
}

// AddStream adds the stream to the sink. By default the stream cursor starts at
// the same timestamp as the sink main stream cursor.  This can be overridden
// with opts. AddStream does nothing if the stream is already part of the sink.
func (s *Sink) AddStream(ctx context.Context, stream *Stream, opts ...AddStreamOption) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, s := range s.streams {
		if s.Name == stream.Name {
			return nil
		}
	}
	startID := s.startID
	options := defaultAddStreamOptions()
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
	if err := s.rdb.XGroupDelConsumer(ctx, stream.key, s.Name, s.consumer).Err(); err != nil {
		err = fmt.Errorf("failed to delete Redis consumer for consumer group %s: %w", s.Name, err)
		s.logger.Error(err, "consumer", s.consumer)
		return err
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
	ctx := context.Background()
	defer func() {
		s.lock.Lock()
		s.removeConsumer(ctx, s.consumer)
		s.lock.Unlock()
		for _, c := range s.chans {
			close(c)
		}
		s.wait.Done()
		s.logger.Debug("stopped")
	}()
	for {
		streamsEvents, err := readOnce(ctx, s.readGroup, s.streamschan, s.donechan, s.logger)
		if s.isClosing() {
			return
		}
		if err != nil {
			if err := handleReadError(err, s.logger); err != nil {
				s.logger.Error(fmt.Errorf("fatal error reading event: %w", err))
			}
			continue
		}

		s.lock.Lock()
		for _, events := range streamsEvents {
			streamName := events.Stream[len(streamKeyPrefix):]
			streamEvents(streamName, events.Stream, s.Name, events.Messages, s.eventMatcher, s.chans, s.rdb, s.logger)
		}
		s.lock.Unlock()
	}
}

// manageStaleMessages does 3 things:
//
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
			// Check for stale messages and consumers.  All the
			// consumers run the stale check loop but the check only
			// needs to happen once every 250ms-500ms.  A simple
			// redis key is used to lock the check.
			acquired, err := s.rdb.SetNX(ctx, "stale-check-lock", "1", checkStalePeriod).Result()
			if err != nil {
				s.logger.Error(fmt.Errorf("failed to acquire stale check lock: %v", err))
				continue
			}
			if !acquired {
				continue
			}
			s.removeStaleConsumers(ctx)

		case <-s.donechan:
			return
		}
	}
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
	ackSeconds := int64(s.ackGracePeriod.Seconds())
	for consumer, lastKeepAlive := range consumers {
		// 1. Claim stale messages
		for _, stream := range s.streams {
			args := redis.XAutoClaimArgs{
				Stream:   stream.key,
				Group:    s.Name,
				MinIdle:  s.ackGracePeriod,
				Start:    "0-0",
				Consumer: consumer,
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
		// 2. Remove stale consumers
		if consumer == s.consumer {
			continue
		}
		lastKeepAliveTime, err := strconv.ParseInt(lastKeepAlive, 10, 64)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to parse sink keep-alive: %v", err))
			continue
		}
		// We consider a consumer stale if it has not sent a keep-alive
		// for more than twice the ackGracePeriod. This is to avoid
		// removing a consumer that still owns messages in the PEL.
		if time.Now().Unix()-lastKeepAliveTime > 2*ackSeconds {
			// Consumer is stale, remove it
			s.logger.Info("removing", "stale-consumer", consumer)
			s.removeConsumer(ctx, consumer)
		}
	}
}

// Helper function to claim messages from a stream used by removeStaleConsumers.
func (s *Sink) claim(ctx context.Context, streamName string, args redis.XAutoClaimArgs) (string, error) {
	messages, start, err := s.rdb.XAutoClaim(ctx, &args).Result()
	if len(messages) > 0 {
		s.logger.Info("claimed", "stale-consumer", args.Consumer, "messages", len(messages))
		streamEvents(streamName, args.Stream, s.Name, messages, s.eventMatcher, s.chans, s.rdb, s.logger)
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

// removeConsumer removes the consumer from the consumer groups and removes the
// sink from the sinks map. This method is called automatically when the sink is
// closed.
func (s *Sink) removeConsumer(ctx context.Context, consumer string) {
	for _, stream := range s.streams {
		if err := s.rdb.XGroupDelConsumer(ctx, stream.key, s.Name, consumer).Err(); err != nil {
			s.logger.Error(fmt.Errorf("failed to delete Redis consumer: %w", err), "stream", stream.Name)
		}
		cm := s.consumersMap[stream.Name]
		remains, err := cm.RemoveValues(ctx, s.Name, consumer)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to remove consumer from consumer map: %w", err), "stream", stream.Name)
		}
		if len(remains) == 0 {
			cm.Close()
			if err := s.deleteConsumerGroup(ctx, stream); err != nil {
				s.logger.Error(err, "stream", stream.Name)
			}
		}
	}
	if _, err := s.consumersKeepAliveMap.Delete(ctx, consumer); err != nil {
		s.logger.Error(fmt.Errorf("failed to remove consumer from keep-alive map: %w", err))
	}
}

func (s *Sink) deleteConsumerGroup(ctx context.Context, stream *Stream) error {
	if err := s.rdb.XGroupDestroy(ctx, stream.key, s.Name).Err(); err != nil {
		return fmt.Errorf("failed to destroy Redis consumer group %s for stream %s: %w", s.Name, stream.Name, err)
	}
	delete(s.consumersMap, stream.Name)
	return nil
}

// isClosing returns true if the sink is closing.
func (s *Sink) isClosing() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.closing
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
	return fmt.Sprintf("sink:%s:stalelock", sink)
}
