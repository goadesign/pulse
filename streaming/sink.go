package streaming

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"

	"goa.design/ponos/ponos"
	"goa.design/ponos/rmap"
)

const (
	// maxJitterMs is the maximum retry backoff jitter in milliseconds.
	maxJitterMs = 5000

	// pollDuration is the maximum stream read block time duration.
	pollDuration = 3 * time.Second

	// maxMessages is the maximum number of messages to read from the stream.
	maxMessages = 100
)

type (
	// Sink represents a stream sink.
	Sink struct {
		// Name is the sink name.
		Name string
		// C is the sink event channel.
		C <-chan *AckedEvent
		// stopped is true if Stop completed.
		stopped bool
		// consumer is the sink consumer name.
		consumer string
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
		// readchan is used to communicate the results of XREADGROU
		readchan chan []redis.XStream
		// errchan is used to communicate errors from XREADGROUP.
		errchan chan error
		// c is the sink event channel.
		c chan *AckedEvent
		// done is the sink done channel.
		done chan struct{}
		// wait is the sink cleanup wait group.
		wait sync.WaitGroup
		// stopping is true if Stop was called.
		stopping bool
		// eventMatcher is the event matcher if any.
		eventMatcher EventMatcherFunc
		// consumersMap is the replicated map used to track stream sink consumers.
		// The map key is the sink name and the value is a list of consumer names.
		// consumersMap is indexed by stream name.
		consumersMap map[string]*rmap.Map
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
	c := make(chan *AckedEvent, options.BufferSize)
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
	m, err := rmap.Join(ctx, consumersMapName(stream), stream.rdb, rmap.WithLogger(stream.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join replicated map for sink %s: %w", name, err)
	}
	consumer := uuid.New().String()
	if _, err := m.AppendValues(ctx, name, consumer); err != nil {
		return nil, fmt.Errorf("failed to append store consumer %s for sink %s: %w", consumer, name, err)
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
		Name:          name,
		C:             c,
		consumer:      consumer,
		startID:       options.LastEventID,
		noAck:         options.NoAck,
		streams:       []*Stream{stream},
		streamCursors: []string{stream.key, ">"},
		readchan:      make(chan []redis.XStream),
		errchan:       make(chan error),
		c:             c,
		done:          make(chan struct{}),
		eventMatcher:  eventMatcher,
		consumersMap:  map[string]*rmap.Map{stream.Name: m},
		logger:        stream.logger.WithPrefix("sink", name),
		rdb:           stream.rdb,
	}

	sink.wait.Add(1)
	go sink.read()
	return sink, nil
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

	m, err := rmap.Join(ctx, consumersMapName(stream), stream.rdb, rmap.WithLogger(stream.logger))
	if err != nil {
		return fmt.Errorf("failed to join consumer replicated map for stream %s: %w", stream.Name, err)
	}
	if _, err := m.AppendValues(ctx, s.Name, s.consumer); err != nil {
		return fmt.Errorf("failed to append consumer %s to replicated map for stream %s: %w", s.consumer, stream.Name, err)
	}
	if err := stream.rdb.XGroupCreateMkStream(ctx, stream.key, s.Name, startID).Err(); err != nil && !isBusyGroupErr(err) {
		return fmt.Errorf("failed to create Redis consumer group %s for stream %s: %w", s.Name, stream.Name, err)
	}
	if err := stream.rdb.XGroupCreateConsumer(ctx, stream.key, s.Name, s.consumer).Err(); err != nil {
		err = fmt.Errorf("failed to create Redis consumer for consumer group %s: %w", s.Name, err)
		s.logger.Error(err, "consumer", s.consumer)
		return err
	}
	s.streams = append(s.streams, stream)
	s.streamCursors = make([]string, len(s.streams)*2)
	for i, stream := range s.streams {
		s.streamCursors[i] = stream.key
		s.streamCursors[len(s.streams)+i] = ">"
	}
	s.consumersMap[stream.Name] = m
	s.logger.Info("added stream to sink", "stream", stream.Name)
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
	s.logger.Info("removed stream from sink", "stream", stream.Name)
	return nil
}

// Stop stops event polling and closes the sink channel, it is idempotent.
func (s *Sink) Stop() {
	s.lock.Lock()
	if s.stopping {
		return
	}
	s.stopping = true
	close(s.done)
	s.lock.Unlock()
	s.wait.Wait()
	s.lock.Lock()
	defer s.lock.Unlock()
	s.stopped = true
	s.logger.Info("stopped")
}

// Stopped returns true if the sink is stopped.
func (s *Sink) Stopped() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.stopped
}

// read reads events from the streams and sends them to the sink channel.
func (s *Sink) read() {
	defer s.cleanup()
	for {
		events, err := s.readOnce()
		if s.isStopping() {
			return
		}
		if err != nil {
			if err == redis.Nil {
				continue // No event at this time, just loop
			}
			if strings.Contains(err.Error(), "NOGROUP") {
				continue // Consumer group was removed with RemoveStream, just loop (s.streamCursors will be updated)
			}
			d := time.Duration(rand.Intn(maxJitterMs)) * time.Millisecond
			s.logger.Error(fmt.Errorf("sink consumer failed to read events: %w, retrying in %v", err, d))
			time.Sleep(d)
			continue // Full jitter eternal retry
		}

		var evs []*AckedEvent
		for _, streamEvents := range events {
			if len(streamEvents.Messages) == 0 {
				continue
			}
			for _, event := range streamEvents.Messages {
				var topic string
				if t, ok := event.Values["topic"]; ok {
					topic = t.(string)
				}
				ev := newEvent(
					s.rdb,
					streamEvents.Stream,
					s.Name,
					event.ID,
					event.Values[nameKey].(string),
					topic,
					[]byte(event.Values[payloadKey].(string)),
				)
				if s.eventMatcher != nil && !s.eventMatcher(ev) {
					s.logger.Debug("event did not match event matcher", "stream", streamEvents.Stream, "event", ev.ID)
					continue
				}
				s.logger.Debug("event received", "stream", streamEvents.Stream, "event", ev.ID)
				evs = append(evs, (*AckedEvent)(ev))
			}
		}

		s.lock.Lock()
		for _, ev := range evs {
			s.c <- ev
		}
		s.lock.Unlock()
	}
}

// readOnce reads events from the streams using the backing consumer group.  It
// blocks up to blockMs milliseconds or until the sink is stopped if no events
// are available.
func (s *Sink) readOnce() ([]redis.XStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.lock.Lock()
	readStreams := s.streamCursors
	s.lock.Unlock()
	go func() {
		events, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.Name,
			Consumer: s.consumer,
			Streams:  readStreams,
			Count:    maxMessages,
			Block:    pollDuration,
			NoAck:    s.noAck,
		}).Result()
		func() {
			s.lock.Lock()
			defer s.lock.Unlock()
			if s.stopping {
				return
			}
			if err != nil {
				s.errchan <- err
				return
			}
			s.readchan <- events
		}()
	}()

	select {
	case events := <-s.readchan:
		return events, nil
	case err := <-s.errchan:
		return nil, err
	case <-s.done:
		return nil, nil
	}
}

// cleanup removes the consumer from the consumer groups and removes the sink
// from the sinks map. This method is called automatically when the sink is
// stopped.
func (s *Sink) cleanup() {
	s.lock.Lock()
	defer s.lock.Unlock()
	close(s.readchan)
	close(s.errchan)
	ctx := context.Background()
	for _, stream := range s.streams {
		if err := s.rdb.XGroupDelConsumer(ctx, stream.key, s.Name, s.consumer).Err(); err != nil {
			s.logger.Error(fmt.Errorf("failed to delete Redis consumer: %w", err), "stream", stream.Name)
		}
		remains, err := s.consumersMap[stream.Name].RemoveValues(ctx, s.Name, s.consumer)
		if err != nil {
			s.logger.Error(fmt.Errorf("failed to remove consumer from consumer map: %w", err), "stream", stream.Name)
		}
		if len(remains) == 0 {
			if err := s.deleteConsumerGroup(ctx, stream); err != nil {
				s.logger.Error(err, "stream", stream.Name)
			}
		}
	}
	close(s.c)
	s.wait.Done()
}

func (s *Sink) deleteConsumerGroup(ctx context.Context, stream *Stream) error {
	if err := s.rdb.XGroupDestroy(ctx, stream.key, s.Name).Err(); err != nil {
		return fmt.Errorf("failed to destroy Redis consumer group %s for stream %s: %w", s.Name, stream.Name, err)
	}
	delete(s.consumersMap, stream.Name)
	return nil
}

// isStopping returns true if the sink is stopping.
func (s *Sink) isStopping() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.stopping
}

// isBusyGroupErr returns true if the error is a busy group error.
func isBusyGroupErr(err error) bool {
	return strings.Contains(err.Error(), "BUSYGROUP")
}

// consumersMapName is the name of the replicated map that backs a sink.
func consumersMapName(stream *Stream) string {
	return fmt.Sprintf("%s:sinks", stream.Name)
}
