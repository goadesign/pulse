package streams

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

const (
	// maxJitterMs is the maximum retry backoff jitter in milliseconds.
	maxJitterMs = 5000

	// blockMs is the maximum stream block time in milliseconds.
	blockMs = 3000

	// maxMessages is the maximum number of messages to read from the stream.
	maxMessages = 100
)

type (
	// Sink represents a stream sink.
	Sink struct {
		// Name is the sink name.
		Name string
		// C is the sink event channel.
		C <-chan *Event
		// rdb is the redis connection.
		rdb *redis.Client
		// consumer is the sink consumer name.
		consumer string
		// noAck is true if there is no need to acknowledge events.
		noAck bool
		// lock is the sink mutex.
		lock sync.Mutex
		// streams are the streams the sink consumes events from.
		streams []*EventStream
		// cursors are the stream cursors.
		cursors map[string]string
		// c is the sink event channel.
		c chan *Event
		// closed is the closed flag.
		closed bool
		// errorReporter is the sink error handler.
		errorReporter func(context.Context, error)
		// eventMatcher is the event matcher if any.
		eventMatcher EventMatcherFunc
	}
)

// newSink creates a new sink.
func newSink(ctx context.Context, name string, stream *EventStream, opts ...SinkOption) (*Sink, error) {
	options := defaultSinkOptions()
	for _, option := range opts {
		option(&options)
	}
	c := make(chan *Event, options.BufferSize)
	eventMatcher := options.EventMatcher
	if eventMatcher == nil {
		var topicPatternRegexp *regexp.Regexp
		if options.TopicPattern != "" {
			topicPatternRegexp = regexp.MustCompile(options.TopicPattern)
		}
		if options.Topic != "" {
			eventMatcher = func(e *Event) bool { return e.Topic == options.Topic }
		} else if options.TopicPattern != "" {
			eventMatcher = func(e *Event) bool { return topicPatternRegexp.MatchString(e.Topic) }
		}
	}
	if err := stream.rdb.XGroupCreateMkStream(ctx, stream.Name, name, options.LastEventID).Err(); err != nil {
		return nil, fmt.Errorf("failed to create Redis consumer group %s for stream %s: %w", name, stream.Name, err)
	}
	consumer := uuid.New().String()
	if err := stream.rdb.XGroupCreateConsumer(ctx, stream.Name, name, consumer).Err(); err != nil {
		return nil, fmt.Errorf("failed to create Redis consumer %s for consumer group %s: %w", consumer, name, err)
	}
	sink := &Sink{
		Name:          name,
		C:             c,
		rdb:           stream.rdb,
		consumer:      uuid.New().String(),
		noAck:         options.NoAck,
		streams:       []*EventStream{stream},
		cursors:       map[string]string{stream.Name: "0"},
		c:             c,
		errorReporter: options.ErrorReporter,
		eventMatcher:  eventMatcher,
	}
	go sink.read(ctx)
	return sink, nil
}

// AddStream adds the stream to the sink, it is idempotent.
func (s *Sink) AddStream(ctx context.Context, stream *EventStream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.cursors[stream.Name]; ok {
		return nil
	}
	if err := stream.rdb.XGroupCreateConsumer(ctx, stream.Name, s.Name, s.consumer).Err(); err != nil {
		return fmt.Errorf("failed to create Redis consumer %s for consumer group %s: %w", s.consumer, s.Name, err)
	}
	s.streams = append(s.streams, stream)
	s.cursors[stream.Name] = "0"
	return nil
}

// RemoveStream removes the stream from the sink, it is idempotent.
func (s *Sink) RemoveStream(ctx context.Context, stream *EventStream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.cursors, stream.Name)
	for i, st := range s.streams {
		if st == stream {
			s.streams = append(s.streams[:i], s.streams[i+1:]...)
			return nil
		}
	}
	if err := s.rdb.XGroupDelConsumer(ctx, stream.Name, s.Name, s.consumer).Err(); err != nil {
		return fmt.Errorf("failed to delete Redis consumer %s for consumer group %s: %w", s.consumer, s.Name, err)
	}
	return nil
}

// Stop stops event polling and closes the sink channel, it is idempotent.
func (s *Sink) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return nil
	}
	close(s.c)
	s.closed = true
	return nil
}

// Destroy deletes the sink backing consumer group and closes the sink channel.
// This causes all pending events to be lost.
func (s *Sink) Destroy(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return nil
	}
	for _, stream := range s.streams {
		if err := s.rdb.XGroupDestroy(ctx, stream.Name, s.Name).Err(); err != nil {
			return fmt.Errorf("failed to destroy Redis consumer group %s for stream %s: %w", s.Name, stream.Name, err)
		}
	}
	close(s.c)
	s.closed = true
	return nil
}

// read reads events from the streams and sends them to the sink channel.
func (s *Sink) read(ctx context.Context) {
	defer func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		for _, stream := range s.streams {
			if err := s.rdb.XGroupDelConsumer(ctx, stream.Name, s.Name, s.consumer).Err(); err != nil {
				s.errorReporter(ctx, fmt.Errorf("failed to delete Redis consumer %s for consumer group %s: %w", s.consumer, s.Name, err))
			}
		}
	}()
	for {
		var readStreams []string
		{
			s.lock.Lock()
			streams := make([]string, 0, len(s.streams))
			for _, stream := range s.streams {
				streams = append(streams, stream.Name)
			}
			readStreams := streams
			for _, stream := range streams {
				readStreams = append(readStreams, s.cursors[stream])
			}
			s.lock.Unlock()
		}
		events, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.Name,
			Consumer: s.consumer,
			Streams:  readStreams,
			Count:    maxMessages,
			Block:    blockMs,
			NoAck:    s.noAck,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			s.errorReporter(ctx, err)
			// Full jitter backoff.
			// TBD: Should we exit after a certain number of retries?
			time.Sleep(time.Duration(rand.Intn(maxJitterMs)) * time.Millisecond)
			continue
		}
		s.lock.Lock()
		{
			if s.closed {
				return
			}
			for _, streamEvents := range events {
				for _, event := range streamEvents.Messages {
					var topic string
					if t, ok := event.Values["topic"]; ok {
						topic = t.(string)
					}
					event := newEvent(
						s.rdb,
						streamEvents.Stream,
						s.Name,
						event.ID,
						event.Values["event"].(string),
						topic,
						event.Values["payload"].([]byte),
					)
					if s.eventMatcher != nil && !s.eventMatcher(event) {
						continue
					}
					s.c <- event
				}
			}
		}
		s.lock.Unlock()
	}
}
