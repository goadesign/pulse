package streams

import (
	"context"
	"math/rand"
	"regexp"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v8"
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
		// conn is the redis connection.
		conn *Conn
		// lock is the sink mutex.
		lock sync.Mutex
		// streams are the streams the sink consumes events from.
		streams []*Stream
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
func newSink(ctx context.Context, name string, stream *Stream, opts ...SinkOption) *Sink {
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
	sink := &Sink{
		Name:          name,
		C:             c,
		conn:          stream.conn,
		streams:       []*Stream{stream},
		cursors:       map[string]string{stream.Name: "0"},
		c:             c,
		errorReporter: options.ErrorReporter,
		eventMatcher:  eventMatcher,
	}
	go sink.read(ctx)
	return sink
}

// AddStream adds the stream to the sink, it is idempotent.
func (s *Sink) AddStream(stream *Stream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.cursors[stream.Name]; ok {
		return nil
	}
	s.streams = append(s.streams, stream)
	s.cursors[stream.Name] = "0"
	return nil
}

// RemoveStream removes the stream from the sink, it is idempotent.
func (s *Sink) RemoveStream(stream *Stream) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.cursors, stream.Name)
	for i, st := range s.streams {
		if st == stream {
			s.streams = append(s.streams[:i], s.streams[i+1:]...)
			return nil
		}
	}
	return nil
}

// Close closes the sink, it is idempotent.
func (s *Sink) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return nil
	}
	close(s.c)
	s.closed = true
	return nil
}

// read reads events from the streams and sends them to the sink channel.
func (s *Sink) read(ctx context.Context) {
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
		events, err := s.conn.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: readStreams,
			Count:   maxMessages,
			Block:   blockMs,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			s.errorReporter(ctx, err)
			// Full jitter backoff.
			time.Sleep(time.Duration(rand.Intn(maxJitterMs)) * time.Millisecond)
			continue
		}
		s.lock.Lock()
		defer s.lock.Unlock()
		if s.closed {
			return
		}
		for _, streamEvents := range events {
			for _, event := range streamEvents.Messages {
				var topic string
				if event.Values["topic"] != nil {
					topic = event.Values["topic"].(string)
				}
				event := &Event{
					ID:         event.ID,
					StreamName: streamEvents.Stream,
					EventName:  event.Values["event"].(string),
					Topic:      topic,
					Payload:    event.Values["payload"].([]byte),
				}
				if s.eventMatcher != nil && !s.eventMatcher(event) {
					continue
				}
				s.c <- event
			}
		}
	}
}
