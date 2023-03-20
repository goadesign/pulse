package streams

import (
	"context"

	redis "github.com/redis/go-redis/v9"
)

type (
	// EventStream encapsulates a stream of events.  Events published to a stream
	// can optionally be associated with a topic.  EventStream consumers can
	// subscribe to a stream and optionally provide a topic matching
	// criteria. Consumers can be created within a group. Each consumer
	// group receives a unique copy of the stream events.
	EventStream struct {
		// Name of the stream.
		Name string
		// MaxLen is the maximum number of events in the stream.
		MaxLen int
		// C is the channel the stream receives events on.
		C <-chan *Event
		// rdb is the redis connection.
		rdb *redis.Client
	}

	// Topic represents a stream topic. A topic can be used to publish
	// events to a stream.
	Topic struct {
		// Name is the topic name.
		Name string
		// Stream is the stream the topic belongs to.
		Stream *EventStream
		// Pattern is the topic pattern if any, empty string if none.
		Pattern string
		// Matcher is the topic event matcher if any, nil if none.
		Matcher EventMatcherFunc
	}
)

// Stream returns or creates the stream with the given name.
func Stream(ctx context.Context, name string, rdb *redis.Client, opts ...StreamOption) *EventStream {
	options := defaultStreamOptions()
	for _, option := range opts {
		option(&options)
	}
	s := &EventStream{
		Name:   name,
		MaxLen: options.MaxLen,
		rdb:    rdb,
	}
	return s
}

// Destroy deletes the entire stream and all its messages.
func (s *EventStream) Destroy(ctx context.Context) error {
	return s.rdb.Del(ctx, s.Name).Err()
}

// NewSink creates a new stream sink with the given name. All sink instances
// with the same name share the same stream cursor.
func (s *EventStream) NewSink(ctx context.Context, name string, opts ...SinkOption) (*Sink, error) {
	return newSink(ctx, name, s, opts...)
}

// NewTopic creates a new topic with the given name.
func (s *EventStream) NewTopic(name string) *Topic {
	return &Topic{
		Name:   name,
		Stream: s,
	}
}

// Add appends an event to the stream.
func (s *EventStream) Add(ctx context.Context, name string, payload []byte) error {
	values := map[string]any{
		"event":   name,
		"payload": payload,
	}
	return s.add(ctx, values)
}

// Remove removes the events with the given IDs from the stream.
// Note: clients should not need to call this method in normal operation,
// instead they should use the Ack method to acknowledge events.
func (s *EventStream) Remove(ctx context.Context, ids ...string) error {
	return s.rdb.XDel(ctx, s.Name, ids...).Err()
}

// Add appends an event to the topic.
func (t *Topic) Add(ctx context.Context, name string, payload []byte) error {
	values := map[string]any{
		"event":   name,
		"payload": payload,
		"topic":   t.Name,
	}
	return t.Stream.add(ctx, values)
}

// add appends an event to the stream.
func (s *EventStream) add(ctx context.Context, values map[string]any) error {
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.Name,
		Values: values,
		MaxLen: int64(s.MaxLen),
		Approx: true,
	}).Err()
}
