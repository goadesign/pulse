package streams

import (
	"context"
	"sync"

	redis "github.com/go-redis/redis/v8"
)

type (
	// Conn represents a redis connection.
	Conn struct {
		// rdb is the redis client.
		rdb *redis.Client
		// lock is the connection mutex.
		lock sync.Mutex
		// streams is the stream map.
		streams map[string]*Stream
	}

	// Stream encapsulates a stream of events.  Events published to a stream
	// can optionally be associated with a topic.  Stream consumers can
	// subscribe to a stream and optionally provide a topic matching
	// criteria. Consumers can be created within a group. Each consumer
	// group receives a unique copy of the stream events.
	Stream struct {
		// Name of the stream.
		Name string
		// MaxLen is the maximum number of events in the stream.
		MaxLen int
		// C is the channel the stream receives events on.
		C <-chan *Event
		// conn is the redis connection.
		conn *Conn
	}

	// Topic represents a stream topic. A topic can be used to publish
	// events to a stream.
	Topic struct {
		// Name is the topic name.
		Name string
		// Stream is the stream the topic belongs to.
		Stream *Stream
		// Pattern is the topic pattern if any, empty string if none.
		Pattern string
		// Matcher is the topic event matcher if any, nil if none.
		Matcher EventMatcherFunc
	}

	// Event is a stream event.
	Event struct {
		// ID is the unique event ID.
		ID string
		// StreamName is the name of the stream the event belongs to.
		StreamName string
		// EventName is the producer-defined event name.
		EventName string
		// Topic is the producer-defined event topic if any, empty string if none.
		Topic string
		// Payload is the event payload.
		Payload []byte
	}
)

// New creates a new Redis connection.
func New(ctx context.Context, rdb *redis.Client) (*Conn, error) {
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &Conn{
		rdb:     rdb,
		streams: make(map[string]*Stream),
	}, nil
}

// Stream returns or creates the stream with the given name.
func (c *Conn) Stream(name string, opts ...StreamOption) *Stream {
	c.lock.Lock()
	defer c.lock.Unlock()
	if s, ok := c.streams[name]; ok {
		return s
	}
	options := defaultStreamOptions()
	for _, option := range opts {
		option(&options)
	}
	s := &Stream{
		Name:   name,
		MaxLen: options.MaxLen,
		conn:   c,
	}
	c.streams[name] = s
	return s
}

// NewSink creates a new stream sink with the given name. All sink instances
// with the same name share the same stream cursor.
func (s *Stream) NewSink(ctx context.Context, name string, opts ...SinkOption) *Sink {
	return newSink(ctx, name, s, opts...)
}

// NewTopic creates a new topic with the given name.
func (s *Stream) NewTopic(name string) *Topic {
	return &Topic{
		Name:   name,
		Stream: s,
	}
}

// Add appends an event to the stream.
func (s *Stream) Add(ctx context.Context, name string, payload []byte) error {
	values := map[string]interface{}{
		"event":   name,
		"payload": payload,
	}
	return s.add(ctx, values)
}

// Add appends an event to the topic.
func (t *Topic) Add(ctx context.Context, name string, payload []byte) error {
	values := map[string]interface{}{
		"event":   name,
		"payload": payload,
		"topic":   t.Name,
	}
	return t.Stream.add(ctx, values)
}

// add appends an event to the stream.
func (s *Stream) add(ctx context.Context, values map[string]interface{}) error {
	return s.conn.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.Name,
		Values: values,
		MaxLen: int64(s.MaxLen),
	}).Err()
}
