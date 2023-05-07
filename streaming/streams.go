package streaming

import (
	"context"
	"fmt"
	"regexp"

	redis "github.com/redis/go-redis/v9"
	"goa.design/ponos/ponos"
)

type (
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
		// logger is the logger used by the stream.
		logger ponos.Logger
		// rootLogger is the prefix-free logger used to create sink loggers.
		rootLogger ponos.Logger
		// key is the redis key used for the stream.
		key string
		// rdb is the redis connection.
		rdb *redis.Client
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
)

const (
	// streamKeyPrefix is the prefix used for stream keys.
	streamKeyPrefix = "ponos:stream:"
	// nameKey is the key used to store the event name.
	nameKey = "n"
	// payloadKey is the key used to store the event payload.
	payloadKey = "p"
	// topicKey is the key used to store the event topic.
	topicKey = "t"
)

// NewStream returns the stream with the given name. All stream instances
// with the same name share the same events.
func NewStream(ctx context.Context, name string, rdb *redis.Client, opts ...StreamOption) (*Stream, error) {
	if !isValidRedisKeyName(name) {
		return nil, fmt.Errorf("ponos stream: not a valid name %q", name)
	}
	options := defaultStreamOptions()
	for _, option := range opts {
		option(&options)
	}
	var logger, rootLogger ponos.Logger
	if options.Logger != nil {
		logger = options.Logger.WithPrefix("stream", name)
		rootLogger = options.Logger
	} else {
		logger = ponos.NoopLogger()
		rootLogger = logger
	}
	s := &Stream{
		Name:       name,
		MaxLen:     options.MaxLen,
		logger:     logger,
		rootLogger: rootLogger,
		key:        streamKeyPrefix + name,
		rdb:        rdb,
	}
	return s, nil
}

// NewReader creates a new stream reader. All reader instances get all the
// events in the stream. Events are read starting:
//   - from the last event by default
//   - from the oldest event stored in the stream if the
//     WithReaderStartAtOldest option is used
//   - after the event with the ID provided via WithReaderLastEventID if the
//     event is still in the stream, oldest event otherwise
//   - from the event added on or after the timestamp provided via
//     WithReaderStartAt if still in the stream, oldest event otherwise
func (s *Stream) NewReader(ctx context.Context, opts ...ReaderOption) (*Reader, error) {
	reader, err := newReader(ctx, s, opts...)
	if err != nil {
		err := fmt.Errorf("failed to create reader: %w", err)
		s.logger.Error(err)
		return nil, err
	}
	s.logger.Info("create reader", "start", reader.startID)
	return reader, nil
}

// NewSink creates a new stream sink with the given name. All sink instances
// with the same name share the same stream cursor. Events read through a sink
// are not removed from the stream until they are acked by the client unless the
// WithNoAck option is used. Events are read starting:
//   - from the last event by default
//   - from the oldest event stored in the stream if the WithSinkStartAtOldest
//     option is used
//   - after the event with the ID provided via WithSinkLastEventID if the
//     event is still in the stream, oldest event otherwise
//   - from the event added on or after the timestamp provided via
//     WithSinkStartAt if still in the stream, oldest event otherwise
func (s *Stream) NewSink(ctx context.Context, name string, opts ...SinkOption) (*Sink, error) {
	sink, err := newSink(ctx, name, s, opts...)
	if err != nil {
		err := fmt.Errorf("failed to create sink: %w", err)
		s.logger.Error(err, "sink", name)
		return nil, err
	}
	s.logger.Info("create", "sink", name, "start", sink.startID)
	return sink, nil
}

// NewTopic creates a new topic with the given name.
func (s *Stream) NewTopic(name string) *Topic {
	return &Topic{
		Name:   name,
		Stream: s,
	}
}

// Add appends an event to the stream and returns its ID.
func (s *Stream) Add(ctx context.Context, name string, payload []byte) (string, error) {
	res, err := s.add(ctx, name, payload, nil)
	if err != nil {
		err := fmt.Errorf("failed to add event: %w", err)
		s.logger.Error(err, "event", name)
		return "", err
	}
	s.logger.Debug("add", "event", name, "id", res)
	return res, nil
}

// Remove removes the events with the given IDs from the stream.
// Note: clients should not need to call this method in normal operation,
// instead they should use the Ack method to acknowledge events.
func (s *Stream) Remove(ctx context.Context, ids ...string) error {
	err := s.rdb.XDel(ctx, s.key, ids...).Err()
	if err != nil {
		err = fmt.Errorf("failed to remove events: %w", err)
		s.logger.Error(err, "events", ids)
		return err
	}
	s.logger.Debug("remove", "events", ids)
	return nil
}

// Destroy deletes the entire stream and all its messages.
// TBD: this should also destroy the sinks
func (s *Stream) Destroy(ctx context.Context) error {
	if err := s.rdb.Del(ctx, s.key).Err(); err != nil {
		err := fmt.Errorf("failed to destroy stream: %w", err)
		s.logger.Error(err)
		return err
	}
	s.logger.Info("stream deleted")
	return nil
}

// Add appends an event to the topic and returns its id.
func (t *Topic) Add(ctx context.Context, name string, payload []byte) (string, error) {
	res, err := t.Stream.add(ctx, name, payload, &t.Name)
	if err != nil {
		err = fmt.Errorf("failed to add event %w", err)
		t.Stream.logger.Error(err, "event", name, "topic", t.Name)
		return "", err
	}
	t.Stream.logger.Debug("add", "event", name, "topic", t.Name, "id", res)
	return res, nil
}

// add appends an event to the stream.
func (s *Stream) add(ctx context.Context, name string, payload []byte, topic *string) (string, error) {
	values := []any{nameKey, name, payloadKey, payload}
	if topic != nil {
		values = append(values, topicKey, *topic)
	}
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.key,
		Values: values,
		MaxLen: int64(s.MaxLen),
		Approx: true,
	}).Result()
}

// redisKeyRegex is a regular expression that matches valid Redis keys.
var redisKeyRegex = regexp.MustCompile(`^[^ \0\*\?\[\]]{1,512}$`)

func isValidRedisKeyName(key string) bool {
	return redisKeyRegex.MatchString(key)
}
