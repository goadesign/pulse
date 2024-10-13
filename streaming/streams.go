package streaming

import (
	"context"
	"fmt"
	"regexp"

	redis "github.com/redis/go-redis/v9"
	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
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
		// logger is the logger used by the stream.
		logger pulse.Logger
		// rootLogger is the prefix-free logger used to create sink loggers.
		rootLogger pulse.Logger
		// key is the redis key used for the stream.
		key string
		// rdb is the redis connection.
		rdb *redis.Client
	}
)

const (
	// streamKeyPrefix is the prefix used for stream keys.
	streamKeyPrefix = "pulse:stream:"
	// nameKey is the key used to store the event name.
	nameKey = "n"
	// payloadKey is the key used to store the event payload.
	payloadKey = "p"
	// topicKey is the key used to store the event topic.
	topicKey = "t"
)

// NewStream returns the stream with the given name. All stream instances
// with the same name share the same events.
func NewStream(name string, rdb *redis.Client, opts ...options.Stream) (*Stream, error) {
	if !isValidRedisKeyName(name) {
		return nil, fmt.Errorf("pulse stream: not a valid name %q", name)
	}
	o := options.ParseStreamOptions(opts...)
	var logger pulse.Logger
	if o.Logger != nil {
		logger = o.Logger.WithPrefix("stream", name)
	} else {
		logger = pulse.NoopLogger()
	}
	s := &Stream{
		Name:       name,
		MaxLen:     o.MaxLen,
		logger:     logger,
		rootLogger: logger,
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
func (s *Stream) NewReader(ctx context.Context, opts ...options.Reader) (*Reader, error) {
	reader, err := newReader(s, opts...)
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
func (s *Stream) NewSink(ctx context.Context, name string, opts ...options.Sink) (*Sink, error) {
	sink, err := newSink(ctx, name, s, opts...)
	if err != nil {
		err := fmt.Errorf("failed to create sink: %w", err)
		s.logger.Error(err, "sink", name)
		return nil, err
	}
	s.logger.Info("create", "sink", name, "start", sink.startID)
	return sink, nil
}

// Add appends an event to the stream and returns its ID. If the option
// WithOnlyIfStreamExists is used and the stream does not exist then no event is
// added and the empty string is returned. The stream is created if the option
// is omitted or when NewSink is called.
func (s *Stream) Add(ctx context.Context, name string, payload []byte, opts ...options.AddEvent) (string, error) {
	o := options.ParseAddEventOptions(opts...)
	for _, option := range opts {
		option(&o)
	}
	values := []any{nameKey, name, payloadKey, payload}
	if o.Topic != "" {
		values = append(values, topicKey, o.Topic)
	}
	res, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream:     s.key,
		Values:     values,
		MaxLen:     int64(s.MaxLen),
		Approx:     true,
		NoMkStream: o.OnlyIfStreamExists,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			// Stream does not exist and OnlyIfStreamExists option was used.
			return "", nil
		}
		err = fmt.Errorf("failed to add event: %w", err)
		s.logger.Error(err, "event", name)
		return "", err
	}
	s.logger.Info("add", "event", name, "id", res)
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
func (s *Stream) Destroy(ctx context.Context) error {
	if err := s.rdb.Del(ctx, s.key).Err(); err != nil {
		err := fmt.Errorf("failed to destroy stream: %w", err)
		s.logger.Error(err)
		return err
	}
	s.logger.Info("stream deleted")
	return nil
}

// redisKeyRegex is a regular expression that matches valid Redis keys.
var redisKeyRegex = regexp.MustCompile(`^[^ \0\*\?\[\]]{1,512}$`)

func isValidRedisKeyName(key string) bool {
	return redisKeyRegex.MatchString(key)
}
