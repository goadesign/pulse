package streaming

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

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
		// MaxLen is the maximum retained event count requested at
		// construction. It is immutable after NewStream and remains exported
		// for v1 source compatibility; the generation's canonical bound is
		// tracked privately so concurrent readers of this field never race
		// with binding.
		MaxLen int
		// maxLen is the canonical maximum retained event count adopted from
		// the bound generation. Zero means the stream is explicitly
		// unbounded.
		maxLen int
		// ttl configures an expiry for the Redis key backing the stream.
		ttl time.Duration
		// ttlSliding controls whether ttl is refreshed on every Add call.
		ttlSliding bool
		// deadline is the Redis-canonical absolute expiry for this generation.
		deadline time.Time
		// deadlineConfigured records whether construction explicitly requested
		// deadline, so lifecycle binding can reject a conflicting value.
		deadlineConfigured bool
		// retention is the canonical immutable generation configuration.
		retention string
		// retentionExplicit distinguishes writer configuration from an
		// unconfigured handle that adopts the active generation.
		retentionExplicit bool
		// logger is the logger used by the stream.
		logger pulse.Logger
		// rootLogger is the prefix-free logger used to create sink loggers.
		rootLogger pulse.Logger
		// key is the immutable lifecycle-selected Redis event-stream key.
		key string
		// lifecycleKey is the Redis-owned logical stream identity.
		lifecycleKey string
		// generation is the immutable incarnation established by the first
		// caller-context operation.
		generation string
		// generationLock serializes lazy generation establishment.
		generationLock sync.Mutex
		// rdb is the redis connection.
		rdb *redis.Client
	}
)

var (
	// ErrStreamDestroyed is returned when an operation uses a Stream whose
	// immutable generation is no longer active.
	ErrStreamDestroyed = errors.New("pulse streaming: stream generation is destroyed")
	// ErrIdempotencyConflict is returned when an AddOnce key already identifies
	// different event content in the same stream generation.
	ErrIdempotencyConflict = errors.New("pulse streaming: idempotency key conflicts with existing event")
	// ErrDeadlineElapsed is returned when a deadline-owned stream operation is
	// attempted at or after its Redis-authoritative absolute deadline.
	ErrDeadlineElapsed = errors.New("pulse streaming: stream deadline elapsed")
	// ErrStreamNotFound is returned when Snapshot observes no initialized
	// lifecycle. Snapshot never creates one.
	ErrStreamNotFound = errors.New("pulse streaming: stream is not initialized")
	// ErrStreamConfigMismatch is returned when an explicitly configured handle
	// differs from the active generation's immutable retention contract.
	ErrStreamConfigMismatch = errors.New("pulse streaming: stream retention configuration mismatch")
	// ErrSnapshotUnbounded is returned before XRANGE when Snapshot is called on
	// a generation whose immutable retention contract has no MaxLen bound.
	ErrSnapshotUnbounded = errors.New("pulse streaming: snapshot requires bounded stream retention")
	// ErrSnapshotBoundExceeded reports physical data that exceeds the active
	// generation's immutable MaxLen contract.
	ErrSnapshotBoundExceeded = errors.New("pulse streaming: snapshot retention bound violated")
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

// NewStream validates options and returns a local stream handle without
// performing Redis I/O. The first caller-context operation establishes or
// loads the active generation. WithStreamDeadline claims an immutable absolute
// generation deadline and cannot be combined with TTL options.
func NewStream(name string, rdb *redis.Client, opts ...options.Stream) (*Stream, error) {
	if !isValidRedisKeyName(name) {
		return nil, fmt.Errorf("pulse stream: not a valid name %q", name)
	}
	o := options.ParseStreamOptions(opts...)
	if o.Unbounded && o.MaxLenSet {
		return nil, fmt.Errorf("pulse stream: maximum length and unbounded options are mutually exclusive")
	}
	if o.Unbounded {
		o.MaxLen = 0
	} else if o.MaxLen <= 0 {
		return nil, fmt.Errorf("pulse stream: maximum length must be greater than zero")
	}
	if o.TTLSet && o.TTL < time.Millisecond {
		return nil, fmt.Errorf("pulse stream: ttl must be at least 1ms")
	}
	if o.DeadlineSet && o.Deadline.IsZero() {
		return nil, fmt.Errorf("pulse stream: deadline must not be zero")
	}
	if o.DeadlineSet && o.TTL != 0 {
		return nil, fmt.Errorf("pulse stream: deadline and ttl options are mutually exclusive")
	}
	var (
		logger     pulse.Logger
		rootLogger pulse.Logger
	)
	if o.Logger != nil {
		rootLogger = o.Logger
		logger = o.Logger.WithPrefix("stream", name)
	} else {
		rootLogger = pulse.NoopLogger()
		logger = rootLogger
	}
	s := &Stream{
		Name:               name,
		MaxLen:             o.MaxLen,
		maxLen:             o.MaxLen,
		ttl:                o.TTL,
		ttlSliding:         o.TTLSliding,
		deadline:           o.Deadline,
		deadlineConfigured: o.DeadlineSet,
		retentionExplicit:  o.MaxLenSet || o.Unbounded || o.TTLSet || o.DeadlineSet,
		logger:             logger,
		rootLogger:         rootLogger,
		key:                streamKey(name),
		lifecycleKey:       streamLifecycleKey(name),
		rdb:                rdb,
	}
	s.retention = s.retentionConfig()
	return s, nil
}

// Generation returns the immutable Redis-owned incarnation established for s,
// or the empty string before the first caller-context operation.
func (s *Stream) Generation() string {
	s.generationLock.Lock()
	defer s.generationLock.Unlock()
	return s.generation
}

// Open establishes or adopts the active Redis incarnation and verifies that a
// previously bound handle still names the active generation. Higher-level
// runtimes use Open as their lifecycle fence before mutating generation-owned
// resources outside the event stream.
func (s *Stream) Open(ctx context.Context) error {
	return s.verifyGeneration(ctx)
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
//
// NewReader does not initialize an absent lifecycle; it returns
// ErrStreamNotFound until a writer or Open establishes the stream.
func (s *Stream) NewReader(ctx context.Context, opts ...options.Reader) (*Reader, error) {
	reader, err := newReader(ctx, s, opts...)
	if err != nil {
		s.logger.Error(fmt.Errorf("failed to create reader: %w", err))
		return nil, err
	}
	if err := s.verifyExistingGeneration(ctx); err != nil {
		reader.cancel()
		return nil, err
	}
	reader.streamKeys[0] = s.key
	s.logger.Info("create reader", "start", reader.startID)
	return reader, nil
}

// NewSink creates a new stream sink with the given name. All sink instances
// with the same name share the same stream cursor. Events read through a sink
// are not removed from the stream until they are acked by the client unless
// WithSinkNoAck is used. Events are read starting:
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
		s.logger.Error(fmt.Errorf("failed to create sink: %w", err), "sink", name)
		return nil, err
	}
	return sink, nil
}

// Add appends an event to the stream and returns its ID. If the option
// WithOnlyIfStreamExists is used and the stream does not exist then no event is
// added and the empty string is returned. The stream is created if the option
// is omitted or when NewSink is called. On a deadline-owned generation, Add
// rejects elapsed deadlines and reapplies the same absolute expiry without
// extending it.
func (s *Stream) Add(ctx context.Context, name string, payload []byte, opts ...options.AddEvent) (string, error) {
	o := options.ParseAddEventOptions(opts...)
	res, err := s.addEvent(ctx, name, payload, o.OnlyIfStreamExists, o.Topic)
	if err != nil {
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
	if err := s.removeEvents(ctx, ids); err != nil {
		err = fmt.Errorf("failed to remove events: %w", err)
		s.logger.Error(err, "events", ids)
		return err
	}
	s.logger.Debug("remove", "events", ids)
	return nil
}

// Destroy invalidates this exact generation and atomically deletes its event
// data, consumer groups, recovery cursors, exact-publication records, and
// membership state. It is idempotent after this handle has bound a generation;
// an unbound absent name returns ErrStreamNotFound without creating lifecycle
// state. Existing Stream, Reader, Sink, Event, and Acker values cannot affect a
// later generation.
func (s *Stream) Destroy(ctx context.Context) error {
	if err := destroyStream(ctx, s); err != nil {
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
