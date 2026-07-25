package options

import (
	"time"

	"goa.design/pulse/pulse"
)

type (
	// Stream is a stream creation option.
	Stream func(*StreamOptions)

	// StreamOptions keeps its v1 fields first, in v1 order; new fields are
	// only ever appended so existing keyed construction and field access
	// remain source-compatible across feature releases.
	StreamOptions struct {
		MaxLen int
		Logger pulse.Logger
		// TTL configures a retention window for the Redis key backing the stream.
		// When zero, no TTL is applied.
		TTL time.Duration
		// TTLSliding controls whether the TTL is refreshed on every published event.
		// When false, the TTL is applied once (absolute TTL) and never extended.
		TTLSliding bool

		// MaxLenSet distinguishes an omitted MaxLen from an explicit request.
		MaxLenSet bool
		// Unbounded disables MAXLEN trimming entirely.
		Unbounded bool
		// TTLSet distinguishes an omitted TTL from an explicitly invalid zero.
		TTLSet bool
		// Deadline is the immutable absolute expiry claimed by this stream
		// generation when DeadlineSet is true.
		Deadline time.Time
		// DeadlineSet distinguishes an omitted deadline from the zero time.
		DeadlineSet bool
	}
)

// WithStreamMaxLen sets the positive maximum number of events stored by the
// stream. NewStream rejects zero and negative values.
func WithStreamMaxLen(len int) Stream {
	return func(o *StreamOptions) {
		o.MaxLen = len
		o.MaxLenSet = true
	}
}

// WithUnboundedStream disables MAXLEN trimming. Callers must remove settled
// events explicitly. It cannot be combined with WithStreamMaxLen.
func WithUnboundedStream() Stream {
	return func(o *StreamOptions) {
		o.Unbounded = true
	}
}

// WithStreamLogger sets the logger used by the stream.
func WithStreamLogger(logger pulse.Logger) Stream {
	return func(o *StreamOptions) {
		o.Logger = logger
	}
}

// WithStreamTTL sets an absolute TTL on the Redis key backing the stream. The
// TTL is set once and never extended; NewStream rejects values below Redis's
// one-millisecond precision.
func WithStreamTTL(ttl time.Duration) Stream {
	return func(o *StreamOptions) {
		o.TTL = ttl
		o.TTLSet = true
		o.TTLSliding = false
	}
}

// WithStreamSlidingTTL sets a sliding TTL on the Redis key backing the stream.
// The TTL is refreshed on every published event; NewStream rejects values
// below Redis's one-millisecond precision.
func WithStreamSlidingTTL(ttl time.Duration) Stream {
	return func(o *StreamOptions) {
		o.TTL = ttl
		o.TTLSet = true
		o.TTLSliding = true
	}
}

// WithStreamDeadline makes the stream generation expire at deadline. The
// deadline is persisted when the handle first opens; handles for the same
// generation must not request a different deadline. TTL options cannot be
// combined with this option.
func WithStreamDeadline(deadline time.Time) Stream {
	return func(o *StreamOptions) {
		o.Deadline = deadline
		o.DeadlineSet = true
	}
}

// ParseStreamOptions parses the given options and returns the corresponding
// StreamOptions.
func ParseStreamOptions(opts ...Stream) StreamOptions {
	o := defaultStreamOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// defaultStreamOptions returns the default options.
func defaultStreamOptions() StreamOptions {
	return StreamOptions{
		MaxLen: 1000,
		Logger: pulse.NoopLogger(),
	}
}
