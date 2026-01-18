package options

import (
	"time"

	"goa.design/pulse/pulse"
)

type (
	// Stream is a stream creation option.
	Stream func(*StreamOptions)

	StreamOptions struct {
		MaxLen int
		Logger pulse.Logger

		// TTL configures a retention window for the Redis key backing the stream.
		// When zero, no TTL is applied.
		TTL time.Duration
		// TTLSliding controls whether the TTL is refreshed on every published event.
		// When false, the TTL is applied once (absolute TTL) and never extended.
		TTLSliding bool
	}
)

// WithStreamMaxLen sets the maximum number of events stored by the stream.
func WithStreamMaxLen(len int) Stream {
	return func(o *StreamOptions) {
		o.MaxLen = len
	}
}

// WithStreamLogger sets the logger used by the stream.
func WithStreamLogger(logger pulse.Logger) Stream {
	return func(o *StreamOptions) {
		o.Logger = logger
	}
}

// WithStreamTTL sets an absolute TTL on the Redis key backing the stream.
// The TTL is set once (when the key is created) and never extended.
func WithStreamTTL(ttl time.Duration) Stream {
	return func(o *StreamOptions) {
		o.TTL = ttl
		o.TTLSliding = false
	}
}

// WithStreamSlidingTTL sets a sliding TTL on the Redis key backing the stream.
// The TTL is refreshed on every published event.
func WithStreamSlidingTTL(ttl time.Duration) Stream {
	return func(o *StreamOptions) {
		o.TTL = ttl
		o.TTLSliding = true
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
