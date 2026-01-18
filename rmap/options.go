package rmap

import (
	"time"

	"goa.design/pulse/pulse"
)

type (
	// MapOption is a Map creation option.
	MapOption func(*options)

	options struct {
		// Logger
		Logger pulse.Logger

		// TTL configures a retention window for the Redis hash backing the map.
		// When zero, no TTL is applied.
		TTL time.Duration
		// TTLSliding controls whether the TTL is refreshed on every write.
		// When false, the TTL is applied once (absolute TTL) and never extended.
		TTLSliding bool
	}
)

// WithLogger sets the logger used by the map.
func WithLogger(logger pulse.Logger) MapOption {
	return func(o *options) {
		o.Logger = logger
	}
}

// WithTTL sets an absolute TTL on the Redis hash backing the map.
// The TTL is set once (when the hash is created) and never extended.
func WithTTL(ttl time.Duration) MapOption {
	return func(o *options) {
		o.TTL = ttl
		o.TTLSliding = false
	}
}

// WithSlidingTTL sets a sliding TTL on the Redis hash backing the map.
// The TTL is refreshed on every write operation.
func WithSlidingTTL(ttl time.Duration) MapOption {
	return func(o *options) {
		o.TTL = ttl
		o.TTLSliding = true
	}
}

// parseOptions parses the given options and returns the corresponding
// options.
func parseOptions(opts ...MapOption) *options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// defaultOptions returns the default options.
func defaultOptions() *options {
	return &options{
		Logger: pulse.NoopLogger(),
	}
}
