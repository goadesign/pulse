package rmap

import "goa.design/pulse/pulse"

type (
	// MapOption is a Map creation option.
	MapOption func(*options)

	options struct {
		// Channel name
		// Logger
		Logger pulse.Logger
	}
)

// WithLogger sets the logger used by the map.
func WithLogger(logger pulse.Logger) MapOption {
	return func(o *options) {
		o.Logger = logger
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
