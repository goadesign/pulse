package options

import (
	"goa.design/ponos/ponos"
)

type (
	// Stream is a stream creation option.
	Stream func(*StreamOptions)

	StreamOptions struct {
		MaxLen int
		Logger ponos.Logger
	}
)

// WithStreamMaxLen sets the maximum number of events stored by the stream.
func WithStreamMaxLen(len int) Stream {
	return func(o *StreamOptions) {
		o.MaxLen = len
	}
}

// WithStreamLogger sets the logger used by the stream.
func WithStreamLogger(logger ponos.Logger) Stream {
	return func(o *StreamOptions) {
		o.Logger = logger
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
		Logger: ponos.NoopLogger(),
	}
}
