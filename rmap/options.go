package rmap

import "goa.design/ponos/ponos"

type (
	// MapOption is a Map creation option.
	MapOption func(*options)

	options struct {
		// Channel name
		// Logger
		Logger ponos.Logger
	}
)

// WithLogger sets the logger used by the map.
func WithLogger(logger ponos.Logger) MapOption {
	return func(o *options) {
		o.Logger = logger
	}
}

// defaultOptions returns the default options.
func defaultOptions() *options {
	return &options{
		Logger: &ponos.NilLogger{},
	}
}
