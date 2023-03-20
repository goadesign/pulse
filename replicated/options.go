package replicated

import "goa.design/ponos/ponos"

type (
	// MapOption is a Map creation option.
	MapOption func(*options) error

	options struct {
		// Channel name
		// Logger
		Logger ponos.Logger
	}
)

// WithLogger sets the logger used to report errors.
func WithLogger(logger ponos.Logger) MapOption {
	return func(o *options) error {
		o.Logger = logger
		return nil
	}
}

// defaultOptions returns the default options.
func defaultOptions() *options {
	return &options{
		Logger: &ponos.NilLogger{},
	}
}
