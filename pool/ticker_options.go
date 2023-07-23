package pool

import (
	"goa.design/pulse/pulse"
)

type (
	// TickerOption is a worker creation option.
	TickerOption func(*tickerOptions)

	tickerOptions struct {
		logger pulse.Logger
	}
)

// WithTickerLogger sets the handler used to report temporary errors.
func WithTickerLogger(logger pulse.Logger) TickerOption {
	return func(o *tickerOptions) {
		o.logger = logger
	}
}

// parseOptions parses the given options and returns the corresponding
// options.
func parseTickerOptions(opts ...TickerOption) *tickerOptions {
	o := defaultTickerOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// defaultTickerOptions returns the default options.
func defaultTickerOptions() *tickerOptions {
	return &tickerOptions{
		logger: pulse.NoopLogger(),
	}
}
