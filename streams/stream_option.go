package streams

type (
	// StreamOption is a stream creation option.
	StreamOption func(*streamOption)

	streamOption struct {
		MaxLen int
	}
)

// WithStreamMaxLen sets the stream max length.
func WithStreamMaxLen(len int) StreamOption {
	return func(o *streamOption) {
		o.MaxLen = len
	}
}

// defaultStreamOptions returns the default options.
func defaultStreamOptions() streamOption {
	return streamOption{
		MaxLen: 1000,
	}
}
