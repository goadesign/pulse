package options

type (
	// AddEvent is an option for adding an event to a stream.
	AddEvent func(*AddEventOptions)

	AddEventOptions struct {
		Topic              string
		OnlyIfStreamExists bool
	}
)

// WithTopic sets the topic for the added event.
func WithTopic(topic string) AddEvent {
	return func(o *AddEventOptions) {
		o.Topic = topic
	}
}

// WithOnlyIfStreamExists only adds the event if the stream exists.
func WithOnlyIfStreamExists() AddEvent {
	return func(o *AddEventOptions) {
		o.OnlyIfStreamExists = true
	}
}

// ParseAddEventOptions parses the given options and returns the corresponding
// AddEventOptions.
func ParseAddEventOptions(opts ...AddEvent) AddEventOptions {
	o := defaultAddEventOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// defaultAddEventOptions returns the default options.
func defaultAddEventOptions() AddEventOptions {
	return AddEventOptions{}
}
