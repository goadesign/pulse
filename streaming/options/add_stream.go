package options

import (
	"fmt"
	"time"
)

type (
	// AddStream is an option for adding a stream to a sink.
	AddStream func(*AddStreamOptions)

	AddStreamOptions struct {
		LastEventID string
	}
)

// WithAddStreamStartAtNewest sets the sink start position for the added stream
// to the newest event.  Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAtNewest() AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = "$"
	}
}

// WithAddStreamStartAtOldest sets the sink start position for the added stream
// to the oldest event.  Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAtOldest() AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = "0"
	}
}

// WithAddStreamStartAfter sets the last read event ID, the sink will start
// reading from the next event. Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAfter(id string) AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = id
	}
}

// WithAddStreamStartAt sets the start position for the added stream to the
// event added on or after startAt. Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAt(startAt time.Time) AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
	}
}

// ParseAddStreamOptions parses the options and returns the add stream options.
func ParseAddStreamOptions(opts ...AddStream) AddStreamOptions {
	options := defaultAddStreamOptions()
	for _, o := range opts {
		o(&options)
	}
	return options
}

// defaultAddStreamOptions returns the default options.
func defaultAddStreamOptions() AddStreamOptions {
	return AddStreamOptions{}
}
