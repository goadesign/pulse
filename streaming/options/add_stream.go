package options

import (
	"fmt"
	"time"
)

type (
	// AddStream is an option for adding a stream to a sink.
	AddStream func(*AddStreamOptions)

	// AddStreamOptions keeps its v1 field first; new fields are only ever
	// appended so existing keyed construction remains source-compatible.
	AddStreamOptions struct {
		// LastEventID is the ID after which delivery starts for this stream.
		LastEventID string
		// startOptions counts applied start-position options to reject
		// conflicting combinations.
		startOptions int
	}
)

// WithAddStreamStartAtNewest sets the sink start position for the added stream
// to the newest event.  Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAtNewest() AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = "$"
		o.startOptions++
	}
}

// WithAddStreamStartAtOldest sets the sink start position for the added stream
// to the oldest event.  Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAtOldest() AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = "0"
		o.startOptions++
	}
}

// WithAddStreamStartAfter sets the last read event ID, the sink will start
// reading from the next event. Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAfter(id string) AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = id
		o.startOptions++
	}
}

// WithAddStreamStartAt sets the start position for the added stream to the
// event added on or after startAt. Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAt(startAt time.Time) AddStream {
	return func(o *AddStreamOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
		o.startOptions++
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

// HasConflictingStartOptions reports whether more than one cursor-start option
// was supplied. AddStream rejects this instead of silently accepting the last
// option.
func (o AddStreamOptions) HasConflictingStartOptions() bool {
	return o.startOptions > 1
}

// defaultAddStreamOptions returns the default options.
func defaultAddStreamOptions() AddStreamOptions {
	return AddStreamOptions{}
}
