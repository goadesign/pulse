package options

import (
	"fmt"
	"time"
)

type (
	// Reader is a sink creation option.
	Reader func(*ReaderOptions)

	// ReaderOptions keeps its v1 fields first, in v1 order; new fields are
	// only ever appended so existing keyed construction and field access
	// remain source-compatible across feature releases.
	ReaderOptions struct {
		// BlockDuration is the XREAD block duration.
		BlockDuration time.Duration
		// MaxPolled is the maximum number of events read per XREAD call.
		MaxPolled int64
		// Topic delivers only events published with this exact topic.
		Topic string
		// TopicPattern delivers only events whose topic matches this regex.
		TopicPattern string
		// BufferSize is the capacity of each subscription channel.
		BufferSize int
		// LastEventID is the ID after which delivery starts.
		LastEventID string
		// startOptions counts applied start-position options to reject
		// conflicting combinations.
		startOptions int
	}
)

// WithReaderBlockDuration sets the maximum amount of time the reader waits for
// MaxPolled events. The default block duration is 5 seconds. NewReader rejects
// durations below one millisecond because Redis block timing is millisecond
// precision and every read must have a finite shutdown bound.
func WithReaderBlockDuration(d time.Duration) Reader {
	return func(o *ReaderOptions) {
		o.BlockDuration = d
	}
}

// WithReaderMaxPolled sets the maximum number of events polled by the reader at once. The
// default maximum number of events is 1000.
func WithReaderMaxPolled(n int64) Reader {
	return func(o *ReaderOptions) {
		o.MaxPolled = n
	}
}

// WithReaderTopic sets the reader topic.
func WithReaderTopic(topic string) Reader {
	return func(o *ReaderOptions) {
		o.Topic = topic
	}
}

// WithReaderTopicPattern sets the reader topic pattern.
// NewReader returns an error when pattern is not a valid regular expression.
func WithReaderTopicPattern(pattern string) Reader {
	return func(o *ReaderOptions) {
		o.TopicPattern = pattern
	}
}

// WithReaderBufferSize sets the reader channel buffer size.  The default buffer
// size is 1000. If the buffer is full the reader blocks until the buffer has
// space available.
func WithReaderBufferSize(size int) Reader {
	return func(o *ReaderOptions) {
		o.BufferSize = size
	}
}

// WithReaderStartAtNewest sets the reader start position to the newest event,
// this is the default. Only one of WithReaderStartAtNewest,
// WithReaderStartAtOldest, WithReaderStartAfter or WithReaderStartAt can be
// used.
func WithReaderStartAtNewest() Reader {
	return func(o *ReaderOptions) {
		o.LastEventID = "$"
		o.startOptions++
	}
}

// WithReaderStartAtOldest sets the reader start position to the oldest event.
// Only one of WithReaderStartAtOldest, WithReaderStartAfter or
// WithReaderStartAt should be used.
func WithReaderStartAtOldest() Reader {
	return func(o *ReaderOptions) {
		o.LastEventID = "0"
		o.startOptions++
	}
}

// WithReaderStartAfter sets the last read event ID, the reader will start
// reading from the next event.
func WithReaderStartAfter(id string) Reader {
	return func(o *ReaderOptions) {
		o.LastEventID = id
		o.startOptions++
	}
}

// WithReaderStartAt sets the start position for the reader to the event added
// on or after startAt.
func WithReaderStartAt(startAt time.Time) Reader {
	return func(o *ReaderOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
		o.startOptions++
	}
}

// ParseReaderOptions parses the given options and returns the corresponding
// reader options.
func ParseReaderOptions(opts ...Reader) ReaderOptions {
	o := defaultReaderOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// HasConflictingStartOptions reports whether more than one cursor-start option
// was supplied. Constructors reject this instead of silently accepting the
// last option.
func (o ReaderOptions) HasConflictingStartOptions() bool {
	return o.startOptions > 1
}

// defaultReaderOptions returns the default options.
func defaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		BlockDuration: 5 * time.Second,
		MaxPolled:     1000,
		BufferSize:    1000,
		LastEventID:   "$",
	}
}
