package options

import (
	"fmt"
	"time"
)

type (
	// Reader is a sink creation option.
	Reader func(*ReaderOptions)

	ReaderOptions struct {
		BlockDuration time.Duration
		MaxPolled     int64
		Topic         string
		TopicPattern  string
		BufferSize    int
		LastEventID   string
	}
)

// WithReaderBlockDuration sets the maximum amount of time the reader waits for
// MaxPolled events. The default block duration is 5 seconds. If the block
// duration is set to 0 then the reader blocks indefinitely.
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
// pattern must be a valid regular expression or NewReader panics.
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
	}
}

// WithReaderStartAtOldest sets the reader start position to the oldest event.
// Only one of WithReaderStartAtOldest, WithReaderStartAfter or
// WithReaderStartAt should be used.
func WithReaderStartAtOldest() Reader {
	return func(o *ReaderOptions) {
		o.LastEventID = "0"
	}
}

// WithReaderStartAfter sets the last read event ID, the reader will start
// reading from the next event.
func WithReaderStartAfter(id string) Reader {
	return func(o *ReaderOptions) {
		o.LastEventID = id
	}
}

// WithReaderStartAt sets the start position for the reader to the event added
// on or after startAt.
func WithReaderStartAt(startAt time.Time) Reader {
	return func(o *ReaderOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
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

// defaultReaderOptions returns the default options.
func defaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		BlockDuration: 5 * time.Second,
		MaxPolled:     1000,
		BufferSize:    1000,
		LastEventID:   "$",
	}
}
