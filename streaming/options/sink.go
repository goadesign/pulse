package options

import (
	"fmt"
	"time"
)

type (
	// Sink is a sink creation option.
	Sink func(*SinkOptions)

	SinkOptions struct {
		BlockDuration  time.Duration
		MaxPolled      int64
		Topic          string
		TopicPattern   string
		BufferSize     int
		LastEventID    string
		NoAck          bool
		AckGracePeriod time.Duration
	}
)

// WithSinkBlockDuration sets the maximum amount of time the sink waits for
// MaxPolled events. The default block duration is 5 seconds. If the block
// duration is set to 0 then the sink blocks indefinitely.
func WithSinkBlockDuration(d time.Duration) Sink {
	return func(o *SinkOptions) {
		o.BlockDuration = d
	}
}

// WithSinkMaxPolled sets the maximum number of events polled by the sink at once. The
// default maximum number of events is 1000.
func WithSinkMaxPolled(n int64) Sink {
	return func(o *SinkOptions) {
		o.MaxPolled = n
	}
}

// WithSinkTopic sets the sink topic.
func WithSinkTopic(topic string) Sink {
	return func(o *SinkOptions) {
		o.Topic = topic
	}
}

// WithSinkTopicPattern sets the sink topic pattern.
// pattern must be a valid regular expression or NewSink panics.
func WithSinkTopicPattern(pattern string) Sink {
	return func(o *SinkOptions) {
		o.TopicPattern = pattern
	}
}

// WithSinkBufferSize sets the sink channel buffer size.  The default buffer
// size is 1000. If the buffer is full the sink blocks until the buffer has
// space available.
func WithSinkBufferSize(size int) Sink {
	return func(o *SinkOptions) {
		o.BufferSize = size
	}
}

// WithSinkStartAtNewest sets the sink start position to the newest event,
// this is the default. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAtNewest() Sink {
	return func(o *SinkOptions) {
		o.LastEventID = "$"
	}
}

// WithSinkStartAtOldest sets the sink start position to the oldest event.
// Only one of WithSinkStartAtNewest, WithSinkStartAtOldest, WithSinkStartAfter
// or WithSinkStartAt can be used.
func WithSinkStartAtOldest() Sink {
	return func(o *SinkOptions) {
		o.LastEventID = "0"
	}
}

// WithSinkStartAfter sets the last read event ID, the sink will start reading
// from the next event. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAfter(id string) Sink {
	return func(o *SinkOptions) {
		o.LastEventID = id
	}
}

// WithSinkStartAt sets the start position for the sink, defaults
// to the last event. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAt(startAt time.Time) Sink {
	return func(o *SinkOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
	}
}

// WithSinkNoAck removes the need to acknowledge events read from the sink.
func WithSinkNoAck() Sink {
	return func(o *SinkOptions) {
		o.NoAck = true
	}
}

// WithSinkAckGracePeriod sets the grace period for acknowledging events.  The
// default grace period is 20 seconds.
func WithSinkAckGracePeriod(d time.Duration) Sink {
	return func(o *SinkOptions) {
		o.AckGracePeriod = d
	}
}

// ParseSinkOptions parses the options and returns the sink options.
func ParseSinkOptions(opts ...Sink) SinkOptions {
	o := defaultSinkOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// defaultSinkOptions returns the default options.
func defaultSinkOptions() SinkOptions {
	return SinkOptions{
		BlockDuration:  5 * time.Second,
		MaxPolled:      1000,
		BufferSize:     1000,
		LastEventID:    "$",
		AckGracePeriod: 20 * time.Second,
	}
}
