package options

import (
	"fmt"
	"time"
)

type (
	// Sink is a sink creation option.
	Sink func(*SinkOptions)

	// SinkOptions keeps its v1 fields first, in v1 order; new fields are
	// only ever appended so existing keyed construction and field access
	// remain source-compatible across feature releases.
	SinkOptions struct {
		// BlockDuration is the XREADGROUP block duration.
		BlockDuration time.Duration
		// MaxPolled is the maximum number of events read per XREADGROUP call.
		MaxPolled int64
		// Topic delivers only events published with this exact topic.
		Topic string
		// TopicPattern delivers only events whose topic matches this regex.
		TopicPattern string
		// BufferSize is the capacity of each subscription channel.
		BufferSize int
		// LastEventID is the ID after which delivery starts.
		LastEventID string
		// NoAck atomically acknowledges each event before delivery.
		NoAck bool
		// AckGracePeriod bounds how long an unacknowledged event stays owned
		// by one consumer before stale recovery may reclaim it.
		AckGracePeriod time.Duration
		// startOptions counts applied start-position options to reject
		// conflicting combinations.
		startOptions int
	}
)

// WithSinkBlockDuration sets the maximum amount of time the sink waits for
// MaxPolled events. The default block duration is 5 seconds. NewSink rejects
// durations below one millisecond because Redis block timing is millisecond
// precision and every read must have a finite shutdown bound.
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
// NewSink returns an error when pattern is not a valid regular expression.
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
		o.startOptions++
	}
}

// WithSinkStartAtOldest sets the sink start position to the oldest event.
// Only one of WithSinkStartAtNewest, WithSinkStartAtOldest, WithSinkStartAfter
// or WithSinkStartAt can be used.
func WithSinkStartAtOldest() Sink {
	return func(o *SinkOptions) {
		o.LastEventID = "0"
		o.startOptions++
	}
}

// WithSinkStartAfter sets the last read event ID, the sink will start reading
// from the next event. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAfter(id string) Sink {
	return func(o *SinkOptions) {
		o.LastEventID = id
		o.startOptions++
	}
}

// WithSinkStartAt sets the start position for the sink, defaults
// to the last event. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAt(startAt time.Time) Sink {
	return func(o *SinkOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
		o.startOptions++
	}
}

// WithSinkNoAck atomically acknowledges each event before delivering it to
// subscribers, preserving at-most-once delivery without requiring Sink.Ack.
func WithSinkNoAck() Sink {
	return func(o *SinkOptions) {
		o.NoAck = true
	}
}

// WithSinkAckGracePeriod sets the grace period for acknowledging events. The
// default grace period is 20 seconds; NewSink rejects values below one
// millisecond.
// Note: all sinks with identical names must have the same ack grace period.
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

// HasConflictingStartOptions reports whether more than one cursor-start option
// was supplied. Constructors reject this instead of silently accepting the
// last option.
func (o SinkOptions) HasConflictingStartOptions() bool {
	return o.startOptions > 1
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
