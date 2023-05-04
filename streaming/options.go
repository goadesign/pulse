package streaming

import (
	"fmt"
	"time"

	"goa.design/ponos/ponos"
)

type (
	// StreamOption is a stream creation option.
	StreamOption func(*streamOptions)

	// ReaderOption is a sink creation option.
	ReaderOption func(*readerOptions)

	// SinkOption is a sink creation option.
	SinkOption func(*sinkOptions)

	// AddStreamOption is an option for adding an event to a stream.
	AddStreamOption func(*addStreamOptions)

	// EventMatcherFunc is a function that matches an event.
	EventMatcherFunc func(event *Event) bool

	streamOptions struct {
		MaxLen int
		Logger ponos.Logger
	}

	readerOptions struct {
		BlockDuration time.Duration
		MaxPolled     int64
		Topic         string
		TopicPattern  string
		EventMatcher  EventMatcherFunc
		BufferSize    int
		LastEventID   string
	}

	sinkOptions struct {
		BlockDuration  time.Duration
		MaxPolled      int64
		Topic          string
		TopicPattern   string
		EventMatcher   EventMatcherFunc
		BufferSize     int
		LastEventID    string
		NoAck          bool
		AckGracePeriod time.Duration
	}

	addStreamOptions struct {
		LastEventID string
	}
)

// WithStreamMaxLen sets the maximum number of events stored by the stream.
func WithStreamMaxLen(len int) StreamOption {
	return func(o *streamOptions) {
		o.MaxLen = len
	}
}

// WithStreamLogger sets the logger used by the stream.
func WithStreamLogger(logger ponos.Logger) StreamOption {
	return func(o *streamOptions) {
		o.Logger = logger
	}
}

// WithReaderBlockDuration sets the maximum amount of time the reader waits for
// MaxPolled events. The default block duration is 5 seconds. If the block
// duration is set to 0 then the reader blocks indefinitely.
func WithReaderBlockDuration(d time.Duration) ReaderOption {
	return func(o *readerOptions) {
		o.BlockDuration = d
	}
}

// WithReaderMaxPolled sets the maximum number of events polled by the reader at once. The
// default maximum number of events is 1000.
func WithReaderMaxPolled(n int64) ReaderOption {
	return func(o *readerOptions) {
		o.MaxPolled = n
	}
}

// WithReaderTopic sets the reader topic.
func WithReaderTopic(topic string) ReaderOption {
	return func(o *readerOptions) {
		o.Topic = topic
	}
}

// WithReaderTopicPattern sets the reader topic pattern.
// pattern must be a valid regular expression or NewReader panics.
func WithReaderTopicPattern(pattern string) ReaderOption {
	return func(o *readerOptions) {
		o.TopicPattern = pattern
	}
}

// WithReaderEventMatcher sets the reader topic matcher.
func WithReaderEventMatcher(matcher EventMatcherFunc) ReaderOption {
	return func(o *readerOptions) {
		o.EventMatcher = matcher
	}
}

// WithReaderBufferSize sets the reader channel buffer size.  The default buffer
// size is 1000. If the buffer is full the reader blocks until the buffer has
// space available.
func WithReaderBufferSize(size int) ReaderOption {
	return func(o *readerOptions) {
		o.BufferSize = size
	}
}

// WithReaderStartAtNewest sets the reader start position to the newest event,
// this is the default. Only one of WithReaderStartAtNewest,
// WithReaderStartAtOldest, WithReaderStartAfter or WithReaderStartAt can be
// used.
func WithReaderStartAtNewest() ReaderOption {
	return func(o *readerOptions) {
		o.LastEventID = "$"
	}
}

// WithReaderStartAtOldest sets the reader start position to the oldest event.
// Only one of WithReaderStartAtOldest, WithReaderStartAfter or
// WithReaderStartAt should be used.
func WithReaderStartAtOldest() ReaderOption {
	return func(o *readerOptions) {
		o.LastEventID = "0"
	}
}

// WithReaderStartAfter sets the last read event ID, the reader will start
// reading from the next event.
func WithReaderStartAfter(id string) ReaderOption {
	return func(o *readerOptions) {
		o.LastEventID = id
	}
}

// WithReaderStartAt sets the start position for the reader to the event added
// on or after startAt.
func WithReaderStartAt(startAt time.Time) ReaderOption {
	return func(o *readerOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
	}
}

// WithSinkBlockDuration sets the maximum amount of time the sink waits for
// MaxPolled events. The default block duration is 5 seconds. If the block
// duration is set to 0 then the sink blocks indefinitely.
func WithSinkBlockDuration(d time.Duration) SinkOption {
	return func(o *sinkOptions) {
		o.BlockDuration = d
	}
}

// WithSinkMaxPolled sets the maximum number of events polled by the sink at once. The
// default maximum number of events is 1000.
func WithSinkMaxPolled(n int64) SinkOption {
	return func(o *sinkOptions) {
		o.MaxPolled = n
	}
}

// WithSinkTopic sets the sink topic.
func WithSinkTopic(topic string) SinkOption {
	return func(o *sinkOptions) {
		o.Topic = topic
	}
}

// WithSinkTopicPattern sets the sink topic pattern.
// pattern must be a valid regular expression or NewSink panics.
func WithSinkTopicPattern(pattern string) SinkOption {
	return func(o *sinkOptions) {
		o.TopicPattern = pattern
	}
}

// WithSinkEventMatcher sets the sink topic matcher.
func WithSinkEventMatcher(matcher EventMatcherFunc) SinkOption {
	return func(o *sinkOptions) {
		o.EventMatcher = matcher
	}
}

// WithSinkBufferSize sets the sink channel buffer size.  The default buffer
// size is 1000. If the buffer is full the sink blocks until the buffer has
// space available.
func WithSinkBufferSize(size int) SinkOption {
	return func(o *sinkOptions) {
		o.BufferSize = size
	}
}

// WithSinkStartAtNewest sets the sink start position to the newest event,
// this is the default. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAtNewest() SinkOption {
	return func(o *sinkOptions) {
		o.LastEventID = "$"
	}
}

// WithSinkStartAtOldest sets the sink start position to the oldest event.
// Only one of WithSinkStartAtNewest, WithSinkStartAtOldest, WithSinkStartAfter
// or WithSinkStartAt can be used.
func WithSinkStartAtOldest() SinkOption {
	return func(o *sinkOptions) {
		o.LastEventID = "0"
	}
}

// WithSinkStartAfter sets the last read event ID, the sink will start reading
// from the next event. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAfter(id string) SinkOption {
	return func(o *sinkOptions) {
		o.LastEventID = id
	}
}

// WithSinkStartAt sets the start position for the sink, defaults
// to the last event. Only one of WithSinkStartAtNewest,
// WithSinkStartAtOldest, WithSinkStartAfter or WithSinkStartAt can be used.
func WithSinkStartAt(startAt time.Time) SinkOption {
	return func(o *sinkOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
	}
}

// WithSinkNoAck removes the need to acknowledge events read from the sink.
func WithSinkNoAck() SinkOption {
	return func(o *sinkOptions) {
		o.NoAck = true
	}
}

// WithSinkAckGracePeriod sets the grace period for acknowledging events.  The
// default grace period is 30 seconds, the minimum is 1s.
func WithSinkAckGracePeriod(d time.Duration) SinkOption {
	if d < time.Second {
		d = time.Second
	}
	return func(o *sinkOptions) {
		o.AckGracePeriod = d
	}
}

// WithAddStreamStartAtNewest sets the sink start position for the added stream
// to the newest event.  Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAtNewest() AddStreamOption {
	return func(o *addStreamOptions) {
		o.LastEventID = "$"
	}
}

// WithAddStreamStartAtOldest sets the sink start position for the added stream
// to the oldest event.  Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAtOldest() AddStreamOption {
	return func(o *addStreamOptions) {
		o.LastEventID = "0"
	}
}

// WithAddStreamStartAfter sets the last read event ID, the sink will start
// reading from the next event. Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAfter(id string) AddStreamOption {
	return func(o *addStreamOptions) {
		o.LastEventID = id
	}
}

// WithAddStreamStartAt sets the start position for the added stream to the
// event added on or after startAt. Only one of WithAddStreamStartAtNewest,
// WithAddStreamStartAtOldest, WithAddStreamStartAfter or WithAddStreamStartAt
// can be used.
func WithAddStreamStartAt(startAt time.Time) AddStreamOption {
	return func(o *addStreamOptions) {
		o.LastEventID = fmt.Sprintf("%d-0", startAt.UnixMilli())
	}
}

// defaultStreamOptions returns the default options.
func defaultStreamOptions() streamOptions {
	return streamOptions{
		MaxLen: 1000,
		Logger: ponos.NoopLogger(),
	}
}

// defaultReaderOptions returns the default options.
func defaultReaderOptions() readerOptions {
	return readerOptions{
		BlockDuration: 5 * time.Second,
		MaxPolled:     1000,
		BufferSize:    1000,
		LastEventID:   "$",
	}
}

// defaultSinkOptions returns the default options.
func defaultSinkOptions() sinkOptions {
	return sinkOptions{
		BlockDuration:  5 * time.Second,
		MaxPolled:      1000,
		BufferSize:     1000,
		LastEventID:    "$",
		AckGracePeriod: 30 * time.Second,
	}
}

// defaultAddStreamOptions returns the default options.
func defaultAddStreamOptions() addStreamOptions {
	return addStreamOptions{}
}
