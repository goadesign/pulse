package streams

import (
	"context"
	"log"
	"time"
)

type (
	// SinkOption is a sink creation option.
	SinkOption func(*sinkOption)

	// EventMatcherFunc is a function that matches an event.
	EventMatcherFunc func(event *Event) bool

	sinkOption struct {
		Topic          string
		TopicPattern   string
		EventMatcher   EventMatcherFunc
		BufferSize     int
		LastEventID    string
		NoAck          bool
		AckGracePeriod time.Duration
		ErrorReporter  func(context.Context, error)
	}
)

// WithSinkTopic sets the sink topic.
func WithSinkTopic(topic string) SinkOption {
	return func(o *sinkOption) {
		o.Topic = topic
	}
}

// WithSinkTopicPattern sets the sink topic pattern.
// pattern must be a valid regular expression or NewSink panics.
func WithSinkTopicPattern(pattern string) SinkOption {
	return func(o *sinkOption) {
		o.TopicPattern = pattern
	}
}

// WithSinkEventMatcher sets the sink topic matcher.
func WithSinkEventMatcher(matcher EventMatcherFunc) SinkOption {
	return func(o *sinkOption) {
		o.EventMatcher = matcher
	}
}

// WithSinkBufferSize sets the sink channel buffer size.
// The default buffer size is 1000.
func WithSinkBufferSize(size int) SinkOption {
	return func(o *sinkOption) {
		o.BufferSize = size
	}
}

// WithSinkLastEventID sets the last read event ID, the sink will start reading
// from the next event. Use the special ID "0" to start from the beginning of
// the stream. Starts from the last event by default.
func WithSinkLastEventID(id string) SinkOption {
	return func(o *sinkOption) {
		o.LastEventID = id
	}
}

// WithSinkNoAck removes the need to acknowledge events read from the sink.
func WithSinkNoAck() SinkOption {
	return func(o *sinkOption) {
		o.NoAck = true
	}
}

// WithSinkAckGracePeriod sets the grace period for acknowledging events.  The
// default grace period is 30 seconds. Events that are not acknowledged within
// the grace period will be redelivered.
func WithSinkAckGracePeriod(d time.Duration) SinkOption {
	return func(o *sinkOption) {
		o.AckGracePeriod = d
	}
}

// WithSinkErrorReporter sets the sink error reporter.
// The default reporter prints to the console.
func WithSinkErrorReporter(handler func(context.Context, error)) SinkOption {
	return func(o *sinkOption) {
		o.ErrorReporter = handler
	}
}

// defaultSinkOptions returns the default options.
func defaultSinkOptions() sinkOption {
	return sinkOption{
		BufferSize: 1000,
		ErrorReporter: func(ctx context.Context, err error) {
			log.Println(err)
		},
		LastEventID:    "$",
		AckGracePeriod: 30 * time.Second,
	}
}
