package streams

import (
	"context"
	"log"
)

type (
	// SinkOption is a sink creation option.
	SinkOption func(*sinkOption)

	// EventMatcherFunc is a function that matches an event.
	EventMatcherFunc func(event *Event) bool

	sinkOption struct {
		Topic         string
		TopicPattern  string
		EventMatcher  EventMatcherFunc
		BufferSize    int
		ErrorReporter func(context.Context, error)
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

// WithSinkBufferSize sets the sink buffer size.
// The default buffer size is 1000.
func WithSinkBufferSize(size int) SinkOption {
	return func(o *sinkOption) {
		o.BufferSize = size
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
	}
}
