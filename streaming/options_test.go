package streaming

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"goa.design/ponos/ponos"
)

func TestStreamOptions(t *testing.T) {
	cases := []struct {
		name string
		opts []StreamOption
		want streamOptions
	}{
		{
			name: "default",
			opts: []StreamOption{},
			want: streamOptions{
				MaxLen: 1000,
				Logger: &ponos.NilLogger{},
			},
		},
		{
			name: "maxlen",
			opts: []StreamOption{WithMaxLen(10)},
			want: streamOptions{
				MaxLen: 10,
				Logger: &ponos.NilLogger{},
			},
		},
		{
			name: "custom logger",
			opts: []StreamOption{WithLogger(ponos.StdLogger(log.Default()))},
			want: streamOptions{
				MaxLen: 1000,
				Logger: ponos.StdLogger(log.Default()),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := defaultStreamOptions()
			for _, opt := range c.opts {
				opt(&got)
			}
			assert.Equal(t, c.want, got)
		})
	}
}

func TestReaderOptions(t *testing.T) {
	cases := []struct {
		name string
		opts []ReaderOption
		want readerOptions
	}{
		{
			name: "default",
			opts: []ReaderOption{},
			want: readerOptions{
				BufferSize:  1000,
				LastEventID: "$",
			},
		},
		{
			name: "topic",
			opts: []ReaderOption{WithReaderTopic("foo")},
			want: readerOptions{
				BufferSize:  1000,
				LastEventID: "$",
				Topic:       "foo",
			},
		},
		{
			name: "topic pattern",
			opts: []ReaderOption{WithReaderTopicPattern("foo*")},
			want: readerOptions{
				BufferSize:   1000,
				LastEventID:  "$",
				TopicPattern: "foo*",
			},
		},
		{
			name: "event matcher",
			opts: []ReaderOption{WithReaderEventMatcher(nil)},
			want: readerOptions{
				BufferSize:   1000,
				LastEventID:  "$",
				EventMatcher: nil,
			},
		},
		{
			name: "buffer size",
			opts: []ReaderOption{WithReaderBufferSize(10)},
			want: readerOptions{
				BufferSize:  10,
				LastEventID: "$",
			},
		},
		{
			name: "last event ID",
			opts: []ReaderOption{WithReaderLastEventID("foo")},
			want: readerOptions{
				BufferSize:  1000,
				LastEventID: "foo",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := defaultReaderOptions()
			for _, opt := range c.opts {
				opt(&got)
			}
			assert.Equal(t, c.want, got)
		})
	}
}

func TestSinkOptions(t *testing.T) {
	date := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		opts []SinkOption
		want sinkOptions
	}{
		{
			name: "default",
			opts: []SinkOption{},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 30 * time.Second,
			},
		},
		{
			name: "topic",
			opts: []SinkOption{WithSinkTopic("foo")},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 30 * time.Second,
				Topic:          "foo",
			},
		},
		{
			name: "topic pattern",
			opts: []SinkOption{WithSinkTopicPattern("foo*")},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 30 * time.Second,
				TopicPattern:   "foo*",
			},
		},
		{
			name: "event matcher",
			opts: []SinkOption{WithSinkEventMatcher(nil)},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 30 * time.Second,
				EventMatcher:   nil,
			},
		},
		{
			name: "buffer size",
			opts: []SinkOption{WithSinkBufferSize(10)},
			want: sinkOptions{
				BufferSize:     10,
				LastEventID:    "$",
				AckGracePeriod: 30 * time.Second,
			},
		},
		{
			name: "last event ID",
			opts: []SinkOption{WithSinkLastEventID("foo")},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    "foo",
				AckGracePeriod: 30 * time.Second,
			},
		},
		{
			name: "start at",
			opts: []SinkOption{WithSinkStartAt(date)},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    fmt.Sprintf("%d-0", date.UnixMilli()),
				AckGracePeriod: 30 * time.Second,
			},
		},
		{
			name: "no ack",
			opts: []SinkOption{WithSinkNoAck()},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 30 * time.Second,
				NoAck:          true,
			},
		},
		{
			name: "ack grace period",
			opts: []SinkOption{WithSinkAckGracePeriod(10 * time.Second)},
			want: sinkOptions{
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 10 * time.Second,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := defaultSinkOptions()
			for _, opt := range c.opts {
				opt(&got)
			}
			assert.Equal(t, c.want, got)
		})
	}
}

func TestAddStreamOptions(t *testing.T) {
	date := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		opts []AddStreamOption
		want addStreamOptions
	}{
		{
			name: "default",
			opts: []AddStreamOption{},
			want: addStreamOptions{
				LastEventID: "",
			},
		},
		{
			name: "last event ID",
			opts: []AddStreamOption{WithAddStreamLastEventID("foo")},
			want: addStreamOptions{
				LastEventID: "foo",
			},
		},
		{
			name: "start at",
			opts: []AddStreamOption{WithAddStreamStartAt(date)},
			want: addStreamOptions{
				LastEventID: fmt.Sprintf("%d-0", date.UnixMilli()),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := defaultAddStreamOptions()
			for _, opt := range c.opts {
				opt(&got)
			}
			assert.Equal(t, c.want, got)
		})
	}
}
