package options

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"goa.design/pulse/pulse"
)

func TestStreamOptions(t *testing.T) {
	cases := []struct {
		name string
		opts []Stream
		want StreamOptions
	}{
		{
			name: "default",
			opts: []Stream{},
			want: StreamOptions{
				MaxLen: 1000,
				Logger: pulse.NoopLogger(),
			},
		},
		{
			name: "maxlen",
			opts: []Stream{WithStreamMaxLen(10)},
			want: StreamOptions{
				MaxLen: 10,
				Logger: pulse.NoopLogger(),
			},
		},
		{
			name: "custom logger",
			opts: []Stream{WithStreamLogger(pulse.StdLogger(log.Default()))},
			want: StreamOptions{
				MaxLen: 1000,
				Logger: pulse.StdLogger(log.Default()),
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
		opts []Reader
		want ReaderOptions
	}{
		{
			name: "default",
			opts: []Reader{},
			want: ReaderOptions{
				BlockDuration: 5 * time.Second,
				MaxPolled:     1000,
				BufferSize:    1000,
				LastEventID:   "$",
			},
		},
		{
			name: "block duration",
			opts: []Reader{WithReaderBlockDuration(10 * time.Second)},
			want: ReaderOptions{
				BlockDuration: 10 * time.Second,
				MaxPolled:     1000,
				BufferSize:    1000,
				LastEventID:   "$",
			},
		},
		{
			name: "max polled",
			opts: []Reader{WithReaderMaxPolled(10)},
			want: ReaderOptions{
				BlockDuration: 5 * time.Second,
				MaxPolled:     10,
				BufferSize:    1000,
				LastEventID:   "$",
			},
		},
		{
			name: "topic",
			opts: []Reader{WithReaderTopic("foo")},
			want: ReaderOptions{
				BlockDuration: 5 * time.Second,
				MaxPolled:     1000,
				BufferSize:    1000,
				LastEventID:   "$",
				Topic:         "foo",
			},
		},
		{
			name: "topic pattern",
			opts: []Reader{WithReaderTopicPattern("foo*")},
			want: ReaderOptions{
				BlockDuration: 5 * time.Second,
				MaxPolled:     1000,
				BufferSize:    1000,
				LastEventID:   "$",
				TopicPattern:  "foo*",
			},
		},
		{
			name: "buffer size",
			opts: []Reader{WithReaderBufferSize(10)},
			want: ReaderOptions{
				BlockDuration: 5 * time.Second,
				MaxPolled:     1000,
				BufferSize:    10,
				LastEventID:   "$",
			},
		},
		{
			name: "last event ID",
			opts: []Reader{WithReaderStartAfter("foo")},
			want: ReaderOptions{
				BlockDuration: 5 * time.Second,
				MaxPolled:     1000,
				BufferSize:    1000,
				LastEventID:   "foo",
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
		opts []Sink
		want SinkOptions
	}{
		{
			name: "default",
			opts: []Sink{},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 20 * time.Second,
			},
		},
		{
			name: "block duration",
			opts: []Sink{WithSinkBlockDuration(10 * time.Second)},
			want: SinkOptions{
				BlockDuration:  10 * time.Second,
				MaxPolled:      1000,
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 20 * time.Second,
			},
		},
		{
			name: "max polled",
			opts: []Sink{WithSinkMaxPolled(10)},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      10,
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 20 * time.Second,
			},
		},
		{
			name: "topic",
			opts: []Sink{WithSinkTopic("foo")},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 20 * time.Second,
				Topic:          "foo",
			},
		},
		{
			name: "topic pattern",
			opts: []Sink{WithSinkTopicPattern("foo*")},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 20 * time.Second,
				TopicPattern:   "foo*",
			},
		},
		{
			name: "buffer size",
			opts: []Sink{WithSinkBufferSize(10)},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
				BufferSize:     10,
				LastEventID:    "$",
				AckGracePeriod: 20 * time.Second,
			},
		},
		{
			name: "last event ID",
			opts: []Sink{WithSinkStartAfter("foo")},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
				BufferSize:     1000,
				LastEventID:    "foo",
				AckGracePeriod: 20 * time.Second,
			},
		},
		{
			name: "start at",
			opts: []Sink{WithSinkStartAt(date)},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
				BufferSize:     1000,
				LastEventID:    fmt.Sprintf("%d-0", date.UnixMilli()),
				AckGracePeriod: 20 * time.Second,
			},
		},
		{
			name: "no ack",
			opts: []Sink{WithSinkNoAck()},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
				BufferSize:     1000,
				LastEventID:    "$",
				AckGracePeriod: 20 * time.Second,
				NoAck:          true,
			},
		},
		{
			name: "ack grace period",
			opts: []Sink{WithSinkAckGracePeriod(10 * time.Second)},
			want: SinkOptions{
				BlockDuration:  5 * time.Second,
				MaxPolled:      1000,
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
		opts []AddStream
		want AddStreamOptions
	}{
		{
			name: "default",
			opts: []AddStream{},
			want: AddStreamOptions{
				LastEventID: "",
			},
		},
		{
			name: "last event ID",
			opts: []AddStream{WithAddStreamStartAfter("foo")},
			want: AddStreamOptions{
				LastEventID: "foo",
			},
		},
		{
			name: "start at",
			opts: []AddStream{WithAddStreamStartAt(date)},
			want: AddStreamOptions{
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
