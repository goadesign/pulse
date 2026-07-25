// Package options_test locks the source compatibility of the exported option
// structs: every v1 field keeps its name, type, and position (v1 fields form a
// stable prefix, in v1 order), and new fields are only ever appended. Keyed
// construction and field access from v1 code must keep compiling; unkeyed
// literals were only guaranteed within the v1 patch line and are not part of
// the contract for feature releases.
package options_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
)

// TestOptionStructsKeepV1Fields compiles keyed literals of every v1 field of
// every exported option struct. The assertions only exist to consume the
// literals; the test is about compilation.
func TestOptionStructsKeepV1Fields(t *testing.T) {
	reader := options.ReaderOptions{
		BlockDuration: time.Second,
		MaxPolled:     100,
		Topic:         "topic",
		TopicPattern:  "pattern",
		BufferSize:    10,
		LastEventID:   "0",
	}
	sink := options.SinkOptions{
		BlockDuration:  time.Second,
		MaxPolled:      100,
		Topic:          "topic",
		TopicPattern:   "pattern",
		BufferSize:     10,
		LastEventID:    "0",
		NoAck:          true,
		AckGracePeriod: time.Minute,
	}
	addStream := options.AddStreamOptions{LastEventID: "42-0"}
	stream := options.StreamOptions{
		MaxLen:     1000,
		Logger:     pulse.NoopLogger(),
		TTL:        time.Minute,
		TTLSliding: true,
	}
	addEvent := options.AddEventOptions{Topic: "topic", OnlyIfStreamExists: true}

	assert.Equal(t, time.Second, reader.BlockDuration)
	assert.Equal(t, time.Minute, sink.AckGracePeriod)
	assert.Equal(t, "42-0", addStream.LastEventID)
	assert.Equal(t, 1000, stream.MaxLen)
	assert.True(t, addEvent.OnlyIfStreamExists)
}
