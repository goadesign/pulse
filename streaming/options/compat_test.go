// Package options_test locks the source compatibility of the exported option
// structs: every v1 field keeps its name, type, and position (v1 fields form a
// stable prefix, in v1 order), and new fields are only ever appended. Keyed
// construction and field access from v1 code must keep compiling; unkeyed
// literals were only guaranteed within the v1 patch line and are not part of
// the contract for feature releases.
package options_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestV1FieldsFormStableOrderedPrefix pins the v1 fields to the leading
// positions of every exported option struct, in v1 order. Keyed literals
// cannot detect reordering, so this is the check that enforces the stable
// prefix the package documents.
func TestV1FieldsFormStableOrderedPrefix(t *testing.T) {
	assertFieldPrefix(t, options.ReaderOptions{},
		"BlockDuration", "MaxPolled", "Topic", "TopicPattern", "BufferSize", "LastEventID")
	assertFieldPrefix(t, options.SinkOptions{},
		"BlockDuration", "MaxPolled", "Topic", "TopicPattern", "BufferSize", "LastEventID",
		"NoAck", "AckGracePeriod")
	assertFieldPrefix(t, options.AddStreamOptions{}, "LastEventID")
	assertFieldPrefix(t, options.StreamOptions{}, "MaxLen", "Logger", "TTL", "TTLSliding")
	assertFieldPrefix(t, options.AddEventOptions{}, "Topic", "OnlyIfStreamExists")
}

// assertFieldPrefix asserts that the struct's leading fields carry exactly
// the given names in order.
func assertFieldPrefix(t *testing.T, v any, names ...string) {
	t.Helper()
	typ := reflect.TypeOf(v)
	require.GreaterOrEqual(t, typ.NumField(), len(names), typ.Name())
	for i, name := range names {
		assert.Equal(t, name, typ.Field(i).Name, "%s field %d", typ.Name(), i)
	}
}
