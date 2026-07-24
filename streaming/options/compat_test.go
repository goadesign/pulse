// Package options_test locks the v1 source compatibility of the exported
// option structs: external code constructing them with unkeyed (positional)
// literals must keep compiling. Adding, removing, reordering, or unexporting
// a field breaks the literals below at compile time.
package options_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
)

// TestOptionStructsKeepV1PositionalShape compiles v1 unkeyed literals of
// every exported option struct. The assertions only exist to consume the
// literals; the test is about compilation.
func TestOptionStructsKeepV1PositionalShape(t *testing.T) {
	reader := options.ReaderOptions{time.Second, 100, "topic", "pattern", 10, "0"}
	sink := options.SinkOptions{time.Second, 100, "topic", "pattern", 10, "0", true, time.Minute}
	addStream := options.AddStreamOptions{"42-0"}
	stream := options.StreamOptions{1000, pulse.NoopLogger(), time.Minute, true}
	addEvent := options.AddEventOptions{"topic", true}

	assert.Equal(t, time.Second, reader.BlockDuration)
	assert.Equal(t, time.Minute, sink.AckGracePeriod)
	assert.Equal(t, "42-0", addStream.LastEventID)
	assert.Equal(t, 1000, stream.MaxLen)
	assert.True(t, addEvent.OnlyIfStreamExists)
}
