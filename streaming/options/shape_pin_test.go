// Positional pins of the full current shape of every exported option struct.
// Any field insertion, reorder, or retype breaks these literals at compile
// time, forcing a deliberate decision; appending a field only updates the
// literal it extends. This complements compat_test.go, which pins the
// external keyed-construction contract for v1 fields.
package options

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"goa.design/pulse/pulse"
)

// TestOptionStructsPositionalShape compiles unkeyed literals of the current
// full shapes. The assertions only consume the literals.
func TestOptionStructsPositionalShape(t *testing.T) {
	reader := ReaderOptions{time.Second, 100, "topic", "pattern", 10, "0", 0}
	sink := SinkOptions{time.Second, 100, "topic", "pattern", 10, "0", true, time.Minute, 0}
	addStream := AddStreamOptions{"42-0", 0}
	stream := StreamOptions{1000, pulse.NoopLogger(), time.Minute, true, true, false, true, time.Time{}, false}
	addEvent := AddEventOptions{"topic", true}

	assert.Equal(t, time.Second, reader.BlockDuration)
	assert.Equal(t, time.Minute, sink.AckGracePeriod)
	assert.Equal(t, "42-0", addStream.LastEventID)
	assert.Equal(t, 1000, stream.MaxLen)
	assert.True(t, addEvent.OnlyIfStreamExists)
}
