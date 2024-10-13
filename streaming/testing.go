package streaming

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	max   = time.Second
	delay = 10 * time.Millisecond
)

// readOneEvent reads one event from the channel and acks it or fails the test
// if it takes more than a second.
func readOneEvent(t *testing.T, ctx context.Context, c <-chan *Event, sink *Sink) *Event {
	t.Helper()
	var read *Event
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		defer w.Done()
		tck := time.NewTicker(time.Second)
		select {
		case read = <-c:
			sink.Ack(ctx, read)
		case <-tck.C:
			t.Error("timeout waiting for event")
		}
	}()
	w.Wait()
	require.NotNil(t, read)
	return read
}

// readOneReaderEvent reads one event from the channel or fails the test if it
// takes more than a second.
func readOneReaderEvent(t *testing.T, c <-chan *Event) *Event {
	t.Helper()
	var read *Event
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		defer w.Done()
		tck := time.NewTicker(time.Second)
		select {
		case read = <-c:
			return
		case <-tck.C:
			t.Error("timeout waiting for event")
			return
		}
	}()
	w.Wait()
	require.NotNil(t, read)
	return read
}

// cleanupSink closes the sink and asserts that it is closed within a second.
func cleanupSink(t *testing.T, ctx context.Context, s *Stream, sink *Sink) {
	t.Helper()
	if sink != nil {
		sink.Close()
		assert.Eventually(t, func() bool { return sink.IsClosed() }, max, delay)
	}
	if s != nil {
		assert.NoError(t, s.Destroy(ctx))
	}
}

// cleanupReader closes the reader and asserts that it is closed within a second.
func cleanupReader(t *testing.T, ctx context.Context, s *Stream, reader *Reader) {
	t.Helper()
	reader.Close()
	assert.Eventually(t, func() bool { return reader.IsClosed() }, max, delay)
	assert.NoError(t, s.Destroy(ctx))
}
