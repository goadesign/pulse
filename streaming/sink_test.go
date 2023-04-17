package streaming

import (
	"context"
	"sync"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"
	"goa.design/ponos/ponos"
)

var (
	wf  = time.Second
	tck = time.Millisecond
)

func TestNewSink(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testNewSink", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink")
	assert.NoError(t, err)
	assert.NotNil(t, sink)
	cleanup(t, s, sink)
}

func TestReadOnce(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testReadOnce", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink")
	require.NoError(t, err)
	defer cleanup(t, s, sink)

	assert.NoError(t, s.Add(ctx, "event", []byte("payload")))
	read := readOneEvent(t, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
}

func TestReadSinceLastEvent(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testReadSinceLastEvent", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)

	// Add and read 2 events consecutively
	sink, err := s.NewSink(ctx, "sink")
	require.NoError(t, err)
	defer cleanup(t, s, sink)
	assert.NoError(t, s.Add(ctx, "event", []byte("payload")))
	read := readOneEvent(t, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	eventID := read.ID
	assert.NoError(t, s.Add(ctx, "event", []byte("payload2")))
	read = readOneEvent(t, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new sink with last event ID set to first event and read last event
	sink2, err := s.NewSink(ctx, "sink2", WithSinkLastEventID(eventID))
	require.NoError(t, err)
	defer sink2.Stop()
	read = readOneEvent(t, sink2)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new sink with last event ID set to 0 and read the 2 events
	sink3, err := s.NewSink(ctx, "sink3", WithSinkLastEventID("0"))
	require.NoError(t, err)
	defer sink3.Stop()
	read = readOneEvent(t, sink3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneEvent(t, sink3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestCleanup(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testCleanup", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink")
	require.NoError(t, err)

	// Write and read 1 event
	assert.NoError(t, s.Add(ctx, "event", []byte("payload")))
	read := readOneEvent(t, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Stop sink, destroy stream and check Redis keys are gone
	sink.Stop()
	assert.Eventually(t, func() bool { return sink.Stopped() }, wf, tck)
	assert.Equal(t, rdb.Exists(ctx, s.key).Val(), int64(1))
	assert.NoError(t, s.Destroy(ctx))
	assert.Eventually(t, func() bool { return rdb.Exists(ctx, s.key).Val() == 0 }, wf, tck)
}

func TestAddStream(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testAddStream", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink")
	require.NoError(t, err)
	s2, err := NewStream(ctx, "testAddStream2", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	err = sink.AddStream(ctx, s2)
	assert.NoError(t, err)
	err = sink.AddStream(ctx, s2) // Make sure it's idempotent
	assert.NoError(t, err)
	defer s2.Destroy(ctx)
	defer cleanup(t, s, sink)

	// Add events to both streams
	assert.NoError(t, s.Add(ctx, "event", []byte("payload")))
	assert.NoError(t, s2.Add(ctx, "event", []byte("payload2")))

	// Read events from sink
	read := readOneEvent(t, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneEvent(t, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestRemoveStream(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testRemoveStream", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink")
	require.NoError(t, err)
	s2, err := NewStream(ctx, "testRemoveStream2", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	err = sink.AddStream(ctx, s2)
	assert.NoError(t, err)
	defer s2.Destroy(ctx)
	defer cleanup(t, s, sink)

	// Read events from both streams
	assert.NoError(t, s.Add(ctx, "event", []byte("payload")))
	assert.NoError(t, s2.Add(ctx, "event2", []byte("payload2")))
	read := readOneEvent(t, sink)
	read2 := readOneEvent(t, sink)
	names := []string{read.EventName, read2.EventName}
	assert.ElementsMatch(t, names, []string{"event", "event2"})
	payloads := []string{string(read.Payload), string(read2.Payload)}
	assert.ElementsMatch(t, payloads, []string{"payload", "payload2"})

	// Remove one stream and read again
	err = sink.RemoveStream(ctx, s2)
	assert.NoError(t, err)
	assert.NoError(t, s.Add(ctx, "event3", []byte("payload3")))
	read = readOneEvent(t, sink)
	assert.Equal(t, "event3", read.EventName)
	assert.Equal(t, []byte("payload3"), read.Payload)

	// Add back and remove other stream
	err = sink.AddStream(ctx, s2)
	assert.NoError(t, err)
	err = sink.RemoveStream(ctx, s)
	assert.NoError(t, err)
	assert.NoError(t, s2.Add(ctx, "event4", []byte("payload4")))
	read = readOneEvent(t, sink)
	assert.Equal(t, "event4", read.EventName)
	assert.Equal(t, []byte("payload4"), read.Payload)
}

func readOneEvent(t *testing.T, sink *Sink) *AckedEvent {
	t.Helper()
	var read *AckedEvent
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		defer w.Done()
		tck := time.NewTicker(time.Second)
		select {
		case read = <-sink.C:
			read.Ack(ctx)
		case <-tck.C:
			t.Error("timeout waiting for event")
		}
	}()
	w.Wait()
	require.NotNil(t, read)
	read.Ack(context.Background())
	return read
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx = log.Context(context.Background())
	log.FlushAndDisableBuffering(ctx)
	return ctx
}

func cleanup(t *testing.T, s *Stream, sink *Sink) {
	t.Helper()
	sink.Stop()
	assert.Eventually(t, func() bool { return sink.Stopped() }, wf, tck)
	assert.NoError(t, s.Destroy(ctx))
}
