package streaming

import (
	"sync"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/ponos/ponos"
)

func TestNewReader(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testNewReader", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	cleanupReader(t, s, reader)
}

func TestReaderReadOnce(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testReadOnce", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, WithReaderStartAtOldest(), WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	// defer cleanupReader(t, s, reader)

	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, reader)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
}

func TestReaderReadSinceLastEvent(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testReadSinceLastEvent", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)

	// Add and read 2 events consecutively
	reader, err := s.NewReader(ctx, WithReaderStartAtOldest(), WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, s, reader)
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, reader)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	eventID := read.ID
	_, err = s.Add(ctx, "event", []byte("payload2"))
	require.NoError(t, err)
	read = readOneReaderEvent(t, reader)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new reader with last event ID set to first event and read last event
	reader2, err := s.NewReader(ctx, WithReaderStartAfter(eventID), WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer reader2.Stop()
	read = readOneReaderEvent(t, reader2)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new reader with last event ID set to 0 and read the 2 events
	reader3, err := s.NewReader(ctx, WithReaderStartAfter("0"), WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer reader3.Stop()
	read = readOneReaderEvent(t, reader3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneReaderEvent(t, reader3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestCleanupReader(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testCleanupReader", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, WithReaderStartAtOldest(), WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)

	// Write and read 1 event
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, reader)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Stop reader, destroy stream and check Redis keys are gone
	reader.Stop()
	assert.Eventually(t, func() bool { return reader.Stopped() }, wf, tck)
	assert.Equal(t, rdb.Exists(ctx, s.key).Val(), int64(1))
	assert.NoError(t, s.Destroy(ctx))
	assert.Eventually(t, func() bool { return rdb.Exists(ctx, s.key).Val() == 0 }, wf, tck)
}

func TestAddReaderStream(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testAddStream", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, WithReaderStartAtOldest(), WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	s2, err := NewStream(ctx, "testAddStream2", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	assert.NoError(t, reader.AddStream(ctx, s2))
	assert.NoError(t, reader.AddStream(ctx, s2)) // Make sure it's idempotent
	defer s2.Destroy(ctx)
	defer cleanupReader(t, s, reader)

	// Add events to both streams
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	_, err = s2.Add(ctx, "event", []byte("payload2"))
	require.NoError(t, err)

	// Read events from reader
	read := readOneReaderEvent(t, reader)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneReaderEvent(t, reader)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestRemoveReaderStream(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx = testContext(t)
	s, err := NewStream(ctx, "testRemoveStream", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	s2, err := NewStream(ctx, "testRemoveStream2", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, WithReaderStartAtOldest(), WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	assert.NoError(t, reader.AddStream(ctx, s2))
	defer s2.Destroy(ctx)
	defer cleanupReader(t, s, reader)

	// Read events from both streams
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	_, err = s2.Add(ctx, "event2", []byte("payload2"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, reader)
	read2 := readOneReaderEvent(t, reader)
	names := []string{read.EventName, read2.EventName}
	assert.ElementsMatch(t, names, []string{"event", "event2"})
	payloads := []string{string(read.Payload), string(read2.Payload)}
	assert.ElementsMatch(t, payloads, []string{"payload", "payload2"})

	// Remove one stream and read again
	assert.NoError(t, reader.RemoveStream(ctx, s2))
	_, err = s.Add(ctx, "event3", []byte("payload3"))
	assert.NoError(t, err)
	read = readOneReaderEvent(t, reader)
	assert.Equal(t, "event3", read.EventName)
	assert.Equal(t, []byte("payload3"), read.Payload)
}

func readOneReaderEvent(t *testing.T, reader *Reader) *Event {
	t.Helper()
	var read *Event
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		defer w.Done()
		tck := time.NewTicker(time.Second)
		select {
		case read = <-reader.C:
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

func cleanupReader(t *testing.T, s *Stream, reader *Reader) {
	t.Helper()
	reader.Stop()
	assert.Eventually(t, func() bool { return reader.Stopped() }, wf, tck)
	assert.NoError(t, s.Destroy(ctx))
}
