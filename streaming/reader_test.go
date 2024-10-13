package streaming

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

func TestNewReader(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	defer cleanupReader(t, ctx, s, reader)
}

func TestReaderReadOnce(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, ctx, s, reader)

	c := reader.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, c)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
}

func TestReaderReadSinceLastEvent(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)

	// Add and read 2 events consecutively
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, ctx, s, reader)
	c := reader.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, c)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	eventID := read.ID
	_, err = s.Add(ctx, "event", []byte("payload2"))
	require.NoError(t, err)
	read = readOneReaderEvent(t, c)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new reader with last event ID set to first event and read last event
	reader2, err := s.NewReader(ctx, options.WithReaderStartAfter(eventID), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, ctx, s, reader2)
	c2 := reader2.Subscribe()
	read = readOneReaderEvent(t, c2)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new reader with last event ID set to 0 and read the 2 events
	reader3, err := s.NewReader(ctx, options.WithReaderStartAfter("0"), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, ctx, s, reader3)
	c3 := reader3.Subscribe()
	read = readOneReaderEvent(t, c3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneReaderEvent(t, c3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestCleanupReader(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)

	// Write and read 1 event
	c := reader.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, c)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Stop reader, destroy stream and check Redis keys are gone
	reader.Close()
	assert.Eventually(t, func() bool { return reader.IsClosed() }, max, delay)
	assert.Equal(t, rdb.Exists(ctx, s.key).Val(), int64(1))
	assert.NoError(t, s.Destroy(ctx))
	assert.Eventually(t, func() bool { return rdb.Exists(ctx, s.key).Val() == 0 }, max, delay)
}

func TestAddReaderStream(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream("testAddStream", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	s2, err := NewStream("testAddStream2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	assert.NoError(t, reader.AddStream(ctx, s2))
	assert.NoError(t, reader.AddStream(ctx, s2)) // Make sure it's idempotent
	defer s2.Destroy(ctx)
	defer cleanupReader(t, ctx, s, reader)

	// Add events to both streams
	c := reader.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	_, err = s2.Add(ctx, "event", []byte("payload2"))
	require.NoError(t, err)

	// Read events from reader
	read := readOneReaderEvent(t, c)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneReaderEvent(t, c)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestRemoveReaderStream(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream("testRemoveStream", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	s2, err := NewStream("testRemoveStream2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	assert.NoError(t, reader.AddStream(ctx, s2))
	defer s2.Destroy(ctx)
	defer cleanupReader(t, ctx, s, reader)

	// Read events from both streams
	c := reader.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	_, err = s2.Add(ctx, "event2", []byte("payload2"))
	require.NoError(t, err)
	read := readOneReaderEvent(t, c)
	read2 := readOneReaderEvent(t, c)
	names := []string{read.EventName, read2.EventName}
	assert.ElementsMatch(t, names, []string{"event", "event2"})
	payloads := []string{string(read.Payload), string(read2.Payload)}
	assert.ElementsMatch(t, payloads, []string{"payload", "payload2"})

	// Remove one stream and read again
	assert.NoError(t, reader.RemoveStream(ctx, s2))
	_, err = s.Add(ctx, "event3", []byte("payload3"))
	assert.NoError(t, err)
	read = readOneReaderEvent(t, c)
	assert.Equal(t, "event3", read.EventName)
	assert.Equal(t, []byte("payload3"), read.Payload)
}
