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
	wf                = time.Second
	tck               = time.Millisecond
	testStalePeriod   = 10 * time.Millisecond
	testBlockDuration = 50 * time.Millisecond
	testAckDuration   = 20 * time.Millisecond
)

func TestNewSink(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testNewSink", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink")
	assert.NoError(t, err)
	assert.NotNil(t, sink)
	cleanup(t, ctx, s, sink)
}

func TestReadOnce(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testReadOnce", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanup(t, ctx, s, sink)

	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
}

func TestReadSinceLastEvent(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testReadSinceLastEvent", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)

	// Add and read 2 events consecutively
	sink, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	defer cleanup(t, ctx, s, sink)
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	eventID := read.ID
	_, err = s.Add(ctx, "event", []byte("payload2"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new sink with last event ID set to first event and read last event
	sink2, err := s.NewSink(ctx, "sink2",
		WithSinkStartAfter(eventID),
		WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer sink2.Close()
	read = readOneEvent(t, ctx, sink2)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new sink with last event ID set to 0 and read the 2 events
	sink3, err := s.NewSink(ctx, "sink3",
		WithSinkStartAfter("0"),
		WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer sink3.Close()
	read = readOneEvent(t, ctx, sink3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneEvent(t, ctx, sink3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestCleanup(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testCleanup", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)

	// Write and read 1 event
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Stop sink, destroy stream and check Redis keys are gone
	sink.Close()
	assert.Eventually(t, func() bool { return sink.IsClosed() }, wf, tck)
	assert.Equal(t, rdb.Exists(ctx, s.key).Val(), int64(1))
	assert.NoError(t, s.Destroy(ctx))
	assert.Eventually(t, func() bool { return rdb.Exists(ctx, s.key).Val() == 0 }, wf, tck)
}

func TestAddStream(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testAddStream", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	s2, err := NewStream(ctx, "testAddStream2", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	assert.NoError(t, sink.AddStream(ctx, s2))
	assert.NoError(t, sink.AddStream(ctx, s2)) // Make sure it's idempotent
	defer s2.Destroy(ctx)
	defer cleanup(t, ctx, s, sink)

	// Add events to both streams
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "event", []byte("payload2"))
	assert.NoError(t, err)

	// Read events from sink
	read := readOneEvent(t, ctx, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneEvent(t, ctx, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestRemoveStream(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testRemoveStream", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	s2, err := NewStream(ctx, "testRemoveStream2", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	err = sink.AddStream(ctx, s2)
	assert.NoError(t, err)
	defer s2.Destroy(ctx)
	defer cleanup(t, ctx, s, sink)

	// Read events from both streams
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "event2", []byte("payload2"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, sink)
	read2 := readOneEvent(t, ctx, sink)
	names := []string{read.EventName, read2.EventName}
	assert.ElementsMatch(t, names, []string{"event", "event2"})
	payloads := []string{string(read.Payload), string(read2.Payload)}
	assert.ElementsMatch(t, payloads, []string{"payload", "payload2"})

	// Remove one stream and read again
	err = sink.RemoveStream(ctx, s2)
	assert.NoError(t, err)
	eventID, err := s.Add(ctx, "event3", []byte("payload3"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, sink)
	assert.Equal(t, "event3", read.EventName)
	assert.Equal(t, []byte("payload3"), read.Payload)

	// Add back and remove other stream
	err = sink.AddStream(ctx, s2, WithAddStreamStartAfter(eventID))
	assert.NoError(t, err)
	err = sink.RemoveStream(ctx, s)
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "event4", []byte("payload4"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, sink)
	assert.Equal(t, "event4", read.EventName)
	assert.Equal(t, []byte("payload4"), read.Payload)
}

func TestMultipleConsumers(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testClaimStaleMessages", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration),
		withSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer cleanup(t, ctx, s, sink)

	// Create other sink
	sink2, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration),
		withSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer func() {
		sink2.Close()
		assert.Eventually(t, func() bool { return sink2.IsClosed() }, wf, tck)
	}()

	// Add event
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)

	// Read and ack event
	var read *Event
	select {
	case read = <-sink.C:
		sink.Ack(ctx, read)
	case read = <-sink2.C:
		sink2.Ack(ctx, read)
	case <-time.After(testAckDuration):
		t.Fatal("timeout waiting for initial event")
	}
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Make sure event is delivered only once
	select {
	case <-sink.C:
		t.Error("event delivered twice")
	case <-sink2.C:
		t.Error("event delivered twice")
	case <-time.After(2 * testAckDuration):
	}
}
func TestClaimStaleMessages(t *testing.T) {
	var origStalePeriod time.Duration
	origStalePeriod, checkStalePeriod = checkStalePeriod, testStalePeriod
	defer func() { checkStalePeriod = origStalePeriod }()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream(ctx, "testClaimStaleMessages", rdb, WithStreamLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		WithSinkStartAtOldest(),
		WithSinkBlockDuration(testBlockDuration),
		withSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer cleanup(t, ctx, s, sink)

	// Add event
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)

	// Read event but don't ack, could be read from any sink
	var read *Event
	select {
	case read = <-sink.C:
	case <-time.After(testAckDuration):
		t.Fatal("timeout waiting for initial event")
	}
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Read stale claimed event and ack
	select {
	case read = <-sink.C:
		sink.Ack(ctx, read)
	case <-time.After(testAckDuration * 2):
		t.Fatal("timeout waiting for claimed event")
	}
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Make sure event is delivered only once
	select {
	case <-sink.C:
		t.Error("event delivered twice")
	case <-time.After(2 * testAckDuration):
	}
}

// Make it possible to set grace periods lower than 1s
func withSinkAckGracePeriod(d time.Duration) SinkOption {
	return func(o *sinkOptions) {
		o.AckGracePeriod = d
	}
}

func readOneEvent(t *testing.T, ctx context.Context, sink *Sink) *Event {
	t.Helper()
	var read *Event
	var w sync.WaitGroup
	w.Add(1)
	go func() {
		defer w.Done()
		tck := time.NewTicker(time.Second)
		select {
		case read = <-sink.C:
			sink.Ack(ctx, read)
		case <-tck.C:
			t.Error("timeout waiting for event")
		}
	}()
	w.Wait()
	require.NotNil(t, read)
	return read
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	return log.Context(context.Background(), log.WithDebug())
}

func cleanup(t *testing.T, ctx context.Context, s *Stream, sink *Sink) {
	t.Helper()
	if sink != nil {
		sink.Close()
		assert.Eventually(t, func() bool { return sink.IsClosed() }, wf, tck)
	}
	if s != nil {
		assert.NoError(t, s.Destroy(ctx))
	}
}
