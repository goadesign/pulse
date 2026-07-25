package streaming

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redis/go-redis/v9"
	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

func TestReaderRejectsInvalidOptions(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	cases := []struct {
		name string
		opt  options.Reader
	}{
		{name: "max polled", opt: options.WithReaderMaxPolled(0)},
		{name: "buffer", opt: options.WithReaderBufferSize(-1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := NewStream(t.Name(), rdb)
			require.NoError(t, err)
			_, err = stream.NewReader(ctx, tc.opt)
			require.Error(t, err)
		})
	}
	stream, err := NewStream(t.Name()+"-conflicts", rdb)
	require.NoError(t, err)
	_, err = stream.NewReader(
		ctx,
		options.WithReaderTopic("alarms"),
		options.WithReaderTopicPattern("alarm.*"),
	)
	require.ErrorContains(t, err, "mutually exclusive")
	_, err = stream.NewReader(
		ctx,
		options.WithReaderStartAtNewest(),
		options.WithReaderStartAtOldest(),
	)
	require.ErrorContains(t, err, "reader cursor-start options are mutually exclusive")
	require.ErrorIs(t, stream.Destroy(ctx), ErrStreamNotFound)
}

func TestReaderDropsMalformedRedisEventWithoutPanicking(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	reader, err := stream.NewReader(
		ctx,
		options.WithReaderStartAtOldest(),
		options.WithReaderBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	events := reader.Subscribe()
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream.key,
		Values: map[string]any{payloadKey: "missing name"},
	}).Err())
	_, err = stream.Add(ctx, "valid", []byte("payload"))
	require.NoError(t, err)

	select {
	case event := <-events:
		require.Equal(t, "valid", event.EventName)
		require.Equal(t, stream.Generation(), event.StreamGeneration)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for valid event after malformed entry")
	}
	reader.Close()
	require.NoError(t, stream.Destroy(ctx))
}

func TestReaderClosingFencesSubscriptionsAndStreamChanges(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name()+"-main", rdb)
	require.NoError(t, err)
	added, err := NewStream(t.Name()+"-added", rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	require.NoError(t, added.Open(ctx))
	reader, err := stream.NewReader(ctx, options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	reader.Subscribe()

	start := make(chan struct{})
	subscriptions := make(chan (<-chan *Event), 32)
	results := make(chan error, 64)
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			subscriptions <- reader.Subscribe()
		}()
		go func() {
			defer wait.Done()
			<-start
			results <- reader.AddStream(ctx, added)
			results <- reader.RemoveStream(ctx, added)
		}()
	}
	close(start)
	reader.Close()
	wait.Wait()
	close(subscriptions)
	close(results)

	for result := range results {
		if result != nil {
			require.ErrorIs(t, result, ErrReaderClosed)
		}
	}
	for subscription := range subscriptions {
		_, ok := <-subscription
		require.False(t, ok)
	}
	closed := reader.Subscribe()
	_, ok := <-closed
	require.False(t, ok)
	require.ErrorIs(t, reader.AddStream(ctx, added), ErrReaderClosed)
	require.ErrorIs(t, reader.RemoveStream(ctx, added), ErrReaderClosed)
	require.NoError(t, stream.Destroy(ctx))
	require.NoError(t, added.Destroy(ctx))
}

func TestNewReader(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	require.NoError(t, s.Open(ctx))
	defer func() { require.NoError(t, s.Destroy(ctx)) }()
	reader, err := s.NewReader(ctx, options.WithReaderBlockDuration(testBlockDuration))
	assert.NoError(t, err)
	if assert.NotNil(t, reader) {
		defer cleanupReader(t, reader)
	}

	_, err = s.NewReader(ctx, options.WithReaderTopicPattern("("))
	assert.EqualError(t, err, "topic pattern must be a valid regex: error parsing regexp: missing closing ): `(`")
}

func TestReaderReadOnce(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	require.NoError(t, s.Open(ctx))
	defer func() { require.NoError(t, s.Destroy(ctx)) }()
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, reader)

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
	require.NoError(t, s.Open(ctx))
	defer func() { require.NoError(t, s.Destroy(ctx)) }()

	// Add and read 2 events consecutively
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, reader)
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
	defer cleanupReader(t, reader2)
	c2 := reader2.Subscribe()
	read = readOneReaderEvent(t, c2)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new reader with last event ID set to 0 and read the 2 events
	reader3, err := s.NewReader(ctx, options.WithReaderStartAfter("0"), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupReader(t, reader3)
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
	require.NoError(t, s.Open(ctx))
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

func TestReaderCloseOnFatalReadError(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	require.NoError(t, s.Open(ctx))
	defer func() { require.NoError(t, s.Destroy(ctx)) }()
	// Simulate exact-generation destruction before the read goroutine starts.
	// The read loop reacts by closing the reader; because Close waits on the
	// read goroutine, it must run asynchronously or it would deadlock and leak
	// the reader and its Redis connection.
	defer func(orig func(*Reader, context.Context, []string) ([]redis.XStream, error)) { xreadFn = orig }(xreadFn)
	xreadFn = func(*Reader, context.Context, []string) ([]redis.XStream, error) {
		return nil, ErrStreamDestroyed
	}

	reader, err := s.NewReader(ctx, options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	reader.Subscribe()

	require.Eventually(t, func() bool { return reader.IsClosed() }, max, delay,
		"reader did not close after a fatal read error (Close likely deadlocked on its own read goroutine)")
}

func TestReaderLifetimeDoesNotUseConstructorContext(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	setupCtx, cancel := context.WithCancel(ptesting.NewTestContext(t))
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(setupCtx))
	reader, err := stream.NewReader(
		setupCtx,
		options.WithReaderStartAtOldest(),
		options.WithReaderBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	events := reader.Subscribe()
	cancel()

	eventID, err := stream.Add(context.Background(), "event", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, eventID, receiveSinkEvent(t, events).ID)
	reader.Close()
	require.NoError(t, stream.Destroy(context.Background()))
}

func TestReaderRejectsRemovingFinalStream(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	reader, err := stream.NewReader(ctx, options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)

	unattached, err := NewStream(t.Name()+"-unattached", rdb)
	require.NoError(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(t, reader.RemoveStream(cancelled, unattached))
	require.Empty(t, unattached.Generation())
	require.ErrorIs(t, reader.RemoveStream(ctx, stream), ErrLastStream)
	reader.Close()
	require.NoError(t, stream.Destroy(ctx))
}

func TestAddReaderStream(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream("testAddStream", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	require.NoError(t, s.Open(ctx))
	defer func() { assert.NoError(t, s.Destroy(ctx)) }()
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	s2, err := NewStream("testAddStream2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	require.NoError(t, s2.Open(ctx))
	assert.NoError(t, reader.AddStream(ctx, s2))
	assert.NoError(t, reader.AddStream(ctx, s2)) // Make sure it's idempotent
	defer func() { assert.NoError(t, s2.Destroy(ctx)) }()
	defer cleanupReader(t, reader)

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
	require.NoError(t, s.Open(ctx))
	defer func() { assert.NoError(t, s.Destroy(ctx)) }()
	s2, err := NewStream("testRemoveStream2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	require.NoError(t, s2.Open(ctx))
	reader, err := s.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	assert.NoError(t, reader.AddStream(ctx, s2))
	defer func() { assert.NoError(t, s2.Destroy(ctx)) }()
	defer cleanupReader(t, reader)

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

func TestReaderCloseWithStalledSubscriber(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	require.NoError(t, s.Open(ctx))

	// Tiny buffer so the read loop's fan-out send blocks after a couple of
	// events when the subscriber never drains its channel.
	reader, err := s.NewReader(ctx,
		options.WithReaderStartAtOldest(),
		options.WithReaderBlockDuration(testBlockDuration),
		options.WithReaderBufferSize(1))
	require.NoError(t, err)

	// Subscribe but deliberately never read from the channel so the read
	// loop fills the buffer and then parks on the next fan-out send.
	c := reader.Subscribe()

	// Add more events than the buffer can hold so the read loop parks on
	// `c <- ev` inside streamEvents.
	for range 5 {
		_, err = s.Add(ctx, "event", []byte("payload"))
		require.NoError(t, err)
	}

	// Wait until the buffer is full, which means the read loop has consumed
	// events and is now parked on the fan-out send to the stalled subscriber.
	require.Eventually(t, func() bool { return len(c) == cap(c) }, max, delay)

	// Close must return even though the subscriber stalled the read loop.
	done := make(chan struct{})
	go func() {
		reader.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reader.Close() hung with a stalled subscriber")
	}
	assert.True(t, reader.IsClosed())

	require.NoError(t, s.Destroy(ctx))
}

func TestReaderRejectsSubMillisecondBlockDuration(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	for _, duration := range []time.Duration{0, -time.Second, 500 * time.Microsecond} {
		reader, err := stream.NewReader(ctx, options.WithReaderBlockDuration(duration))
		require.Nil(t, reader)
		require.EqualError(t, err, "reader block duration must be at least 1ms")
	}
	require.ErrorIs(t, stream.Destroy(ctx), ErrStreamNotFound)
}

func TestEventCreatedAt(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)

	// Use Redis to create a new event ID
	eventID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "test-stream",
		Values: map[string]interface{}{"key": "value"},
	}).Result()
	require.NoError(t, err)

	event := &Event{ID: eventID}

	// Call CreatedAt() method
	createdAt := event.CreatedAt()

	// Parse the timestamp from the event ID
	parts := strings.Split(eventID, "-")
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	require.NoError(t, err)
	expectedTime := time.UnixMilli(ts).UTC()

	// Assert that the returned time is exactly the expected time
	assert.Equal(t, expectedTime, createdAt)

	// Assert that the returned time is in UTC
	assert.Equal(t, time.UTC, createdAt.Location())
}
