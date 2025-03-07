package streaming

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

var (
	testCheckIdlePeriod = 50 * time.Millisecond
	testBlockDuration   = 50 * time.Millisecond
	testAckDuration     = 50 * time.Millisecond
)

func TestNewSink(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	assert.NoError(t, err)
	if assert.NotNil(t, sink) {
		defer cleanupSink(t, ctx, s, sink)
	}

	_, err = s.NewSink(ctx, "sink", options.WithSinkTopicPattern("("))
	assert.EqualError(t, err, "topic pattern must be a valid regex: error parsing regexp: missing closing ): `(`")
}

func TestReadOnce(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	c := sink.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
}

func TestReadSinceLastEvent(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)

	// Add and read 2 events consecutively
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)
	c := sink.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	eventID := read.ID
	_, err = s.Add(ctx, "event", []byte("payload2"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new sink with last event ID set to first event and read last event
	sink2, err := s.NewSink(ctx, "sink2",
		options.WithSinkStartAfter(eventID),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink2)
	c2 := sink2.Subscribe()
	read = readOneEvent(t, ctx, c2, sink2)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new sink with last event ID set to 0 and read the 2 events
	sink3, err := s.NewSink(ctx, "sink3",
		options.WithSinkStartAfter("0"),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink3)
	c3 := sink3.Subscribe()
	read = readOneEvent(t, ctx, c3, sink3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneEvent(t, ctx, c3, sink3)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestCleanup(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)

	// Write and read 1 event
	c := sink.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Stop sink, destroy stream and check Redis keys are gone
	sink.Close(ctx)
	assert.Eventually(t, func() bool { return sink.IsClosed() }, max, delay)
	assert.Equal(t, rdb.Exists(ctx, s.key).Val(), int64(1))
	assert.NoError(t, s.Destroy(ctx))
	assert.Eventually(t, func() bool { return rdb.Exists(ctx, s.key).Val() == 0 }, max, delay)
}

func TestAddStream(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)

	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	s2, err := NewStream(testName+"2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	defer func() { assert.NoError(t, s2.Destroy(ctx)) }()
	defer cleanupSink(t, ctx, s, sink)

	assert.NoError(t, sink.AddStream(ctx, s2))
	assert.NoError(t, sink.AddStream(ctx, s2)) // Make sure it's idempotent

	// Add events to both streams
	c := sink.Subscribe()
	defer sink.Unsubscribe(c)
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "event", []byte("payload2"))
	assert.NoError(t, err)

	// Read events from sink
	read := readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
	read = readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)
}

func TestRemoveStream(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	s2, err := NewStream("testRemoveStream2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	err = sink.AddStream(ctx, s2)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, s2.Destroy(ctx)) }()
	defer cleanupSink(t, ctx, s, sink)

	// Read events from both streams
	c := sink.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "event2", []byte("payload2"))
	assert.NoError(t, err)
	read := readOneEvent(t, ctx, c, sink)
	read2 := readOneEvent(t, ctx, c, sink)
	names := []string{read.EventName, read2.EventName}
	assert.ElementsMatch(t, names, []string{"event", "event2"})
	payloads := []string{string(read.Payload), string(read2.Payload)}
	assert.ElementsMatch(t, payloads, []string{"payload", "payload2"})

	// Remove one stream and read again
	err = sink.RemoveStream(ctx, s2)
	assert.NoError(t, err)
	eventID, err := s.Add(ctx, "event3", []byte("payload3"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event3", read.EventName)
	assert.Equal(t, []byte("payload3"), read.Payload)

	// Add back and remove other stream
	err = sink.AddStream(ctx, s2, options.WithAddStreamStartAfter(eventID))
	assert.NoError(t, err)
	err = sink.RemoveStream(ctx, s)
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "event4", []byte("payload4"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event4", read.EventName)
	assert.Equal(t, []byte("payload4"), read.Payload)
}

func TestMultipleConsumers(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	// Create other sink
	sink2, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer func() {
		sink2.Close(ctx)
		assert.Eventually(t, func() bool { return sink2.IsClosed() }, max, delay)
	}()

	// Add event
	c := sink.Subscribe()
	c2 := sink2.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)

	// Read and ack event
	var read *Event
	select {
	case read = <-c:
		assert.NoError(t, sink.Ack(ctx, read))
	case read = <-c2:
		assert.NoError(t, sink2.Ack(ctx, read))
	case <-time.After(testAckDuration):
		t.Fatal("timeout waiting for initial event")
	}
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Make sure event is delivered only once
	select {
	case <-c:
		t.Error("event delivered twice")
	case <-c2:
		t.Error("event delivered twice")
	case <-time.After(2 * testAckDuration):
	}
}
func TestClaimStaleMessages(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	var origStalePeriod time.Duration
	origStalePeriod, checkIdlePeriod = checkIdlePeriod, testCheckIdlePeriod
	defer func() { checkIdlePeriod = origStalePeriod }()

	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	assert.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	// Add event
	c := sink.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	assert.NoError(t, err)

	// Read event but don't ack, could be read from any sink
	var read *Event
	select {
	case read = <-c:
	case <-time.After(testAckDuration):
		t.Fatal("timeout waiting for initial event")
	}
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Read stale claimed event and ack
	select {
	case read = <-c:
		assert.NoError(t, sink.Ack(ctx, read))
	case <-time.After(testAckDuration * 2):
		t.Fatal("timeout waiting for claimed event")
	}
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Make sure event is delivered only once
	select {
	case <-c:
		t.Error("event delivered twice")
	case <-time.After(2 * testAckDuration):
	}
}

func TestNonAckMessageDeliveredToAnotherConsumer(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	var origCheckIdlePeriod time.Duration
	origCheckIdlePeriod, checkIdlePeriod = checkIdlePeriod, testCheckIdlePeriod
	defer func() { checkIdlePeriod = origCheckIdlePeriod }()
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	logger := pulse.ClueLogger(ctx)

	// Create a stream
	s, err := NewStream(testName, rdb, options.WithStreamLogger(logger))
	assert.NoError(t, err)

	// Create two sinks with identical names
	sink1, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink1)

	sink2, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer sink2.Close(ctx)

	// Subscribe to both sinks
	c1 := sink1.Subscribe()
	c2 := sink2.Subscribe()

	// Add an event to the stream
	_, err = s.Add(ctx, "test_event", []byte("test_payload"))
	assert.NoError(t, err)

	// Read event but don't ack
	var read1 *Event
	var receiverSink, otherSink *Sink
	var otherChan <-chan *Event
	select {
	case read1 = <-c1:
		logger.Info("Read from sink1")
		receiverSink = sink1
		otherSink = sink2
		otherChan = c2
	case read1 = <-c2:
		logger.Info("Read from sink2")
		receiverSink = sink2
		otherSink = sink1
		otherChan = c1
	case <-time.After(testAckDuration):
		t.Fatal("Timeout waiting for event on first sink")
	}
	assert.Equal(t, "test_event", read1.EventName)
	assert.Equal(t, []byte("test_payload"), read1.Payload)

	// Close the receiver sink
	receiverSink.Close(ctx)
	assert.Eventually(t, func() bool { return receiverSink.IsClosed() }, max, delay)

	// The message should now be redelivered to the other sink
	var read2 *Event
	select {
	case read2 = <-otherChan:
		logger.Info("Read from other sink")
		assert.NoError(t, otherSink.Ack(ctx, read2))
	case <-time.After(testAckDuration * 4):
		t.Fatal("Timeout waiting for event on other sink")
	}
	assert.Equal(t, "test_event", read2.EventName)
	assert.Equal(t, []byte("test_payload"), read2.Payload)
}

func TestStaleConsumerDeletionAndMessageClaiming(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	var origCheckIdlePeriod time.Duration
	origCheckIdlePeriod, checkIdlePeriod = checkIdlePeriod, testCheckIdlePeriod
	defer func() { checkIdlePeriod = origCheckIdlePeriod }()

	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	logger := pulse.ClueLogger(ctx)

	// Create a stream
	s, err := NewStream(testName, rdb, options.WithStreamLogger(logger))
	assert.NoError(t, err)

	// Create first sink
	sink1, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)

	// Subscribe to sink1
	c1 := sink1.Subscribe()

	// Add an event to the stream
	_, err = s.Add(ctx, "test_event", []byte("test_payload"))
	assert.NoError(t, err)

	// Read event but don't ack
	var read *Event
	select {
	case read = <-c1:
	case <-time.After(testAckDuration):
		t.Fatal("Timeout waiting for event on first sink")
	}
	assert.Equal(t, "test_event", read.EventName)
	assert.Equal(t, []byte("test_payload"), read.Payload)

	// Create another sink with the same name
	sink2, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink2)

	// Verify that the two consumers are present
	assert.Eventually(t, func() bool {
		consumers, err := rdb.XInfoConsumers(ctx, s.key, "sink").Result()
		if err != nil {
			t.Logf("Error getting consumers: %v", err)
			return false
		}
		return len(consumers) == 2
	}, max, delay, "Expected two consumers")

	// Close the sink to stop keep-alive refresh
	sink1.Close(ctx)
	assert.Eventually(t, func() bool { return sink1.IsClosed() }, max, delay)

	// Verify that the stale consumer is deleted
	assert.Eventually(t, func() bool {
		consumers, err := rdb.XInfoConsumers(ctx, s.key, "sink").Result()
		if err != nil {
			t.Logf("Error getting consumers: %v", err)
			return false
		}
		return len(consumers) == 1
	}, max, delay, "Expected only one consumer to remain")

	// Subscribe to sink2
	c2 := sink2.Subscribe()

	// Verify that the message is claimed by the remaining sink
	var claimedRead *Event
	select {
	case claimedRead = <-c2:
		assert.NoError(t, sink2.Ack(ctx, claimedRead))
	case <-time.After(testAckDuration * 2):
		t.Fatal("Timeout waiting for claimed event")
	}
	assert.Equal(t, "test_event", claimedRead.EventName)
	assert.Equal(t, []byte("test_payload"), claimedRead.Payload)
}
