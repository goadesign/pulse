// Tests for lossless consumer group recovery, the destroy lifecycle fence,
// fenced leases, close semantics, and failure-atomic stream ownership
// changes. All tests run against a live Redis instance.
package streaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

// TestSinkRecoversGroupAfterExternalDestroy is the core lossless recovery
// scenario: publish events, ack some, force XGROUP DESTROY, and verify the
// group is recreated at the recovery cursor so every unacked event is
// redelivered exactly once and no acked event is redelivered.
func TestSinkRecoversGroupAfterExternalDestroy(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	c := sink.Subscribe()
	ids := make([]string, 5)
	for i := range ids {
		ids[i], err = s.Add(ctx, fmt.Sprintf("event%d", i), []byte("payload"))
		require.NoError(t, err)
	}
	events := make([]*Event, 5)
	for i := range events {
		events[i] = receiveEvent(t, c)
	}
	require.NoError(t, sink.Ack(ctx, events[0]))
	require.NoError(t, sink.Ack(ctx, events[1]))
	// Acks advance the recovery cursor synchronously.
	assert.Equal(t, ids[1], recoveryCursor(t, ctx, rdb, s, "sink"))

	// Simulate Redis consumer group state loss.
	require.NoError(t, rdb.XGroupDestroy(ctx, s.key, "sink").Err())
	futureID, err := s.Add(ctx, "future", []byte("payload"))
	require.NoError(t, err)

	// Every unacked event and the new event must be redelivered exactly once.
	var redelivered []string
	for range 4 {
		ev := receiveEvent(t, c)
		redelivered = append(redelivered, ev.ID)
		require.NoError(t, sink.Ack(ctx, ev))
	}
	assert.Equal(t, []string{ids[2], ids[3], ids[4], futureID}, redelivered)
	select {
	case ev := <-c:
		t.Errorf("unexpected redelivery of event %s", ev.ID)
	case <-time.After(4 * testBlockDuration):
	}
	groups, err := rdb.XInfoGroups(ctx, s.key).Result()
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "sink", groups[0].Name)
}

// TestSinkRecoveryCursorIsSharedAcrossReplicas verifies that sink replicas
// share one durable recovery cursor per stream and group so whichever replica
// recovers first resumes from the same acknowledged position.
func TestSinkRecoveryCursorIsSharedAcrossReplicas(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink1, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink1)
	sink2, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer sink2.Close(ctx)

	c1, c2 := sink1.Subscribe(), sink2.Subscribe()
	ids := make([]string, 2)
	for i := range ids {
		ids[i], err = s.Add(ctx, fmt.Sprintf("event%d", i), []byte("payload"))
		require.NoError(t, err)
	}
	for range ids {
		select {
		case ev := <-c1:
			require.NoError(t, sink1.Ack(ctx, ev))
		case ev := <-c2:
			require.NoError(t, sink2.Ack(ctx, ev))
		case <-time.After(max):
			t.Fatal("timeout waiting for event")
		}
	}
	assert.Equal(t, ids[1], recoveryCursor(t, ctx, rdb, s, "sink"))

	require.NoError(t, rdb.XGroupDestroy(ctx, s.key, "sink").Err())
	futureID, err := s.Add(ctx, "future", []byte("payload"))
	require.NoError(t, err)

	// Whichever replica recovers, only the new event is delivered.
	select {
	case ev := <-c1:
		assert.Equal(t, futureID, ev.ID)
		require.NoError(t, sink1.Ack(ctx, ev))
	case ev := <-c2:
		assert.Equal(t, futureID, ev.ID)
		require.NoError(t, sink2.Ack(ctx, ev))
	case <-time.After(max):
		t.Fatal("timeout waiting for post-recovery event")
	}
	select {
	case ev := <-c1:
		t.Errorf("unexpected redelivery of event %s", ev.ID)
	case ev := <-c2:
		t.Errorf("unexpected redelivery of event %s", ev.ID)
	case <-time.After(4 * testBlockDuration):
	}
}

// TestEnsureGroupRestoresTTLOnBusyGroup verifies that ensuring an existing
// consumer group (BUSYGROUP path) restores the stream TTL and that recovery
// metadata itself never carries a TTL.
func TestEnsureGroupRestoresTTLOnBusyGroup(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb,
		options.WithStreamLogger(pulse.ClueLogger(ctx)),
		options.WithStreamTTL(time.Hour))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, rdb.Persist(ctx, s.key).Err())
	require.Equal(t, time.Duration(-1), rdb.PTTL(ctx, s.key).Val())

	created, _, err := ensureConsumerGroup(ctx, s, "sink", "$", false)
	require.NoError(t, err)
	assert.False(t, created, "group must already exist")
	assert.Positive(t, rdb.PTTL(ctx, s.key).Val(), "stream TTL must be restored on BUSYGROUP")
	assert.Equal(t, time.Duration(-1), rdb.PTTL(ctx, cursorsKey(s.key)).Val(),
		"recovery cursors must outlive the event TTL")
}

// TestSinkRecoveryAfterEventStreamExpires verifies that when the Redis key
// backing a TTL stream expires (deleting the consumer group with it), the
// sink recreates the group from the durable recovery cursor and delivers
// events published afterwards.
func TestSinkRecoveryAfterEventStreamExpires(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb,
		options.WithStreamLogger(pulse.ClueLogger(ctx)),
		options.WithStreamTTL(500*time.Millisecond))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	c := sink.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, sink.Ack(ctx, receiveEvent(t, c)))

	// Wait for the stream key (and with it the consumer group) to expire.
	require.Eventually(t, func() bool {
		return rdb.Exists(ctx, s.key).Val() == 0
	}, 2*time.Second, delay)

	futureID, err := s.Add(ctx, "future", []byte("payload"))
	require.NoError(t, err)
	ev := receiveEvent(t, c)
	assert.Equal(t, futureID, ev.ID)
	require.NoError(t, sink.Ack(ctx, ev))
}

// TestSinkCloseCancelsBlockingRedisRead verifies that Close cancels
// sink-owned Redis I/O so a blocked XREADGROUP cannot stall shutdown.
func TestSinkCloseCancelsBlockingRedisRead(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	hook := newRedisCommandHook()
	rdb.AddHook(hook)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkBlockDuration(time.Hour)) // would block Close without cancellation
	require.NoError(t, err)

	hook.blockRead.Store(true)
	select {
	case <-hook.readStarted:
	case <-time.After(max):
		t.Fatal("read loop never blocked on XREADGROUP")
	}

	done := make(chan struct{})
	go func() {
		sink.Close(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel the blocked Redis read")
	}
	assert.True(t, sink.IsClosed())
	hook.blockRead.Store(false)
	require.NoError(t, s.Destroy(ctx))
}

// TestSinkRejectsStreamMutationAfterClose verifies AddStream and RemoveStream
// fail with ErrSinkClosed once Close was called.
func TestSinkRejectsStreamMutationAfterClose(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	sink.Close(ctx)
	require.True(t, sink.IsClosed())

	s2, err := NewStream(testName+"2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	assert.ErrorIs(t, sink.AddStream(ctx, s2), ErrSinkClosed)
	assert.ErrorIs(t, sink.RemoveStream(ctx, s), ErrSinkClosed)
	require.NoError(t, s.Destroy(ctx))
}

// TestSinkAddStreamRollsBackPartialFailure verifies that a failed AddStream
// compensates the consumer group and cursor it created so no dangling
// ownership state survives, and that a subsequent AddStream succeeds.
func TestSinkAddStreamRollsBackPartialFailure(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	s2, err := NewStream(testName+"2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	hook := newRedisCommandHook()
	rdb.AddHook(hook)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)
	defer func() { assert.NoError(t, s2.Destroy(ctx)) }()

	hook.failScript(registerConsumerScript.Hash(), s2.key)
	require.Error(t, sink.AddStream(ctx, s2))
	hook.clearFailure()

	sink.lock.Lock()
	_, owned := sink.streams[s2.key]
	sink.lock.Unlock()
	assert.False(t, owned, "failed AddStream must not leave the stream owned")
	assert.Zero(t, rdb.Exists(ctx, cursorsKey(s2.key)).Val(), "compensation must delete the cursor")
	groups, err := rdb.XInfoGroups(ctx, s2.key).Result()
	require.NoError(t, err)
	assert.Empty(t, groups, "compensation must delete the consumer group")

	// AddStream succeeds once the failure clears.
	require.NoError(t, sink.AddStream(ctx, s2))
	c := sink.Subscribe()
	_, err = s2.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, sink.Ack(ctx, receiveEvent(t, c)))
}

// TestSinkConsumerRotationRegistersEveryStreamOrRollsBack verifies that
// replacement consumer creation is failure-atomic across all sink streams:
// when registration fails for one stream the registrations already made are
// detached so ownership state never diverges.
func TestSinkConsumerRotationRegistersEveryStreamOrRollsBack(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	s2, err := NewStream(testName+"2", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	hook := newRedisCommandHook()
	rdb.AddHook(hook)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	require.NoError(t, sink.AddStream(ctx, s2))
	defer cleanupSink(t, ctx, s, sink)
	defer func() { assert.NoError(t, s2.Destroy(ctx)) }()
	original := sink.consumer

	hook.failScript(registerConsumerScript.Hash(), s2.key)
	sink.lock.Lock()
	_, err = sink.newConsumer(ctx)
	sink.lock.Unlock()
	require.Error(t, err)
	hook.clearFailure()

	for _, stream := range []*Stream{s, s2} {
		assert.Equal(t, []string{original}, memberConsumers(t, ctx, rdb, stream, "sink"),
			"membership of %s must be unchanged after rollback", stream.Name)
		assert.Equal(t, []string{original}, groupConsumers(t, ctx, rdb, stream, "sink"),
			"consumer group of %s must be unchanged after rollback", stream.Name)
	}

	// Rotation succeeds once the failure clears and registers both streams.
	sink.lock.Lock()
	replacement, err := sink.newConsumer(ctx)
	sink.lock.Unlock()
	require.NoError(t, err)
	for _, stream := range []*Stream{s, s2} {
		assert.ElementsMatch(t, []string{original, replacement}, memberConsumers(t, ctx, rdb, stream, "sink"))
	}
}

// TestDestroyFencesSinkMetadataWrites is the P1-A regression test: once
// Stream.Destroy runs, a live sink (setup paths, replacement-consumer
// creation, keepalive-driven loops, lease work) must not resurrect any
// stream-scoped metadata.
func TestDestroyFencesSinkMetadataWrites(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	var origCheckIdlePeriod time.Duration
	origCheckIdlePeriod, checkIdlePeriod = checkIdlePeriod, testCheckIdlePeriod
	defer func() { checkIdlePeriod = origCheckIdlePeriod }()

	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(testAckDuration))
	require.NoError(t, err)
	defer sink.Close(ctx)

	c := sink.Subscribe()
	_, err = s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, sink.Ack(ctx, receiveEvent(t, c)))
	require.Equal(t, int64(1), rdb.Exists(ctx, cursorsKey(s.key)).Val())

	// Destroy the stream while the sink keepalive and idle-check loops are
	// due to run.
	require.NoError(t, s.Destroy(ctx))

	// Every fenced metadata write must fail with ErrStreamDestroyed.
	assert.ErrorIs(t, registerSinkConsumer(ctx, s, "sink", "ghost"), ErrStreamDestroyed)
	_, _, err = ensureConsumerGroup(ctx, s, "sink", "$", false)
	assert.ErrorIs(t, err, ErrStreamDestroyed)
	_, _, err = acquireSinkLease(ctx, s, "sink", "ghost-owner", 1000)
	assert.ErrorIs(t, err, ErrStreamDestroyed)

	// The sink read loop observes NOGROUP, fails recovery with
	// ErrStreamDestroyed, and drops the stream instead of resurrecting it.
	assert.Eventually(t, func() bool {
		sink.lock.Lock()
		defer sink.lock.Unlock()
		return len(sink.streams) == 0
	}, max, delay, "sink must drop the destroyed stream")

	// Let the periodic keepalive and idle-check loops tick several times,
	// then verify no stream-scoped metadata was recreated.
	time.Sleep(5 * testCheckIdlePeriod)
	assert.Zero(t, rdb.Exists(ctx, s.key).Val(), "event stream must stay deleted")
	assert.Zero(t, rdb.Exists(ctx, cursorsKey(s.key)).Val(), "recovery cursor must stay deleted")
	assert.Zero(t, rdb.Exists(ctx, leaseKey(s.key, "sink")).Val(), "lease must stay deleted")
	content, err := rdb.HGetAll(ctx, membershipContentKey(s.Name)).Result()
	require.NoError(t, err)
	assert.Equal(t, "destroy", content["=kind"], "membership map must remain a destroy tombstone")
	assert.NotContains(t, content, "sink", "membership must not be resurrected")
	assert.Equal(t, "destroyed", rdb.HGet(ctx, lifecycleKey(s.key), "state").Val())
}

// TestDispatchLeavesRemovedStreamEventsPending verifies that a batch read for
// a stream removed from this sink concurrently with the read is left pending
// for the surviving group members instead of being acknowledged undelivered,
// which would permanently drop the events for the whole group.
func TestDispatchLeavesRemovedStreamEventsPending(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	removed, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer removed.Close(ctx)
	s2, err := NewStream(testName, ptesting.NewRedisClient(t), options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	survivor, err := s2.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s2, survivor)

	// Drop the stream from one sink; the group survives through the other
	// member. Wait out reads issued before the removal so the event below is
	// deterministically delivered to the survivor's consumer PEL, unacked.
	c := survivor.Subscribe()
	require.NoError(t, removed.RemoveStream(ctx, s))
	time.Sleep(3 * testBlockDuration)
	id, err := s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	ev := receiveEvent(t, c)
	require.Equal(t, id, ev.ID)

	// Replay the racing batch against the sink that no longer owns the
	// stream: dispatch must not settle the group's pending entry.
	require.NoError(t, removed.dispatch([]redis.XStream{{
		Stream:   s.key,
		Messages: []redis.XMessage{{ID: id, Values: map[string]any{nameKey: "event", payloadKey: "payload"}}},
	}}))
	pending, err := rdb.XPending(ctx, s.key, "sink").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending.Count, "unowned batch must stay pending for surviving members")
	require.NoError(t, survivor.Ack(ctx, ev))
}

// TestSinkAcknowledgesFilteredEvents verifies that events dropped by the sink
// topic filter are acknowledged so they cannot hold back the recovery cursor.
func TestSinkAcknowledgesFilteredEvents(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkTopic("keep"))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	c := sink.Subscribe()
	_, err = s.Add(ctx, "dropped", []byte("payload"), options.WithTopic("drop"))
	require.NoError(t, err)
	keepID, err := s.Add(ctx, "kept", []byte("payload"), options.WithTopic("keep"))
	require.NoError(t, err)

	ev := receiveEvent(t, c)
	assert.Equal(t, keepID, ev.ID)
	require.NoError(t, sink.Ack(ctx, ev))
	assert.Equal(t, keepID, recoveryCursor(t, ctx, rdb, s, "sink"))
	pending, err := rdb.XPending(ctx, s.key, "sink").Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count, "filtered events must be settled")
}

// TestSinkNoAckAdvancesRecoveryCursor verifies that NoAck sinks advance the
// recovery cursor on delivery so recovery never replays delivered events.
func TestSinkNoAckAdvancesRecoveryCursor(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkNoAck())
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	c := sink.Subscribe()
	id, err := s.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	ev := receiveEvent(t, c)
	assert.Equal(t, id, ev.ID)
	assert.Equal(t, id, recoveryCursor(t, ctx, rdb, s, "sink"))

	require.NoError(t, rdb.XGroupDestroy(ctx, s.key, "sink").Err())
	futureID, err := s.Add(ctx, "future", []byte("payload"))
	require.NoError(t, err)
	ev = receiveEvent(t, c)
	assert.Equal(t, futureID, ev.ID, "recovery must resume after the delivered event")
	select {
	case ev := <-c:
		t.Errorf("unexpected redelivery of event %s", ev.ID)
	case <-time.After(4 * testBlockDuration):
	}
}

// TestEventAckerAdvancesRecoveryCursor verifies the cursor arithmetic of the
// recovery acker: the cursor is always the entry preceding the oldest pending
// event, and the group last-delivered-id once the PEL drains, even when
// events are acknowledged out of order.
func TestEventAckerAdvancesRecoveryCursor(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, s, sink)

	c := sink.Subscribe()
	ids := make([]string, 3)
	for i := range ids {
		ids[i], err = s.Add(ctx, fmt.Sprintf("event%d", i), []byte("payload"))
		require.NoError(t, err)
	}
	events := make(map[string]*Event, 3)
	for range ids {
		ev := receiveEvent(t, c)
		events[ev.ID] = ev
	}

	// Ack out of order: the middle event first.
	require.NoError(t, sink.Ack(ctx, events[ids[1]]))
	assert.Equal(t, "0-0", recoveryCursor(t, ctx, rdb, s, "sink"),
		"oldest pending event is the first entry so nothing is durably settled")
	require.NoError(t, sink.Ack(ctx, events[ids[0]]))
	assert.Equal(t, ids[1], recoveryCursor(t, ctx, rdb, s, "sink"),
		"cursor must jump past the contiguous acknowledged prefix")
	require.NoError(t, sink.Ack(ctx, events[ids[2]]))
	assert.Equal(t, ids[2], recoveryCursor(t, ctx, rdb, s, "sink"),
		"cursor must reach last-delivered-id once the PEL drains")
}

// TestReadRetryJitterBounds verifies the retry backoff is jittered between
// half and full of the current backoff and doubles up to the cap.
func TestReadRetryJitterBounds(t *testing.T) {
	var r readRetry
	expected := minReadRetryBackoff
	for range 10 {
		d := r.next()
		assert.GreaterOrEqual(t, d, expected/2)
		assert.LessOrEqual(t, d, expected)
		expected = 2 * expected
		if expected > maxReadRetryBackoff {
			expected = maxReadRetryBackoff
		}
	}
	r.reset()
	d := r.next()
	assert.GreaterOrEqual(t, d, minReadRetryBackoff/2)
	assert.LessOrEqual(t, d, minReadRetryBackoff)
}

// receiveEvent reads one event from the channel without acknowledging it or
// fails the test after the standard timeout.
func receiveEvent(t *testing.T, c <-chan *Event) *Event {
	t.Helper()
	select {
	case ev := <-c:
		require.NotNil(t, ev)
		return ev
	case <-time.After(max):
		t.Fatal("timeout waiting for event")
		return nil
	}
}

// recoveryCursor returns the durable recovery cursor stored for the sink on
// the stream.
func recoveryCursor(t *testing.T, ctx context.Context, rdb *redis.Client, s *Stream, sink string) string {
	t.Helper()
	cursor, err := rdb.HGet(ctx, cursorsKey(s.key), sink).Result()
	require.NoError(t, err)
	return cursor
}

// memberConsumers returns the consumer names recorded for the sink in the
// stream membership map.
func memberConsumers(t *testing.T, ctx context.Context, rdb *redis.Client, s *Stream, sink string) []string {
	t.Helper()
	raw, err := rdb.HGet(ctx, membershipContentKey(s.Name), sink).Result()
	require.NoError(t, err)
	var names []string
	require.NoError(t, json.Unmarshal([]byte(raw), &names))
	return names
}

// groupConsumers returns the Redis consumer names of the sink group on the
// stream.
func groupConsumers(t *testing.T, ctx context.Context, rdb *redis.Client, s *Stream, sink string) []string {
	t.Helper()
	consumers, err := rdb.XInfoConsumers(ctx, s.key, sink).Result()
	require.NoError(t, err)
	names := make([]string, len(consumers))
	for i, c := range consumers {
		names[i] = c.Name
	}
	return names
}

type (
	// redisCommandHook injects command-specific failures and blocking reads
	// into the sink Redis client to prove cancellation, bounded retries, and
	// failure-atomic ownership changes.
	redisCommandHook struct {
		// blockRead blocks XREADGROUP calls until their context is canceled.
		blockRead atomic.Bool
		// readStarted is closed the first time a read blocks.
		readStarted chan struct{}
		// started guards readStarted.
		started sync.Once
		// mu guards the failure configuration below.
		mu sync.Mutex
		// failHash is the script hash whose EVALSHA calls fail.
		failHash string
		// failKey restricts injected failures to invocations naming this key.
		failKey string
	}
)

// errInjected is the transport failure injected by redisCommandHook.
var errInjected = errors.New("injected redis failure")

// newRedisCommandHook returns a hook with no active failure.
func newRedisCommandHook() *redisCommandHook {
	return &redisCommandHook{readStarted: make(chan struct{})}
}

// failScript makes EVALSHA calls of the script with the given hash fail when
// their arguments include key.
func (h *redisCommandHook) failScript(hash, key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failHash, h.failKey = hash, key
}

// clearFailure removes the active script failure.
func (h *redisCommandHook) clearFailure() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failHash, h.failKey = "", ""
}

// DialHook preserves the client's normal Redis connection behavior.
func (h *redisCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook blocks reads and injects script failures per the hook
// configuration.
func (h *redisCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "xreadgroup":
			if h.blockRead.Load() {
				h.started.Do(func() { close(h.readStarted) })
				<-ctx.Done()
				return ctx.Err()
			}
		case "evalsha":
			h.mu.Lock()
			hash, key := h.failHash, h.failKey
			h.mu.Unlock()
			if hash != "" && len(cmd.Args()) > 1 && cmd.Args()[1] == hash {
				for _, arg := range cmd.Args() {
					if s, ok := arg.(string); ok && s == key {
						return errInjected
					}
				}
			}
		}
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook preserves pipeline behavior; the sink does not issue
// pipelines on the paths exercised by these tests.
func (h *redisCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}
