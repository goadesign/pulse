// Package streaming tests consumer-group recovery against real Redis. The
// cases focus on delivery safety, shared replica state, lifecycle boundaries,
// and compensation when a multi-step ownership change fails.
package streaming

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

type (
	// recordingAcker proves Sink.Ack preserves Event's public acknowledgement
	// boundary instead of bypassing it with the sink's Redis client.
	recordingAcker struct {
		streamKey string
		group     string
		ids       []string
	}
)

func TestSinkAdoptsFlatStreamWithQueuedAndPendingEvents(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	key := streamKey(t.Name())

	firstID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]any{nameKey: "first", payloadKey: "one"},
	}).Result()
	require.NoError(t, err)
	secondID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]any{nameKey: "second", payloadKey: "two"},
	}).Result()
	require.NoError(t, err)
	require.NoError(t, rdb.XGroupCreate(ctx, key, "sink", "0").Err())
	legacy, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "sink",
		Consumer: "legacy",
		Streams:  []string{key, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)
	require.Equal(t, firstID, legacy[0].Messages[0].ID)

	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.Empty(t, stream.Generation())
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkAckGracePeriod(50*time.Millisecond),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	events := sink.Subscribe()

	received := map[string]*Event{}
	for len(received) < 2 {
		select {
		case event := <-events:
			received[event.ID] = event
		case <-time.After(max):
			t.Fatalf("timed out waiting for upgraded events; received %v", received)
		}
	}
	require.Contains(t, received, firstID)
	require.Contains(t, received, secondID)
	pending, err := rdb.XPending(ctx, key, "sink").Result()
	require.NoError(t, err)
	require.EqualValues(t, 2, pending.Count)
	require.Equal(t, "1", stream.Generation())
	require.Equal(t, key, stream.key)
	require.Equal(t, key, rdb.HGet(ctx, stream.lifecycleKey, streamPhysicalKey).Val())

	require.NoError(t, sink.Ack(ctx, received[firstID]))
	require.NoError(t, sink.Ack(ctx, received[secondID]))
	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkAcknowledgesFilteredEvents(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkTopic("wanted"),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	events := sink.Subscribe()

	filteredID, err := stream.Add(ctx, "filtered", []byte("filtered"), options.WithTopic("other"))
	require.NoError(t, err)
	wantedID, err := stream.Add(ctx, "wanted", []byte("wanted"), options.WithTopic("wanted"))
	require.NoError(t, err)
	wanted := receiveSinkEvent(t, events)
	require.Equal(t, wantedID, wanted.ID)

	require.Eventually(t, func() bool {
		pending, err := rdb.XPending(ctx, stream.key, sink.Name).Result()
		return err == nil && pending.Count == 1 && pending.Lower == wantedID
	}, max, delay)
	cursor, err := rdb.HGet(ctx, recoveryCursorKey(stream), sink.Name).Result()
	require.NoError(t, err)
	require.Equal(t, filteredID, cursor)

	require.NoError(t, sink.Ack(ctx, wanted))
	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkRecoveryPreservesPendingAndGapEvents(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)
	events := sink.Subscribe()

	for _, name := range []string{"first", "pending", "out-of-order"} {
		_, err := stream.Add(ctx, name, []byte(name))
		require.NoError(t, err)
	}
	first := receiveSinkEvent(t, events)
	pending := receiveSinkEvent(t, events)
	outOfOrder := receiveSinkEvent(t, events)
	require.NoError(t, sink.Ack(ctx, first))
	require.NoError(t, sink.Ack(ctx, outOfOrder))

	cursor, err := rdb.HGet(ctx, recoveryCursorKey(stream), sink.Name).Result()
	require.NoError(t, err)
	require.Equal(t, first.ID, cursor)

	readsBeforeDestroy := hook.xreadGroups.Load()
	require.Eventually(t, func() bool { return hook.xreadGroups.Load() > readsBeforeDestroy }, max, delay)
	sink.lock.Lock()
	destroyed, destroyErr := rdb.XGroupDestroy(ctx, stream.key, sink.Name).Result()
	_, gapErr := stream.Add(ctx, "gap", []byte("gap"))
	sink.lock.Unlock()
	require.NoError(t, destroyErr)
	require.EqualValues(t, 1, destroyed)
	require.NoError(t, gapErr)

	recovered := []*Event{
		receiveSinkEvent(t, events),
		receiveSinkEvent(t, events),
		receiveSinkEvent(t, events),
	}
	assert.Equal(t, []string{"pending", "out-of-order", "gap"}, eventNames(recovered))
	assert.Equal(t, pending.ID, recovered[0].ID)
	for _, event := range recovered {
		require.NoError(t, sink.Ack(ctx, event))
	}
}

func TestSinkRecoveryCursorIsSharedAcrossReplicas(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink1, err := stream.NewSink(ctx, "sink", options.WithSinkStartAtOldest(), options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	sink2, err := stream.NewSink(ctx, "sink", options.WithSinkStartAtOldest(), options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink2)
	events1 := sink1.Subscribe()
	events2 := sink2.Subscribe()

	eventID, err := stream.Add(ctx, "shared", []byte("shared"))
	require.NoError(t, err)
	select {
	case event := <-events1:
		require.NoError(t, sink1.Ack(ctx, event))
	case event := <-events2:
		require.NoError(t, sink2.Ack(ctx, event))
	case <-time.After(max):
		t.Fatal("timed out waiting for shared event")
	}
	cursor, err := rdb.HGet(ctx, recoveryCursorKey(stream), sink1.Name).Result()
	require.NoError(t, err)
	require.Equal(t, eventID, cursor)

	require.NoError(t, sink1.Close(ctx))
	require.True(t, sink1.IsClosed())
	destroyed, err := rdb.XGroupDestroy(ctx, stream.key, sink2.Name).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, destroyed)
	require.Eventually(t, func() bool {
		return consumerGroupExists(ctx, rdb, stream.key, sink2.Name)
	}, max, delay)

	_, err = stream.Add(ctx, "future", []byte("future"))
	require.NoError(t, err)
	future := receiveSinkEvent(t, events2)
	require.Equal(t, "future", future.EventName)
	require.NoError(t, sink2.Ack(ctx, future))
}

func TestSinkCloseCancelsBlockingRedisRead(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{
		readStarted: make(chan struct{}),
	}
	hook.blockRead.Store(true)
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)

	select {
	case <-hook.readStarted:
	case <-time.After(max):
		t.Fatal("sink never entered blocking Redis read")
	}
	closed := make(chan struct{})
	go func() {
		require.NoError(t, sink.Close(ctx))
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(max):
		t.Fatal("Close did not cancel blocking Redis I/O")
	}
	require.True(t, sink.IsClosed())
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkCloseRetriesDistributedDetach(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	failure := errors.New("injected close detach failure")
	hook := &redisCommandHook{failure: failure}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)

	hook.failMembershipKey = consumersMapContentKey(stream)
	hook.failRemove.Store(true)
	err = sink.Close(ctx)
	require.ErrorIs(t, err, failure)
	require.False(t, sink.IsClosed())
	require.Contains(t, sink.streams, stream.key)
	hook.failRemove.Store(false)

	require.NoError(t, sink.Close(ctx))
	require.True(t, sink.IsClosed())
	require.Empty(t, sink.streams)
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkRejectsStreamMutationAfterClose(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	added, err := NewStream(t.Name()+"-added", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	defer func() { require.ErrorIs(t, added.Destroy(ctx), ErrStreamNotFound) }()
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)

	require.NoError(t, sink.Close(ctx))
	require.ErrorIs(t, sink.AddStream(ctx, added), ErrSinkClosed)
	require.ErrorIs(t, sink.RemoveStream(ctx, stream), ErrSinkClosed)
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkConsumerRotationRegistersEveryStreamOrRollsBack(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	failure := errors.New("injected replacement consumer failure")
	hook := &redisCommandHook{failure: failure}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	mainStream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	addedStream, err := NewStream(t.Name()+"-added", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	defer func() { require.NoError(t, addedStream.Destroy(ctx)) }()
	sink, err := mainStream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, mainStream, sink)
	require.NoError(t, sink.AddStream(ctx, addedStream))
	original := sink.consumer

	sink.lock.Lock()
	hook.failCreateConsumerKey = addedStream.key
	hook.failCreateConsumer.Store(true)
	_, err = sink.newConsumer(ctx)
	hook.failCreateConsumer.Store(false)
	sink.lock.Unlock()
	require.ErrorIs(t, err, failure)
	require.Equal(t, original, sink.consumer)
	for _, stream := range []*Stream{mainStream, addedStream} {
		require.Eventually(t, func() bool {
			members, ok := sink.streams[stream.key].consumers.GetValues(sink.Name)
			return ok && assert.ObjectsAreEqual([]string{original}, members)
		}, max, delay)
		consumers, err := rdb.XInfoConsumers(ctx, stream.key, sink.Name).Result()
		require.NoError(t, err)
		require.Len(t, consumers, 1)
		require.Equal(t, original, consumers[0].Name)
	}

	sink.lock.Lock()
	replacement, err := sink.newConsumer(ctx)
	if err == nil {
		sink.consumer = replacement
	}
	sink.lock.Unlock()
	require.NoError(t, err)
	require.NotEqual(t, original, replacement)
	for _, stream := range []*Stream{mainStream, addedStream} {
		// Registration writes membership in Redis atomically; the local rmap
		// replica converges through the update channel, so poll it.
		require.Eventually(t, func() bool {
			members, ok := sink.streams[stream.key].consumers.GetValues(sink.Name)
			return ok && slices.Contains(members, replacement)
		}, max, delay)
		consumers, err := rdb.XInfoConsumers(ctx, stream.key, sink.Name).Result()
		require.NoError(t, err)
		assert.Contains(t, consumerNames(consumers), replacement)
	}
}


func TestSinkStreamMutationRollback(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	failure := errors.New("injected ownership failure")
	hook := &redisCommandHook{failure: failure}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	mainStream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	addedStream, err := NewStream(t.Name()+"-added", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	defer func() { require.NoError(t, addedStream.Destroy(ctx)) }()
	sink, err := mainStream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, mainStream, sink)

	hook.failCreateConsumer.Store(true)
	hook.failCreateConsumerKey = addedStream.key
	err = sink.AddStream(ctx, addedStream)
	hook.failCreateConsumer.Store(false)
	require.ErrorIs(t, err, failure)
	assert.False(t, sinkOwnsStream(sink, addedStream))
	assert.NotContains(t, sink.streams, addedStream.key)
	assert.True(t, consumerGroupExists(ctx, rdb, addedStream.key, sink.Name))
	assert.True(t, rdb.HExists(ctx, recoveryCursorKey(addedStream), sink.Name).Val())

	consumerMap, err := rmap.Join(ctx, consumersMapName(addedStream), rdb)
	require.NoError(t, err)
	assert.NotContains(t, consumerMap.Map(), sink.Name)
	consumerMap.Close()

	require.NoError(t, sink.AddStream(ctx, addedStream))
	hook.failMembershipKey = consumersMapContentKey(addedStream)
	hook.failRemove.Store(true)
	err = sink.RemoveStream(ctx, addedStream)
	hook.failRemove.Store(false)
	require.ErrorIs(t, err, failure)
	assert.True(t, sinkOwnsStream(sink, addedStream))
	assert.Contains(t, sink.streams, addedStream.key)
	assert.True(t, consumerGroupExists(ctx, rdb, addedStream.key, sink.Name))

	require.NoError(t, sink.RemoveStream(ctx, addedStream))
	assert.False(t, sinkOwnsStream(sink, addedStream))
	assert.True(t, consumerGroupExists(ctx, rdb, addedStream.key, sink.Name))
	assert.True(t, rdb.HExists(ctx, recoveryCursorKey(addedStream), sink.Name).Val())
}

func TestSinkRecoveryMetadataOutlivesEventTTL(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(
		t.Name(),
		rdb,
		options.WithStreamTTL(5*time.Second),
		options.WithStreamLogger(pulse.ClueLogger(ctx)),
	)
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)

	require.NoError(t, rdb.Persist(ctx, stream.key).Err())
	require.NoError(t, rdb.Persist(ctx, recoveryCursorKey(stream)).Err())
	require.Equal(t, time.Duration(-1), rdb.PTTL(ctx, stream.key).Val())
	require.Equal(t, time.Duration(-1), rdb.PTTL(ctx, recoveryCursorKey(stream)).Val())
	sink.lock.Lock()
	err = sink.recoverConsumerGroups(ctx)
	sink.lock.Unlock()
	require.NoError(t, err)
	assert.Greater(t, rdb.PTTL(ctx, stream.key).Val(), time.Duration(0))
	assert.Equal(t, time.Duration(-1), rdb.PTTL(ctx, recoveryCursorKey(stream)).Val())
}

func TestSinkAcknowledgesBatchFromConcurrentlyRemovedSnapshot(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	main, err := NewStream(t.Name()+"-main", rdb)
	require.NoError(t, err)
	removed, err := NewStream(t.Name()+"-removed", rdb)
	require.NoError(t, err)
	sink, err := main.NewSink(ctx, "sink", options.WithSinkBlockDuration(time.Second))
	require.NoError(t, err)
	require.NoError(t, sink.AddStream(ctx, removed, options.WithAddStreamStartAtOldest()))
	events := sink.Subscribe()
	reads := hook.xreadGroups.Load()
	require.Eventually(t, func() bool {
		return hook.xreadGroups.Load() > reads
	}, max, delay)

	require.NoError(t, sink.RemoveStream(ctx, removed))
	eventID, err := removed.Add(ctx, "removed", []byte("payload"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		cursor := rdb.HGet(ctx, recoveryCursorKey(removed), sink.Name).Val()
		pending, pendingErr := rdb.XPending(ctx, removed.key, sink.Name).Result()
		return pendingErr == nil && pending.Count == 0 && cursor == eventID
	}, max, delay)
	select {
	case event := <-events:
		require.NotEqual(t, eventID, event.ID)
	case <-time.After(2 * testBlockDuration):
	}

	require.NoError(t, sink.Close(ctx))
	require.NoError(t, main.Destroy(ctx))
	require.NoError(t, removed.Destroy(ctx))
}

func TestSinkRecoveryAfterEventStreamExpiresDeliversNewEvents(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamTTL(2*time.Second))
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	events := sink.Subscribe()
	_, err = stream.Add(ctx, "old", []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, sink.Ack(ctx, receiveSinkEvent(t, events)))
	require.NoError(t, sink.Close(ctx))
	require.NoError(t, rdb.PExpire(ctx, stream.key, 10*time.Millisecond).Err())
	require.Eventually(t, func() bool {
		return rdb.Exists(ctx, stream.key).Val() == 0
	}, max, delay)
	require.Equal(t, time.Duration(-1), rdb.PTTL(ctx, recoveryCursorKey(stream)).Val())

	freshID, err := stream.Add(ctx, "fresh", []byte("payload"))
	require.NoError(t, err)
	recovered, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	fresh := receiveSinkEvent(t, recovered.Subscribe())
	require.Equal(t, freshID, fresh.ID)
	require.NoError(t, recovered.Ack(ctx, fresh))
	require.NoError(t, recovered.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkNoAckUsesPendingEntryListBeforeDelivery(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkNoAck(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)
	events := sink.Subscribe()

	eventID, err := stream.Add(ctx, "at-most-once", []byte("payload"))
	require.NoError(t, err)
	event := receiveSinkEvent(t, events)
	require.Equal(t, eventID, event.ID)
	require.False(t, hook.usedNoAck.Load(), "sink sent Redis NOACK")
	pending, err := rdb.XPending(ctx, stream.key, sink.Name).Result()
	require.NoError(t, err)
	require.Zero(t, pending.Count)
	cursor, err := rdb.HGet(ctx, recoveryCursorKey(stream), sink.Name).Result()
	require.NoError(t, err)
	require.Equal(t, eventID, cursor)

	require.NoError(t, rdb.XGroupDestroy(ctx, stream.key, sink.Name).Err())
	require.Eventually(t, func() bool {
		return consumerGroupExists(ctx, rdb, stream.key, sink.Name)
	}, max, delay)
	_, err = stream.Add(ctx, "future", []byte("future"))
	require.NoError(t, err)
	require.Equal(t, "future", receiveSinkEvent(t, events).EventName)
}

func TestEventAckerAdvancesRecoveryCursor(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)
	events := sink.Subscribe()

	eventID, err := stream.Add(ctx, "direct-ack", []byte("payload"))
	require.NoError(t, err)
	event := receiveSinkEvent(t, events)
	acked, err := event.Acker.XAck(ctx, stream.key, sink.Name, event.ID).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, acked)
	cursor, err := rdb.HGet(ctx, recoveryCursorKey(stream), sink.Name).Result()
	require.NoError(t, err)
	require.Equal(t, eventID, cursor)
}

func TestEventAckerAcknowledgesMultipleIDs(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)
	events := sink.Subscribe()
	for _, name := range []string{"first", "second"} {
		_, err := stream.Add(ctx, name, []byte(name))
		require.NoError(t, err)
	}
	first := receiveSinkEvent(t, events)
	second := receiveSinkEvent(t, events)

	acked, err := first.Acker.XAck(ctx, stream.key, sink.Name, first.ID, second.ID).Result()
	require.NoError(t, err)
	require.EqualValues(t, 2, acked)
	cursor, err := rdb.HGet(ctx, recoveryCursorKey(stream), sink.Name).Result()
	require.NoError(t, err)
	require.Equal(t, second.ID, cursor)
}

func TestSinkAckDelegatesToEventAcker(t *testing.T) {
	ctx := context.Background()
	acker := &recordingAcker{}
	sink := &Sink{logger: pulse.NoopLogger()}
	event := &Event{
		ID:         "1-0",
		StreamName: "stream",
		SinkName:   "sink",
		Acker:      acker,
		streamKey:  "pulse:stream:stream",
	}

	require.NoError(t, sink.Ack(ctx, event))
	require.Equal(t, event.streamKey, acker.streamKey)
	require.Equal(t, event.SinkName, acker.group)
	require.Equal(t, []string{event.ID}, acker.ids)
}

func TestRecoveryCursorsShareOneStreamHash(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink1, err := stream.NewSink(ctx, "first", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	sink2, err := stream.NewSink(ctx, "second", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink1)
	defer func() { require.NoError(t, sink2.Close(ctx)) }()

	cursors, err := rdb.HGetAll(ctx, recoveryCursorKey(stream)).Result()
	require.NoError(t, err)
	require.Len(t, cursors, 2)
	require.Contains(t, cursors, sink1.Name)
	require.Contains(t, cursors, sink2.Name)
	legacyKeys, err := rdb.Keys(ctx, stream.key+":sink:*:recovery").Result()
	require.NoError(t, err)
	require.Empty(t, legacyKeys)
}

func TestReadRetryJitterBounds(t *testing.T) {
	low := readRetry{jitter: func(int64) int64 { return 0 }}
	high := readRetry{jitter: func(n int64) int64 { return n - 1 }}
	limit := readRetryInitialDelay
	for range 10 {
		assert.Equal(t, limit/2, low.nextDelay())
		assert.Equal(t, limit, high.nextDelay())
		limit = min(limit*2, readRetryMaxDelay)
	}
}

// XAck records the Event acknowledgement invoked by Sink.Ack.
func (a *recordingAcker) XAck(ctx context.Context, streamKey, group string, ids ...string) *redis.IntCmd {
	a.streamKey = streamKey
	a.group = group
	a.ids = append([]string(nil), ids...)
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(len(ids)))
	return cmd
}

// receiveSinkEvent reads one event without acknowledging it.
func receiveSinkEvent(t *testing.T, events <-chan *Event) *Event {
	t.Helper()
	select {
	case event := <-events:
		require.NotNil(t, event)
		return event
	case <-time.After(max):
		t.Fatal("timed out waiting for sink event")
		return nil
	}
}

// eventNames projects event names in delivery order.
func eventNames(events []*Event) []string {
	names := make([]string, len(events))
	for i, event := range events {
		names[i] = event.EventName
	}
	return names
}

// consumerNames projects Redis consumer info for membership assertions.
func consumerNames(consumers []redis.XInfoConsumer) []string {
	names := make([]string, len(consumers))
	for i, consumer := range consumers {
		names[i] = consumer.Name
	}
	return names
}

// sinkOwnsStream reports local ownership under the sink lock.
func sinkOwnsStream(sink *Sink, stream *Stream) bool {
	sink.lock.Lock()
	defer sink.lock.Unlock()
	for _, owned := range sink.streams {
		if owned.stream == stream {
			return true
		}
	}
	return false
}

func TestDestroyedGenerationMetadataCannotBeRecreated(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close(ctx)) }()
	state := sink.streams[stream.key]
	require.NotNil(t, state)
	keepAliveKey := rmapContentKey(sinkKeepAliveMapName(stream, "sink"))
	membershipKey := consumersMapContentKey(stream)
	require.EqualValues(t, 1, rdb.Exists(ctx, keepAliveKey).Val())
	require.EqualValues(t, 1, rdb.Exists(ctx, membershipKey).Val())

	require.NoError(t, stream.Destroy(ctx))
	require.EqualValues(t, 0, rdb.Exists(ctx, keepAliveKey).Val())
	require.EqualValues(t, 0, rdb.Exists(ctx, membershipKey).Val())

	// Neither a periodic keep-alive tick nor a consumer registration may
	// resurrect metadata for the destroyed generation.
	err = setSinkKeepAlive(ctx, state, "sink", sink.consumer, time.Now().UnixNano())
	require.ErrorIs(t, err, ErrStreamDestroyed)
	err = registerSinkConsumer(ctx, state, "sink", "ghost-consumer", time.Now().UnixNano())
	require.ErrorIs(t, err, ErrStreamDestroyed)
	require.EqualValues(t, 0, rdb.Exists(ctx, keepAliveKey).Val())
	require.EqualValues(t, 0, rdb.Exists(ctx, membershipKey).Val())
}
