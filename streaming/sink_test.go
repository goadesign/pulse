package streaming

import (
	"context"
	"errors"
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
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

// redisCommandHook observes real Redis commands and can fail selected commands
// at the client boundary without replacing Redis in recovery tests.
type redisCommandHook struct {
	xreadGroups        atomic.Int64
	failRead           atomic.Bool
	failGroup          atomic.Bool
	failCreateConsumer atomic.Bool
	failRemove         atomic.Bool
	usedNoAck          atomic.Bool
	blockRead          atomic.Bool
	// failMembershipKey selects the replicated membership map whose mutation
	// should fail while failRemove is set.
	failMembershipKey string
	// failGroupKey selects the stream lifecycle key whose group Lua operation
	// should fail while failGroup is set.
	failGroupKey string
	// failCreateConsumerKey narrows consumer creation failure to one stream.
	failCreateConsumerKey string
	readStarted           chan struct{}
	started               sync.Once
	failure               error
}

var (
	testCheckIdlePeriod = 50 * time.Millisecond
	testBlockDuration   = 50 * time.Millisecond
	testAckDuration     = 50 * time.Millisecond
)

func TestSinkClosingFencesSubscriptions(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)

	start := make(chan struct{})
	subscriptions := make(chan (<-chan *Event), 64)
	var wait sync.WaitGroup
	for range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			subscriptions <- sink.Subscribe()
		}()
	}
	close(start)
	require.NoError(t, sink.Close(ctx))
	wait.Wait()
	close(subscriptions)
	for subscription := range subscriptions {
		_, ok := <-subscription
		require.False(t, ok)
	}
	closed := sink.Subscribe()
	_, ok := <-closed
	require.False(t, ok)
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkRejectsRemovingFinalStream(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)

	unattached, err := NewStream(t.Name()+"-unattached", rdb)
	require.NoError(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.NoError(t, sink.RemoveStream(cancelled, unattached))
	require.Empty(t, unattached.Generation())
	require.ErrorIs(t, sink.RemoveStream(ctx, stream), ErrLastStream)
	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkReplicaConfigurationAndLeaseScope(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	first, err := NewStream(t.Name()+"-first", rdb)
	require.NoError(t, err)
	second, err := NewStream(t.Name()+"-second", rdb)
	require.NoError(t, err)
	opts := []options.Sink{
		options.WithSinkTopic("alarms"),
		options.WithSinkNoAck(),
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	}
	firstReplica, err := first.NewSink(ctx, "shared", opts...)
	require.NoError(t, err)
	secondReplica, err := first.NewSink(ctx, "shared", opts...)
	require.NoError(t, err)
	unrelated, err := second.NewSink(ctx, "shared", opts...)
	require.NoError(t, err)

	firstState := firstReplica.streams[first.key]
	secondState := secondReplica.streams[first.key]
	unrelatedState := unrelated.streams[second.key]
	require.Equal(t, firstState.leaseKey, secondState.leaseKey)
	require.NotEqual(t, firstState.leaseKey, unrelatedState.leaseKey)
	require.Equal(
		t,
		firstState.keepAlives.Name,
		secondState.keepAlives.Name,
	)
	require.NotEqual(
		t,
		firstState.keepAlives.Name,
		unrelatedState.keepAlives.Name,
	)

	_, err = first.NewSink(
		ctx,
		"shared",
		options.WithSinkTopic("readings"),
		options.WithSinkNoAck(),
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.ErrorContains(t, err, "configuration differs")

	require.NoError(t, firstReplica.Close(ctx))
	require.NoError(t, secondReplica.Close(ctx))
	require.NoError(t, unrelated.Close(ctx))
	firstConfigKey := sinkConfigurationKey(first, "shared")
	firstKeepAliveKey := rmapContentKey(sinkKeepAliveMapName(first, "shared"))
	firstRegistryKey := streamResourceRegistryKey(first)
	require.NoError(t, first.Destroy(ctx))
	require.NoError(t, second.Destroy(ctx))
	require.EqualValues(
		t,
		0,
		rdb.Exists(ctx, firstConfigKey, firstKeepAliveKey, firstRegistryKey).Val(),
	)
}

func TestCrossPrimarySinkReplicasCoordinatePerAttachedStream(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	first, err := NewStream(t.Name()+"-first", rdb)
	require.NoError(t, err)
	second, err := NewStream(t.Name()+"-second", rdb)
	require.NoError(t, err)
	third, err := NewStream(t.Name()+"-third", rdb)
	require.NoError(t, err)
	compatible := []options.Sink{
		options.WithSinkTopic("alarms"),
		options.WithSinkStartAtOldest(),
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	}
	firstPrimary, err := first.NewSink(ctx, "shared", compatible...)
	require.NoError(t, err)
	secondPrimary, err := second.NewSink(ctx, "shared", compatible...)
	require.NoError(t, err)
	require.NoError(t, firstPrimary.AddStream(ctx, second, options.WithAddStreamStartAtOldest()))
	require.NoError(t, secondPrimary.AddStream(ctx, first, options.WithAddStreamStartAtOldest()))

	require.Equal(
		t,
		firstPrimary.streams[first.key].keepAlives.Name,
		secondPrimary.streams[first.key].keepAlives.Name,
	)
	require.Equal(
		t,
		firstPrimary.streams[second.key].leaseKey,
		secondPrimary.streams[second.key].leaseKey,
	)
	require.NotEqual(
		t,
		firstPrimary.streams[second.key].leaseOwner,
		secondPrimary.streams[second.key].leaseOwner,
	)

	incompatible, err := third.NewSink(
		ctx,
		"shared",
		options.WithSinkTopic("readings"),
		options.WithSinkStartAtOldest(),
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	before, err := rdb.XInfoConsumers(ctx, second.key, "shared").Result()
	require.NoError(t, err)
	err = incompatible.AddStream(ctx, second, options.WithAddStreamStartAtOldest())
	require.ErrorContains(t, err, "configuration differs")
	require.NotContains(t, incompatible.streams, second.key)
	after, err := rdb.XInfoConsumers(ctx, second.key, "shared").Result()
	require.NoError(t, err)
	require.Len(t, after, len(before))

	require.NoError(t, firstPrimary.Close(ctx))
	require.NoError(t, secondPrimary.Close(ctx))
	require.NoError(t, incompatible.Close(ctx))
	require.NoError(t, first.Destroy(ctx))
	require.NoError(t, second.Destroy(ctx))
	require.NoError(t, third.Destroy(ctx))
}

func TestSinkRecoveryLeaseFencesExpiredOwner(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	state := sink.streams[stream.key]
	duration := time.Second
	require.NoError(t, rdb.HSet(ctx, state.leaseKey, "lease_until", "0").Err())
	first, acquired, err := acquireSinkRecoveryLease(
		ctx,
		stream,
		state.leaseKey,
		"first-owner",
		duration,
	)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, rdb.HSet(ctx, state.leaseKey, "lease_until", "0").Err())
	second, acquired, err := acquireSinkRecoveryLease(
		ctx,
		stream,
		state.leaseKey,
		"second-owner",
		duration,
	)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Greater(t, second.fence, first.fence)
	require.ErrorContains(
		t,
		renewSinkRecoveryLease(ctx, stream, state.leaseKey, first, duration),
		"SINKLEASELOST",
	)
	require.NoError(t, renewSinkRecoveryLease(ctx, stream, state.leaseKey, second, duration))

	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkRecoveryLeaseFencesAutoClaimAfterTakeover(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	_, err = stream.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, rdb.XGroupCreate(ctx, stream.key, "sink", "0").Err())
	claimed, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "sink",
		Consumer: "predecessor",
		Streams:  []string{stream.key, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkAckGracePeriod(time.Hour),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	state := sink.streams[stream.key]

	require.NoError(t, rdb.HSet(ctx, state.leaseKey, "lease_until", "0").Err())
	first, acquired, err := acquireSinkRecoveryLease(
		ctx,
		stream,
		state.leaseKey,
		"first-owner",
		20*time.Millisecond,
	)
	require.NoError(t, err)
	require.True(t, acquired)
	time.Sleep(30 * time.Millisecond)
	second, acquired, err := acquireSinkRecoveryLease(
		ctx,
		stream,
		state.leaseKey,
		"second-owner",
		time.Second,
	)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, rdb.ScriptFlush(ctx).Err())
	_, _, err = fencedAutoClaim(
		ctx,
		stream,
		state.leaseKey,
		first,
		time.Second,
		sink.Name,
		sink.consumer,
		0,
		"0-0",
		1,
	)
	require.ErrorContains(t, err, "SINKLEASELOST")
	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream.key,
		Group:  sink.Name,
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()
	require.NoError(t, err)
	require.Equal(t, "predecessor", pending[0].Consumer)

	_, messages, err := fencedAutoClaim(
		ctx,
		stream,
		state.leaseKey,
		second,
		time.Second,
		sink.Name,
		sink.consumer,
		0,
		"0-0",
		1,
	)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	pending, err = rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream.key,
		Group:  sink.Name,
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()
	require.NoError(t, err)
	require.Equal(t, sink.consumer, pending[0].Consumer)

	require.NoError(t, rdb.XGroupCreateConsumer(ctx, stream.key, sink.Name, "stale").Err())
	_, err = state.keepAlives.Set(ctx, "stale", "0")
	require.NoError(t, err)
	require.NoError(t, rdb.ScriptFlush(ctx).Err())
	_, _, err = cleanupStaleConsumers(
		ctx,
		stream,
		state.leaseKey,
		first,
		time.Second,
		sink.Name,
		time.Millisecond,
	)
	require.ErrorContains(t, err, "SINKLEASELOST")
	consumers, err := rdb.XInfoConsumers(ctx, stream.key, sink.Name).Result()
	require.NoError(t, err)
	require.Contains(t, consumerNames(consumers), "stale")

	removed, malformed, err := cleanupStaleConsumers(
		ctx,
		stream,
		state.leaseKey,
		second,
		time.Second,
		sink.Name,
		time.Millisecond,
	)
	require.NoError(t, err)
	require.Contains(t, removed, "stale")
	require.Empty(t, malformed)
	consumers, err = rdb.XInfoConsumers(ctx, stream.key, sink.Name).Result()
	require.NoError(t, err)
	require.NotContains(t, consumerNames(consumers), "stale")

	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}
func TestStaleConsumerSweepSkipsMalformedKeepAliveAndContinues(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	state := sink.streams[stream.key]
	require.NoError(t, rdb.XGroupCreateConsumer(ctx, stream.key, sink.Name, "malformed").Err())
	require.NoError(t, rdb.XGroupCreateConsumer(ctx, stream.key, sink.Name, "stale").Err())
	_, err = state.keepAlives.Set(ctx, "malformed", "not-a-timestamp")
	require.NoError(t, err)
	_, err = state.keepAlives.Set(ctx, "stale", "0")
	require.NoError(t, err)
	lease, acquired, err := acquireSinkRecoveryLease(
		ctx,
		stream,
		state.leaseKey,
		state.leaseOwner,
		time.Second,
	)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, sink.deleteStreamStaleConsumers(ctx, state, lease, time.Second))
	consumers, err := rdb.XInfoConsumers(ctx, stream.key, sink.Name).Result()
	require.NoError(t, err)
	names := make([]string, len(consumers))
	for i, consumer := range consumers {
		names[i] = consumer.Name
	}
	require.Contains(t, names, "malformed")
	require.NotContains(t, names, "stale")

	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestStreamingScriptsRecoverAfterScriptFlush(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	events := sink.Subscribe()

	require.NoError(t, rdb.ScriptFlush(ctx).Err())
	require.NoError(t, rdb.XGroupDestroy(ctx, stream.key, sink.Name).Err())
	_, err = stream.Add(ctx, "after-flush", []byte("payload"))
	require.NoError(t, err)
	event := receiveSinkEvent(t, events)
	require.Equal(t, "after-flush", event.EventName)
	require.NoError(t, sink.Ack(ctx, event))

	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkRejectsInvalidOptions(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	cases := []struct {
		name string
		opt  options.Sink
	}{
		{name: "max polled", opt: options.WithSinkMaxPolled(0)},
		{name: "buffer", opt: options.WithSinkBufferSize(-1)},
		{name: "ack grace", opt: options.WithSinkAckGracePeriod(0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := NewStream(t.Name(), rdb)
			require.NoError(t, err)
			_, err = stream.NewSink(ctx, "sink", tc.opt)
			require.Error(t, err)
		})
	}
	stream, err := NewStream(t.Name()+"-conflicts", rdb)
	require.NoError(t, err)
	_, err = stream.NewSink(
		ctx,
		"sink",
		options.WithSinkTopic("alarms"),
		options.WithSinkTopicPattern("alarm.*"),
	)
	require.ErrorContains(t, err, "mutually exclusive")
	_, err = stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtNewest(),
		options.WithSinkStartAtOldest(),
	)
	require.ErrorContains(t, err, "sink cursor-start options are mutually exclusive")

	sink, err := stream.NewSink(ctx, "sink")
	require.NoError(t, err)
	added, err := NewStream(t.Name()+"-added", rdb)
	require.NoError(t, err)
	err = sink.AddStream(
		ctx,
		added,
		options.WithAddStreamStartAtNewest(),
		options.WithAddStreamStartAtOldest(),
	)
	require.ErrorContains(t, err, "added stream cursor-start options are mutually exclusive")
	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
	require.ErrorIs(t, added.Destroy(ctx), ErrStreamNotFound)
}

func TestSinkAcknowledgesMalformedRedisEvent(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream.key,
		Values: map[string]any{payloadKey: "missing name"},
	}).Err())
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkAckGracePeriod(testAckDuration),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	events := sink.Subscribe()
	_, err = stream.Add(ctx, "valid", []byte("payload"))
	require.NoError(t, err)
	event := receiveSinkEvent(t, events)
	require.Equal(t, "valid", event.EventName)
	require.NoError(t, sink.Ack(ctx, event))
	require.Eventually(t, func() bool {
		pending, pendingErr := rdb.XPending(ctx, stream.key, sink.Name).Result()
		return pendingErr == nil && pending.Count == 0
	}, time.Second, delay)

	require.NoError(t, sink.Close(ctx))
	require.NoError(t, stream.Destroy(ctx))
}

func TestDestroyedStreamSetupRemovesOrphanMembership(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	membership, err := rmap.Join(ctx, consumersMapName(stream), rdb)
	require.NoError(t, err)
	membershipKey := consumersMapContentKey(stream)
	require.NoError(t, stream.Destroy(ctx))

	_, setupErr := ensureConsumerGroup(ctx, stream, "sink", "0")
	require.ErrorIs(t, setupErr, ErrStreamDestroyed)
	closeSetupMembership(ctx, stream, membership, setupErr)
	require.EqualValues(t, 0, rdb.Exists(ctx, membershipKey).Val())
}

func TestSinkCloseWithStalledSubscriber(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)

	// Tiny buffer so the read loop's fan-out send blocks after a couple of
	// events when the subscriber never drains its channel.
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkBufferSize(1))
	require.NoError(t, err)

	// Subscribe but deliberately never read from the channel so the read
	// loop fills the buffer and then parks on the next fan-out send.
	c := sink.Subscribe()

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
	done := make(chan error, 1)
	go func() {
		done <- sink.Close(ctx)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("sink.Close() hung with a stalled subscriber")
	}
	assert.True(t, sink.IsClosed())

	require.NoError(t, s.Destroy(ctx))
}

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

func TestSinkRejectsSubMillisecondDurations(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	for _, duration := range []time.Duration{0, -time.Second, 500 * time.Microsecond} {
		sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(duration))
		require.Nil(t, sink)
		require.EqualError(t, err, "sink block duration must be at least 1ms")
	}
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkAckGracePeriod(500*time.Microsecond),
	)
	require.Nil(t, sink)
	require.EqualError(t, err, "sink acknowledgement grace period must be at least 1ms")
	require.ErrorIs(t, stream.Destroy(ctx), ErrStreamNotFound)
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
	defer func() { require.NoError(t, s.Destroy(ctx)) }()

	// Add and read 2 events consecutively
	sink, err := s.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close(ctx)) }()
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
	defer func() { require.NoError(t, sink2.Close(ctx)) }()
	c2 := sink2.Subscribe()
	read = readOneEvent(t, ctx, c2, sink2)
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload2"), read.Payload)

	// Create new sink with last event ID set to 0 and read the 2 events
	sink3, err := s.NewSink(ctx, "sink3",
		options.WithSinkStartAfter("0"),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer func() { require.NoError(t, sink3.Close(ctx)) }()
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
	require.NoError(t, sink.Close(ctx))
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
	_, err = s.Add(ctx, "event3", []byte("payload3"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event3", read.EventName)
	assert.Equal(t, []byte("payload3"), read.Payload)

	// Add back and remove other stream
	err = sink.AddStream(ctx, s2)
	assert.NoError(t, err)
	err = sink.RemoveStream(ctx, s)
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "event4", []byte("payload4"))
	assert.NoError(t, err)
	read = readOneEvent(t, ctx, c, sink)
	assert.Equal(t, "event4", read.EventName)
	assert.Equal(t, []byte("payload4"), read.Payload)
}

func TestRemoveStreamDeletesOnlyEmptyConsumerMetadata(t *testing.T) {
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	primary, err := NewStream(testName+"-primary", rdb)
	require.NoError(t, err)
	secondary, err := NewStream(testName+"-secondary", rdb)
	require.NoError(t, err)
	sink, err := primary.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	require.NoError(t, sink.AddStream(ctx, secondary))
	defer func() {
		assert.NoError(t, sink.Close(ctx))
		assert.NoError(t, primary.Destroy(ctx))
		assert.NoError(t, secondary.Destroy(ctx))
	}()

	require.NoError(t, rdb.XGroupCreateConsumer(
		ctx,
		secondary.key,
		sink.Name,
		"pending-consumer",
	).Err())
	eventID, err := secondary.Add(ctx, "event", nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		pending, pendingErr := rdb.XPending(ctx, secondary.key, sink.Name).Result()
		return pendingErr == nil && pending.Count == 1
	}, max, delay)
	_, err = rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   secondary.key,
		Group:    sink.Name,
		Consumer: "pending-consumer",
		MinIdle:  0,
		Messages: []string{eventID},
	}).Result()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		pending, pendingErr := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   secondary.key,
			Group:    sink.Name,
			Consumer: sink.consumer,
			Start:    "-",
			End:      "+",
			Count:    1,
		}).Result()
		return pendingErr == nil && len(pending) == 0
	}, max, delay)

	require.NoError(t, sink.RemoveStream(ctx, secondary))
	consumers, err := rdb.XInfoConsumers(ctx, secondary.key, sink.Name).Result()
	require.NoError(t, err)
	names := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		names = append(names, consumer.Name)
	}
	assert.NotContains(t, names, sink.consumer)
	assert.Contains(t, names, "pending-consumer")
}

func TestSinkRecoversExternallyDeletedStream(t *testing.T) {
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)

	stream, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)
	events := sink.Subscribe()

	require.Eventually(t, func() bool { return hook.xreadGroups.Load() > 0 }, max, delay)
	readsBeforeDelete := hook.xreadGroups.Load()
	require.NoError(t, rdb.Del(ctx, stream.key).Err())

	require.Eventually(t, func() bool { return consumerGroupExists(ctx, rdb, stream.key, sink.Name) }, max, delay)
	time.Sleep(4 * testBlockDuration)
	assert.LessOrEqual(t, hook.xreadGroups.Load()-readsBeforeDelete, int64(10),
		"NOGROUP recovery issued XREADGROUP in a hot loop")

	_, err = stream.Add(ctx, "future", []byte("payload"))
	require.NoError(t, err)
	read := readOneEvent(t, ctx, events, sink)
	assert.Equal(t, "future", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)
}

func TestSinkRecoversDestroyedGroupAcrossStreams(t *testing.T) {
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)

	mainStream, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	addedStream, err := NewStream(testName+"-added", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	defer func() { assert.NoError(t, addedStream.Destroy(ctx)) }()

	sink, err := mainStream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, mainStream, sink)
	require.NoError(t, sink.AddStream(ctx, addedStream, options.WithAddStreamStartAtOldest()))
	events := sink.Subscribe()

	readsBeforeAdd := hook.xreadGroups.Load()
	require.Eventually(t, func() bool { return hook.xreadGroups.Load() > readsBeforeAdd }, max, delay)

	// Hold the ownership lock after the multi-stream read has started. Redis
	// reports NOGROUP for that read when the added stream's group is destroyed,
	// but recovery cannot run until the event below exists. This proves the
	// added stream's own start position is retained for recovery.
	sink.lock.Lock()
	destroyed, destroyErr := rdb.XGroupDestroy(ctx, addedStream.key, sink.Name).Result()
	_, addErr := addedStream.Add(ctx, "recovered", []byte("added payload"))
	sink.lock.Unlock()
	require.NoError(t, destroyErr)
	require.EqualValues(t, 1, destroyed)
	require.NoError(t, addErr)

	read := readOneEvent(t, ctx, events, sink)
	assert.Equal(t, addedStream.Name, read.StreamName)
	assert.Equal(t, "recovered", read.EventName)
	require.True(t, consumerGroupExists(ctx, rdb, mainStream.key, sink.Name))
	require.True(t, consumerGroupExists(ctx, rdb, addedStream.key, sink.Name))

	_, err = mainStream.Add(ctx, "main", []byte("main payload"))
	require.NoError(t, err)
	read = readOneEvent(t, ctx, events, sink)
	assert.Equal(t, mainStream.Name, read.StreamName)
	assert.Equal(t, "main", read.EventName)
}

func TestSinkRecoveryDoesNotRecreateRemovedStream(t *testing.T) {
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)

	mainStream, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	removedStream, err := NewStream(testName+"-removed", rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	defer func() { assert.NoError(t, removedStream.Destroy(ctx)) }()

	sink, err := mainStream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, mainStream, sink)
	require.NoError(t, sink.AddStream(ctx, removedStream))
	events := sink.Subscribe()

	readsBeforeAdd := hook.xreadGroups.Load()
	require.Eventually(t, func() bool { return hook.xreadGroups.Load() > readsBeforeAdd }, max, delay)
	require.NoError(t, sink.RemoveStream(ctx, removedStream))
	destroyed, err := rdb.XGroupDestroy(ctx, removedStream.key, sink.Name).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, destroyed)

	assert.Never(t, func() bool {
		return consumerGroupExists(ctx, rdb, removedStream.key, sink.Name)
	}, 4*testBlockDuration, delay, "recovery recreated a group for a removed stream")

	_, err = mainStream.Add(ctx, "remaining", []byte("payload"))
	require.NoError(t, err)
	read := readOneEvent(t, ctx, events, sink)
	assert.Equal(t, mainStream.Name, read.StreamName)
	assert.Equal(t, "remaining", read.EventName)
}

func TestSinkBoundsRetriesDuringRedisFailure(t *testing.T) {
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	failure := errors.New("test Redis outage")
	hook := &redisCommandHook{failure: failure}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)

	stream, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(20*time.Millisecond))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)
	events := sink.Subscribe()

	hook.failGroup.Store(true)
	hook.failGroupKey = stream.lifecycleKey
	sink.lock.Lock()
	err = sink.recoverConsumerGroups(ctx)
	sink.lock.Unlock()
	require.ErrorIs(t, err, failure)
	assert.EqualError(t, err,
		`failed to ensure Redis consumer group "sink" for stream "`+testName+`" generation 1: test Redis outage`)
	hook.failGroup.Store(false)

	require.Eventually(t, func() bool { return hook.xreadGroups.Load() > 0 }, max, delay)
	readsBeforeFailure := hook.xreadGroups.Load()
	hook.failRead.Store(true)
	time.Sleep(350 * time.Millisecond)
	hook.failRead.Store(false)
	assert.LessOrEqual(t, hook.xreadGroups.Load()-readsBeforeFailure, int64(4),
		"Redis outage retries exceeded the bounded backoff rate")

	_, err = stream.Add(ctx, "after-outage", []byte("payload"))
	require.NoError(t, err)
	read := readOneEvent(t, ctx, events, sink)
	assert.Equal(t, "after-outage", read.EventName)
}

func TestSinkCloseDuringConsumerGroupRecovery(t *testing.T) {
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &redisCommandHook{}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)

	stream, err := NewStream(testName, rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(10*time.Millisecond))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return hook.xreadGroups.Load() > 0 }, max, delay)
	disruptionDone := make(chan error, 1)
	go func() {
		for range 50 {
			if err := rdb.XGroupDestroy(ctx, stream.key, sink.Name).Err(); err != nil {
				disruptionDone <- err
				return
			}
		}
		disruptionDone <- nil
	}()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- sink.Close(ctx)
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("sink.Close hung during consumer-group recovery")
	}
	require.NoError(t, <-disruptionDone)
	require.True(t, sink.IsClosed())

	readsAfterClose := hook.xreadGroups.Load()
	time.Sleep(4 * testBlockDuration)
	assert.Equal(t, readsAfterClose, hook.xreadGroups.Load(), "closed sink continued reading")
	require.NoError(t, stream.Destroy(ctx))
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
		require.NoError(t, sink2.Close(ctx))
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
	case <-time.After(max):
		t.Fatal("timeout waiting for initial event")
	}
	assert.Equal(t, "event", read.EventName)
	assert.Equal(t, []byte("payload"), read.Payload)

	// Read stale claimed event and ack
	select {
	case read = <-c:
		assert.NoError(t, sink.Ack(ctx, read))
	case <-time.After(max):
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
	defer func() { require.NoError(t, sink2.Close(ctx)) }()

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
	case <-time.After(max):
		t.Fatal("Timeout waiting for event on first sink")
	}
	assert.Equal(t, "test_event", read1.EventName)
	assert.Equal(t, []byte("test_payload"), read1.Payload)

	// Close the receiver sink
	require.NoError(t, receiverSink.Close(ctx))
	assert.Eventually(t, func() bool { return receiverSink.IsClosed() }, max, delay)

	// The message should now be redelivered to the other sink
	var read2 *Event
	select {
	case read2 = <-otherChan:
		logger.Info("Read from other sink")
		assert.NoError(t, otherSink.Ack(ctx, read2))
	case <-time.After(testAckDuration * 20):
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
	case <-time.After(max):
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
	require.NoError(t, sink1.Close(ctx))
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
	case <-time.After(max):
		t.Fatal("Timeout waiting for claimed event")
	}
	assert.Equal(t, "test_event", claimedRead.EventName)
	assert.Equal(t, []byte("test_payload"), claimedRead.Payload)
}

// consumerGroupExists reports whether Redis currently holds group for stream.
func consumerGroupExists(ctx context.Context, rdb *redis.Client, stream, group string) bool {
	groups, err := rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return false
	}
	for _, candidate := range groups {
		if candidate.Name == group {
			return true
		}
	}
	return false
}

// DialHook preserves the client's normal Redis connection behavior.
func (h *redisCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook counts group reads and injects command-specific transport
// failures used to prove bounded retries and exact error propagation.
func (h *redisCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "xreadgroup":
			h.xreadGroups.Add(1)
			for _, arg := range cmd.Args() {
				if value, ok := arg.(string); ok && strings.EqualFold(value, "noack") {
					h.usedNoAck.Store(true)
				}
			}
			if h.blockRead.Load() {
				h.started.Do(func() {
					close(h.readStarted)
				})
				<-ctx.Done()
				return ctx.Err()
			}
			if h.failRead.Load() {
				return h.failure
			}
		case "xgroup":
			if h.failGroup.Load() {
				return h.failure
			}
		case "evalsha":
			if h.failCreateConsumer.Load() && len(cmd.Args()) > 1 && cmd.Args()[1] == registerSinkConsumerScript.Hash() {
				for _, arg := range cmd.Args() {
					if key, ok := arg.(string); ok && key == h.failCreateConsumerKey {
						return h.failure
					}
				}
			}
			if h.failGroup.Load() {
				for _, arg := range cmd.Args() {
					if key, ok := arg.(string); ok && key == h.failGroupKey {
						return h.failure
					}
				}
			}
			if h.failRemove.Load() {
				for _, arg := range cmd.Args() {
					if key, ok := arg.(string); ok && key == h.failMembershipKey {
						return h.failure
					}
				}
			}
		}
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook preserves pipeline behavior; recovery uses direct Redis
// commands so no injected failure is expected through this path.
func (h *redisCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}
