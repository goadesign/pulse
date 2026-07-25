package streaming

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

type (
	// ambiguousAddOnceHook returns one client error after Redis committed the
	// selected AddOnce script, reproducing an ambiguous network outcome.
	ambiguousAddOnceHook struct {
		fail atomic.Bool
		err  error
	}
)

func TestAddOnceConcurrentClientsPublishExactlyOnce(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	other := redis.NewClient(rdb.Options())
	defer func() { require.NoError(t, other.Close()) }()
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(10 * time.Second).Truncate(time.Millisecond)
	const clients = 12
	results := make(chan string, clients)
	errs := make(chan error, clients)
	var wait sync.WaitGroup
	for index := range clients {
		wait.Add(1)
		go func(client *redis.Client) {
			defer wait.Done()
			stream, err := NewStream(t.Name(), client, options.WithStreamDeadline(deadline))
			if err != nil {
				errs <- err
				return
			}
			result, err := stream.AddOnce(
				ctx,
				"command",
				"created",
				[]byte("payload"),
				options.WithTopic("alarms"),
			)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}([]*redis.Client{rdb, other}[index%2])
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var (
		eventID string
	)
	for result := range results {
		if eventID == "" {
			eventID = result
		}
		require.Equal(t, eventID, result)
	}
	require.EqualValues(t, 1, rdb.XLen(ctx, streamKey(t.Name())).Val())
}

func TestAddOnceConflictAndAmbiguousCommitRetry(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &ambiguousAddOnceHook{err: errors.New("ambiguous client result")}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(10 * time.Second).Truncate(time.Millisecond)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)

	hook.fail.Store(true)
	_, err = stream.AddOnce(ctx, "command", "created", []byte("payload"))
	require.ErrorIs(t, err, hook.err)
	retry, err := stream.AddOnce(ctx, "command", "created", []byte("payload"))
	require.NoError(t, err)
	require.NotEmpty(t, retry)
	require.EqualValues(t, 1, rdb.XLen(ctx, stream.key).Val())

	_, err = stream.AddOnce(ctx, "command", "created", []byte("different"))
	require.ErrorIs(t, err, ErrIdempotencyConflict)
	require.EqualValues(t, 1, rdb.XLen(ctx, stream.key).Val())
	require.NoError(t, stream.Destroy(ctx))
}

func TestAddOnceMetadataSurvivesMaxLenAndScriptFlush(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(10 * time.Second).Truncate(time.Millisecond)
	stream, err := NewStream(
		t.Name(),
		rdb,
		options.WithStreamMaxLen(1),
		options.WithStreamDeadline(deadline),
	)
	require.NoError(t, err)
	first, err := stream.AddOnce(ctx, "first", "first", []byte("payload"))
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		_, err = stream.Add(ctx, "trim", []byte{byte(i)})
		require.NoError(t, err)
	}
	require.Empty(t, rdb.XRangeN(ctx, stream.key, first, first, 1).Val())

	require.NoError(t, rdb.ScriptFlush(ctx).Err())
	retry, err := stream.AddOnce(ctx, "first", "first", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, first, retry)
	_, err = stream.AddOnce(ctx, "second", "second", []byte("payload"))
	require.NoError(t, err)
	_, err = stream.Snapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Destroy(ctx))
}

func TestAddOnceAdoptsPreGenerationFlatStreamInPlace(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	flatKey := streamKey(t.Name())
	legacyID, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: flatKey,
		Values: map[string]any{nameKey: "legacy", payloadKey: "payload"},
	}).Result()
	require.NoError(t, err)
	deadline := time.Now().Add(10 * time.Second).Truncate(time.Millisecond)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	published, err := stream.AddOnce(ctx, "new", "current", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, flatKey, stream.key)
	require.Equal(t, "1", stream.Generation())
	events, err := stream.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{legacyID, published}, []string{events[0].ID(), events[1].ID()})
	require.NoError(t, stream.Destroy(ctx))
}

func TestExplicitBoundedLegacyAdoptionTrimsBeforePublishingLifecycle(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	const (
		legacyEvents = 5_000
		maxLen       = 100
	)
	flatKey := streamKey(t.Name())
	pipe := rdb.Pipeline()
	for index := 0; index < legacyEvents; index++ {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: flatKey,
			Values: map[string]any{
				nameKey:    "legacy",
				payloadKey: strconv.Itoa(index),
			},
		})
	}
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)

	stream, err := NewStream(t.Name(), rdb, options.WithStreamMaxLen(maxLen))
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	require.EqualValues(t, maxLen, rdb.XLen(ctx, flatKey).Val())
	events, err := stream.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, events, maxLen)
	require.Equal(t, strconv.Itoa(legacyEvents-maxLen), string(events[0].Payload()))
	require.NoError(t, stream.Destroy(ctx))
}

func TestSnapshotRejectsPhysicalBoundViolation(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	const maxLen = 3
	stream, err := NewStream(t.Name(), rdb, options.WithStreamMaxLen(maxLen))
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	for index := 0; index <= maxLen; index++ {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream.key,
			Values: map[string]any{nameKey: "event", payloadKey: strconv.Itoa(index)},
		}).Err())
	}
	_, err = stream.Snapshot(ctx)
	require.ErrorIs(t, err, ErrSnapshotBoundExceeded)
	require.NoError(t, stream.Destroy(ctx))
}

func TestDeadlineIsAbsoluteAndNeverExtended(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(1500 * time.Millisecond).Truncate(time.Millisecond)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	_, err = stream.Add(ctx, "first", []byte("payload"))
	require.NoError(t, err)
	firstExpiry, err := rdb.PExpireTime(ctx, stream.key).Result()
	require.NoError(t, err)
	expectedExpiry := time.Duration(deadline.UnixMilli()) * time.Millisecond
	require.Equal(t, expectedExpiry, firstExpiry)

	time.Sleep(100 * time.Millisecond)
	reopened, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	_, err = reopened.Add(ctx, "second", []byte("payload"))
	require.NoError(t, err)
	secondExpiry, err := rdb.PExpireTime(ctx, stream.key).Result()
	require.NoError(t, err)
	require.Equal(t, firstExpiry, secondExpiry)

	_, err = stream.AddOnce(ctx, "command", "third", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, expectedExpiry, rdb.PExpireTime(ctx, idempotencyKeyMap(stream)).Val())
	require.Equal(t, expectedExpiry, rdb.PExpireTime(ctx, stream.key).Val())
	require.Equal(t, expectedExpiry, rdb.PExpireTime(ctx, recoveryCursorKey(stream)).Val())
	require.NoError(t, stream.Destroy(ctx))
}

func TestDeadlineValidationReopenAndElapsedOperations(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(150 * time.Millisecond).Truncate(time.Millisecond)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	_, err = stream.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)
	same, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	require.NoError(t, same.Open(ctx))
	conflicting, err := NewStream(
		t.Name(),
		rdb,
		options.WithStreamDeadline(deadline.Add(time.Second)),
	)
	require.NoError(t, err)
	require.ErrorIs(t, conflicting.Open(ctx), ErrStreamConfigMismatch)

	require.Eventually(t, func() bool {
		_, addErr := stream.Add(ctx, "late", []byte("payload"))
		return errors.Is(addErr, ErrDeadlineElapsed)
	}, time.Second, 10*time.Millisecond)
	_, err = stream.AddOnce(ctx, "late", "late", []byte("payload"))
	require.ErrorIs(t, err, ErrDeadlineElapsed)
	events, err := stream.Snapshot(ctx)
	require.ErrorIs(t, err, ErrDeadlineElapsed)
	require.Nil(t, events)
	require.NoError(t, stream.Destroy(ctx))
}

func TestAddOnceDestroyRecreateIsolatesGeneration(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	firstDeadline := time.Now().Add(10 * time.Second).Truncate(time.Millisecond)
	first, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(firstDeadline))
	require.NoError(t, err)
	old, err := first.AddOnce(ctx, "command", "old", []byte("payload"))
	require.NoError(t, err)
	oldDedupe := idempotencyKeyMap(first)
	require.NoError(t, first.Destroy(ctx))
	require.EqualValues(t, 0, rdb.Exists(ctx, oldDedupe).Val())

	secondDeadline := firstDeadline.Add(time.Second)
	second, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(secondDeadline))
	require.NoError(t, err)
	fresh, err := second.AddOnce(ctx, "command", "new", []byte("different"))
	require.NoError(t, err)
	require.NotEqual(t, first.Generation(), second.Generation())
	require.NotEqual(t, first.key, second.key)
	require.ErrorIs(t, func() error {
		_, snapshotErr := first.Snapshot(ctx)
		return snapshotErr
	}(), ErrStreamDestroyed)
	require.NotEqual(t, old+"@"+first.key, fresh+"@"+second.key)
	require.NoError(t, second.Destroy(ctx))
}

func TestAddOnceDestroyRaceCannotCrossGeneration(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(10 * time.Second).Truncate(time.Millisecond)
	old, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	require.NoError(t, old.Open(ctx))
	start := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		<-start
		_, addErr := old.AddOnce(ctx, "shared", "old", []byte("payload"))
		result <- addErr
	}()
	close(start)
	destroyErr := old.Destroy(ctx)
	require.NoError(t, destroyErr)
	addErr := <-result
	if addErr != nil {
		require.ErrorIs(t, addErr, ErrStreamDestroyed)
	}

	freshDeadline := deadline.Add(time.Second)
	fresh, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(freshDeadline))
	require.NoError(t, err)
	freshResult, err := fresh.AddOnce(
		ctx,
		"shared",
		"fresh",
		[]byte("new payload"),
	)
	require.NoError(t, err)
	require.NotEmpty(t, freshResult)
	events, err := fresh.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "fresh", events[0].EventName())
	require.NoError(t, fresh.Destroy(ctx))
}

func TestSnapshotOrderedSideEffectFreeAndMalformedAtomic(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	firstID, err := stream.Add(ctx, "first", []byte("one"))
	require.NoError(t, err)
	secondID, err := stream.Add(ctx, "second", []byte("two"), options.WithTopic("topic"))
	require.NoError(t, err)
	events, err := stream.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, []string{firstID, secondID}, []string{events[0].ID(), events[1].ID()})
	require.Equal(t, []string{"first", "second"}, []string{events[0].EventName(), events[1].EventName()})
	require.Equal(t, "topic", events[1].Topic())
	require.EqualValues(t, 0, rdb.Exists(ctx, recoveryCursorKey(stream)).Val())
	groups, err := rdb.XInfoGroups(ctx, stream.key).Result()
	require.NoError(t, err)
	require.Empty(t, groups)

	_, err = rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream.key,
		Values: map[string]any{payloadKey: "missing name"},
	}).Result()
	require.NoError(t, err)
	events, err = stream.Snapshot(ctx)
	require.ErrorContains(t, err, "malformed event")
	require.Nil(t, events)
	require.NoError(t, stream.Destroy(ctx))
}

func TestExactPublicationOptionValidation(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(time.Second)
	_, err := NewStream(
		t.Name(),
		rdb,
		options.WithStreamTTL(time.Second),
		options.WithStreamDeadline(deadline),
	)
	require.ErrorContains(t, err, "mutually exclusive")
	_, err = NewStream(t.Name(), rdb, options.WithStreamDeadline(time.Time{}))
	require.ErrorContains(t, err, "must not be zero")

	stream, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	_, err = stream.AddOnce(ctx, "", "event", nil)
	require.ErrorContains(t, err, "must not be empty")
	_, err = stream.AddOnce(ctx, string(make([]byte, maxIdempotencyKeyBytes+1)), "event", nil)
	require.ErrorContains(t, err, "exceeds")
	_, err = stream.AddOnce(ctx, "key", "", nil)
	require.ErrorContains(t, err, "event name must not be empty")
	_, err = stream.AddOnce(
		ctx,
		"key",
		"event",
		nil,
		options.WithOnlyIfStreamExists(),
	)
	require.ErrorContains(t, err, "does not support")
	past := time.Now().Add(-time.Second)
	pastStream, err := NewStream(t.Name()+"-past", rdb, options.WithStreamDeadline(past))
	require.NoError(t, err)
	_, err = pastStream.AddOnce(ctx, "past", "event", nil)
	require.ErrorIs(t, err, ErrDeadlineElapsed)
	valid, err := stream.AddOnce(ctx, "valid", "event", nil)
	require.NoError(t, err)
	require.NotEmpty(t, valid)
	require.NoError(t, stream.Destroy(ctx))

	ttlStream, err := NewStream(t.Name()+"-ttl", rdb, options.WithStreamTTL(time.Second))
	require.NoError(t, err)
	_, err = ttlStream.Add(ctx, "event", nil)
	require.NoError(t, err)
	deadlineHandle, err := NewStream(
		t.Name()+"-ttl",
		rdb,
		options.WithStreamDeadline(deadline),
	)
	require.NoError(t, err)
	require.ErrorIs(t, deadlineHandle.Open(ctx), ErrStreamConfigMismatch)
	require.NoError(t, ttlStream.Destroy(ctx))
}

func TestSnapshotDoesNotInitializeAbsentStream(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)

	events, err := stream.Snapshot(ctx)
	require.ErrorIs(t, err, ErrStreamNotFound)
	require.Nil(t, events)
	require.EqualValues(t, 0, rdb.Exists(ctx, stream.lifecycleKey, streamKey(t.Name())).Val())
	require.Empty(t, stream.Generation())
}

func TestSnapshotRejectsUnboundedRetentionBeforeRange(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithUnboundedStream())
	require.NoError(t, err)
	_, err = stream.Add(ctx, "event", []byte("payload"))
	require.NoError(t, err)

	events, err := stream.Snapshot(ctx)
	require.ErrorIs(t, err, ErrSnapshotUnbounded)
	require.Nil(t, events)
	require.NoError(t, stream.Destroy(ctx))
}

func TestRejectedLegacyRetentionAdoptionDoesNotMutateLifecycle(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	name := t.Name()
	lifecycle := streamLifecycleKey(name)
	deadline := time.Now().Add(time.Hour).UnixMilli()
	require.NoError(t, rdb.HSet(
		ctx,
		lifecycle,
		"generation", "1",
		"state", streamStateActive,
		streamPhysicalKey, streamKey(name),
		streamDeadlineKey, deadline,
	).Err())
	before, err := rdb.HGetAll(ctx, lifecycle).Result()
	require.NoError(t, err)
	opener, err := NewStream(name, rdb, options.WithStreamTTL(time.Minute))
	require.NoError(t, err)

	require.ErrorIs(t, opener.Open(ctx), ErrStreamConfigMismatch)
	after, err := rdb.HGetAll(ctx, lifecycle).Result()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRetentionConfigurationIsImmutable(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamMaxLen(12))
	require.NoError(t, err)
	_, err = stream.Add(ctx, "event", nil)
	require.NoError(t, err)

	differentLimit, err := NewStream(t.Name(), rdb, options.WithStreamMaxLen(13))
	require.NoError(t, err)
	require.ErrorIs(t, differentLimit.Open(ctx), ErrStreamConfigMismatch)
	differentMode, err := NewStream(t.Name(), rdb, options.WithStreamTTL(time.Second))
	require.NoError(t, err)
	require.ErrorIs(t, differentMode.Open(ctx), ErrStreamConfigMismatch)
	_, err = NewStream(
		t.Name()+"-invalid",
		rdb,
		options.WithStreamMaxLen(1),
		options.WithUnboundedStream(),
	)
	require.ErrorContains(t, err, "mutually exclusive")
	require.NoError(t, stream.Destroy(ctx))
}

func TestDefaultHandlesAdoptWriterRetention(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	writer, err := NewStream(
		t.Name(),
		rdb,
		options.WithStreamMaxLen(50_000),
		options.WithStreamSlidingTTL(time.Minute),
	)
	require.NoError(t, err)
	_, err = writer.Add(ctx, "event", nil)
	require.NoError(t, err)

	readerHandle, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	reader, err := readerHandle.NewReader(ctx)
	require.NoError(t, err)
	reader.Close()
	require.Equal(t, 50_000, readerHandle.MaxLen)
	require.Equal(t, time.Minute, readerHandle.ttl)
	require.True(t, readerHandle.ttlSliding)

	destroyHandle, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, destroyHandle.Destroy(ctx))
}

func TestNonCreatingOperationsDoNotInitializeLifecycle(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)

	_, err = stream.Add(ctx, "event", nil, options.WithOnlyIfStreamExists())
	require.ErrorIs(t, err, ErrStreamNotFound)
	reader, err := stream.NewReader(ctx)
	require.Nil(t, reader)
	require.ErrorIs(t, err, ErrStreamNotFound)
	require.ErrorIs(t, stream.Destroy(ctx), ErrStreamNotFound)
	require.EqualValues(t, 0, rdb.Exists(ctx, stream.lifecycleKey, streamKey(t.Name())).Val())
}

func TestSubMillisecondRetentionDurationsAreRejected(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	for _, option := range []options.Stream{
		options.WithStreamTTL(0),
		options.WithStreamSlidingTTL(0),
		options.WithStreamTTL(500 * time.Microsecond),
		options.WithStreamSlidingTTL(500 * time.Microsecond),
	} {
		_, err := NewStream(t.Name(), rdb, option)
		require.ErrorContains(t, err, "at least 1ms")
	}
}

func TestAddOnceExactIdentityAndBodyLimit(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	first, err := stream.AddOnce(ctx, "key", "a:1", []byte("b"))
	require.NoError(t, err)
	retry, err := stream.AddOnce(ctx, "key", "a:1", []byte("b"))
	require.NoError(t, err)
	require.Equal(t, first, retry)
	_, err = stream.AddOnce(ctx, "key", "a", []byte("1:b"))
	require.ErrorIs(t, err, ErrIdempotencyConflict)
	_, err = stream.AddOnce(ctx, "large", "event", make([]byte, maxAddOnceBodyBytes+1))
	require.ErrorContains(t, err, "exceeds")
	require.NoError(t, stream.Destroy(ctx))
}

func TestSinkCloseTreatsDeadlineElapsedAsTerminal(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	deadline := time.Now().Add(2 * time.Second).Truncate(time.Millisecond)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamDeadline(deadline))
	require.NoError(t, err)
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkBlockDuration(25*time.Millisecond),
	)
	require.NoError(t, err)
	events := sink.Subscribe()
	require.Eventually(t, func() bool {
		return errors.Is(stream.verifyGeneration(ctx), ErrDeadlineElapsed)
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, sink.Close(ctx))
	_, open := <-events
	require.False(t, open)
	require.True(t, sink.IsClosed())
	require.NoError(t, stream.Destroy(ctx))
}

// DialHook preserves normal Redis dialing.
func (h *ambiguousAddOnceHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook injects one error only after Redis committed AddOnce.
func (h *ambiguousAddOnceHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err != nil || cmd.Name() != "evalsha" {
			return err
		}
		args := cmd.Args()
		if len(args) > 1 && args[1] == addOnceScript.Hash() &&
			h.fail.CompareAndSwap(true, false) {
			return h.err
		}
		return err
	}
}

// ProcessPipelineHook preserves Redis pipelines.
func (h *ambiguousAddOnceHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}
