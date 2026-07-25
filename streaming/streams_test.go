package streaming

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

func TestDestroy(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)

	s, err := NewStream("testDestroy", rdb)
	assert.NoError(t, err)
	require.ErrorIs(t, s.Destroy(ctx), ErrStreamNotFound)
	require.NoError(t, s.Open(ctx))
	assert.NoError(t, s.Destroy(ctx))
	exists, err := rdb.Exists(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)
	generation := s.Generation()
	require.NoError(t, s.Destroy(ctx))
	require.Equal(t, generation, s.Generation())

	s2, err := NewStream("testDestroy2", rdb)
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)
	exists, err = rdb.Exists(ctx, s2.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)
	assert.NoError(t, s2.Destroy(ctx))
}

func TestNewStreamLazilyLoadsActiveRedisGeneration(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)

	first, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	second, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.Empty(t, first.generation)
	require.Empty(t, second.generation)
	require.Zero(t, rdb.DBSize(ctx).Val())
	require.Equal(t, first.generation, second.generation)
	require.Equal(t, first.key, second.key)
	_, err = first.Add(ctx, "first", []byte("payload"))
	require.NoError(t, err)
	_, err = second.Add(ctx, "second", []byte("payload"))
	require.NoError(t, err)
	require.NotEmpty(t, first.generation)
	require.Equal(t, first.generation, second.generation)
	require.NoError(t, first.Destroy(ctx))
	require.NoError(t, second.Destroy(ctx))
}

func TestOptions(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream("testOptions", rdb, options.WithStreamMaxLen(10), options.WithStreamLogger(nil))
	assert.NoError(t, err)
	assert.Equal(t, 10, s.MaxLen)
	assert.Equal(t, pulse.NoopLogger(), s.logger)
	require.NoError(t, s.Open(ctx))
	assert.NoError(t, s.Destroy(ctx))
	_, err = NewStream("invalidMaxLen", rdb, options.WithStreamMaxLen(0))
	assert.EqualError(t, err, "pulse stream: maximum length must be greater than zero")
}

func TestAdd(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream("testAdd", rdb)
	assert.NoError(t, err)

	_, err = s.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)
	l, err := rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), l)
	v, err := rdb.XRange(ctx, s.key, "-", "+").Result()
	assert.NoError(t, err)
	assert.Equal(t, "foo", v[0].Values[nameKey])
	assert.Equal(t, "bar", v[0].Values[payloadKey])

	assert.NoError(t, s.Destroy(ctx))
}

func TestStreamTTLAbsolute(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)

	const ttl = 2 * time.Second
	s, err := NewStream("testStreamTTLAbsolute", rdb, options.WithStreamTTL(ttl))
	assert.NoError(t, err)

	_, err = s.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)

	time.Sleep(250 * time.Millisecond)
	before, err := rdb.PTTL(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Greater(t, before, time.Duration(0))
	assert.LessOrEqual(t, before, ttl)
	assert.Less(t, before, ttl-(100*time.Millisecond))

	_, err = s.Add(ctx, "foo2", []byte("bar2"))
	assert.NoError(t, err)
	after, err := rdb.PTTL(ctx, s.key).Result()
	assert.NoError(t, err)

	// Absolute TTL: adding more events must not refresh the expiry.
	assert.LessOrEqual(t, after, before)
	assert.Less(t, after, ttl-(100*time.Millisecond))

	assert.NoError(t, s.Destroy(ctx))
}

func TestStreamTTLSliding(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)

	const ttl = 2 * time.Second
	s, err := NewStream("testStreamTTLSliding", rdb, options.WithStreamSlidingTTL(ttl))
	assert.NoError(t, err)

	_, err = s.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)

	time.Sleep(250 * time.Millisecond)
	before, err := rdb.PTTL(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Greater(t, before, time.Duration(0))
	assert.LessOrEqual(t, before, ttl)
	assert.Less(t, before, ttl-(100*time.Millisecond))

	_, err = s.Add(ctx, "foo2", []byte("bar2"))
	assert.NoError(t, err)
	after, err := rdb.PTTL(ctx, s.key).Result()
	assert.NoError(t, err)

	// Sliding TTL: adding events refreshes the expiry back toward ttl.
	assert.Greater(t, after, ttl-(500*time.Millisecond))

	assert.NoError(t, s.Destroy(ctx))
}

func TestStreamDestroyDeletesGenerationMembership(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)

	s, err := NewStream("testStreamDestroyDeletesSinkMap", rdb)
	assert.NoError(t, err)

	sink, err := s.NewSink(ctx, "gateway")
	assert.NoError(t, err)
	assert.True(t, rdb.HExists(ctx, recoveryCursorKey(s), sink.Name).Val())
	require.NoError(t, sink.Close(ctx))

	mapKey := consumersMapContentKey(s)
	exists, err := rdb.Exists(ctx, mapKey).Result()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, exists)

	assert.NoError(t, s.Destroy(ctx))
	assert.EqualValues(t, 0, rdb.Exists(ctx, mapKey).Val())
	assert.EqualValues(t, 0, rdb.Exists(ctx, recoveryCursorKey(s)).Val())
}

func TestStreamDestroyInvalidatesExactGeneration(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := s.NewSink(ctx, "gateway", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	events := sink.Subscribe()
	eventID, err := s.Add(ctx, "before-destroy", []byte("payload"))
	require.NoError(t, err)
	event := receiveSinkEvent(t, events)
	require.Equal(t, eventID, event.ID)

	require.NoError(t, s.Destroy(ctx))
	require.EqualValues(t, 0, rdb.Exists(ctx, s.key).Val())
	require.EqualValues(t, 0, rdb.Exists(ctx, recoveryCursorKey(s)).Val())
	require.EqualValues(t, 0, rdb.Exists(ctx, consumersMapContentKey(s)).Val())
	require.Eventually(t, sink.IsClosed, max, delay)
	_, err = s.Add(ctx, "stale", []byte("payload"))
	require.ErrorIs(t, err, ErrStreamDestroyed)
	require.ErrorIs(t, s.Remove(ctx, eventID), ErrStreamDestroyed)
	require.ErrorIs(t, sink.AddStream(ctx, s), ErrSinkClosed)
	require.ErrorIs(t, sink.RemoveStream(ctx, s), ErrSinkClosed)
	require.ErrorIs(t, sink.Ack(ctx, event), ErrStreamDestroyed)
	_, err = s.NewReader(ctx)
	require.ErrorIs(t, err, ErrStreamDestroyed)
	_, err = s.NewSink(ctx, "new-sink")
	require.ErrorIs(t, err, ErrStreamDestroyed)
	require.EqualValues(t, 0, rdb.Exists(ctx, consumersMapContentKey(s)).Val())

	late, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	_, err = late.Add(ctx, "late", []byte("payload"), options.WithOnlyIfStreamExists())
	require.ErrorIs(t, err, ErrStreamDestroyed)
	require.Equal(t, s.generation, rdb.HGet(ctx, s.lifecycleKey, "generation").Val())
	require.Equal(t, streamStateDestroyed, rdb.HGet(ctx, s.lifecycleKey, "state").Val())

	next, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.Empty(t, next.generation)
	require.Equal(t, s.key, next.key)
	_, err = next.Add(ctx, "fresh", []byte("payload"))
	require.NoError(t, err)
	require.NotEqual(t, s.generation, next.generation)
	require.NotEqual(t, s.key, next.key)
	require.EqualValues(t, 0, rdb.Exists(ctx, s.key).Val())
	require.EqualValues(t, 1, rdb.Exists(ctx, next.key).Val())
	require.EqualValues(t, 0, rdb.Exists(ctx, recoveryCursorKey(s)).Val())
	require.NoError(t, next.Destroy(ctx))
}

func TestReaderStopsWhenGenerationIsDestroyed(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	reader, err := stream.NewReader(ctx, options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	reader.Subscribe()

	require.NoError(t, stream.Destroy(ctx))
	require.Eventually(t, reader.IsClosed, max, delay)
}

func TestRecreatedStreamIsIsolatedFromStaleReadersAndSinks(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	reader, err := stream.NewReader(ctx, options.WithReaderStartAtOldest(), options.WithReaderBlockDuration(testBlockDuration))
	require.NoError(t, err)
	readerEvents := reader.Subscribe()
	sink, err := stream.NewSink(
		ctx,
		"sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	sinkEvents := sink.Subscribe()

	require.NoError(t, stream.Destroy(ctx))
	require.Eventually(t, reader.IsClosed, max, delay)
	require.Eventually(t, sink.IsClosed, max, delay)

	recreated, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	freshID, err := recreated.Add(ctx, "fresh", []byte("payload"))
	require.NoError(t, err)
	require.NotEqual(t, stream.key, recreated.key)
	select {
	case event := <-readerEvents:
		require.Nil(t, event)
	default:
	}
	select {
	case event := <-sinkEvents:
		require.Nil(t, event)
	default:
	}

	freshReader, err := recreated.NewReader(
		ctx,
		options.WithReaderStartAtOldest(),
		options.WithReaderBlockDuration(testBlockDuration),
	)
	require.NoError(t, err)
	event := receiveSinkEvent(t, freshReader.Subscribe())
	require.Equal(t, freshID, event.ID)
	freshReader.Close()
	require.NoError(t, recreated.Destroy(ctx))
}

func TestRepeatedStreamGenerationsLeaveBoundedKeys(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	for range 10 {
		stream, err := NewStream(t.Name(), rdb)
		require.NoError(t, err)
		sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
		require.NoError(t, err)
		require.NoError(t, sink.Close(ctx))
		require.NoError(t, stream.Destroy(ctx))
	}

	generationKeys, err := rdb.Keys(ctx, streamKeyPrefix+t.Name()+":generation:*").Result()
	require.NoError(t, err)
	require.Empty(t, generationKeys)
	streamKeys, err := rdb.Keys(ctx, streamKeyPrefix+t.Name()+"*").Result()
	require.NoError(t, err)
	require.Equal(t, []string{streamLifecycleKey(t.Name())}, streamKeys)
}

func TestConcurrentRemoveAndDestroyNeverRecreatesGeneration(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	mainStream, err := NewStream(t.Name()+"-main", rdb)
	require.NoError(t, err)
	addedStream, err := NewStream(t.Name()+"-added", rdb)
	require.NoError(t, err)
	sink, err := mainStream.NewSink(ctx, "gateway", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	require.NoError(t, sink.AddStream(ctx, addedStream))

	start := make(chan struct{})
	removeResult := make(chan error, 1)
	destroyResult := make(chan error, 1)
	go func() {
		<-start
		removeResult <- sink.RemoveStream(ctx, addedStream)
	}()
	go func() {
		<-start
		destroyResult <- addedStream.Destroy(ctx)
	}()
	close(start)

	removeErr := <-removeResult
	if removeErr != nil {
		require.True(
			t,
			errors.Is(removeErr, ErrStreamDestroyed) || errors.Is(removeErr, ErrSinkClosed),
			"unexpected remove result: %v",
			removeErr,
		)
	}
	destroyErr := <-destroyResult
	require.NoError(t, destroyErr)
	time.Sleep(4 * testBlockDuration)
	require.EqualValues(t, 0, rdb.Exists(ctx, addedStream.key, recoveryCursorKey(addedStream)).Val())
	require.NoError(t, sink.Close(ctx))
	require.NoError(t, mainStream.Destroy(ctx))
}

func TestRemove(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream("testRemove", rdb)
	assert.NoError(t, err)

	_, err = s.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)
	_, err = s.Add(ctx, "foo2", []byte("bar2"))
	assert.NoError(t, err)

	l, err := rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), l)

	v, err := rdb.XRange(ctx, s.key, "-", "+").Result()
	assert.NoError(t, err)
	assert.NoError(t, s.Remove(ctx, v[0].ID))

	l, err = rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), l)

	assert.NoError(t, s.Destroy(ctx))
}

func TestTopic(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream("testTopic", rdb)
	assert.NoError(t, err)

	_, err = s.Add(ctx, "bar", []byte("baz"), options.WithTopic("foo"))
	assert.NoError(t, err)

	l, err := rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), l)
	v, err := rdb.XRange(ctx, s.key, "-", "+").Result()
	assert.NoError(t, err)
	assert.Equal(t, "foo", v[0].Values[topicKey])
	assert.Equal(t, "bar", v[0].Values[nameKey])
	assert.Equal(t, "baz", v[0].Values[payloadKey])

	assert.NoError(t, s.Destroy(ctx))
}
