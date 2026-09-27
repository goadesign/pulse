// These tests exercise Sink.Close with real Redis and real local map readers.
// They observe local shutdown separately from distributed detachment, including
// exact retries, pending events, partial setup, and automatic fatal-read cleanup.
package streaming

import (
	"context"
	"errors"
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

type (
	// sinkCloseContextKey distinguishes test Close commands from background I/O.
	sinkCloseContextKey struct{}

	// sinkCloseHook intercepts selected commands; every other operation uses
	// the real Redis client and the existing command-observation fixture.
	sinkCloseHook struct {
		redisCommandHook
		process func(context.Context, redis.Cmder) error
	}

	// sinkCloseObservation retains the actual attachment objects and their
	// notification channels before shutdown starts.
	sinkCloseObservation struct {
		states map[string]*sinkStream
		maps   map[*rmap.Map]<-chan rmap.EventKind
		events <-chan *Event
	}

	// sinkCloseGateLogger holds automatic cleanup's final error report so a
	// concurrent public Close must wait for that owned operation to finish.
	sinkCloseGateLogger struct {
		pulse.Logger
		entered  chan struct{}
		release  chan struct{}
		finished chan struct{}
		once     sync.Once
	}
)

func TestSinkCloseLocalCleanupRetainsDistributedRetry(t *testing.T) {
	for _, stage := range []string{"verification", "detachment", "canceled_context"} {
		t.Run(stage, func(t *testing.T) {
			rdb := ptesting.NewRedisClient(t)
			t.Cleanup(func() { ptesting.CleanupRedis(t, rdb, false, "") })
			ctx := ptesting.NewTestContext(t)
			name := strings.ReplaceAll(t.Name(), "/", "_")
			first, err := NewStream(name+"-first", rdb)
			require.NoError(t, err)
			second, err := NewStream(name+"-second", rdb)
			require.NoError(t, err)
			failure := errors.New("injected distributed cleanup failure")
			var commandLock sync.Mutex
			detachArgs := make(map[bool][][]any)
			hook := &sinkCloseHook{process: func(callCtx context.Context, cmd redis.Cmder) error {
				phase, marked := callCtx.Value(sinkCloseContextKey{}).(bool)
				if !marked || cmd.Name() != "evalsha" || cmd.Args()[3] != first.lifecycleKey {
					return nil
				}
				if cmd.Args()[1] == detachSinkConsumerScript.Hash() {
					commandLock.Lock()
					detachArgs[phase] = append(detachArgs[phase], append([]any(nil), cmd.Args()[3:]...))
					commandLock.Unlock()
					if phase && stage == "detachment" {
						return failure
					}
				}
				if phase && stage == "verification" && cmd.Args()[1] == verifyStreamScript.Hash() {
					return failure
				}
				return nil
			}}
			rdb.AddHook(hook)
			sink, err := first.NewSink(ctx, "sink",
				options.WithSinkStartAtOldest(),
				options.WithSinkBlockDuration(testBlockDuration),
				options.WithSinkAckGracePeriod(time.Hour))
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, sink.Close(ctx)) })
			require.NoError(t, sink.AddStream(ctx, second))
			observed := observeSinkClose(t, sink)
			consumer := sink.consumer
			generation := first.generation
			eventID, err := first.Add(ctx, "retained", []byte("caller data"))
			require.NoError(t, err)
			event := receiveSinkEvent(t, observed.events)
			require.Equal(t, eventID, event.ID)

			closeCtx := context.WithValue(ctx, sinkCloseContextKey{}, true)
			expectedErr := failure
			if stage == "canceled_context" {
				canceled, cancel := context.WithCancel(closeCtx)
				cancel()
				closeCtx = canceled
				expectedErr = context.Canceled
			}
			require.ErrorIs(t, sink.Close(closeCtx), expectedErr)
			assert.False(t, sink.IsClosed())
			assertSinkLocalCleanup(t, sink, observed)
			require.Same(t, observed.states[first.key], sink.streams[first.key])
			assert.Equal(t, consumer, sink.consumer)
			assert.Equal(t, generation, first.generation)
			if stage == "canceled_context" {
				assert.Len(t, sink.streams, 2)
			} else {
				assert.Len(t, sink.streams, 1)
				assert.NotContains(t, sink.streams, second.key)
			}
			membership, err := rdb.HGet(ctx, consumersMapContentKey(first), sink.Name).Result()
			require.NoError(t, err)
			assert.Contains(t, membership, consumer)
			alive, err := rdb.HExists(ctx, rmapContentKey(sinkKeepAliveMapName(first, sink.Name)), consumer).Result()
			require.NoError(t, err)
			assert.True(t, alive)
			assertSinkPendingEvent(t, ctx, rdb, first, sink.Name, consumer, eventID)

			require.NoError(t, sink.Close(context.WithValue(ctx, sinkCloseContextKey{}, false)))
			assert.True(t, sink.IsClosed())
			assert.Empty(t, sink.streams)
			assertSinkLocalCleanup(t, sink, observed)
			assertSinkPendingEvent(t, ctx, rdb, first, sink.Name, consumer, eventID)
			wantArgs := []any{
				first.lifecycleKey, first.key,
				consumersMapContentKey(first), consumersMapChannelKey(first),
				rmapContentKey(sinkKeepAliveMapName(first, sink.Name)),
				rmapChannelKey(sinkKeepAliveMapName(first, sink.Name)),
				streamStateActive, generation, sink.Name, consumer,
			}
			commandLock.Lock()
			assert.Equal(t, [][]any{wantArgs}, detachArgs[false])
			if stage == "detachment" {
				assert.Equal(t, [][]any{wantArgs}, detachArgs[true])
			}
			commandLock.Unlock()
			membersRemain, err := rdb.HExists(ctx, consumersMapContentKey(first), sink.Name).Result()
			require.NoError(t, err)
			assert.False(t, membersRemain)
			alive, err = rdb.HExists(ctx, rmapContentKey(sinkKeepAliveMapName(first, sink.Name)), consumer).Result()
			require.NoError(t, err)
			assert.False(t, alive)
		})
	}
}

func TestSinkCloseLocalCleanupConcurrentStalledSubscriber(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	t.Cleanup(func() { ptesting.CleanupRedis(t, rdb, false, "") })
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	failure := errors.New("injected concurrent detach failure")
	rdb.AddHook(&sinkCloseHook{process: func(callCtx context.Context, cmd redis.Cmder) error {
		if callCtx.Value(sinkCloseContextKey{}) == true && cmd.Name() == "evalsha" &&
			cmd.Args()[1] == detachSinkConsumerScript.Hash() && cmd.Args()[3] == stream.lifecycleKey {
			return failure
		}
		return nil
	}})
	sink, err := stream.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkBufferSize(1),
		options.WithSinkAckGracePeriod(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sink.Close(ctx)) })
	observed := observeSinkClose(t, sink)

	siblingStream, err := NewStream(t.Name()+"-sibling", rdb)
	require.NoError(t, err)
	sibling, err := siblingStream.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sibling.Close(ctx)) })
	siblingEvents := sibling.Subscribe()

	for range 5 {
		_, err := stream.Add(ctx, "queued", []byte("retain"))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return len(observed.events) == cap(observed.events)
	}, max, delay)

	const callers = 8
	start := make(chan struct{})
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			results <- sink.Close(context.WithValue(ctx, sinkCloseContextKey{}, true))
		}()
	}
	close(start)
	t.Cleanup(wait.Wait)
	for range callers {
		select {
		case closeErr := <-results:
			assert.ErrorIs(t, closeErr, failure)
		case <-time.After(max):
			t.Fatal("concurrent Close did not finish with a stalled subscriber")
		}
	}
	wait.Wait()
	assertSinkLocalCleanup(t, sink, observed)
	assert.False(t, sink.IsClosed())
	require.Same(t, observed.states[stream.key], sink.streams[stream.key])
	id, err := siblingStream.Add(ctx, "sibling", []byte("still available"))
	require.NoError(t, err)
	event := receiveSinkEvent(t, siblingEvents)
	assert.Equal(t, id, event.ID)
	require.NoError(t, sibling.Ack(ctx, event))
	assert.False(t, sibling.IsClosed())

	require.NoError(t, sink.Close(ctx))
	assert.True(t, sink.IsClosed())
	length, err := rdb.XLen(ctx, stream.key).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 5, length)
	pending, err := rdb.XPending(ctx, stream.key, sink.Name).Result()
	require.NoError(t, err)
	assert.Positive(t, pending.Count)
	assert.Positive(t, pending.Consumers[sink.consumer])
}

func TestSinkCloseLocalCleanupJoinsFatalRead(t *testing.T) {
	for _, origin := range []string{"read", "snapshot"} {
		t.Run(origin, func(t *testing.T) {
			rdb := ptesting.NewRedisClient(t)
			t.Cleanup(func() { ptesting.CleanupRedis(t, rdb, false, "") })
			ctx := ptesting.NewTestContext(t)
			logger := &sinkCloseGateLogger{
				Logger:   pulse.NoopLogger(),
				entered:  make(chan struct{}),
				release:  make(chan struct{}),
				finished: make(chan struct{}),
			}
			var release sync.Once
			var armed atomic.Bool
			var readCtx context.Context
			var snapshotVerifications atomic.Int64
			var readOnce, externalOnce sync.Once
			readStarted := make(chan struct{})
			trigger := make(chan struct{})
			externalAttempt := make(chan struct{})
			failure := errors.New("injected fatal cleanup verification failure")
			rdb.AddHook(&sinkCloseHook{process: func(callCtx context.Context, cmd redis.Cmder) error {
				if cmd.Name() == "xreadgroup" {
					readOnce.Do(func() { close(readStarted) })
					select {
					case <-trigger:
						if origin == "read" {
							return ErrStreamDestroyed
						}
						return redis.Nil
					case <-callCtx.Done():
						return callCtx.Err()
					}
				}
				if armed.Load() && cmd.Name() == "evalsha" && cmd.Args()[1] == verifyStreamScript.Hash() {
					// Generation-one verification may retry after checking
					// for a lost record. Keep that whole read operation fatal;
					// cleanup uses a different context and gets its own error.
					if origin == "snapshot" && callCtx == readCtx {
						snapshotVerifications.Add(1)
						return ErrStreamDestroyed
					}
					if callCtx.Value(sinkCloseContextKey{}) == true {
						externalOnce.Do(func() { close(externalAttempt) })
					}
					return failure
				}
				return nil
			}})
			stream, err := NewStream(strings.ReplaceAll(t.Name(), "/", "_"), rdb,
				options.WithStreamLogger(logger))
			require.NoError(t, err)
			sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
			require.NoError(t, err)
			t.Cleanup(func() {
				release.Do(func() { close(logger.release) })
				armed.Store(false)
				assert.NoError(t, sink.Close(ctx))
			})
			observed := observeSinkClose(t, sink)
			select {
			case <-readStarted:
			case <-time.After(max):
				t.Fatal("sink did not enter the actual read worker")
			}
			// Publish the exact read context before the hook observes armed.
			readCtx = sink.ctx
			armed.Store(true)
			close(trigger)
			select {
			case <-logger.entered:
			case <-time.After(max):
				t.Fatalf("fatal read did not reach automatic cleanup's error report (terminal snapshot verifications: %d)",
					snapshotVerifications.Load())
			}
			if origin == "snapshot" {
				attempts := snapshotVerifications.Load()
				t.Logf("terminal snapshot verifications, including generation-one retry: %d", attempts)
				require.EqualValues(t, 2, attempts,
					"both checks in generation-one verification must receive the terminal error")
			}
			assertSinkLocalCleanup(t, sink, observed)
			assert.False(t, sink.IsClosed())

			result := make(chan error, 1)
			var wait sync.WaitGroup
			wait.Add(1)
			go func() {
				defer wait.Done()
				result <- sink.Close(context.WithValue(ctx, sinkCloseContextKey{}, true))
			}()
			t.Cleanup(func() {
				release.Do(func() { close(logger.release) })
				armed.Store(false)
				wait.Wait()
			})
			select {
			case <-externalAttempt:
			case <-time.After(max):
				t.Fatal("public Close did not attempt its distributed cleanup")
			}
			received := false
			select {
			case closeErr := <-result:
				received = true
				assert.ErrorIs(t, closeErr, failure)
				t.Error("public Close returned while automatic cleanup was still reporting its error")
			case <-time.After(testBlockDuration):
			}
			release.Do(func() { close(logger.release) })
			if !received {
				select {
				case closeErr := <-result:
					assert.ErrorIs(t, closeErr, failure)
				case <-time.After(max):
					t.Fatal("public Close did not join released automatic cleanup")
				}
			}
			wait.Wait()
			assertSinkClosedChannel(t, logger.finished)
			assertSinkLocalCleanup(t, sink, observed)
			assert.False(t, sink.IsClosed())
			armed.Store(false)
			require.NoError(t, sink.Close(ctx))
			assert.True(t, sink.IsClosed())
		})
	}
}

func TestSinkCloseLocalCleanupPartialSetup(t *testing.T) {
	for _, stage := range []string{"second_map", "group", "consumer", "add_consumer"} {
		t.Run(stage, func(t *testing.T) {
			rdb := ptesting.NewRedisClient(t)
			t.Cleanup(func() { ptesting.CleanupRedis(t, rdb, false, "") })
			ctx := ptesting.NewTestContext(t)
			name := strings.ReplaceAll(t.Name(), "/", "_")
			stream, err := NewStream(name, rdb)
			require.NoError(t, err)
			require.NoError(t, stream.Open(ctx))

			channels := []string{
				consumersMapChannelKey(stream),
				rmapChannelKey(sinkKeepAliveMapName(stream, "sink")),
			}
			failure := errors.New("injected partial setup failure")
			var capture sync.Once
			var before map[string]int64
			var captureErr error
			rdb.AddHook(&sinkCloseHook{process: func(callCtx context.Context, cmd redis.Cmder) error {
				selected := stage == "second_map" && cmd.Name() == "hgetall" &&
					cmd.Args()[1] == rmapContentKey(sinkKeepAliveMapName(stream, "sink"))
				if cmd.Name() == "evalsha" && cmd.Args()[3] == stream.lifecycleKey {
					selected = (stage == "group" && cmd.Args()[1] == ensureConsumerGroupScript.Hash()) ||
						((stage == "consumer" || stage == "add_consumer") &&
							cmd.Args()[1] == registerSinkConsumerScript.Hash())
				}
				if !selected {
					return nil
				}
				capture.Do(func() {
					before, captureErr = rdb.PubSubNumSub(callCtx, channels...).Result()
				})
				return failure
			}})
			if stage == "add_consumer" {
				main, err := NewStream(name+"-main", rdb)
				require.NoError(t, err)
				sink, err := main.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
				require.NoError(t, err)
				t.Cleanup(func() { assert.NoError(t, sink.Close(ctx)) })
				require.ErrorIs(t, sink.AddStream(ctx, stream), failure)
				assert.NotContains(t, sink.streams, stream.key)
				assert.False(t, sink.IsClosed())
			} else {
				created, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
				assert.ErrorIs(t, err, failure)
				assert.Nil(t, created)
				if created != nil {
					assert.NoError(t, created.Close(ctx))
				}
			}
			require.NoError(t, captureErr)
			require.Equal(t, map[string]int64{channels[0]: 1, channels[1]: 1}, before)
			// Redis observes socket closure asynchronously; the bounded poll
			// checks its two actual subscriptions, not a global goroutine count.
			require.Eventually(t, func() bool {
				counts, err := rdb.PubSubNumSub(ctx, channels...).Result()
				return err == nil && counts[channels[0]] == 0 && counts[channels[1]] == 0
			}, max, delay)
		})
	}
}

func TestSinkCloseLocalCleanupRedisDetachError(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	t.Cleanup(func() { ptesting.CleanupRedis(t, rdb, false, "") })
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(testBlockDuration),
		options.WithSinkAckGracePeriod(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sink.Close(ctx)) })
	observed := observeSinkClose(t, sink)
	eventID, err := stream.Add(ctx, "retained", []byte("caller data"))
	require.NoError(t, err)
	require.Equal(t, eventID, receiveSinkEvent(t, observed.events).ID)
	key := rmapContentKey(sinkKeepAliveMapName(stream, sink.Name))
	saved, err := rdb.Dump(ctx, key).Result()
	require.NoError(t, err)
	restore := true
	// Replace only this test's keep-alive hash. The actual detach script must
	// reject its Redis type before changing membership or pending ownership.
	t.Cleanup(func() {
		if restore {
			assert.NoError(t, rdb.Do(ctx, "RESTORE", key, 0, saved, "REPLACE").Err())
		}
	})
	require.NoError(t, rdb.Set(ctx, key, "wrong type for detach", 0).Err())
	closeErr := sink.Close(ctx)
	require.ErrorContains(t, closeErr, "SINKKEEPALIVEINVALID")
	assert.False(t, sink.IsClosed())
	assertSinkLocalCleanup(t, sink, observed)
	require.Same(t, observed.states[stream.key], sink.streams[stream.key])
	assertSinkPendingEvent(t, ctx, rdb, stream, sink.Name, sink.consumer, eventID)

	require.NoError(t, rdb.Do(ctx, "RESTORE", key, 0, saved, "REPLACE").Err())
	restore = false
	require.NoError(t, sink.Close(ctx))
	assert.True(t, sink.IsClosed())
	assert.Empty(t, sink.streams)
	assertSinkPendingEvent(t, ctx, rdb, stream, sink.Name, sink.consumer, eventID)
}

// observeSinkClose subscribes to each real map before Close can stop it. Holding
// the sink mutex prevents consumer recovery or stream changes during capture.
func observeSinkClose(t *testing.T, sink *Sink) sinkCloseObservation {
	t.Helper()
	observed := sinkCloseObservation{
		states: make(map[string]*sinkStream),
		maps:   make(map[*rmap.Map]<-chan rmap.EventKind),
		events: sink.Subscribe(),
	}
	sink.lock.Lock()
	defer sink.lock.Unlock()
	for key, state := range sink.streams {
		observed.states[key] = state
		for _, replica := range []*rmap.Map{state.consumers, state.keepAlives} {
			changes := replica.Subscribe()
			require.NotNil(t, changes)
			observed.maps[replica] = changes
		}
	}
	return observed
}

// assertSinkLocalCleanup checks stop observations at the completed Close call,
// without retrying Close or waiting for eventual local cleanup.
func assertSinkLocalCleanup(t *testing.T, sink *Sink, observed sinkCloseObservation) {
	t.Helper()
	assert.ErrorIs(t, sink.ctx.Err(), context.Canceled)
	assertSinkClosedChannel(t, sink.donechan)
	assertSinkClosedChannel(t, observed.events)
	for replica, changes := range observed.maps {
		assertSinkClosedChannel(t, changes)
		assert.Nil(t, replica.Subscribe(), "map %s still accepts local subscribers", replica.Name)
	}
}

// assertSinkClosedChannel drains at most the channel's finite buffer and checks
// that it is already closed. An open or nil channel fails immediately.
func assertSinkClosedChannel[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	for i := 0; i <= cap(ch); i++ {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
		default:
			t.Error("owned subscription or completion channel is still open")
			return
		}
	}
	t.Error("owned channel did not close after its finite buffer")
}

// assertSinkPendingEvent verifies that local shutdown and distributed retry
// preserve the exact unacknowledged event and its consumer ownership.
func assertSinkPendingEvent(t *testing.T, ctx context.Context, rdb *redis.Client, stream *Stream, group, consumer, eventID string) {
	t.Helper()
	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream.key,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  2,
	}).Result()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, eventID, pending[0].ID)
	assert.Equal(t, consumer, pending[0].Consumer)
	events, err := rdb.XRange(ctx, stream.key, eventID, eventID).Result()
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "caller data", events[0].Values[payloadKey])
}

// ProcessHook applies the selected fault before the real command is sent.
func (h *sinkCloseHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	next = h.redisCommandHook.ProcessHook(next)
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := h.process(ctx, cmd); err != nil {
			return err
		}
		return next(ctx, cmd)
	}
}

// WithPrefix keeps all sink and map logging on the same test observation.
func (l *sinkCloseGateLogger) WithPrefix(_ ...any) pulse.Logger {
	return l
}

// Error holds only automatic cleanup's final error; other reports continue.
func (l *sinkCloseGateLogger) Error(err error, kvs ...any) {
	if strings.Contains(err.Error(), "failed to close terminal sink") {
		l.once.Do(func() {
			close(l.entered)
			<-l.release
			close(l.finished)
		})
	}
	l.Logger.Error(err, kvs...)
}
