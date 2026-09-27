// These tests use only the existing leased synthetic Redis test database.
// They execute the real replay Lua and real Add/AddOnce trimming. Their known
// fixture sizes prove functional admission, not production entry-size capacity.
package streaming

import (
	"context"
	"math"
	"strings"
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
	replayObserver struct {
		after func(context.Context, redis.Cmder, error) error
	}
)

func TestReplayReaderRedisEstablishedEmptyAndReadOnly(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	_, err = s.NewReplayReader(ctx, "0-0", replayTestOptions())
	require.ErrorIs(t, err, ErrStreamNotFound)
	_, err = s.NewReplayReader(ctx, "1-0", replayTestOptions())
	require.ErrorIs(t, err, ErrReplayPositionUnavailable)
	require.Zero(t, rdb.Exists(ctx, s.lifecycleKey, s.key).Val())

	require.NoError(t, s.Open(ctx))
	// Compare every hash field/value; DUMP serialization order is not canonical.
	before, err := rdb.HGetAll(ctx, s.lifecycleKey).Result()
	require.NoError(t, err)
	var commands sync.Map
	waits := make(chan struct{}, 4)
	client := redis.NewClient(rdb.Options())
	defer func() {
		require.NoError(t, client.Close())
	}()
	client.AddHook(&replayObserver{after: func(ctx context.Context, cmd redis.Cmder, err error) error {
		commands.Store(cmd.Name(), true)
		if cmd.Name() == "xread" {
			select {
			case waits <- struct{}{}:
			default:
			}
		}
		return err
	}})
	handle, err := NewStream(t.Name(), client)
	require.NoError(t, err)
	r, err := handle.NewReplayReader(ctx, "0-0", replayTestOptions())
	require.NoError(t, err)
	defer r.Close()
	_, hasAnchor := r.Anchor()
	require.False(t, hasAnchor)
	awaitReplaySignal(t, waits)
	awaitReplaySignal(t, waits)
	select {
	case <-r.Subscribe():
		t.Fatal("empty established history or a tail timeout must not be EOF")
	default:
	}
	r.Close()
	require.NoError(t, r.Err())
	after, err := rdb.HGetAll(ctx, s.lifecycleKey).Result()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Zero(t, rdb.Exists(ctx, s.key).Val())
	commands.Range(func(key, value any) bool {
		// Client connection initialization is not stream initialization.
		require.Contains(t, []string{"evalsha", "eval", "xread", "hello", "client"}, key)
		return true
	})
	require.NoError(t, client.Ping(ctx).Err(), "Close must not close the caller's Redis client")
}

func TestReplayReaderRedisExactCompletePrefixBounds(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb, options.WithUnboundedStream())
	require.NoError(t, err)
	require.NoError(t, s.Open(ctx))
	for _, id := range []string{"1-0", "2-0", "3-0"} {
		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: s.key, ID: id, Values: []any{"n", "e", "p", "xx", "t", "t"},
		}).Result()
		require.NoError(t, err)
	}
	// Each event is exactly 3 ID + 1 name + 2 payload + 1 topic = 7 bytes.
	for _, test := range []struct {
		name  string
		count int64
		bytes int64
		want  int
	}{
		{"below_one", 3, 6, 0},
		{"at_one", 3, 7, 1},
		{"below_two", 3, 13, 1},
		{"at_two", 3, 14, 2},
		{"count_one", 1, 21, 1},
		{"count_two", 2, 21, 2},
		{"all", 3, 21, 3},
		{"word_borrow", math.MaxInt64, 1<<32 + 1, 3},
		{"full_int64", math.MaxInt64, math.MaxInt64, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := replayTestOptions()
			opts.MaxEvents, opts.MaxBytes = test.count, test.bytes
			r, batch, err := replayFetchFixture(ctx, s, "0-0", opts)
			if test.want == 0 {
				require.ErrorIs(t, err, ErrReplayEventTooLarge)
				require.Nil(t, batch)
				return
			}
			require.NoError(t, err)
			require.Len(t, batch, test.want)
			if test.want < 3 {
				r.position = batch[len(batch)-1].ID
				next, err := r.fetch(false)
				require.NoError(t, err)
				require.Equal(t, []string{"2-0", "3-0"}[test.want-1], next[0].ID)
			}
		})
	}
	opts := replayTestOptions()
	opts.MaxBytes = 7
	r, batch, err := replayFetchFixture(ctx, s, "1-0", opts)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, "2-0", batch[0].ID)
	anchor, ok := r.Anchor()
	require.True(t, ok)
	require.Equal(t, "1-0", anchor.ID(), "opening anchor has its own B allowance")
	opts.MaxBytes = 6
	_, _, err = replayFetchFixture(ctx, s, "1-0", opts)
	require.ErrorIs(t, err, ErrReplayEventTooLarge)
}

func TestReplayReaderRedisUninitializedPhysicalDoesNotRepair(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.key, Values: []any{"n", "orphan", "p", "synthetic"},
	}).Result()
	require.NoError(t, err)
	before, err := rdb.Dump(ctx, s.key).Result()
	require.NoError(t, err)
	_, err = s.NewReplayReader(ctx, "0-0", replayTestOptions())
	require.ErrorIs(t, err, ErrStreamNotFound)
	_, err = s.NewReplayReader(ctx, id, replayTestOptions())
	require.ErrorIs(t, err, ErrReplayPositionUnavailable)
	after, err := rdb.Dump(ctx, s.key).Result()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Zero(t, rdb.Exists(ctx, s.lifecycleKey).Val())
	require.Empty(t, s.Generation())
}

func TestReplayReaderRedisOversizeAfterCompletedPrefix(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	first, err := s.Add(ctx, "small", []byte("x"))
	require.NoError(t, err)
	// This unkeyed Add intentionally exceeds B. It demonstrates why B is an
	// output allowance, not a pre-materialization ceiling or producer policy.
	_, err = s.Add(ctx, "large", []byte(strings.Repeat("x", 8192)))
	require.NoError(t, err)
	opts := replayTestOptions()
	opts.MaxBytes = int64(len(first) + len("small") + 1)
	r, err := s.NewReplayReader(ctx, "0-0", opts)
	require.NoError(t, err)
	defer r.Close()
	require.Equal(t, first, takeReplayEvent(t, r).ID)
	awaitReplayClosed(t, r)
	require.ErrorIs(t, r.Err(), ErrReplayEventTooLarge)
	require.NotErrorIs(t, r.Err(), ErrReplayPositionUnavailable)
}

func TestReplayReaderRedisTailWakeupDeliversCheckedSuccessor(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, s.Open(ctx))
	waits := make(chan struct{}, 4)
	var checks atomic.Int64
	rdb.AddHook(&replayObserver{after: func(ctx context.Context, cmd redis.Cmder, err error) error {
		if err == nil && cmd.Name() == "evalsha" && cmd.Args()[1] == replayReadScript.Hash() {
			checks.Add(1)
		}
		if cmd.Name() == "xread" {
			select {
			case waits <- struct{}{}:
			default:
			}
		}
		return err
	}})
	r, err := s.NewReplayReader(ctx, "0-0", replayTestOptions())
	require.NoError(t, err)
	defer r.Close()
	awaitReplaySignal(t, waits)
	before := checks.Load()
	id, err := s.Add(ctx, "live", []byte("exact\x00body"), options.WithTopic("topic"))
	require.NoError(t, err)
	event := takeReplayEvent(t, r)
	require.Equal(t, id, event.ID)
	require.Equal(t, "live", event.EventName)
	require.Equal(t, "topic", event.Topic)
	require.Equal(t, []byte("exact\x00body"), event.Payload)
	require.Greater(t, checks.Load(), before, "a successful atomic recheck must precede delivery")
	r.Close()
	require.NoError(t, r.Err())
}

func TestReplayReaderRedisTrimBeforeOpenAndBetweenBatches(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb, options.WithStreamMaxLen(2),
		options.WithStreamTTL(10*time.Second))
	require.NoError(t, err)
	first, err := s.AddOnce(ctx, "first-key", "first", []byte("one"))
	require.NoError(t, err)
	second, err := s.Add(ctx, "second", []byte("two"))
	require.NoError(t, err)
	opts := replayTestOptions()
	opts.MaxEvents = 2
	r, err := s.NewReplayReader(ctx, "0-0", opts)
	require.NoError(t, err)
	defer r.Close()
	for _, value := range []string{"third", "fourth", "fifth"} {
		_, err := s.Add(ctx, value, []byte(value))
		require.NoError(t, err)
	}
	_, err = s.NewReplayReader(ctx, first, opts)
	require.ErrorIs(t, err, ErrReplayPositionUnavailable)
	// The copied prefix is still valid while a slow consumer is stalled.
	require.Equal(t, first, takeReplayEvent(t, r).ID)
	require.Equal(t, second, takeReplayEvent(t, r).ID)
	awaitReplayClosed(t, r)
	require.ErrorIs(t, r.Err(), ErrReplayPositionUnavailable)
}

func TestReplayReaderRedisRechecksAfterWakeup(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb, options.WithStreamMaxLen(2))
	require.NoError(t, err)
	first, err := s.Add(ctx, "anchor", []byte("one"))
	require.NoError(t, err)
	client := redis.NewClient(rdb.Options())
	defer func() {
		require.NoError(t, client.Close())
	}()
	copied, release := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	client.AddHook(&replayObserver{after: func(ctx context.Context, cmd redis.Cmder, err error) error {
		if cmd.Name() == "xread" && err == nil && held.CompareAndSwap(false, true) {
			close(copied)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return err
	}})
	handle, err := NewStream(t.Name(), client)
	require.NoError(t, err)
	r, err := handle.NewReplayReader(ctx, first, replayTestOptions())
	require.NoError(t, err)
	defer r.Close()
	_, err = s.Add(ctx, "wakeup", []byte("two"))
	require.NoError(t, err)
	awaitReplaySignal(t, copied)
	for _, value := range []string{"third", "fourth"} {
		_, err := s.Add(ctx, value, []byte(value))
		require.NoError(t, err)
	}
	close(release)
	awaitReplayClosed(t, r)
	require.ErrorIs(t, r.Err(), ErrReplayPositionUnavailable,
		"the unchecked wakeup entry must never escape")
}

func TestReplayReaderRedisLifecycleTermination(t *testing.T) {
	for _, mode := range []string{"destroy", "recreate", "missing", "physical_expiry", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			rdb := ptesting.NewRedisClient(t)
			defer ptesting.CleanupRedis(t, rdb, false, "")
			ctx := ptesting.NewTestContext(t)
			var opts []options.Stream
			if mode == "deadline" {
				opts = append(opts, options.WithStreamDeadline(time.Now().Add(500*time.Millisecond)))
			}
			s, err := NewStream(t.Name(), rdb, opts...)
			require.NoError(t, err)
			first, err := s.Add(ctx, "anchor", []byte("one"))
			require.NoError(t, err)
			r, err := s.NewReplayReader(ctx, first, replayTestOptions())
			require.NoError(t, err)
			defer r.Close()
			switch mode {
			case "destroy", "recreate":
				require.NoError(t, s.Destroy(ctx))
				if mode == "recreate" {
					fresh, err := NewStream(t.Name(), rdb)
					require.NoError(t, err)
					_, err = fresh.Add(ctx, "new generation", []byte("not delivered"))
					require.NoError(t, err)
				}
			case "missing":
				require.NoError(t, rdb.Del(ctx, s.lifecycleKey).Err())
			case "physical_expiry":
				require.NoError(t, rdb.PExpire(ctx, s.key, time.Millisecond).Err())
			}
			awaitReplayClosed(t, r)
			require.ErrorIs(t, r.Err(), ErrReplayPositionUnavailable)
			if mode == "destroy" || mode == "recreate" {
				require.ErrorIs(t, r.Err(), ErrStreamDestroyed)
			}
			if mode == "deadline" {
				require.ErrorIs(t, r.Err(), ErrDeadlineElapsed)
			}
		})
	}
}

func TestReplayReaderRedisAdoptsExistingRetention(t *testing.T) {
	for _, streamOptions := range [][]options.Stream{
		{options.WithStreamMaxLen(9)},
		{options.WithUnboundedStream()},
		{options.WithStreamTTL(time.Second)},
		{options.WithStreamSlidingTTL(time.Second)},
		{options.WithStreamDeadline(time.Now().Add(10 * time.Second))},
	} {
		rdb := ptesting.NewRedisClient(t)
		func() {
			defer ptesting.CleanupRedis(t, rdb, false, "")
			ctx := ptesting.NewTestContext(t)
			writer, err := NewStream(t.Name(), rdb, streamOptions...)
			require.NoError(t, err)
			require.NoError(t, writer.Open(ctx))
			handle, err := NewStream(t.Name(), rdb)
			require.NoError(t, err)
			r, err := handle.NewReplayReader(ctx, "0-0", replayTestOptions())
			require.NoError(t, err)
			r.Close()
			require.Equal(t, writer.Generation(), handle.Generation())
			require.Equal(t, writer.retention, handle.retention)
			conflicting, err := NewStream(t.Name(), rdb, options.WithStreamMaxLen(77))
			require.NoError(t, err)
			_, err = conflicting.NewReplayReader(ctx, "0-0", replayTestOptions())
			require.ErrorIs(t, err, ErrStreamConfigMismatch)
			require.NotErrorIs(t, err, ErrReplayPositionUnavailable)
		}()
	}
}

func TestReplayReaderRedisMalformedLifecycleDoesNotRepair(t *testing.T) {
	for _, test := range []struct {
		name  string
		field string
		value string
		drop  bool
	}{
		{"missing_retention", streamConfigKey, "", true},
		{"missing_physical", streamPhysicalKey, "", true},
		{"missing_generation", "generation", "", true},
		{"foreign_physical", streamPhysicalKey, "not-this-stream", false},
		{"bad_state", "state", "unknown", false},
		{"bad_generation", "generation", "01", false},
		{"bad_retention", streamConfigKey, "v=2|max=1|mode=none|value=1|sliding=false", false},
		{"unexpected_deadline", streamDeadlineKey, "broken", false},
		{"unexpected_ttl", streamTTLOwnedKey, "1", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			rdb := ptesting.NewRedisClient(t)
			defer ptesting.CleanupRedis(t, rdb, false, "")
			ctx := ptesting.NewTestContext(t)
			s, err := NewStream(t.Name(), rdb)
			require.NoError(t, err)
			require.NoError(t, s.Open(ctx))
			if test.drop {
				require.NoError(t, rdb.HDel(ctx, s.lifecycleKey, test.field).Err())
			} else {
				require.NoError(t, rdb.HSet(ctx, s.lifecycleKey, test.field, test.value).Err())
			}
			// Preserve the complete malformed hash, including missing or extra fields.
			// DUMP byte order can change without a field/value change.
			before, err := rdb.HGetAll(ctx, s.lifecycleKey).Result()
			require.NoError(t, err)
			_, err = s.NewReplayReader(ctx, "0-0", replayTestOptions())
			require.ErrorIs(t, err, ErrStreamConfigMismatch)
			require.NotErrorIs(t, err, ErrReplayPositionUnavailable)
			after, err := rdb.HGetAll(ctx, s.lifecycleKey).Result()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Zero(t, rdb.Exists(ctx, s.key).Val())
		})
	}
}

func TestReplayReaderRedisMalformedEntryIsDependencyFailure(t *testing.T) {
	for _, fields := range [][]any{
		{"n", "event"},
		{"n", "", "p", "body"},
		{"n", "event", "t", "missing-payload"},
		{"n", "event", "p", "body", "x", "extra"},
		{"n", "event", "p", "body", "n", "duplicate"},
		{"n", "event", "p", "first", "p", "duplicate"},
	} {
		rdb := ptesting.NewRedisClient(t)
		func() {
			defer ptesting.CleanupRedis(t, rdb, false, "")
			ctx := ptesting.NewTestContext(t)
			s, err := NewStream(t.Name(), rdb)
			require.NoError(t, err)
			require.NoError(t, s.Open(ctx))
			id, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: s.key, Values: fields}).Result()
			require.NoError(t, err)
			for _, position := range []string{"0-0", id} {
				_, err = s.NewReplayReader(ctx, position, replayTestOptions())
				require.ErrorContains(t, err, "malformed replay event")
				require.NotErrorIs(t, err, ErrReplayPositionUnavailable)
				require.NotErrorIs(t, err, ErrReplayEventTooLarge)
			}
		}()
	}
}

func TestReplayReaderRedisExactUint64IDsAndOpaqueBodies(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	s, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	require.NoError(t, s.Open(ctx))
	ids := []string{"9007199254740992-1", "9007199254740992-2", "18446744073709551615-18446744073709551615"}
	body := string([]byte{0, '\n', '$', 0xff})
	for _, id := range ids {
		_, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: s.key, ID: id, Values: []any{"n", "event", "p", body},
		}).Result()
		require.NoError(t, err)
	}
	r, batch, err := replayFetchFixture(ctx, s, ids[0], replayTestOptions())
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.Equal(t, ids[1:], []string{batch[0].ID, batch[1].ID})
	require.Equal(t, []byte(body), batch[0].Payload)
	anchor, ok := r.Anchor()
	require.True(t, ok)
	require.Equal(t, []byte(body), anchor.Payload())
	// Sibling/child/application ownership is not a Pulse field or filter.
	require.Equal(t, "event", anchor.EventName())
	r.position = ids[2]
	tail, err := r.fetch(false)
	require.NoError(t, err)
	require.Empty(t, tail, "maximum uint64 ID is a valid tail, not an interval error")
	_, tail, err = replayFetchFixture(ctx, s, ids[2], replayTestOptions())
	require.NoError(t, err)
	require.Empty(t, tail)
}

// replayFetchFixture uses the production opening/fetch path without starting a
// delivery goroutine, so the tests can inspect one exact atomic batch.
func replayFetchFixture(ctx context.Context, s *Stream, position string, opts ReplayReaderOptions) (*ReplayReader, []*Event, error) {
	r := &ReplayReader{
		options: opts, name: s.Name, lifecycleKey: s.lifecycleKey, baseKey: streamKey(s.Name),
		position: position, ctx: ctx, rdb: s.rdb,
	}
	batch, err := r.open(s)
	return r, batch, err
}

func (h *replayObserver) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *replayObserver) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		return h.after(ctx, cmd, err)
	}
}

func (h *replayObserver) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
