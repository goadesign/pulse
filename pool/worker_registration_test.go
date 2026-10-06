// These tests pause worker creation after its Redis registration is saved.
// Another pool node then checks that worker for cleanup, so a new worker must
// already have a current heartbeat before cleanup can observe it.
package pool

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	ptesting "goa.design/pulse/testing"
)

type (
	workerRegistrationContextKey       struct{}
	workerRegistrationReplayContextKey struct{}

	workerRegistrationHook struct {
		workersKey string
		rdb        *redis.Client
		registered chan string
		resume     chan struct{}
		once       sync.Once
		replyError error
		args       []any
		replaying  chan struct{}
		replay     chan struct{}
	}

	workerRegistrationResult struct {
		worker *Worker
		err    error
	}
)

func TestWorkerRegistrationIncludesFirstHeartbeat(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx, cancel := context.WithTimeout(ptesting.NewTestContext(t), 20*time.Second)
	defer cancel()
	node := newTestNode(t, ctx, rdb, t.Name())
	other := newTestNode(t, ctx, rdb, t.Name())
	defer func() {
		assert.NoError(t, other.Close(context.Background()))
		assert.NoError(t, node.Shutdown(context.Background()))
	}()
	hook := &workerRegistrationHook{
		workersKey: rmapContentKey(node.resources.workers),
		rdb:        rdb,
		registered: make(chan string, 1),
		resume:     make(chan struct{}),
	}
	rdb.AddHook(hook)
	result := make(chan workerRegistrationResult, 1)
	go func() {
		worker, err := node.AddWorker(
			context.WithValue(ctx, workerRegistrationContextKey{}, true),
			newMockJobHandler(),
		)
		result <- workerRegistrationResult{worker: worker, err: err}
	}()
	var workerID string
	select {
	case workerID = <-hook.registered:
	case <-ctx.Done():
		close(hook.resume)
		<-result
		require.NoError(t, ctx.Err(), "worker registration was not observed")
	}
	lease, err := other.acquireWorkerCleanup(ctx, workerID)
	assert.NoError(t, err)
	assert.Nil(t, lease, "cleanup must not acquire a newly registered worker")
	close(hook.resume)
	got := <-result
	assert.NoError(t, got.err)
	assert.NotNil(t, got.worker)
	if lease != nil {
		assert.NoError(t, other.releaseWorkerCleanup(ctx, lease, false))
	}
}

func TestWorkerRegistrationCannotReviveRetiredWorker(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx, cancel := context.WithTimeout(ptesting.NewTestContext(t), 20*time.Second)
	defer cancel()
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() {
		assert.NoError(t, node.Shutdown(context.Background()))
	}()
	worker := newTestWorker(t, ctx, node)

	// Stop local heartbeat writers while keeping the stream active, matching
	// the constructor's state when a Redis operation is repeated.
	require.NoError(t, worker.stopLocal())
	require.NoError(t, node.registerWorker(ctx, worker.ID, worker.CreatedAt, worker.stream))
	createdAt, err := rdb.HGet(ctx, rmapContentKey(node.resources.workers), worker.ID).Result()
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(worker.CreatedAt.UnixNano(), 10), createdAt)

	require.NoError(t, node.RemoveWorker(ctx, worker))
	err = node.registerWorker(ctx, worker.ID, worker.CreatedAt, worker.stream)
	require.ErrorContains(t, err, "STREAMDESTROYED")
	registered, err := rdb.HExists(ctx, rmapContentKey(node.resources.workers), worker.ID).Result()
	require.NoError(t, err)
	assert.False(t, registered)
}

func TestWorkerRegistrationRejectsCleanupInProgress(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx, cancel := context.WithTimeout(ptesting.NewTestContext(t), 20*time.Second)
	defer cancel()
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() {
		assert.NoError(t, node.Shutdown(context.Background()))
	}()
	worker := newTestWorker(t, ctx, node)
	lease, err := node.acquireGracefulRequeue(ctx, worker.ID)
	require.NoError(t, err)
	require.NotNil(t, lease)
	err = node.registerWorker(ctx, worker.ID, worker.CreatedAt, worker.stream)
	assert.ErrorContains(t, err, "WORKERCLEANUPLOST")
	assert.NoError(t, node.releaseWorkerCleanup(ctx, lease, false))
}

func TestWorkerRegistrationFailedReplyRemovesBothEntries(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx, cancel := context.WithTimeout(ptesting.NewTestContext(t), 20*time.Second)
	defer cancel()
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() {
		assert.NoError(t, node.Shutdown(context.Background()))
	}()
	replyError := errors.New("registration reply unavailable")
	hook := &workerRegistrationHook{
		workersKey: rmapContentKey(node.resources.workers),
		rdb:        rdb,
		registered: make(chan string, 1),
		resume:     make(chan struct{}),
		replyError: replyError,
	}
	close(hook.resume)
	rdb.AddHook(hook)

	worker, err := node.AddWorker(
		context.WithValue(ctx, workerRegistrationContextKey{}, true),
		newMockJobHandler(),
	)
	require.ErrorIs(t, err, replyError)
	assert.Nil(t, worker)
	workerID := <-hook.registered
	for _, name := range []string{node.resources.workers, node.resources.workerKeepAlive} {
		saved, err := rdb.HExists(ctx, rmapContentKey(name), workerID).Result()
		require.NoError(t, err)
		assert.False(t, saved, "failed creation must remove %s", name)
	}
	state, err := rdb.HGet(ctx, "pulse:stream:"+workerStreamName(workerID)+":lifecycle", "state").Result()
	require.NoError(t, err)
	assert.Equal(t, "destroyed", state)
}

func TestWorkerRegistrationRollbackPreservesForeignCleanupClaim(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx, cancel := context.WithTimeout(ptesting.NewTestContext(t), 20*time.Second)
	defer cancel()
	node := newTestNode(t, ctx, rdb, t.Name())
	other := newTestNode(t, ctx, rdb, t.Name())
	defer func() {
		assert.NoError(t, other.Close(context.Background()))
		assert.NoError(t, node.Shutdown(context.Background()))
	}()
	replyError := errors.New("registration reply unavailable")
	hook := &workerRegistrationHook{
		workersKey: rmapContentKey(node.resources.workers),
		rdb:        rdb,
		registered: make(chan string, 1),
		resume:     make(chan struct{}),
		replyError: replyError,
	}
	rdb.AddHook(hook)
	result := make(chan workerRegistrationResult, 1)
	go func() {
		worker, err := node.AddWorker(
			context.WithValue(ctx, workerRegistrationContextKey{}, true),
			newMockJobHandler(),
		)
		result <- workerRegistrationResult{worker: worker, err: err}
	}()
	var workerID string
	select {
	case workerID = <-hook.registered:
	case <-ctx.Done():
		close(hook.resume)
		<-result
		require.NoError(t, ctx.Err(), "worker registration was not observed")
	}
	now, err := rdb.Time(ctx).Result()
	assert.NoError(t, err)
	assert.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.workerKeepAlive),
		workerID,
		strconv.FormatInt(now.Add(-2*node.workerTTL).UnixNano(), 10),
	).Err())
	lease, err := other.acquireWorkerCleanup(ctx, workerID)
	assert.NoError(t, err)
	assert.NotNil(t, lease)
	close(hook.resume)
	got := <-result
	assert.ErrorIs(t, got.err, replyError)
	assert.ErrorContains(t, got.err, "WORKERCLEANUPLOST")
	assert.Nil(t, got.worker)
	for _, name := range []string{node.resources.workers, node.resources.workerKeepAlive} {
		saved, err := rdb.HExists(ctx, rmapContentKey(name), workerID).Result()
		assert.NoError(t, err)
		assert.True(t, saved, "another cleanup owner still needs %s", name)
	}
	if lease != nil {
		assert.NoError(t, other.releaseWorkerCleanup(ctx, lease, false))
	}
}

func TestWorkerRegistrationDelayedCommandCannotReviveWorker(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx, cancel := context.WithTimeout(ptesting.NewTestContext(t), 20*time.Second)
	defer cancel()
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() {
		assert.NoError(t, node.Shutdown(context.Background()))
	}()
	hook := &workerRegistrationHook{
		workersKey: rmapContentKey(node.resources.workers),
		rdb:        rdb,
		registered: make(chan string, 1),
		resume:     make(chan struct{}),
		replaying:  make(chan struct{}, 1),
		replay:     make(chan struct{}),
	}
	close(hook.resume)
	rdb.AddHook(hook)
	worker, err := node.AddWorker(
		context.WithValue(ctx, workerRegistrationContextKey{}, true),
		newMockJobHandler(),
	)
	require.NoError(t, err)
	require.Equal(t, worker.ID, <-hook.registered)
	replayed := make(chan error, 1)
	go func() {
		replayed <- rdb.Do(
			context.WithValue(ctx, workerRegistrationReplayContextKey{}, true),
			hook.args...,
		).Err()
	}()
	select {
	case <-hook.replaying:
	case <-ctx.Done():
		close(hook.replay)
		<-replayed
		require.NoError(t, ctx.Err(), "registration replay was not observed")
	}
	removeErr := node.RemoveWorker(ctx, worker)
	close(hook.replay)
	replayErr := <-replayed
	require.NoError(t, removeErr)
	assert.ErrorContains(t, replayErr, "STREAMDESTROYED")
	for _, name := range []string{node.resources.workers, node.resources.workerKeepAlive} {
		saved, err := rdb.HExists(ctx, rmapContentKey(name), worker.ID).Result()
		require.NoError(t, err)
		assert.False(t, saved)
	}
}

func TestWorkerRegistrationRepeatedCommandPreservesConstructorHeartbeat(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx, cancel := context.WithTimeout(ptesting.NewTestContext(t), 20*time.Second)
	defer cancel()
	// Keep worker expiry longer than this test's deadline so an overwritten
	// startup timestamp cannot turn the constructor wait into a cleanup test.
	node, err := AddNode(ctx, t.Name(), rdb,
		WithLogger(pulse.NoopLogger()),
		WithWorkerTTL(time.Minute),
		WithJobSinkBlockDuration(testJobSinkBlockDuration),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, node.Shutdown(context.Background()))
	}()
	hook := &workerRegistrationHook{
		workersKey: rmapContentKey(node.resources.workers),
		rdb:        rdb,
		registered: make(chan string, 1),
		resume:     make(chan struct{}),
	}
	rdb.AddHook(hook)
	result := make(chan workerRegistrationResult, 1)
	go func() {
		worker, err := node.AddWorker(
			context.WithValue(ctx, workerRegistrationContextKey{}, true),
			newMockJobHandler(),
		)
		result <- workerRegistrationResult{worker: worker, err: err}
	}()
	var workerID string
	select {
	case workerID = <-hook.registered:
	case <-ctx.Done():
		close(hook.resume)
		<-result
		require.NoError(t, ctx.Err(), "worker registration was not observed")
	}
	heartbeatKey := rmapContentKey(node.resources.workerKeepAlive)
	before, err := rdb.HGetAll(ctx, heartbeatKey).Result()
	assert.NoError(t, err)

	// Hold the successful constructor response while an identical older
	// command runs. The constructor must still observe its returned timestamp.
	heartbeat, err := rdb.Do(ctx, hook.args...).Text()
	assert.NoError(t, err)
	after, err := rdb.HGetAll(ctx, heartbeatKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, before, after, "a repeated registration must not replace the heartbeat")
	assert.Equal(t, before[workerID], heartbeat)
	assert.NoError(t, waitPoolMapValue(ctx, node.workerKeepAliveMap, workerID, heartbeat))
	close(hook.resume)
	got := <-result
	assert.NoError(t, got.err)
	assert.NotNil(t, got.worker)
}

func (h *workerRegistrationHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook pauses the marked AddWorker call after Redis saves its worker
// entry. Other node operations continue, exposing the actual cleanup race.
// A marked replay waits until the test retires the worker, then reaches Redis
// so the stored stream state decides whether the old command can run.
func (h *workerRegistrationHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if ctx.Value(workerRegistrationReplayContextKey{}) == true {
			h.replaying <- struct{}{}
			select {
			case <-h.replay:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err := next(ctx, cmd)
		if err != nil || ctx.Value(workerRegistrationContextKey{}) != true ||
			(cmd.Name() != "evalsha" && cmd.Name() != "eval") {
			return err
		}
		for _, arg := range cmd.Args() {
			if arg != h.workersKey {
				continue
			}
			h.once.Do(func() {
				h.args = append([]any(nil), cmd.Args()...)
				entries, readErr := h.rdb.HKeys(ctx, h.workersKey).Result()
				if readErr != nil {
					err = readErr
					return
				}
				for _, entry := range entries {
					if entry[0] != '=' {
						h.registered <- entry
						select {
						case <-h.resume:
						case <-ctx.Done():
							err = ctx.Err()
						}
						if err == nil {
							err = h.replyError
						}
						return
					}
				}
			})
			break
		}
		return err
	}
}

func (h *workerRegistrationHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
