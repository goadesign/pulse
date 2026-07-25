// Atomic dispatch tests exercise the Redis-owned admission/publication point,
// immutable pool capacity, and quiescent legacy adoption.
package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	ptesting "goa.design/pulse/testing"
)

type (
	// ambiguousDispatchHook returns one client error after the dispatch script
	// has committed, reproducing an ambiguous transport response.
	ambiguousDispatchHook struct {
		fail       atomic.Bool
		err        error
		scriptHash string
	}

	// cleanupDelayHook pauses after one cleanup-map lease renewal, proving the
	// next destructive step uses CleanupLease rather than WorkerTTL.
	cleanupDelayHook struct {
		delayed    atomic.Bool
		delay      time.Duration
		scriptHash string
	}

	// settlementFailureHook holds exact terminal settlement unavailable until
	// a test releases it.
	settlementFailureHook struct {
		fail       atomic.Bool
		attempted  chan struct{}
		once       sync.Once
		err        error
		scriptHash string
	}
)

func TestDispatchJobOnceRetriesAmbiguousResponse(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &ambiguousDispatchHook{
		err:        errors.New("ambiguous dispatch response"),
		scriptHash: luaDispatchJob.Hash(),
	}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	node, err := AddNode(
		ctx,
		t.Name(),
		rdb,
		WithDispatchTimeout(25*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, luaDispatchJob.Load(ctx, rdb).Err())

	hook.fail.Store(true)
	_, err = node.DispatchJobOnce(ctx, "dispatch-1", "job", []byte("payload"))
	require.ErrorIs(t, err, hook.err)
	eventID, err := node.DispatchJobOnce(ctx, "dispatch-1", "job", []byte("payload"))
	require.ErrorContains(t, err, "timed out")
	require.NotEmpty(t, eventID)
	require.EqualValues(t, 1, rdb.XLen(ctx, generationStreamKey(ctx, rdb, node.poolStream.Name)).Val())
	require.NoError(t, node.completeDispatch(ctx, "job", "dispatch-1"))
	require.EqualValues(t, 0, rdb.XLen(ctx, generationStreamKey(ctx, rdb, node.poolStream.Name)).Val())
	retryID, err := node.DispatchJobOnce(ctx, "dispatch-1", "job", []byte("payload"))
	require.NoError(t, err)
	require.Equal(t, eventID, retryID)
	_, err = node.DispatchJobOnce(ctx, "dispatch-1", "job", []byte("different"))
	require.ErrorIs(t, err, ErrDispatchConflict)
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestDispatchSettlementRetryReturnsOriginalOutcome(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &ambiguousDispatchHook{
		err:        errors.New("ambiguous settlement response"),
		scriptHash: luaSettleDispatch.Hash(),
	}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	node, err := AddNode(ctx, t.Name(), rdb, WithDispatchTimeout(20*time.Millisecond))
	require.NoError(t, err)
	eventID, err := node.DispatchJobOnce(ctx, "dispatch", "job", []byte("payload"))
	require.ErrorContains(t, err, "timed out")
	require.NoError(t, luaSettleDispatch.Load(ctx, rdb).Err())

	hook.fail.Store(true)
	_, err = node.settleDispatch(ctx, "job", "dispatch", errors.New("worker rejected"))
	require.ErrorIs(t, err, hook.err)
	record, err := node.settleDispatch(ctx, "job", "dispatch", errors.New("different"))
	require.NoError(t, err)
	require.Equal(t, eventID, record.eventID)
	require.Equal(t, "worker rejected", record.err)
	retryID, err := node.DispatchJobOnce(ctx, "dispatch", "job", []byte("payload"))
	require.EqualError(t, err, "worker rejected")
	require.Equal(t, eventID, retryID)
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestConcurrentLocalDispatchRetriesShareCompletion(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node, err := AddNode(ctx, t.Name(), rdb, WithDispatchTimeout(time.Second))
	require.NoError(t, err)
	type dispatchResult struct {
		id  string
		err error
	}
	results := make(chan dispatchResult, 2)
	for range 2 {
		go func() {
			id, dispatchErr := node.DispatchJobOnce(ctx, "dispatch", "job", []byte("payload"))
			results <- dispatchResult{id: id, err: dispatchErr}
		}()
	}
	require.Eventually(t, func() bool {
		value, ok := node.pendingJobChannels.Load("dispatch")
		return ok && value.(*dispatchWaiter).refs.Load() == 2
	}, time.Second, time.Millisecond)
	record, err := node.settleDispatch(ctx, "job", "dispatch", nil)
	require.NoError(t, err)
	for range 2 {
		result := <-results
		require.NoError(t, result.err)
		require.Equal(t, record.eventID, result.id)
	}
	require.Eventually(t, func() bool {
		_, ok := node.pendingJobChannels.Load("dispatch")
		return !ok
	}, time.Second, time.Millisecond)
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestCrossNodeDispatchWaiterReadsDurableTerminalState(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	opts := []NodeOption{
		WithDispatchTimeout(time.Second),
		WithRecoveryGrace(100 * time.Millisecond),
		WithDispatchResultRetention(2 * time.Second),
	}
	caller, err := AddNode(ctx, t.Name(), rdb, opts...)
	require.NoError(t, err)
	settler, err := AddNode(ctx, t.Name(), rdb, opts...)
	require.NoError(t, err)

	type crossNodeResult struct {
		eventID string
		err     error
	}
	resultCh := make(chan crossNodeResult, 1)
	go func() {
		eventID, dispatchErr := caller.DispatchJobOnce(
			ctx,
			"cross-node-dispatch",
			"job",
			[]byte("payload"),
		)
		resultCh <- crossNodeResult{eventID: eventID, err: dispatchErr}
	}()
	require.Eventually(t, func() bool {
		return rdb.HGet(
			ctx,
			rmapContentKey(caller.resources.jobPending),
			"job",
		).Val() == "cross-node-dispatch"
	}, time.Second, time.Millisecond)
	settled, err := settler.settleDispatch(
		ctx,
		"job",
		"cross-node-dispatch",
		errors.New("worker rejected"),
	)
	require.NoError(t, err)

	outcome := <-resultCh
	require.Equal(t, settled.eventID, outcome.eventID)
	require.EqualError(t, outcome.err, "worker rejected")
	require.NoError(t, caller.Close(ctx))
	require.NoError(t, settler.Close(ctx))
	require.NoError(t, caller.poolStream.Destroy(ctx))
}

func TestDispatchResultRetentionBoundsReplayState(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	const dispatchID = "retained-dispatch"
	retention := 80 * time.Millisecond
	node, err := AddNode(
		ctx,
		t.Name(),
		rdb,
		WithDispatchTimeout(10*time.Millisecond),
		WithRecoveryGrace(5*time.Millisecond),
		WithDispatchResultRetention(retention),
	)
	require.NoError(t, err)
	recordKey := dispatchRecordKey(node.resources.dispatches, dispatchID)

	eventID, err := node.publishDispatch(
		ctx,
		"job",
		dispatchID,
		marshalJob(&Job{Key: "job", Payload: []byte("payload"), dispatchID: dispatchID}),
	)
	require.NoError(t, err)
	require.EqualValues(t, -1, rdb.PTTL(ctx, recordKey).Val())
	require.True(t, rdb.SIsMember(ctx, dispatchActiveKey(node.resources.dispatches), recordKey).Val())

	settled, err := node.settleDispatch(ctx, "job", dispatchID, errors.New("terminal"))
	require.NoError(t, err)
	require.Equal(t, eventID, settled.eventID)
	require.False(t, rdb.SIsMember(ctx, dispatchActiveKey(node.resources.dispatches), recordKey).Val())
	require.Positive(t, rdb.PTTL(ctx, recordKey).Val())
	replayedID, err := node.DispatchJobOnce(ctx, dispatchID, "job", []byte("payload"))
	require.EqualError(t, err, "terminal")
	require.Equal(t, eventID, replayedID)

	require.Eventually(t, func() bool {
		return rdb.Exists(ctx, recordKey).Val() == 0
	}, time.Second, 10*time.Millisecond)
	newEventID, err := node.publishDispatch(
		ctx,
		"job",
		dispatchID,
		marshalJob(&Job{Key: "job", Payload: []byte("payload"), dispatchID: dispatchID}),
	)
	require.NoError(t, err)
	require.NotEqual(t, eventID, newEventID, "expired replay identity is a new admission")
	_, err = node.settleDispatch(ctx, "job", dispatchID, nil)
	require.NoError(t, err)
	for i := range 10 {
		id := fmt.Sprintf("bounded-%d", i)
		key := fmt.Sprintf("job-%d", i)
		_, err := node.publishDispatch(
			ctx,
			key,
			id,
			marshalJob(&Job{Key: key, dispatchID: id}),
		)
		require.NoError(t, err)
		_, err = node.settleDispatch(ctx, key, id, nil)
		require.NoError(t, err)
	}
	recordPattern := dispatchRecordKey(node.resources.dispatches, "") + "*"
	require.Eventually(t, func() bool {
		return len(rdb.Keys(ctx, recordPattern).Val()) == 0
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestMaxQueuedJobsIsAtomicActiveCapacity(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node, err := AddNode(
		ctx,
		t.Name(),
		rdb,
		WithMaxQueuedJobs(2),
		WithDispatchTimeout(20*time.Millisecond),
	)
	require.NoError(t, err)

	for _, dispatch := range []string{"one", "two"} {
		eventID, dispatchErr := node.DispatchJobOnce(ctx, dispatch, dispatch, nil)
		require.ErrorContains(t, dispatchErr, "timed out")
		require.NotEmpty(t, eventID)
	}
	_, err = node.DispatchJobOnce(ctx, "three", "three", nil)
	require.ErrorIs(t, err, ErrPoolCapacity)
	streamKey := generationStreamKey(ctx, rdb, node.poolStream.Name)
	require.EqualValues(t, 2, rdb.XLen(ctx, streamKey).Val())
	require.NoError(t, node.completeDispatch(ctx, "one", "one"))
	require.NoError(t, node.completeDispatch(ctx, "two", "two"))
	require.EqualValues(t, 0, rdb.XLen(ctx, streamKey).Val())
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestPoolCapacityConfigurationMustMatch(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	first, err := AddNode(ctx, t.Name(), rdb, WithClientOnly(), WithMaxQueuedJobs(2))
	require.NoError(t, err)
	_, err = AddNode(ctx, t.Name(), rdb, WithClientOnly(), WithMaxQueuedJobs(3))
	require.ErrorIs(t, err, ErrPoolConfigMismatch)
	require.NoError(t, first.Close(ctx))
	require.NoError(t, first.poolStream.Destroy(ctx))
}

func TestPoolLeaseAndDispatchRetentionConfigurationMustMatch(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	first, err := AddNode(
		ctx,
		t.Name(),
		rdb,
		WithClientOnly(),
		WithCleanupLease(10*time.Second),
		WithDispatchResultRetention(2*time.Minute),
	)
	require.NoError(t, err)
	_, err = AddNode(
		ctx,
		t.Name(),
		rdb,
		WithClientOnly(),
		WithCleanupLease(11*time.Second),
		WithDispatchResultRetention(2*time.Minute),
	)
	require.ErrorIs(t, err, ErrPoolConfigMismatch)
	_, err = AddNode(
		ctx,
		t.Name(),
		rdb,
		WithClientOnly(),
		WithCleanupLease(10*time.Second),
		WithDispatchResultRetention(3*time.Minute),
	)
	require.ErrorIs(t, err, ErrPoolConfigMismatch)
	require.NoError(t, first.Close(ctx))
	require.NoError(t, first.poolStream.Destroy(ctx))
}

func TestPoolWorkerTTLConfigurationMustMatchWithoutFalseReap(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	first, err := AddNode(
		ctx,
		t.Name(),
		rdb,
		WithWorkerTTL(200*time.Millisecond),
		WithJobSinkBlockDuration(10*time.Millisecond),
	)
	require.NoError(t, err)
	worker := newTestWorker(t, ctx, first)

	_, err = AddNode(
		ctx,
		t.Name(),
		rdb,
		WithWorkerTTL(5*time.Millisecond),
		WithJobSinkBlockDuration(time.Millisecond),
	)
	require.ErrorIs(t, err, ErrPoolConfigMismatch)
	time.Sleep(20 * time.Millisecond)
	first.cleanupInactiveWorkers(ctx)
	_, exists := first.workerMap.Get(worker.ID)
	require.True(t, exists, "rejected short-TTL node must not reap a healthy worker")
	require.Equal(
		t,
		"200",
		rdb.HGet(ctx, poolResourcesKey(t.Name()), "worker_ttl_ms").Val(),
	)

	require.NoError(t, first.Close(ctx))
	require.NoError(t, first.poolStream.Destroy(ctx))
}

func TestTerminalSettlementOutlivesWorkerRemoval(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &settlementFailureHook{
		attempted:  make(chan struct{}),
		err:        errors.New("injected settlement outage"),
		scriptHash: luaSettleDispatch.Hash(),
	}
	hook.fail.Store(true)
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	started := make(chan struct{})
	handler := newMockHandler()
	handler.startFunc = func(*Job) error {
		close(started)
		return nil
	}
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		_, dispatchErr := node.DispatchJobOnce(ctx, "dispatch", "job", []byte("payload"))
		result <- dispatchErr
	}()
	require.Eventually(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, max, delay)
	<-hook.attempted

	removeCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	err = node.RemoveWorker(removeCtx, worker)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	record, err := node.readDispatchRecord(ctx, "dispatch", mustDispatchIdentity(t, "job", []byte("payload")))
	require.NoError(t, err)
	require.Equal(t, dispatchClaimed, record.status)
	require.Equal(t, "dispatch", node.jobPendingMap.Map()["job"])

	hook.fail.Store(false)
	require.NoError(t, node.RemoveWorker(ctx, worker))
	require.NoError(t, <-result)
	record, err = node.readDispatchRecord(ctx, "dispatch", mustDispatchIdentity(t, "job", []byte("payload")))
	require.NoError(t, err)
	require.Equal(t, dispatchTerminal, record.status)
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestTerminalSettlementOutlivesNodeClose(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &settlementFailureHook{
		attempted:  make(chan struct{}),
		err:        errors.New("injected settlement outage"),
		scriptHash: luaSettleDispatch.Hash(),
	}
	hook.fail.Store(true)
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	started := make(chan struct{})
	handler := newMockHandler()
	handler.startFunc = func(*Job) error {
		close(started)
		return nil
	}
	_, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	go func() {
		_, _ = node.DispatchJobOnce(ctx, "dispatch", "job", nil)
	}()
	<-started
	<-hook.attempted

	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	err = node.Close(closeCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, node.IsClosed())
	hook.fail.Store(false)
	require.NoError(t, node.Close(ctx))
	require.True(t, node.IsClosed())
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestCrashedExactDispatchReclaimsOriginalEvent(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	node.stopOnce.Do(func() {
		close(node.stop)
	})
	node.wg.Wait()
	crashed := newTestWorker(t, ctx, node)
	replacement := newTestWorker(t, ctx, node)
	job := &Job{
		Key:        "job",
		Payload:    []byte("payload"),
		CreatedAt:  time.Now(),
		NodeID:     node.ID,
		dispatchID: "dispatch",
	}
	eventID, err := node.publishDispatch(ctx, job.Key, job.dispatchID, marshalJob(job))
	require.NoError(t, err)
	claimed, err := crashed.claimDispatchedStart(ctx, job)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.workerKeepAlive),
		crashed.ID,
		"0",
	).Err())
	node.cleanupWorker(ctx, crashed.ID)
	require.Equal(
		t,
		"dispatch",
		rdb.HGet(ctx, rmapContentKey(node.resources.jobPending), job.Key).Val(),
	)
	require.EqualValues(t, 1, rdb.XLen(ctx, generationStreamKey(ctx, rdb, node.poolStream.Name)).Val())
	record, err := node.readDispatchRecord(
		ctx,
		job.dispatchID,
		mustDispatchIdentity(t, job.Key, job.Payload),
	)
	require.NoError(t, err)
	require.Equal(t, eventID, record.eventID)
	require.Equal(t, dispatchClaimed, record.status)
	durable := rdb.HGetAll(
		ctx,
		dispatchRecordKey(node.resources.dispatches, job.dispatchID),
	).Val()
	require.Equal(t, "dispatch", durable["id"])
	require.Equal(t, "job", durable["key"])
	require.Equal(t, "pending", durable["state"])
	claimed, err = replacement.claimDispatchedStart(ctx, job)
	require.NoError(t, err)
	require.True(t, claimed, "replacement must reclaim the original dispatch identity")
	_, err = node.settleDispatch(ctx, job.Key, job.dispatchID, nil)
	require.NoError(t, err)
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestCleanupUsesConfiguredLeaseNotWorkerTTL(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &cleanupDelayHook{
		delay:      20 * time.Millisecond,
		scriptHash: destroyCleanupMapScript.Hash(),
	}
	rdb.AddHook(hook)
	ctx := ptesting.NewTestContext(t)
	node, err := AddNode(
		ctx,
		t.Name(),
		rdb,
		WithWorkerTTL(4*time.Millisecond),
		WithJobSinkBlockDuration(time.Millisecond),
		WithCleanupLease(200*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, node.Shutdown(ctx))
	require.True(t, hook.delayed.Load())
}

func TestLegacyPoolAdoptionRequiresQuiescence(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	poolName := t.Name()
	resources := flatPoolResources(poolName, "1")
	require.NoError(t, rdb.HSet(ctx, rmapContentKey(resources.nodeKeepAlive), "legacy-node", "1").Err())

	_, err := AddNode(ctx, poolName, rdb, WithClientOnly())
	require.ErrorIs(t, err, ErrQuiescenceRequired)
	require.EqualValues(t, 0, rdb.Exists(ctx, poolResourcesKey(poolName)).Val())
	require.NoError(t, rdb.HDel(ctx, rmapContentKey(resources.nodeKeepAlive), "legacy-node").Err())
	dynamicScheduler := rmapContentKey(poolName + ":legacy-producer")
	require.NoError(t, rdb.HSet(ctx, dynamicScheduler, "job", "owned").Err())
	_, err = AddNode(ctx, poolName, rdb, WithClientOnly())
	require.ErrorIs(t, err, ErrQuiescenceRequired)
	require.NoError(t, rdb.Del(ctx, dynamicScheduler).Err())
	require.NoError(t, rdb.HSet(ctx, rmapContentKey(resources.nodeShutdown), "shutdown", "legacy").Err())
	_, err = AddNode(ctx, poolName, rdb, WithClientOnly())
	require.ErrorIs(t, err, ErrQuiescenceRequired)
	require.NoError(t, rdb.Del(ctx, rmapContentKey(resources.nodeShutdown)).Err())
	require.NoError(t, rdb.HSet(ctx, rmapContentKey(resources.dispatches), "dispatch:state", "pending").Err())
	_, err = AddNode(ctx, poolName, rdb, WithClientOnly())
	require.ErrorIs(t, err, ErrQuiescenceRequired)
	require.NoError(t, rdb.Del(ctx, rmapContentKey(resources.dispatches)).Err())
	legacyNodeStream := "pulse:stream:" + nodeStreamName(poolName, "LEGACYNODE")
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: legacyNodeStream,
		Values: map[string]any{"n": evInit, "p": "legacy"},
	}).Err())
	node, err := AddNode(ctx, poolName, rdb, WithClientOnly())
	require.NoError(t, err)
	require.EqualValues(t, 0, rdb.Exists(ctx, legacyNodeStream).Val())
	require.Equal(t, "7", rdb.HGet(ctx, poolResourcesKey(poolName), "format_version").Val())
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

// mustDispatchIdentity returns the canonical exact-dispatch identity for tests.
func mustDispatchIdentity(t *testing.T, key string, payload []byte) []byte {
	t.Helper()
	identity, err := dispatchIdentity(key, payload)
	require.NoError(t, err)
	return identity
}

// DialHook preserves normal Redis dialing.
func (h *ambiguousDispatchHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook injects one error only after Redis committed atomic dispatch.
func (h *ambiguousDispatchHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err != nil {
			return err
		}
		args := cmd.Args()
		isDispatch := len(args) > 1 && cmd.Name() == "evalsha" &&
			args[1] == h.scriptHash
		if isDispatch &&
			h.fail.CompareAndSwap(true, false) {
			return h.err
		}
		return nil
	}
}

// ProcessPipelineHook preserves Redis pipelines.
func (h *ambiguousDispatchHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}

// DialHook preserves normal Redis dialing.
func (h *cleanupDelayHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook delays after the first successful map cleanup script.
func (h *cleanupDelayHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err != nil {
			return err
		}
		args := cmd.Args()
		if len(args) > 1 && cmd.Name() == "evalsha" &&
			args[1] == h.scriptHash && h.delayed.CompareAndSwap(false, true) {
			time.Sleep(h.delay)
		}
		return nil
	}
}

// ProcessPipelineHook preserves Redis pipelines.
func (h *cleanupDelayHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}

// DialHook preserves normal Redis dialing.
func (h *settlementFailureHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook fails settlement before Redis can commit it.
func (h *settlementFailureHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if len(args) > 1 && cmd.Name() == "evalsha" &&
			args[1] == h.scriptHash && h.fail.Load() {
			h.once.Do(func() {
				close(h.attempted)
			})
			return h.err
		}
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook preserves Redis pipelines.
func (h *settlementFailureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}
