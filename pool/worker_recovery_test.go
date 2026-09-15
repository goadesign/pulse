// Recovery tests keep one running handler attached to its durable job ownership.
// A repeated delivery must preserve that job so later scheduler work can finish.
package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ptesting "goa.design/pulse/testing"
)

type (
	// exclusiveJobHandler rejects a second start until the running job is stopped,
	// matching handlers that own a background subscription or polling goroutine.
	exclusiveJobHandler struct {
		active   sync.Map
		attempts atomic.Int32
	}

	// rebalanceFailureHook rejects one ownership removal or publication before
	// Redis receives it, leaving the worker responsible for restoring its job.
	rebalanceFailureHook struct {
		fail            atomic.Bool
		lifecycleKey    string
		removeOwnership bool
	}
)

func TestRecoveredStartPreservesOwnershipAndScheduleProgress(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
	handler := &exclusiveJobHandler{}
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	job := &Job{
		Key:       "source",
		Payload:   []byte("source configuration"),
		CreatedAt: time.Now(),
		NodeID:    node.ID,
		Requeued:  true,
	}

	// Both deliveries use the real recovery stream and acknowledgement path.
	// Waiting for deletion of each pool event proves the worker handled it.
	deliverRecoveredJob(t, ctx, node, job)
	require.Equal(t, []string{worker.ID}, snapshotJobOwners(t, ctx, node, job.Key))
	deliverRecoveredJob(t, ctx, node, job)

	owners := snapshotJobOwners(t, ctx, node, job.Key)
	payload, exists := snapshotValue(t, ctx, node, node.jobPayloadMap, job.Key)
	_, active := handler.active.Load(job.Key)
	t.Logf("after replay: handler attempts=%d active=%t owners=%v payload_exists=%t",
		handler.attempts.Load(), active, owners, exists)
	assert.EqualValues(t, 1, handler.attempts.Load())
	assert.True(t, active)
	assert.Equal(t, []string{worker.ID}, owners)
	assert.True(t, exists)
	assert.Equal(t, string(job.Payload), payload)

	producer := newTestProducer("source-plan", func() (*JobPlan, error) {
		return &JobPlan{
			Start: []*JobParam{
				{Key: job.Key, Payload: job.Payload},
				{Key: "following", Payload: []byte("next source")},
			},
			StopAll: true,
		}, nil
	})
	encodedName := hex.EncodeToString([]byte(producer.Name()))
	sched := &scheduler{
		name:             node.PoolName + ":" + producer.Name(),
		interval:         time.Second,
		producer:         producer,
		node:             node,
		keyPrefix:        encodedName + ":",
		transitionPrefix: "=transition:" + encodedName + ":",
		owner:            "test-source-plan",
		lease:            node.workerTTL,
		logger:           node.logger,
	}
	fence := claimTestSchedulerTransition(t, ctx, sched)
	nextField := sched.transitionPrefix + "next_ms"
	now, err := rdb.Time(ctx).Result()
	require.NoError(t, err)
	require.NoError(t, rdb.HSet(ctx, rmapContentKey(node.resources.schedulerJobs),
		nextField, now.Add(-sched.interval).UnixMilli()).Err())
	before, err := rdb.HGet(ctx, rmapContentKey(node.resources.schedulerJobs), nextField).Int64()
	require.NoError(t, err)
	_, transitionErr := sched.runTransition(ctx, fence)
	after, err := rdb.HGet(ctx, rmapContentKey(node.resources.schedulerJobs), nextField).Int64()
	require.NoError(t, err)
	_, following := handler.active.Load("following")
	t.Logf("after plan: error=%v following_active=%t next_ms_before=%d next_ms_after=%d",
		transitionErr, following, before, after)
	assert.NoError(t, transitionErr)
	assert.True(t, following)
	assert.Greater(t, after, before)
	assert.Equal(t, []string{worker.ID}, snapshotJobOwners(t, ctx, node, job.Key))
}

func TestRecoveredStartRejectsConflictingPayloadWithoutChangingOwnership(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
	handler := &exclusiveJobHandler{}
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	job := &Job{Key: "source", Payload: []byte("accepted"), NodeID: node.ID, Requeued: true}
	require.NoError(t, worker.startJob(ctx, job))

	conflict := *job
	conflict.Payload = []byte("different")
	assert.ErrorIs(t, worker.startJob(ctx, &conflict), ErrJobExists)
	assert.EqualValues(t, 1, handler.attempts.Load())
	assert.Equal(t, []string{worker.ID}, snapshotJobOwners(t, ctx, node, job.Key))
	payload, exists := snapshotValue(t, ctx, node, node.jobPayloadMap, job.Key)
	assert.True(t, exists)
	assert.Equal(t, "accepted", payload)

	// Stopping the accepted job makes its key reusable for a new payload.
	require.NoError(t, worker.stopJob(ctx, job.Key))
	require.NoError(t, worker.startJob(ctx, &conflict))
	assert.EqualValues(t, 2, handler.attempts.Load())
	payload, exists = snapshotValue(t, ctx, node, node.jobPayloadMap, job.Key)
	assert.True(t, exists)
	assert.Equal(t, "different", payload)
}

func TestAcceptedExactStartValidatesReplayIdentity(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	hook := &settlementFailureHook{
		attempted:  make(chan struct{}),
		err:        errors.New("settlement held for replay"),
		scriptHash: luaSettleDispatch.Hash(),
	}
	rdb.AddHook(hook)
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
	defer hook.fail.Store(false)
	handler := &exclusiveJobHandler{}
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	hook.fail.Store(true)

	result := make(chan error, 1)
	go func() {
		_, err := node.DispatchJobOnce(ctx, "accepted-dispatch", "source", []byte("accepted"))
		result <- err
	}()
	select {
	case <-hook.attempted:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not reach settlement")
	}
	replay := &Job{
		Key:        "source",
		Payload:    []byte("accepted"),
		NodeID:     node.ID,
		dispatchID: "accepted-dispatch",
	}
	require.NoError(t, worker.startJob(ctx, replay))
	tampered := *replay
	tampered.Payload = []byte("different")
	assert.ErrorIs(t, worker.startJob(ctx, &tampered), errDispatchIdentityMismatch)
	_, err = node.DispatchJobOnce(ctx, "another-dispatch", replay.Key, replay.Payload)
	assert.ErrorIs(t, err, ErrJobExists)
	assert.EqualValues(t, 1, handler.attempts.Load())
	assert.Equal(t, []string{worker.ID}, snapshotJobOwners(t, ctx, node, replay.Key))

	hook.fail.Store(false)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch did not settle")
	}
	require.NoError(t, worker.startJob(ctx, replay))
	assert.ErrorIs(t, worker.startJob(ctx, &tampered), errDispatchIdentityMismatch)
	assert.EqualValues(t, 1, handler.attempts.Load())
	payload, exists := snapshotValue(t, ctx, node, node.jobPayloadMap, replay.Key)
	assert.True(t, exists)
	assert.Equal(t, "accepted", payload)
}

func TestConcurrentRecoveredStartsEnterHandlerOnce(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
	handler := &exclusiveJobHandler{}
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	ready := make(chan struct{})
	results := make(chan error, 8)
	for range cap(results) {
		go func() {
			<-ready
			results <- worker.startJob(ctx, &Job{
				Key: "source", Payload: []byte("accepted"), NodeID: node.ID, Requeued: true,
			})
		}()
	}
	close(ready)
	for range cap(results) {
		assert.NoError(t, <-results)
	}
	assert.EqualValues(t, 1, handler.attempts.Load())
	assert.Equal(t, []string{worker.ID}, snapshotJobOwners(t, ctx, node, "source"))
}

func TestAcceptedRecoveryRestoresMissingOwnershipWithoutRestart(t *testing.T) {
	for _, missing := range []string{"owner", "payload", "both"} {
		t.Run(missing, func(t *testing.T) {
			ctx := ptesting.NewTestContext(t)
			rdb := ptesting.NewRedisClient(t)
			defer ptesting.CleanupRedis(t, rdb, false, "")
			hook := &poolRedisHook{
				startCleanupSHA: restoreRunningJobScript.Hash(),
				failure:         errors.New("ownership repair unavailable"),
			}
			rdb.AddHook(hook)
			node := newTestNode(t, ctx, rdb, t.Name())
			defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
			handler := &exclusiveJobHandler{}
			worker, err := node.AddWorker(ctx, handler)
			require.NoError(t, err)
			job := &Job{Key: "source", Payload: []byte("accepted"), NodeID: node.ID, Requeued: true}
			require.NoError(t, worker.startJob(ctx, job))
			if missing != "payload" {
				require.NoError(t, node.removePoolMapValue(ctx, node.resources.jobs, worker.ID, job.Key))
			}
			if missing != "owner" {
				require.NoError(t, node.deletePoolMap(ctx, node.resources.jobPayloads, job.Key))
			}

			// A Redis failure must not call the handler or delete its local job.
			hook.failStartCleanup.Store(true)
			assert.ErrorIs(t, worker.startJob(ctx, job), ErrRequeue)
			assert.EqualValues(t, 1, handler.attempts.Load())
			assert.Len(t, worker.Jobs(), 1)
			_, active := handler.active.Load(job.Key)
			assert.True(t, active)
			hook.failStartCleanup.Store(false)

			deliverRecoveredJob(t, ctx, node, job)
			assert.EqualValues(t, 1, handler.attempts.Load())
			assert.Equal(t, []string{worker.ID}, snapshotJobOwners(t, ctx, node, job.Key))
			payload, exists := snapshotValue(t, ctx, node, node.jobPayloadMap, job.Key)
			assert.True(t, exists)
			assert.Equal(t, "accepted", payload)
		})
	}
}

func TestAcceptedRecoveryNeverOverwritesConflictingOwnership(t *testing.T) {
	for _, conflict := range []string{"incoming_payload", "stored_payload", "worker", "dispatch"} {
		t.Run(conflict, func(t *testing.T) {
			ctx := ptesting.NewTestContext(t)
			rdb := ptesting.NewRedisClient(t)
			defer ptesting.CleanupRedis(t, rdb, false, "")
			node := newTestNode(t, ctx, rdb, t.Name())
			defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
			handler := &exclusiveJobHandler{}
			worker, err := node.AddWorker(ctx, handler)
			require.NoError(t, err)
			job := &Job{Key: "source", Payload: []byte("accepted"), NodeID: node.ID, Requeued: true}
			require.NoError(t, worker.startJob(ctx, job))
			require.NoError(t, node.removePoolMapValue(ctx, node.resources.jobs, worker.ID, job.Key))
			replay := *job
			switch conflict {
			case "incoming_payload":
				replay.Payload = []byte("different")
			case "stored_payload":
				require.NoError(t, node.setPoolMap(ctx, node.resources.jobPayloads, job.Key, "different"))
			case "worker":
				require.NoError(t, node.appendPoolMapValue(ctx, node.resources.jobs, "another-worker", job.Key))
			case "dispatch":
				require.NoError(t, node.setPoolMap(ctx, node.resources.jobPending, job.Key, "another-dispatch"))
			}
			before, err := rdb.HGetAll(ctx, rmapContentKey(node.resources.jobs)).Result()
			require.NoError(t, err)
			payloadBefore, err := rdb.HGetAll(ctx, rmapContentKey(node.resources.jobPayloads)).Result()
			require.NoError(t, err)

			assert.ErrorIs(t, worker.startJob(ctx, &replay), ErrJobExists)
			after, err := rdb.HGetAll(ctx, rmapContentKey(node.resources.jobs)).Result()
			require.NoError(t, err)
			payloadAfter, err := rdb.HGetAll(ctx, rmapContentKey(node.resources.jobPayloads)).Result()
			require.NoError(t, err)
			assert.Equal(t, before, after)
			assert.Equal(t, payloadBefore, payloadAfter)
			assert.EqualValues(t, 1, handler.attempts.Load())
			require.Len(t, worker.Jobs(), 1)
			assert.Equal(t, "accepted", string(worker.Jobs()[0].Payload))

			// Remove only this test's conflicting records before normal shutdown.
			require.NoError(t, node.deletePoolMap(ctx, node.resources.jobs, "another-worker"))
			require.NoError(t, node.deletePoolMap(ctx, node.resources.jobPending, job.Key))
			require.NoError(t, node.setPoolMap(ctx, node.resources.jobPayloads, job.Key, "accepted"))
			require.NoError(t, worker.startJob(ctx, job))
		})
	}
}

func TestRecoveredStartWaitsForRebalanceAndUsesCurrentWorker(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	node := newTestNode(t, ctx, rdb, t.Name())
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
	var move atomic.Bool
	node.h = &ptesting.Hasher{IndexFunc: func(_ string, buckets int64) int64 {
		if move.Load() {
			return buckets - 1
		}
		return 0
	}}
	handler := &exclusiveJobHandler{}
	stopping := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce, stopOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	first, err := node.AddWorker(ctx, &mockJobHandler{
		startFunc: handler.Start,
		stopFunc: func(key string) error {
			stopOnce.Do(func() {
				close(stopping)
				<-release
			})
			return handler.Stop(key)
		},
	})
	require.NoError(t, err)
	second, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(node.activeWorkers()) == 2 },
		time.Second, time.Millisecond)
	require.NoError(t, node.DispatchJob(ctx, "source", []byte("accepted")))
	require.Eventually(t, func() bool {
		value, ok := first.jobs.Load("source")
		return ok && value.(*Job).dispatchID == ""
	}, time.Second, time.Millisecond)

	move.Store(true)
	moved := make(chan struct{})
	go func() {
		first.rebalance(ctx, node.activeWorkers())
		close(moved)
	}()
	select {
	case <-stopping:
	case <-time.After(time.Second):
		t.Fatal("rebalance did not stop the first handler")
	}
	replayed := make(chan error, 1)
	go func() {
		replayed <- first.startJob(ctx, &Job{
			Key: "source", Payload: []byte("accepted"), NodeID: node.ID, Requeued: true,
		})
	}()
	select {
	case err := <-replayed:
		t.Fatalf("recovery start completed during handler stop: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	<-moved
	assert.ErrorIs(t, <-replayed, ErrRequeue)
	require.Eventually(t, func() bool { return len(second.Jobs()) == 1 },
		3*time.Second, time.Millisecond)
	assert.Empty(t, first.Jobs())
	assert.EqualValues(t, 2, handler.attempts.Load(), "one start per completed ownership transfer")
	assert.Equal(t, []string{second.ID}, snapshotJobOwners(t, ctx, node, "source"))
	payload, exists := snapshotValue(t, ctx, node, node.jobPayloadMap, "source")
	assert.True(t, exists)
	assert.Equal(t, "accepted", payload)
}

func TestRebalanceFailureRestoresOwnershipBeforeRetry(t *testing.T) {
	for _, step := range []string{"publication", "ownership_removal"} {
		t.Run(step, func(t *testing.T) {
			ctx := ptesting.NewTestContext(t)
			rdb := ptesting.NewRedisClient(t)
			defer ptesting.CleanupRedis(t, rdb, false, "")
			hook := &rebalanceFailureHook{
				lifecycleKey:    "pulse:stream:" + poolStreamName(t.Name()) + ":lifecycle",
				removeOwnership: step == "ownership_removal",
			}
			rdb.AddHook(hook)
			node := newTestNode(t, ctx, rdb, t.Name())
			defer func() { assert.NoError(t, node.Shutdown(ctx)) }()
			var move atomic.Bool
			node.h = &ptesting.Hasher{IndexFunc: func(_ string, buckets int64) int64 {
				if move.Load() {
					return buckets - 1
				}
				return 0
			}}
			handler := &exclusiveJobHandler{}
			first, err := node.AddWorker(ctx, handler)
			require.NoError(t, err)
			second, err := node.AddWorker(ctx, handler)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return len(node.activeWorkers()) == 2 },
				time.Second, time.Millisecond)
			require.NoError(t, node.DispatchJob(ctx, "source", []byte("accepted")))
			require.Eventually(t, func() bool {
				value, ok := first.jobs.Load("source")
				return ok && value.(*Job).dispatchID == ""
			}, time.Second, time.Millisecond)

			move.Store(true)
			hook.fail.Store(true)
			first.rebalance(ctx, node.activeWorkers())
			require.False(t, hook.fail.Load(), "the move must hit the injected failure")
			owners := snapshotJobOwners(t, ctx, node, "source")
			payload, exists := snapshotValue(t, ctx, node, node.jobPayloadMap, "source")
			t.Logf("after failed move: local_jobs=%d owners=%v payload_exists=%t",
				len(first.Jobs()), owners, exists)
			assert.Len(t, first.Jobs(), 1)
			assert.Empty(t, second.Jobs())
			assert.Equal(t, []string{first.ID}, owners)
			assert.True(t, exists)
			assert.Equal(t, "accepted", payload)
			assert.EqualValues(t, 2, handler.attempts.Load(), "failed move restarts the old handler")

			first.rebalance(ctx, node.activeWorkers())
			require.Eventually(t, func() bool { return len(second.Jobs()) == 1 },
				3*time.Second, time.Millisecond)
			assert.Empty(t, first.Jobs())
			assert.Equal(t, []string{second.ID}, snapshotJobOwners(t, ctx, node, "source"))
			assert.EqualValues(t, 3, handler.attempts.Load(), "the retried move starts the destination once")
		})
	}
}

// deliverRecoveredJob waits for the exact published event to be acknowledged,
// rather than using handler calls or logs as a proxy for delivery completion.
func deliverRecoveredJob(t *testing.T, ctx context.Context, node *Node, job *Job) {
	t.Helper()
	eventID, err := node.poolStream.Add(ctx, evStartJob, marshalJob(job))
	require.NoError(t, err)
	streamKey := generationStreamKey(ctx, node.rdb, node.poolStream.Name)
	require.Eventually(t, func() bool {
		events, err := node.rdb.XRange(ctx, streamKey, eventID, eventID).Result()
		return err == nil && len(events) == 0
	}, 3*time.Second, time.Millisecond)
}

func (h *exclusiveJobHandler) Start(job *Job) error {
	h.attempts.Add(1)
	if _, loaded := h.active.LoadOrStore(job.Key, string(job.Payload)); loaded {
		return fmt.Errorf("source job %q already active", job.Key)
	}
	return nil
}

func (h *exclusiveJobHandler) Stop(key string) error {
	if _, loaded := h.active.LoadAndDelete(key); !loaded {
		return fmt.Errorf("source job %q not active", key)
	}
	return nil
}

func (h *rebalanceFailureHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *rebalanceFailureHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if h.removeOwnership {
			if cmd.Name() == "evalsha" && len(args) > 9 &&
				args[1] == mutatePoolMapScript.Hash() && args[9] == "remove" &&
				h.fail.CompareAndSwap(true, false) {
				return errors.New("ownership removal unavailable")
			}
			return next(ctx, cmd)
		}
		// Stream.Add uses four keys, followed by state, generation, max length
		// and event name. Match that append only, leaving ownership writes live.
		if (cmd.Name() == "evalsha" || cmd.Name() == "eval") &&
			len(args) > 10 && args[3] == h.lifecycleKey && args[10] == evStartJob &&
			h.fail.CompareAndSwap(true, false) {
			return errors.New("requeue publication unavailable")
		}
		return next(ctx, cmd)
	}
}

func (h *rebalanceFailureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
