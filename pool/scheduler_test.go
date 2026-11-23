package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ptesting "goa.design/pulse/testing"
)

func TestSchedule(t *testing.T) {
	var (
		rdb      = ptesting.NewRedisClient(t)
		ctx, buf = ptesting.NewBufferedLogContext(t)
		testName = ulid.Make().String()
		node     = newTestNode(t, ctx, rdb, testName)
		worker   = newTestWorker(t, ctx, node)
		d        = 10 * time.Millisecond
		iter     = 0
		lock     sync.Mutex
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)

	inc := func() { lock.Lock(); iter++; lock.Unlock() }
	it := func() int { lock.Lock(); defer lock.Unlock(); return iter }

	producer := newTestProducer(testName, func() (*JobPlan, error) {
		inc()
		switch it() {
		case 1:
			assert.Equal(t, 0, len(worker.Jobs()), "unexpected number of jobs")
			// First iteration: start a job
			return &JobPlan{Start: []*JobParam{{Key: testName, Payload: []byte("payload")}}}, nil
		case 2:
			assert.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay, "job not started")
			// Second iteration: stop job
			return &JobPlan{Stop: []string{testName}}, nil
		case 3:
			assert.Eventually(t, func() bool { return len(worker.Jobs()) == 0 }, max, delay, "job not stopped")
			// Third iteration: start two jobs
			return &JobPlan{Start: []*JobParam{
				{Key: testName + "1", Payload: []byte("payload")},
				{Key: testName + "2", Payload: []byte("payload")}}}, nil
		case 4:
			assert.Eventually(t, func() bool { return len(worker.Jobs()) == 2 }, max, delay, "jobs not started")
			// Fourth iteration: stop all jobs
			return &JobPlan{StopAll: true}, nil
		case 5:
			assert.Eventually(t, func() bool { return len(worker.Jobs()) == 0 }, max, delay, "jobs not stopped")
			// Fifth iteration: start one
			return &JobPlan{Start: []*JobParam{{Key: testName, Payload: []byte("payload")}}}, nil
		case 6:
			assert.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay, "job not started")
			//  Sixth iteration: start one, stop one
			return &JobPlan{
				Start:   []*JobParam{{Key: testName + "1", Payload: []byte("payload")}},
				StopAll: true,
			}, nil
		case 7:
			assert.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay, "job not started")
			// Seventh iteration: stop schedule
			return nil, ErrScheduleStop
		}
		t.Errorf("unexpected iteration %d", it())
		return nil, nil
	})

	err := node.Schedule(ctx, producer, d)
	require.NoError(t, err)

	assert.Eventually(t, func() bool { return it() == 7 }, 5*time.Second, delay, "schedule should have stopped, got %d", it())
	// The scheduler owns a job through a field named "<hex producer name>:<job
	// key>" in this map; it also keeps private "=transition:..." fields there,
	// which are not ownership.
	ownerPrefix := hex.EncodeToString([]byte(producer.Name())) + ":"
	assert.Eventually(t, func() bool {
		for _, key := range node.schedulerJobMap.Keys() {
			if strings.HasPrefix(key, ownerPrefix) {
				return false
			}
		}
		return true
	}, time.Second, delay, "scheduler ownership should be cleared")
	assert.NotContains(t, buf.String(), "level=error", "unexpected logged error")
}

func TestSchedulePlansOnceAcrossNodes(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	poolName := ulid.Make().String()
	first := newTestNode(t, ctx, rdb, poolName)
	second := newTestNode(t, ctx, rdb, poolName)
	var plans atomic.Int64
	newProducer := func() JobProducer {
		return newTestProducer("shared", func() (*JobPlan, error) {
			plans.Add(1)
			return nil, ErrScheduleStop
		})
	}

	require.NoError(t, first.Schedule(ctx, newProducer(), 20*time.Millisecond))
	require.NoError(t, second.Schedule(ctx, newProducer(), 20*time.Millisecond))
	require.Eventually(t, func() bool {
		return plans.Load() == 1
	}, time.Second, time.Millisecond)
	require.Never(t, func() bool {
		return plans.Load() > 1
	}, 100*time.Millisecond, 5*time.Millisecond)
	require.NoError(t, first.Shutdown(ctx))
}

func TestScheduleRenewsOwnershipAcrossSlowPlan(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	poolName := ulid.Make().String()
	first := newTestNode(t, ctx, rdb, poolName)
	second := newTestNode(t, ctx, rdb, poolName)
	entered := make(chan struct{})
	release := make(chan struct{})
	var (
		plans atomic.Int64
		once  sync.Once
	)
	newProducer := func() JobProducer {
		return &testProducer{
			name: "slow-shared",
			compute: func(ctx context.Context) (*JobPlan, error) {
				plans.Add(1)
				once.Do(func() {
					close(entered)
				})
				select {
				case <-release:
					return nil, ErrScheduleStop
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}
	}
	require.NoError(t, first.Schedule(ctx, newProducer(), time.Millisecond))
	require.NoError(t, second.Schedule(ctx, newProducer(), time.Millisecond))
	<-entered
	time.Sleep(3 * first.workerTTL)
	require.EqualValues(t, 1, plans.Load())
	close(release)
	require.Never(t, func() bool {
		return plans.Load() > 1
	}, 100*time.Millisecond, 5*time.Millisecond)
	require.NoError(t, first.Shutdown(ctx))
}

func TestScheduleRetriesFailedStartTransition(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, ulid.Make().String())
	worker := newTestWorker(t, ctx, node)
	var starts atomic.Int64
	worker.handler.(*mockHandler).startFunc = func(*Job) error {
		if starts.Add(1) == 1 {
			return errors.New("injected start failure")
		}
		return nil
	}
	producer := newTestProducer("retry", func() (*JobPlan, error) {
		if starts.Load() >= 2 {
			return nil, ErrScheduleStop
		}
		return &JobPlan{
			Start: []*JobParam{{Key: "job", Payload: []byte("payload")}},
		}, nil
	})

	require.NoError(t, node.Schedule(ctx, producer, 20*time.Millisecond))
	require.Eventually(t, func() bool {
		return starts.Load() == 2 && len(worker.Jobs()) == 1
	}, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, node.Shutdown(ctx))
}

func TestNodeCloseCancelsAndJoinsSchedules(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, ulid.Make().String())
	entered := make(chan struct{})
	var once sync.Once
	producer := &testProducer{
		name: "owned",
		compute: func(ctx context.Context) (*JobPlan, error) {
			once.Do(func() {
				close(entered)
			})
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	require.NoError(t, node.Schedule(ctx, producer, time.Millisecond))
	<-entered

	closed := make(chan error, 1)
	go func() {
		closed <- node.Close(ctx)
	}()
	select {
	case err := <-closed:
		require.Failf(t, "Close returned before schedule exited", "error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, <-closed)
	require.True(t, node.IsClosed())
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestScheduleCallerCancellationStopsOwnedSchedule(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, ulid.Make().String())
	scheduleCtx, cancel := context.WithCancel(ctx)
	var plans atomic.Int64
	producer := newTestProducer("caller", func() (*JobPlan, error) {
		plans.Add(1)
		return &JobPlan{}, nil
	})
	require.NoError(t, node.Schedule(scheduleCtx, producer, time.Millisecond))
	require.Eventually(t, func() bool {
		return plans.Load() > 0
	}, time.Second, time.Millisecond)
	cancel()
	done := make(chan struct{})
	go func() {
		node.scheduleWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "caller cancellation did not join schedule")
	}
	require.False(t, node.IsClosed())
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestSchedulerDispatchOwnershipIsExactAcrossNodes(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	poolName := ulid.Make().String()
	first := newTestNode(t, ctx, rdb, poolName)
	second := newTestNode(t, ctx, rdb, poolName)
	worker := newTestWorker(t, ctx, first)
	var starts atomic.Int64
	worker.handler.(*mockHandler).startFunc = func(*Job) error {
		starts.Add(1)
		return nil
	}
	producer := newTestProducer("shared", func() (*JobPlan, error) {
		return &JobPlan{}, nil
	})
	newScheduler := func(node *Node) *scheduler {
		encodedName := hex.EncodeToString([]byte(producer.Name()))
		return &scheduler{
			name:             poolName + ":shared",
			interval:         20 * time.Millisecond,
			producer:         producer,
			node:             node,
			keyPrefix:        encodedName + ":",
			transitionPrefix: "=transition:" + encodedName + ":",
			owner:            "test-" + ulid.Make().String(),
			lease:            node.workerTTL,
			logger:           node.logger,
		}
	}
	firstScheduler := newScheduler(first)
	secondScheduler := newScheduler(second)
	job := &JobParam{Key: "scheduled", Payload: []byte("payload")}
	firstFence := claimTestSchedulerTransition(t, ctx, firstScheduler)
	require.NoError(t, firstScheduler.startJobs(ctx, firstFence, []*JobParam{job}))
	require.NoError(t, firstScheduler.releaseTransition(ctx, firstFence))
	secondFence := claimTestSchedulerTransition(t, ctx, secondScheduler)
	require.NoError(t, secondScheduler.startJobs(ctx, secondFence, []*JobParam{job}))
	require.EqualValues(t, 1, starts.Load())
	field := firstScheduler.keyPrefix + job.Key
	dispatchID, err := firstScheduler.schedulerOwnership(ctx, field)
	require.NoError(t, err)
	require.Contains(t, dispatchID, "scheduler-")

	require.NoError(t, secondScheduler.stopJobs(ctx, secondFence, &JobPlan{Stop: []string{job.Key}}))
	require.Eventually(t, func() bool {
		return len(worker.Jobs()) == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, first.Shutdown(ctx))
}

func TestSchedulerStopAllRequiresCurrentTransitionOwner(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	poolName := ulid.Make().String()
	first := newTestNode(t, ctx, rdb, poolName)
	second := newTestNode(t, ctx, rdb, poolName)
	worker := newTestWorker(t, ctx, first)
	producer := newTestProducer("shared", func() (*JobPlan, error) {
		return &JobPlan{}, nil
	})
	newScheduler := func(node *Node) *scheduler {
		encodedName := hex.EncodeToString([]byte(producer.Name()))
		return &scheduler{
			name:             poolName + ":shared",
			interval:         20 * time.Millisecond,
			producer:         producer,
			node:             node,
			keyPrefix:        encodedName + ":",
			transitionPrefix: "=transition:" + encodedName + ":",
			owner:            "test-" + ulid.Make().String(),
			lease:            node.workerTTL,
			logger:           node.logger,
		}
	}
	owner := newScheduler(first)
	contender := newScheduler(second)
	job := &JobParam{Key: "scheduled", Payload: []byte("payload")}
	fence := claimTestSchedulerTransition(t, ctx, owner)
	require.NoError(t, owner.startJobs(ctx, fence, []*JobParam{job}))

	claim, err := contender.claimTransition(ctx)
	require.NoError(t, err)
	require.False(t, claim.owned)
	err = contender.stopJobs(ctx, "not-owner", &JobPlan{StopAll: true})
	require.ErrorContains(t, err, "SCHEDULERLEASELOST")
	require.Len(t, worker.Jobs(), 1)
	dispatchID, err := owner.schedulerOwnership(ctx, owner.keyPrefix+job.Key)
	require.NoError(t, err)
	require.NotEmpty(t, dispatchID)

	require.NoError(t, owner.stopJobs(ctx, fence, &JobPlan{StopAll: true}))
	require.Eventually(t, func() bool {
		return len(worker.Jobs()) == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, owner.releaseTransition(ctx, fence))
	require.NoError(t, first.Shutdown(ctx))
}

func TestSchedulerCollisionNeverOwnsForeignJob(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, ulid.Make().String())
	worker := newTestWorker(t, ctx, node)
	require.NoError(t, node.DispatchJob(ctx, "collision", []byte("foreign")))
	require.Eventually(t, func() bool {
		return len(worker.Jobs()) == 1
	}, time.Second, time.Millisecond)
	producer := newTestProducer("schedule", func() (*JobPlan, error) {
		return &JobPlan{}, nil
	})
	encodedName := hex.EncodeToString([]byte(producer.Name()))
	sched := &scheduler{
		name:             node.PoolName + ":schedule",
		interval:         20 * time.Millisecond,
		producer:         producer,
		node:             node,
		keyPrefix:        encodedName + ":",
		transitionPrefix: "=transition:" + encodedName + ":",
		owner:            "test-" + ulid.Make().String(),
		lease:            node.workerTTL,
		logger:           node.logger,
	}
	fence := claimTestSchedulerTransition(t, ctx, sched)

	// A foreign job already holds the key: the transition is satisfied, so it
	// reports no error and plans the remaining jobs, but it must never take
	// ownership of a job it did not dispatch. Repeated transitions converge on
	// the same outcome instead of erroring every interval.
	plan := []*JobParam{
		{Key: "collision", Payload: []byte("scheduled")},
		{Key: "independent", Payload: []byte("independent")},
	}
	for range 2 {
		require.NoError(t, sched.startJobs(ctx, fence, plan))
		ownership, err := sched.schedulerOwnership(ctx, sched.keyPrefix+"collision")
		require.NoError(t, err)
		require.Empty(t, ownership, "a foreign job must never become scheduler-owned")
	}
	// The job planned after the collision still ran and is scheduler-owned.
	independent, err := sched.schedulerOwnership(ctx, sched.keyPrefix+"independent")
	require.NoError(t, err)
	require.NotEmpty(t, independent, "a collision must not abort the remaining plan")

	require.NoError(t, sched.clearJobs(ctx, fence))
	require.Eventually(t, func() bool {
		jobs := worker.Jobs()
		return len(jobs) == 1 && string(jobs[0].Payload) == "foreign"
	}, time.Second, time.Millisecond, "clearing schedules must not stop the foreign job")
	require.NoError(t, node.Shutdown(ctx))
}

func TestSchedulerKeepsOwnershipWhenItsOwnJobStillRuns(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, ulid.Make().String())
	worker := newTestWorker(t, ctx, node)
	producer := newTestProducer("schedule", func() (*JobPlan, error) { return &JobPlan{}, nil })
	encodedName := hex.EncodeToString([]byte(producer.Name()))
	sched := &scheduler{
		name:             node.PoolName + ":schedule",
		interval:         20 * time.Millisecond,
		producer:         producer,
		node:             node,
		keyPrefix:        encodedName + ":",
		transitionPrefix: "=transition:" + encodedName + ":",
		owner:            "test-" + ulid.Make().String(),
		lease:            node.workerTTL,
		logger:           node.logger,
	}
	fence := claimTestSchedulerTransition(t, ctx, sched)
	plan := []*JobParam{{Key: "recurring", Payload: []byte("scheduled")}}

	require.NoError(t, sched.startJobs(ctx, fence, plan))
	require.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, time.Second, time.Millisecond)
	first, err := sched.schedulerOwnership(ctx, sched.keyPrefix+"recurring")
	require.NoError(t, err)
	require.NotEmpty(t, first)

	// Later transitions observe their own job still running: the exact
	// dispatch identity is preserved instead of proposing a new one, so the
	// scheduler converges silently rather than re-dispatching every interval.
	for range 3 {
		require.NoError(t, sched.startJobs(ctx, fence, plan))
		again, err := sched.schedulerOwnership(ctx, sched.keyPrefix+"recurring")
		require.NoError(t, err)
		require.Equal(t, first, again)
	}
	require.Len(t, worker.Jobs(), 1)
	require.NoError(t, sched.clearJobs(ctx, fence))
	require.NoError(t, node.Shutdown(ctx))
}

func TestStopJobSurvivesWorkerOutagePastPendingEventTTL(t *testing.T) {
	originalTTL := pendingEventTTL
	pendingEventTTL = 40 * time.Millisecond
	t.Cleanup(func() {
		pendingEventTTL = originalTTL
	})

	newFastRecoveryNode := func(t *testing.T, ctx context.Context, rdb *redis.Client, name string) *Node {
		t.Helper()
		node, err := AddNode(
			ctx,
			name,
			rdb,
			WithRequeueTimeout(testFastRequeueTimeout),
			WithJobSinkBlockDuration(20*time.Millisecond),
			WithWorkerTTL(200*time.Millisecond),
			WithDispatchTimeout(2*testAckGracePeriod),
			WithRecoveryGrace(100*time.Millisecond),
		)
		require.NoError(t, err)
		return node
	}

	t.Run("scheduled stop during worker outage stops requeued job after recovery", func(t *testing.T) {
		rdb := ptesting.NewRedisClient(t)
		defer ptesting.CleanupRedis(t, rdb, false, "")
		ctx := ptesting.NewTestContext(t)
		node := newFastRecoveryNode(t, ctx, rdb, ulid.Make().String())
		defer func() {
			assert.NoError(t, node.Shutdown(ctx))
		}()

		firstWorker := newTestWorker(t, ctx, node)
		producer := newTestProducer("schedule", func() (*JobPlan, error) {
			return &JobPlan{}, nil
		})
		encodedName := hex.EncodeToString([]byte(producer.Name()))
		sched := &scheduler{
			name:             node.PoolName + ":schedule",
			interval:         20 * time.Millisecond,
			producer:         producer,
			node:             node,
			keyPrefix:        encodedName + ":",
			transitionPrefix: "=transition:" + encodedName + ":",
			owner:            "test-" + ulid.Make().String(),
			lease:            node.workerTTL,
			logger:           node.logger,
		}
		fence := claimTestSchedulerTransition(t, ctx, sched)
		const jobKey = "retired-during-outage"
		require.NoError(t, sched.startJobs(ctx, fence, []*JobParam{{Key: jobKey, Payload: []byte("scheduled")}}))
		require.Eventually(t, func() bool {
			return len(firstWorker.Jobs()) == 1
		}, time.Second, time.Millisecond)

		// Remove the sole worker so the job is requeued with no active worker
		// available, then retire the job from the schedule and wait past
		// pendingEventTTL (40ms), orphaned-payload grace (2*workerTTL = 400ms),
		// and one 500ms sink idle-reclaim pass before adding a replacement worker.
		require.NoError(t, node.RemoveWorker(ctx, firstWorker))
		require.Empty(t, node.activeWorkers())
		require.NoError(t, sched.stopJobs(ctx, fence, &JobPlan{StopAll: true}))
		require.NoError(t, sched.releaseTransition(ctx, fence))

		node.requeueOrphanedPayloads(ctx)
		time.Sleep(600 * time.Millisecond)
		node.requeueOrphanedPayloads(ctx)

		replacement := newTestWorker(t, ctx, node)
		streamKey := generationStreamKey(ctx, rdb, node.poolStream.Name)
		payloadHash := rmapContentKey(node.resources.jobPayloads)
		pendingHash := rmapContentKey(node.resources.jobPending)
		require.Eventually(t, func() bool {
			_, hasPayload := node.JobPayload(jobKey)
			payloadExists, payloadErr := rdb.HExists(ctx, payloadHash, jobKey).Result()
			pendingExists, pendingErr := rdb.HExists(ctx, pendingHash, jobKey).Result()
			length, lenErr := rdb.XLen(ctx, streamKey).Result()
			return payloadErr == nil &&
				pendingErr == nil &&
				lenErr == nil &&
				!payloadExists &&
				!pendingExists &&
				length == 0 &&
				!hasPayload &&
				len(replacement.Jobs()) == 0
		}, 3*time.Second, 10*time.Millisecond)
		assert.Empty(t, snapshotJobOwners(t, ctx, node, jobKey))
	})

	t.Run("stop requested for pending dispatch before worker joins stops job after recovery", func(t *testing.T) {
		rdb := ptesting.NewRedisClient(t)
		defer ptesting.CleanupRedis(t, rdb, false, "")
		ctx := ptesting.NewTestContext(t)
		node := newFastRecoveryNode(t, ctx, rdb, ulid.Make().String())
		defer func() {
			assert.NoError(t, node.Shutdown(ctx))
		}()

		const jobKey = "pending-dispatch-stop"
		job := &Job{
			Key:        jobKey,
			Payload:    []byte("pending"),
			CreatedAt:  time.Now(),
			NodeID:     node.ID,
			dispatchID: "dispatch-pending-stop",
		}
		_, err := node.publishDispatch(ctx, jobKey, job.dispatchID, marshalJob(job))
		require.NoError(t, err)
		require.NoError(t, node.StopJob(ctx, jobKey))

		time.Sleep(600 * time.Millisecond)

		worker := newTestWorker(t, ctx, node)
		streamKey := generationStreamKey(ctx, rdb, node.poolStream.Name)
		payloadHash := rmapContentKey(node.resources.jobPayloads)
		pendingHash := rmapContentKey(node.resources.jobPending)
		require.Eventually(t, func() bool {
			_, hasPayload := node.JobPayload(jobKey)
			payloadExists, payloadErr := rdb.HExists(ctx, payloadHash, jobKey).Result()
			pendingExists, pendingErr := rdb.HExists(ctx, pendingHash, jobKey).Result()
			length, lenErr := rdb.XLen(ctx, streamKey).Result()
			return payloadErr == nil &&
				pendingErr == nil &&
				lenErr == nil &&
				!payloadExists &&
				!pendingExists &&
				length == 0 &&
				!hasPayload &&
				len(worker.Jobs()) == 0
		}, 3*time.Second, 10*time.Millisecond)
		assert.Empty(t, snapshotJobOwners(t, ctx, node, jobKey))
	})

	t.Run("stop for missing or failed-start job settles without leaking stream entries", func(t *testing.T) {
		rdb := ptesting.NewRedisClient(t)
		defer ptesting.CleanupRedis(t, rdb, false, "")
		ctx := ptesting.NewTestContext(t)
		node := newFastRecoveryNode(t, ctx, rdb, ulid.Make().String())
		defer func() {
			assert.NoError(t, node.Shutdown(ctx))
		}()

		streamKey := generationStreamKey(ctx, rdb, node.poolStream.Name)
		require.NoError(t, node.StopJob(ctx, "missing-job"))
		require.Eventually(t, func() bool {
			length, err := rdb.XLen(ctx, streamKey).Result()
			return err == nil && length == 0
		}, time.Second, 10*time.Millisecond)

		const failedKey = "failed-pending-stop"
		job := &Job{
			Key:        failedKey,
			Payload:    []byte("fail"),
			CreatedAt:  time.Now(),
			NodeID:     node.ID,
			dispatchID: "dispatch-failed-stop",
		}
		_, err := node.publishDispatch(ctx, failedKey, job.dispatchID, marshalJob(job))
		require.NoError(t, err)
		require.NoError(t, node.StopJob(ctx, failedKey))

		time.Sleep(600 * time.Millisecond)

		handler := newMockHandler()
		handler.startFunc = func(*Job) error {
			return errors.New("start failed")
		}
		worker, err := node.AddWorker(ctx, handler)
		require.NoError(t, err)

		payloadHash := rmapContentKey(node.resources.jobPayloads)
		pendingHash := rmapContentKey(node.resources.jobPending)
		require.Eventually(t, func() bool {
			payloadExists, payloadErr := rdb.HExists(ctx, payloadHash, failedKey).Result()
			pendingExists, pendingErr := rdb.HExists(ctx, pendingHash, failedKey).Result()
			length, lenErr := rdb.XLen(ctx, streamKey).Result()
			return payloadErr == nil &&
				pendingErr == nil &&
				lenErr == nil &&
				!payloadExists &&
				!pendingExists &&
				length == 0 &&
				len(worker.Jobs()) == 0
		}, 3*time.Second, 10*time.Millisecond)
		assert.Empty(t, snapshotJobOwners(t, ctx, node, failedKey))
	})

	t.Run("trailing requeued start behind stop does not resurrect stopped job", func(t *testing.T) {
		rdb := ptesting.NewRedisClient(t)
		defer ptesting.CleanupRedis(t, rdb, false, "")
		ctx := ptesting.NewTestContext(t)
		node := newFastRecoveryNode(t, ctx, rdb, ulid.Make().String())
		defer func() {
			assert.NoError(t, node.Shutdown(ctx))
		}()

		worker := newTestWorker(t, ctx, node)
		const jobKey = "trailing-requeue-after-stop"
		require.NoError(t, node.DispatchJob(ctx, jobKey, []byte("payload")))
		require.Eventually(t, func() bool {
			return len(worker.Jobs()) == 1
		}, time.Second, time.Millisecond)

		require.NoError(t, node.StopJob(ctx, jobKey))
		trailingRequeue := &Job{
			Key:       jobKey,
			Payload:   []byte("payload"),
			CreatedAt: time.Now(),
			NodeID:    node.ID,
			Requeued:  true,
		}
		_, err := node.poolStream.Add(ctx, evStartJob, marshalJob(trailingRequeue))
		require.NoError(t, err)

		streamKey := generationStreamKey(ctx, rdb, node.poolStream.Name)
		payloadHash := rmapContentKey(node.resources.jobPayloads)
		require.Eventually(t, func() bool {
			payloadExists, payloadErr := rdb.HExists(ctx, payloadHash, jobKey).Result()
			length, lenErr := rdb.XLen(ctx, streamKey).Result()
			return payloadErr == nil && lenErr == nil && !payloadExists && length == 0 && len(worker.Jobs()) == 0
		}, 3*time.Second, 10*time.Millisecond)
		assert.Empty(t, snapshotJobOwners(t, ctx, node, jobKey))
	})

	t.Run("stop settles delayed exact dispatch and retries cleanup without repeating handler stop", func(t *testing.T) {
		rdb := ptesting.NewRedisClient(t)
		defer ptesting.CleanupRedis(t, rdb, false, "")
		ctx := ptesting.NewTestContext(t)
		hook := &settlementFailureHook{
			attempted:  make(chan struct{}),
			err:        errors.New("transient settlement failure"),
			scriptHash: luaSettleDispatch.Hash(),
		}
		cleanupHook := &poolRedisHook{
			startCleanupSHA: cleanupFailedStartScript.Hash(),
			failure:         errors.New("transient stop cleanup failure"),
		}
		rdb.AddHook(hook)
		rdb.AddHook(cleanupHook)
		node := newFastRecoveryNode(t, ctx, rdb, ulid.Make().String())
		defer func() {
			assert.NoError(t, node.Shutdown(ctx))
		}()
		defer hook.fail.Store(false)
		defer cleanupHook.failStartCleanup.Store(false)

		var stopCalls atomic.Int32
		var failFirstStop atomic.Bool
		failFirstStop.Store(true)
		var stoppedOnce sync.Once
		stoppedCh := make(chan struct{})
		handler := newMockHandler()
		handler.stopFunc = func(string) error {
			stopCalls.Add(1)
			if failFirstStop.CompareAndSwap(true, false) {
				return errors.New("transient handler stop error")
			}
			stoppedOnce.Do(func() {
				close(stoppedCh)
			})
			return nil
		}
		worker, err := node.AddWorker(ctx, handler)
		require.NoError(t, err)
		hook.fail.Store(true)

		const jobKey = "stop-before-settlement"
		job := &Job{
			Key:        jobKey,
			Payload:    []byte("payload"),
			CreatedAt:  time.Now(),
			NodeID:     node.ID,
			dispatchID: "dispatch-stop-before-settlement",
		}
		_, err = node.publishDispatch(ctx, jobKey, job.dispatchID, marshalJob(job))
		require.NoError(t, err)

		select {
		case <-hook.attempted:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not attempt initial settlement")
		}

		payloadHash := rmapContentKey(node.resources.jobPayloads)
		pendingHash := rmapContentKey(node.resources.jobPending)
		require.NoError(t, node.StopJob(ctx, jobKey))

		// Wait until handler.Stop fails once and succeeds on retry while
		// luaSettleDispatch is still blocked; neither jobPending nor jobPayloads
		// may be deleted before dispatch settlement succeeds.
		select {
		case <-stoppedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not complete handler.Stop retry")
		}
		assert.EqualValues(t, 2, stopCalls.Load())
		payloadExists, err := rdb.HExists(ctx, payloadHash, jobKey).Result()
		require.NoError(t, err)
		assert.True(t, payloadExists)
		pendingExists, err := rdb.HExists(ctx, pendingHash, jobKey).Result()
		require.NoError(t, err)
		assert.True(t, pendingExists)

		// Fail cleanupFailedStart across at least two stopJob attempts once
		// settlement unblocks, proving stopJob settles jobPending and retries
		// durable cleanup without invoking handler.Stop again.
		cleanupHook.failStartCleanup.Store(true)
		hook.fail.Store(false)
		require.Eventually(t, func() bool {
			return cleanupHook.startCleanupAttempts.Load() >= 2
		}, 2*time.Second, 10*time.Millisecond)
		assert.EqualValues(t, 2, stopCalls.Load())
		payloadExists, err = rdb.HExists(ctx, payloadHash, jobKey).Result()
		require.NoError(t, err)
		assert.True(t, payloadExists)
		pendingExists, err = rdb.HExists(ctx, pendingHash, jobKey).Result()
		require.NoError(t, err)
		assert.False(t, pendingExists)

		cleanupHook.failStartCleanup.Store(false)
		streamKey := generationStreamKey(ctx, rdb, node.poolStream.Name)
		require.Eventually(t, func() bool {
			payloadExists, payloadErr := rdb.HExists(ctx, payloadHash, jobKey).Result()
			pendingExists, pendingErr := rdb.HExists(ctx, pendingHash, jobKey).Result()
			length, lenErr := rdb.XLen(ctx, streamKey).Result()
			return payloadErr == nil &&
				pendingErr == nil &&
				lenErr == nil &&
				!payloadExists &&
				!pendingExists &&
				length == 0 &&
				len(worker.Jobs()) == 0
		}, 3*time.Second, 10*time.Millisecond)
		assert.EqualValues(t, 2, stopCalls.Load())
		assert.Empty(t, snapshotJobOwners(t, ctx, node, jobKey))
	})

	t.Run("multi-job worker removal hands off requeued jobs without recoveryGrace stall", func(t *testing.T) {
		rdb := ptesting.NewRedisClient(t)
		defer ptesting.CleanupRedis(t, rdb, false, "")
		ctx := ptesting.NewTestContext(t)
		node, err := AddNode(
			ctx,
			ulid.Make().String(),
			rdb,
			WithRequeueTimeout(testFastRequeueTimeout),
			WithJobSinkBlockDuration(20*time.Millisecond),
			WithWorkerTTL(500*time.Millisecond),
			WithDispatchTimeout(2*testAckGracePeriod),
			WithRecoveryGrace(5*time.Second),
		)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, node.Shutdown(ctx))
		}()

		firstWorker := newTestWorker(t, ctx, node)
		require.NoError(t, node.DispatchJob(ctx, "handoff-job-1", []byte("one")))
		require.NoError(t, node.DispatchJob(ctx, "handoff-job-2", []byte("two")))
		require.Eventually(t, func() bool {
			return len(firstWorker.Jobs()) == 2
		}, time.Second, time.Millisecond)

		secondWorker := newTestWorker(t, ctx, node)
		require.NoError(t, node.RemoveWorker(ctx, firstWorker))
		require.Eventually(t, func() bool {
			return len(secondWorker.Jobs()) == 2
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("transient notification and message still expire after pendingEventTTL", func(t *testing.T) {
		rdb := ptesting.NewRedisClient(t)
		defer ptesting.CleanupRedis(t, rdb, false, "")
		ctx := ptesting.NewTestContext(t)
		node := newFastRecoveryNode(t, ctx, rdb, ulid.Make().String())
		defer func() {
			assert.NoError(t, node.Shutdown(ctx))
		}()

		const jobKey = "transient-notify"
		_, err := node.jobPayloadMap.SetAndWait(ctx, jobKey, "payload")
		require.NoError(t, err)
		require.NoError(t, node.NotifyWorker(ctx, jobKey, []byte("ping")))
		require.NoError(t, node.DispatchMessage(ctx, jobKey, []byte("msg")))

		streamKey := generationStreamKey(ctx, rdb, node.poolStream.Name)
		require.Eventually(t, func() bool {
			length, err := rdb.XLen(ctx, streamKey).Result()
			return err == nil && length == 0
		}, 2*time.Second, 10*time.Millisecond)
	})
}

type testProducer struct {
	name    string
	compute func(context.Context) (*JobPlan, error)
}

// newTestProducer returns a producer with the given name and compute schedule
// function.
func newTestProducer(name string, compute func() (*JobPlan, error)) JobProducer {
	return &testProducer{
		name: name,
		compute: func(context.Context) (*JobPlan, error) {
			return compute()
		},
	}
}
func (p *testProducer) Name() string { return p.name }
func (p *testProducer) Plan() (*JobPlan, error) {
	return p.compute(context.Background())
}
func (p *testProducer) PlanContext(ctx context.Context) (*JobPlan, error) {
	return p.compute(ctx)
}

// claimTestSchedulerTransition makes one constructed scheduler immediately due
// and returns its exact Redis fence.
func claimTestSchedulerTransition(t *testing.T, ctx context.Context, sched *scheduler) string {
	t.Helper()
	claim, err := sched.claimTransition(ctx)
	require.NoError(t, err)
	if !claim.owned {
		require.NoError(t, sched.node.rdb.HSet(
			ctx,
			rmapContentKey(sched.node.resources.schedulerJobs),
			sched.transitionPrefix+"next_ms",
			"1",
		).Err())
		claim, err = sched.claimTransition(ctx)
		require.NoError(t, err)
	}
	require.True(t, claim.owned)
	return claim.fence
}
