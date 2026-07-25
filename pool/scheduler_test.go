package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
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
	assert.Eventually(t, func() bool {
		return len(node.schedulerJobMap.Keys()) == 0
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

	err := sched.startJobs(ctx, fence, []*JobParam{{Key: "collision", Payload: []byte("scheduled")}})
	require.ErrorIs(t, err, ErrJobExists)
	ownership, err := sched.schedulerOwnership(ctx, sched.keyPrefix+"collision")
	require.NoError(t, err)
	require.Empty(t, ownership)
	require.NoError(t, sched.clearJobs(ctx, fence))
	require.Len(t, worker.Jobs(), 1)
	require.Equal(t, []byte("foreign"), worker.Jobs()[0].Payload)
	require.NoError(t, node.Shutdown(ctx))
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
