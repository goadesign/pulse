package pool

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	ptesting "goa.design/pulse/testing"
)

func TestAttemptRequeueAccountsForEveryConcurrentHandoff(t *testing.T) {
	worker := &Worker{
		requeueTimeout: 30 * time.Millisecond,
		logger:         pulse.NoopLogger(),
	}
	jobs := map[string]*Job{
		"success": {Key: "success"},
		"late":    {Key: "late"},
		"failed":  {Key: "failed"},
		"blocked": {Key: "blocked"},
	}
	started := make(chan string, len(jobs))
	release := make(chan struct{})
	var running atomic.Int64
	send := func(ctx context.Context, job *Job) error {
		running.Add(1)
		defer running.Add(-1)
		started <- job.Key
		switch job.Key {
		case "success":
			<-release
			return nil
		case "late":
			<-release
			time.Sleep(5 * time.Millisecond)
			return nil
		case "failed":
			<-release
			return errors.New("injected handoff failure")
		case "blocked":
			<-ctx.Done()
			return ctx.Err()
		default:
			panic("unexpected job")
		}
	}
	result := make(chan map[string]*Job, 1)
	go func() {
		result <- worker.attemptRequeueWith(context.Background(), jobs, send)
	}()
	for range jobs {
		select {
		case <-started:
		case <-time.After(time.Second):
			require.Fail(t, "handoffs did not start concurrently")
		}
	}
	close(release)
	remaining := <-result
	require.Equal(t, map[string]*Job{
		"failed":  jobs["failed"],
		"blocked": jobs["blocked"],
	}, remaining)
	require.Zero(t, running.Load(), "attempt returned before all handoff goroutines joined")
}

func TestWorkerRequeueJobs(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)
	node, err := AddNode(
		ctx,
		testName,
		rdb,
		WithLogger(pulse.NoopLogger()),
		WithRequeueTimeout(testFastRequeueTimeout),
		WithJobSinkBlockDuration(testJobSinkBlockDuration),
		WithWorkerTTL(testFastWorkerTTL),
		WithDispatchTimeout(16*time.Second),
		WithRecoveryGrace(8*time.Second),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Create a worker and dispatch a job
	worker := newTestWorker(t, ctx, node)
	assert.NoError(t, node.DispatchJob(ctx, testName, []byte("payload")))

	// Wait for the job to start
	require.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay)

	// Emulate the worker failing by preventing it from refreshing its keepalive
	// This means we can't cleanup cleanly, hence "false" in CleanupRedis
	require.NoError(t, worker.stop(ctx))

	// Create a new worker to pick up the requeued job
	newWorker := newTestWorker(t, ctx, node)

	// Wait at least workerTTL
	time.Sleep(2 * node.workerTTL)

	// Route an event to trigger worker cleanup
	assert.NoError(t, node.DispatchJob(ctx, testName+"2", []byte("payload2")))

	// Wait for the job to be requeued and started by the new worker
	// Increase 'max' to cover the time until requeue happens
	require.Eventually(t, func() bool {
		return len(newWorker.Jobs()) == 2
	}, 10*time.Second, delay, "job was not requeued")

	// Cleanup
	assert.NoError(t, node.Shutdown(ctx))
}

func TestWorkerRebalanceReleasesPreviousJobOwner(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node     = newTestNode(t, ctx, rdb, testName)
	)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	node.h = &ptesting.Hasher{IndexFunc: func(_ string, numBuckets int64) int64 {
		return numBuckets - 1
	}}

	const jobKey = "rebalance-job"
	payload := []byte("payload")
	worker1 := newTestWorker(t, ctx, node)
	require.NoError(t, node.DispatchJob(ctx, jobKey, payload))
	require.Eventually(t, func() bool {
		return len(worker1.Jobs()) == 1
	}, max, delay)
	require.Eventually(t, func() bool {
		return sameStrings(jobOwners(node, jobKey), []string{worker1.ID})
	}, max, delay)

	worker2 := newTestWorker(t, ctx, node)
	worker1.rebalance(ctx, []string{worker1.ID, worker2.ID})
	require.Eventually(t, func() bool {
		return len(worker1.Jobs()) == 0 && len(worker2.Jobs()) == 1
	}, max, delay)
	require.Eventually(t, func() bool {
		return sameStrings(jobOwners(node, jobKey), []string{worker2.ID})
	}, max, delay, "job must have exactly one replicated owner after rebalance")
	gotPayload, ok := node.JobPayload(jobKey)
	require.True(t, ok)
	assert.Equal(t, payload, gotPayload)

	assert.NoError(t, node.Shutdown(ctx))
}

func TestWorkerJobsPreservesRequeuedState(t *testing.T) {
	worker := &Worker{ID: "worker", CreatedAt: time.Unix(1, 0)}
	worker.jobs.Store("job", &Job{
		Key:       "job",
		Payload:   []byte("payload"),
		CreatedAt: time.Unix(2, 0),
		Requeued:  true,
	})

	jobs := worker.Jobs()
	require.Len(t, jobs, 1)
	assert.True(t, jobs[0].Requeued)
}

func TestWorkerStartFailurePayloadOwnership(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node     = newTestNode(t, ctx, rdb, testName)
	)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	errStart := errors.New("start failed")
	worker := newTestWorker(t, ctx, node)
	worker.handler.(*mockHandler).startFunc = func(job *Job) error {
		return errStart
	}

	err := worker.startJob(ctx, &Job{
		Key:       "new-job",
		Payload:   []byte("new payload"),
		CreatedAt: time.Now(),
		NodeID:    node.ID,
	})
	assert.ErrorIs(t, err, errStart)
	_, ok := snapshotValue(t, ctx, node, node.jobPayloadMap, "new-job")
	assert.False(t, ok)
	assert.Empty(t, snapshotJobOwners(t, ctx, node, "new-job"))

	err = worker.startJob(ctx, &Job{
		Key:       "requeued-job",
		Payload:   []byte("requeued payload"),
		CreatedAt: time.Now(),
		NodeID:    node.ID,
		Requeued:  true,
	})
	assert.ErrorIs(t, err, errStart)
	assert.Empty(t, snapshotJobOwners(t, ctx, node, "requeued-job"))
	_, ok = snapshotValue(t, ctx, node, node.jobPayloadMap, "requeued-job")
	assert.False(t, ok)

	assert.NoError(t, node.Shutdown(ctx))
}

func TestWorkerStartFailureRetriesDurableCleanup(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	failure := errors.New("injected start cleanup failure")
	hook := &poolRedisHook{
		failure:         failure,
		startCleanupSHA: cleanupFailedStartScript.Hash(),
	}
	rdb.AddHook(hook)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	worker := newTestWorker(t, ctx, node)
	startFailure := errors.New("start failed")
	worker.handler.(*mockHandler).startFunc = func(job *Job) error {
		return startFailure
	}
	job := &Job{
		Key:       "job",
		Payload:   []byte("payload"),
		CreatedAt: time.Now(),
		NodeID:    node.ID,
	}

	hook.failStartCleanup.Store(true)
	err := worker.startJob(ctx, job)
	require.ErrorIs(t, err, ErrRequeue)
	require.ErrorIs(t, err, failure)
	require.Equal(t, []string{worker.ID}, snapshotJobOwners(t, ctx, node, job.Key))
	payload, ok := snapshotValue(t, ctx, node, node.jobPayloadMap, job.Key)
	require.True(t, ok)
	require.Equal(t, "payload", payload)

	hook.failStartCleanup.Store(false)
	err = worker.startJob(ctx, job)
	require.ErrorIs(t, err, startFailure)
	require.Empty(t, snapshotJobOwners(t, ctx, node, job.Key))
	_, ok = snapshotValue(t, ctx, node, node.jobPayloadMap, job.Key)
	require.False(t, ok)

	require.NoError(t, node.Shutdown(ctx))
}

func TestWorkerLoopDropsMalformedEventAndContinues(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	worker := newTestWorker(t, ctx, node)
	started := make(chan string, 1)
	worker.handler.(*mockHandler).startFunc = func(job *Job) error {
		started <- job.Key
		return nil
	}

	malformedID, err := worker.stream.Add(ctx, evStartJob, []byte{1, 2, 3})
	require.NoError(t, err)
	job := &Job{
		Key:       "valid",
		Payload:   []byte("payload"),
		CreatedAt: time.Now(),
		NodeID:    node.ID,
	}
	_, err = worker.stream.Add(ctx, evStartJob, marshalEnvelope(node.ID, marshalJob(job)))
	require.NoError(t, err)
	require.Equal(t, job.Key, <-started)
	streamKey := generationStreamKey(ctx, rdb, worker.stream.Name)
	require.Eventually(t, func() bool {
		events, rangeErr := rdb.XRange(ctx, streamKey, malformedID, malformedID).Result()
		return rangeErr == nil && len(events) == 0
	}, max, delay)

	require.NoError(t, node.Shutdown(ctx))
}

func TestWorkerControlEventsRequireLocalOwnership(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node     = newTestNode(t, ctx, rdb, testName)
	)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	worker := newTestWorker(t, ctx, node)
	stopCalled := false
	notifyCalled := false
	worker.handler.(*mockHandler).stopFunc = func(key string) error {
		stopCalled = true
		return nil
	}
	worker.handler.(*mockHandler).notifyFunc = func(key string, payload []byte) error {
		notifyCalled = true
		return nil
	}

	assert.ErrorIs(t, worker.stopJob(ctx, "missing-job"), ErrRequeue)
	assert.False(t, stopCalled)

	assert.ErrorIs(t, worker.notify(ctx, "missing-job", []byte("payload")), ErrRequeue)
	assert.False(t, notifyCalled)

	assert.NoError(t, node.Shutdown(ctx))
}

func TestStaleWorkerCleanupInNode(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node     = newFastCleanupTestNode(t, ctx, rdb, testName)
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)

	// Create one active worker
	activeWorker := newTestWorker(t, ctx, node)
	t.Log("active worker", activeWorker.ID)

	// Create five stale workers
	staleWorkers := make([]*Worker, 5)
	for i := 0; i < 5; i++ {
		staleWorkers[i] = newTestWorker(t, ctx, node)
		require.NoError(t, staleWorkers[i].stop(ctx))
		// Set the last seen time to a past time
		_, err := node.workerKeepAliveMap.Set(ctx, staleWorkers[i].ID, strconv.FormatInt(time.Now().Add(-2*node.workerTTL).UnixNano(), 10))
		assert.NoError(t, err)
	}

	// Wait for the cleanup process to run
	time.Sleep(3 * node.workerTTL)

	// Check if only the active worker remains
	workers := node.activeWorkers()
	assert.Len(t, workers, 1, "There should be only one worker remaining")
	assert.Contains(t, workers, activeWorker.ID, "The active worker should still exist")

	// Cleanup
	assert.NoError(t, node.Shutdown(ctx))
}

func TestStaleWorkerCleanupAcrossNodes(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node1    = newFastCleanupTestNode(t, ctx, rdb, testName+"_1")
		node2    = newFastCleanupTestNode(t, ctx, rdb, testName+"_2")
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)

	// Create one active worker on node1
	activeWorker := newTestWorker(t, ctx, node1)

	// Create five stale workers on node2
	staleWorkers := make([]*Worker, 5)
	for i := 0; i < 5; i++ {
		staleWorkers[i] = newTestWorker(t, ctx, node2)
		staleWorkers[i].stop(ctx)
		// Set the last seen time to a past time
		_, err := node2.workerKeepAliveMap.Set(ctx, staleWorkers[i].ID, strconv.FormatInt(time.Now().Add(-2*node2.workerTTL).UnixNano(), 10))
		assert.NoError(t, err)
	}

	// Wait for the cleanup process to run
	time.Sleep(3 * node2.workerTTL)

	// Check if only the active worker remains on node1
	workers1 := node1.activeWorkers()
	assert.Len(t, workers1, 1, "There should be only one worker remaining on node1")
	assert.Contains(t, workers1, activeWorker.ID, "The active worker should still exist on node1")

	// Check if all workers have been removed from node2
	workers2 := node2.activeWorkers()
	assert.Len(t, workers2, 0, "There should be no workers remaining on node2")

	// Verify that stale workers are not in the worker map of node2
	for _, worker := range staleWorkers {
		_, exists := node2.workerMap.Get(worker.ID)
		assert.False(t, exists, "Stale worker %s should not exist in the worker map of node2", worker.ID)
	}

	// Cleanup
	assert.NoError(t, node1.Shutdown(ctx))
	assert.NoError(t, node2.Shutdown(ctx))
}

func jobOwners(node *Node, key string) []string {
	var owners []string
	for workerID := range node.jobMap.Map() {
		values, ok := node.jobMap.GetValues(workerID)
		if !ok {
			continue
		}
		for _, value := range values {
			if value == key {
				owners = append(owners, workerID)
				break
			}
		}
	}
	sort.Strings(owners)
	return owners
}

func snapshotJobOwners(t *testing.T, ctx context.Context, node *Node, key string) []string {
	t.Helper()
	snapshot, err := rmap.Join(ctx, node.jobMap.Name, node.rdb)
	require.NoError(t, err)
	defer snapshot.Close()
	var owners []string
	for workerID := range snapshot.Map() {
		values, ok := snapshot.GetValues(workerID)
		if !ok {
			continue
		}
		for _, value := range values {
			if value == key {
				owners = append(owners, workerID)
				break
			}
		}
	}
	sort.Strings(owners)
	return owners
}

func snapshotValue(t *testing.T, ctx context.Context, node *Node, source *rmap.Map, key string) (string, bool) {
	t.Helper()
	snapshot, err := rmap.Join(ctx, source.Name, node.rdb)
	require.NoError(t, err)
	defer snapshot.Close()
	return snapshot.Get(key)
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
