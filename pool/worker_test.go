package pool

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ptesting "goa.design/pulse/testing"
)

func TestWorkerRequeueJobs(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node     = newTestNode(t, ctx, rdb, testName)
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	// Create a worker and dispatch a job
	worker := newTestWorker(t, ctx, node)
	assert.NoError(t, node.DispatchJob(ctx, testName, []byte("payload")))

	// Wait for the job to start
	require.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay)

	// Emulate the worker failing by preventing it from refreshing its keepalive
	// This means we can't cleanup cleanly, hence "false" in CleanupRedis
	worker.lock.Lock()
	worker.stopped = true
	worker.lock.Unlock()

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
	}, time.Second, delay, "job was not requeued")
}

func TestStaleWorkerCleanup(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node     = newTestNode(t, ctx, rdb, testName)
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)

	// Create one active worker
	activeWorker := newTestWorker(t, ctx, node)

	// Create five stale workers
	staleWorkers := make([]*Worker, 5)
	for i := 0; i < 5; i++ {
		staleWorkers[i] = newTestWorker(t, ctx, node)
		staleWorkers[i].stop(ctx)
		// Set the last seen time to a past time
		_, err := node.keepAliveMap.Set(ctx, staleWorkers[i].ID, strconv.FormatInt(time.Now().Add(-2*node.workerTTL).UnixNano(), 10))
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
		node1    = newTestNode(t, ctx, rdb, testName+"_1")
		node2    = newTestNode(t, ctx, rdb, testName+"_2")
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
		_, err := node2.keepAliveMap.Set(ctx, staleWorkers[i].ID, strconv.FormatInt(time.Now().Add(-2*node2.workerTTL).UnixNano(), 10))
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
