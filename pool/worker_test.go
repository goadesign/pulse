package pool

import (
	"context"
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
