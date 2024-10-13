package pool

import (
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
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create a worker and dispatch a job
	worker := newTestWorker(t, ctx, node)
	assert.NoError(t, node.DispatchJob(ctx, testName, []byte("payload")))

	// Wait for the job to start
	require.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay)

	// stop refreshing keep-alive and remove the worker from the node's keep-alive map
	worker.workerTTL = time.Hour
	_, err := node.keepAliveMap.Delete(ctx, worker.ID)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(node.keepAliveMap.Map()) == 0 }, time.Second, delay)

	// Create a new worker to pick up the requeued job
	newWorker := newTestWorker(t, ctx, node)

	// Wait for the job to be requeued and started by the new worker
	// Increase 'max' to cover the time until requeue happens
	require.Eventually(t, func() bool { return len(newWorker.Jobs()) == 1 }, time.Second, delay)

	// Clean up
	assert.NoError(t, node.Shutdown(ctx))
}
