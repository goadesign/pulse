package pool

import (
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerRequeueJobs(t *testing.T) {
	var (
		ctx      = testContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		node     = newTestNode(t, ctx, rdb, testName)
	)
	defer cleanup(t, rdb, false, testName)

	// Create a worker and dispatch a job
	worker := newTestWorker(t, ctx, node)
	handler := worker.handler.(*mockHandler)
	assert.NoError(t, node.DispatchJob(ctx, testName, []byte("payload")))

	// Wait for the job to start
	require.Eventually(t, func() bool { return len(handler.jobs) == 1 }, max, delay)
	assert.Equal(t, []byte("payload"), handler.jobs[testName].Payload)

	// stop refreshing keep-alive and remove the worker from the node's keep-alive map
	worker.workerTTL = time.Hour
	_, err := node.keepAliveMap.Delete(ctx, worker.ID)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(node.keepAliveMap.Map()) == 0 }, time.Second, delay)

	// Create a new worker to pick up the requeued job
	newWorker := newTestWorker(t, ctx, node)
	newHandler := newWorker.handler.(*mockHandler)

	// Wait for the job to be requeued and started by the new worker
	// Increase 'max' to cover the time until requeue happens
	require.Eventually(t, func() bool { return len(newHandler.jobs) == 1 }, time.Second, delay)
	assert.Equal(t, []byte("payload"), newHandler.jobs[testName].Payload)

	// Clean up
	assert.NoError(t, node.Shutdown(ctx))
}
