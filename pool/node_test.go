package pool

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ptesting "goa.design/pulse/testing"
)

const (
	// delay is the delay between assertion checks
	delay = 10 * time.Millisecond
	// max is the maximum time to wait for an assertion to pass
	max = time.Second
)

func TestDispatchJobOneWorker(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	worker := newTestWorker(t, ctx, node)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	payload := []byte("test payload")

	// Dispatch job
	err := node.DispatchJob(ctx, testName, payload)
	assert.NoError(t, err, "Failed to dispatch job")

	// Verify job was received by worker
	require.Eventually(t, func() bool {
		return len(worker.Jobs()) == 1
	}, max, delay, "Worker did not receive the job within expected time")

	// Check if received payload matches dispatched payload
	assert.Equal(t, payload, worker.Jobs()[0].Payload, "Received payload does not match dispatched payload")

	// Shutdown node
	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestDispatchJobTwoWorkers(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	node.h = &ptesting.Hasher{IndexFunc: func(key string, numBuckets int64) int64 {
		if key == "job1" {
			return 0
		}
		return 1
	}}
	worker1 := newTestWorker(t, ctx, node)
	worker2 := newTestWorker(t, ctx, node)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	job1 := struct {
		key     string
		payload []byte
	}{
		key:     "job1",
		payload: []byte("payload1"),
	}
	job2 := struct {
		key     string
		payload []byte
	}{
		key:     "job2",
		payload: []byte("payload2"),
	}

	// Dispatch jobs
	assert.NoError(t, node.DispatchJob(ctx, job1.key, job1.payload), "Failed to dispatch job1")
	assert.NoError(t, node.DispatchJob(ctx, job2.key, job2.payload), "Failed to dispatch job2")

	// Wait for jobs to be processed
	require.Eventually(t, func() bool { return len(worker1.Jobs()) == 1 }, max, delay, "Worker1 did not receive a job")
	require.Eventually(t, func() bool { return len(worker2.Jobs()) == 1 }, max, delay, "Worker2 did not receive a job")

	// Verify job distribution
	assert.Contains(t, [][]byte{job1.payload, job2.payload}, worker1.Jobs()[0].Payload, "Worker1 received unexpected payload")
	assert.Contains(t, [][]byte{job1.payload, job2.payload}, worker2.Jobs()[0].Payload, "Worker2 received unexpected payload")

	// Shutdown
	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestRemoveWorkerThenShutdown(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	worker := newTestWorker(t, ctx, node)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Dispatch a job and verify it's received by the worker
	assert.NoError(t, node.DispatchJob(ctx, testName, []byte("payload")))
	assert.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay, "Job was not received by the worker")

	// Remove the worker and verify the job is removed
	assert.NoError(t, node.RemoveWorker(ctx, worker))
	assert.Eventually(t, func() bool { return len(worker.Jobs()) == 0 }, max, delay, "Job was not removed from the worker")

	// Shutdown the node
	assert.NoError(t, node.Shutdown(ctx))
}

func TestClose(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	worker := newTestWorker(t, ctx, node)

	defer ptesting.CleanupRedis(t, rdb, false, testName)

	// Dispatch a job and verify it's received
	payload := []byte("payload")
	assert.NoError(t, node.DispatchJob(ctx, testName, payload))
	assert.Eventually(t, func() bool { return len(worker.Jobs()) == 1 }, max, delay, "Job was not received by the worker")

	// Close the node
	assert.NoError(t, node.Close(ctx), "Failed to close the node")

	// Verify node closure and job removal
	assert.Eventually(t, func() bool { return node.IsClosed() }, max, delay, "Node did not close within the expected time")
	assert.Equal(t, 0, len(worker.Jobs()), "Jobs were not removed from the worker after node closure")

	// Shutdown the node
	assert.NoError(t, node.Shutdown(ctx))
}

func TestTwoNodeJobDispatchAndAck(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node1 := newTestNode(t, ctx, rdb, testName)
	node2 := newTestNode(t, ctx, rdb, testName)
	worker1 := newTestWorker(t, ctx, node1)
	worker2 := newTestWorker(t, ctx, node2)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Configure nodes to send all jobs to worker2
	node1.h, node2.h = &ptesting.Hasher{Index: 1}, &ptesting.Hasher{Index: 1}

	// Set up job completion signal
	jobDone := make(chan struct{})

	// Configure worker behaviors
	worker1.handler.(*mockHandler).startFunc = func(job *Job) error {
		t.Errorf("Unexpected job received by worker1: %+v", job)
		return nil
	}
	worker2.handler.(*mockHandler).startFunc = func(job *Job) error {
		close(jobDone)
		return nil
	}

	// Test job dispatch and execution
	payload := []byte("test-payload")
	require.NoError(t, node1.DispatchJob(ctx, testName, payload), "Failed to dispatch job from node1")

	// Verify job started on worker2
	require.Eventually(t, func() bool {
		return len(worker2.Jobs()) > 0
	}, max, delay, "Job was not started on worker2 within expected time")

	// Verify pending events are cleared on node1
	require.Eventually(t, func() bool {
		return len(node1.pendingEvents) == 0
	}, max, delay, "Pending events were not cleared on node1 within expected time")

	// Clean up
	require.NoError(t, node1.Shutdown(ctx), "Failed to shutdown node1")
}

func TestNodeCloseAndRequeue(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node1 := newTestNode(t, ctx, rdb, testName)
	node2 := newTestNode(t, ctx, rdb, testName)
	worker1 := newTestWorker(t, ctx, node1)
	worker2 := newTestWorker(t, ctx, node2)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Configure nodes to send all jobs to worker1
	node1.h, node2.h = &ptesting.Hasher{Index: 0}, &ptesting.Hasher{Index: 0}

	// Set up job requeuing detection
	jobRequeued := make(chan struct{})
	worker2.handler.(*mockHandler).startFunc = func(job *Job) error {
		close(jobRequeued)
		return nil
	}

	// Dispatch a job from node1
	payload := []byte("test-payload")
	require.NoError(t, node1.DispatchJob(ctx, testName, payload), "Failed to dispatch job from node1")

	// Verify job started on worker1
	require.Eventually(t, func() bool {
		return len(worker1.Jobs()) > 0
	}, max, delay, "Job was not started on worker1 within expected time")

	// Close node1 and trigger requeuing
	require.NoError(t, node1.Close(ctx), "Failed to close node1")

	// Wait for job requeuing
	select {
	case <-jobRequeued:
		// Job successfully requeued
	case <-time.After(max):
		t.Error("Timeout: job was not requeued within expected time")
	}

	// Verify job is no longer on worker1
	assert.Empty(t, worker1.Jobs(), "Job should have been removed from worker1 after requeuing")

	// Clean up
	require.NoError(t, node2.Shutdown(ctx), "Failed to shutdown node2")
}
