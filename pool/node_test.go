package pool

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/streaming"
	ptesting "goa.design/pulse/testing"
)

const (
	// delay is the delay between assertion checks
	delay = 10 * time.Millisecond
	// max is the maximum time to wait for an assertion to pass
	max = time.Second
)

func TestWorkers(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create a few workers
	worker1 := newTestWorker(t, ctx, node)
	worker2 := newTestWorker(t, ctx, node)
	worker3 := newTestWorker(t, ctx, node)

	// Get the list of workers
	workers := node.Workers()

	// Check if the number of workers is correct
	assert.Equal(t, 3, len(workers), "Expected 3 workers")

	// Check if all created workers are in the list
	expectedWorkers := []string{worker1.ID, worker2.ID, worker3.ID}
	actualWorkers := make([]string, len(workers))
	for i, w := range workers {
		actualWorkers[i] = w.ID
	}
	assert.ElementsMatch(t, expectedWorkers, actualWorkers, "The list of workers should contain all created workers")

	// Shutdown node
	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestPoolWorkers(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create workers on the current node
	worker1 := newTestWorker(t, ctx, node)
	worker2 := newTestWorker(t, ctx, node)

	// Create a worker on a different node
	otherNode := newTestNode(t, ctx, rdb, testName)
	worker3 := newTestWorker(t, ctx, otherNode)
	defer func() { assert.NoError(t, otherNode.Shutdown(ctx)) }()

	// Check if the number of workers is correct (should include workers from all nodes)
	assert.Eventually(t, func() bool {
		return len(node.PoolWorkers()) == 3
	}, max, delay, "Expected 3 workers in the pool")

	// Check if all created workers are in the list
	poolWorkers := node.PoolWorkers()
	workerIDs := make([]string, len(poolWorkers))
	for i, w := range poolWorkers {
		workerIDs[i] = w.ID
	}

	expectedWorkerIDs := []string{worker1.ID, worker2.ID, worker3.ID}
	assert.ElementsMatch(t, expectedWorkerIDs, workerIDs, "Not all expected workers were found in the pool")

	// Shutdown nodes
	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestJobKeys(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	node1 := newTestNode(t, ctx, rdb, testName)
	node2 := newTestNode(t, ctx, rdb, testName)
	newTestWorker(t, ctx, node1)
	newTestWorker(t, ctx, node2)
	defer func() {
		assert.NoError(t, node1.Shutdown(ctx))
		assert.NoError(t, node2.Shutdown(ctx))
	}()

	// Configure nodes to send jobs to specific workers
	node1.h, node2.h = &ptesting.Hasher{Index: 0}, &ptesting.Hasher{Index: 1}

	jobs := []struct {
		key     string
		payload []byte
	}{
		{key: "job1", payload: []byte("payload1")},
		{key: "job2", payload: []byte("payload2")},
		{key: "job3", payload: []byte("payload3")},
		{key: "job4", payload: []byte("payload4")},
	}

	for _, job := range jobs {
		assert.NoError(t, node1.DispatchJob(ctx, job.key, job.payload), fmt.Sprintf("Failed to dispatch job: %s", job.key))
	}

	// Get job keys from the pool and check if all dispatched job keys are present
	var allJobKeys []string
	assert.Eventually(t, func() bool {
		allJobKeys = node1.JobKeys()
		return len(jobs) == len(allJobKeys)
	}, max, delay, fmt.Sprintf("Number of job keys doesn't match the number of dispatched jobs: %d != %d", len(jobs), len(allJobKeys)))
	for _, job := range jobs {
		assert.Contains(t, allJobKeys, job.key, fmt.Sprintf("Job key %s not found in JobKeys", job.key))
	}

	// Dispatch a job with an existing key to node1
	assert.NoError(t, node1.DispatchJob(ctx, "job1", []byte("updated payload")), "Failed to dispatch job with existing key")

	// Check that the number of job keys hasn't changed
	updatedAllJobKeys := node1.JobKeys()
	assert.Equal(t, len(allJobKeys), len(updatedAllJobKeys), "Number of job keys shouldn't change when updating an existing job")
}

func TestJobPayload(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	node := newTestNode(t, ctx, rdb, testName)
	newTestWorker(t, ctx, node)
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()

	tests := []struct {
		name    string
		key     string
		payload []byte
	}{
		{"job with payload", "job1", []byte("payload1")},
		{"job without payload", "job2", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, node.DispatchJob(ctx, tt.key, tt.payload), "Failed to dispatch job")

			// Check if job payload is correct
			assert.Eventually(t, func() bool {
				payload, ok := node.JobPayload(tt.key)
				fmt.Println(payload, ok)
				fmt.Println(tt.payload)
				return ok && assert.Equal(t, tt.payload, payload)
			}, max, delay, fmt.Sprintf("Failed to get correct payload for job %s", tt.key))
		})
	}

	// Test non-existent job
	payload, ok := node.JobPayload("non-existent-job")
	assert.False(t, ok, "Expected false for non-existent job")
	assert.Nil(t, payload, "Expected nil payload for non-existent job")

	// Update existing job
	updatedPayload := []byte("updated payload")
	assert.NoError(t, node.DispatchJob(ctx, "job1", updatedPayload), "Failed to update existing job")

	// Check if the payload was updated
	assert.Eventually(t, func() bool {
		payload, ok := node.JobPayload("job1")
		return ok && assert.Equal(t, updatedPayload, payload, "Payload was not updated correctly")
	}, max, delay, "Failed to get updated payload for job")
}

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

func TestNotifyWorker(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create a worker
	worker := newTestWorker(t, ctx, node)

	// Set up notification handling
	jobKey := "test-job"
	jobPayload := []byte("job payload")
	notificationPayload := []byte("test notification")
	ch := make(chan []byte, 1)
	worker.handler.(*mockHandler).notifyFunc = func(key string, payload []byte) error {
		assert.Equal(t, jobKey, key, "Received notification for the wrong key")
		assert.Equal(t, notificationPayload, payload, "Received notification for the wrong payload")
		close(ch)
		return nil
	}

	// Dispatch a job to ensure the worker is assigned
	require.NoError(t, node.DispatchJob(ctx, jobKey, jobPayload))

	// Send a notification
	err := node.NotifyWorker(ctx, jobKey, notificationPayload)
	require.NoError(t, err, "Failed to send notification")

	// Wait for the notification to be received
	select {
	case <-ch:
	case <-time.After(max):
		t.Fatal("Timeout waiting for notification to be received")
	}

	// Shutdown node
	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestNotifyWorkerNoHandler(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx, buf := ptesting.NewBufferedLogContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create a worker without NotificationHandler implementation
	worker := newTestWorkerWithoutNotify(t, ctx, node)

	// Dispatch a job to ensure the worker is assigned
	jobKey := "test-job"
	jobPayload := []byte("job payload")
	require.NoError(t, node.DispatchJob(ctx, jobKey, jobPayload))

	// Wait for the job to be received by the worker
	require.Eventually(t, func() bool {
		return len(worker.Jobs()) == 1
	}, max, delay, "Job was not received by the worker")

	// Send a notification
	notificationPayload := []byte("test notification")
	assert.NoError(t, node.NotifyWorker(ctx, jobKey, notificationPayload), "Failed to send notification")

	// Check that an error was logged
	assert.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "worker does not implement NotificationHandler, ignoring notification")
	}, max, delay, "Expected error message was not logged within the timeout period")

	// Ensure the worker is still functioning
	assert.Len(t, worker.Jobs(), 1, "Worker should still have the job")

	// Shutdown node
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
		var count int
		node1.pendingEvents.Range(func(_, _ any) bool { count++; return true })
		return count == 0
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

func TestAckWorkerEventWithMissingPendingEvent(t *testing.T) {
	// Setup
	ctx := ptesting.NewTestContext(t)
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	node := newTestNode(t, ctx, rdb, testName)
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()

	// Create a mock event with a non-existent pending event ID
	mockEvent := &streaming.Event{
		ID:        "non-existent-event-id",
		EventName: evAck,
		Payload:   marshalEnvelope("worker", marshalAck(&ack{EventID: "non-existent-event-id"})),
		Acker: &mockAcker{
			XAckFunc: func(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd {
				return redis.NewIntCmd(ctx, 0)
			},
		},
	}

	// Call ackWorkerEvent with the mock event
	node.ackWorkerEvent(ctx, mockEvent)

	// Verify that no panic occurred and the function completed successfully
	assert.True(t, true, "ackWorkerEvent should complete without panic")
}

func TestStaleEventsAreRemoved(t *testing.T) {
	// Setup
	ctx := ptesting.NewTestContext(t)
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	node := newTestNode(t, ctx, rdb, testName)
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()

	// Add a stale event manually
	staleEventID := fmt.Sprintf("%d-0", time.Now().Add(-2*pendingEventTTL).UnixNano()/int64(time.Millisecond))
	staleEvent := &streaming.Event{
		ID:        staleEventID,
		EventName: "test-event",
		Payload:   []byte("test-payload"),
		Acker: &mockAcker{
			XAckFunc: func(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd {
				return redis.NewIntCmd(ctx, 0)
			},
		},
	}
	node.pendingEvents.Store(pendingEventKey("worker", staleEventID), staleEvent)

	// Add a fresh event
	freshEventID := fmt.Sprintf("%d-0", time.Now().Add(-time.Second).UnixNano()/int64(time.Millisecond))
	freshEvent := &streaming.Event{
		ID:        freshEventID,
		EventName: "test-event",
		Payload:   []byte("test-payload"),
		Acker: &mockAcker{
			XAckFunc: func(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd {
				return redis.NewIntCmd(ctx, 0)
			},
		},
	}
	node.pendingEvents.Store(pendingEventKey("worker", freshEventID), freshEvent)

	// Create a mock event to trigger the ackWorkerEvent function
	mockEventID := "mock-event-id"
	mockEvent := &streaming.Event{
		ID:        mockEventID,
		EventName: evAck,
		Payload:   marshalEnvelope("worker", marshalAck(&ack{EventID: mockEventID})),
		Acker: &mockAcker{
			XAckFunc: func(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd {
				return redis.NewIntCmd(ctx, 0)
			},
		},
	}
	node.pendingEvents.Store(pendingEventKey("worker", mockEventID), mockEvent)

	// Call ackWorkerEvent to trigger the stale event cleanup
	node.ackWorkerEvent(ctx, mockEvent)

	assert.Eventually(t, func() bool {
		_, ok := node.pendingEvents.Load(pendingEventKey("worker", staleEventID))
		return !ok
	}, max, delay, "Stale event should have been removed")

	assert.Eventually(t, func() bool {
		_, ok := node.pendingEvents.Load(pendingEventKey("worker", freshEventID))
		return ok
	}, max, delay, "Fresh event should still be present")
}

type mockAcker struct {
	XAckFunc func(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd
}

func (m *mockAcker) XAck(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd {
	return m.XAckFunc(ctx, streamKey, sinkName, ids...)
}
