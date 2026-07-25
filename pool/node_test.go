package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming"
	ptesting "goa.design/pulse/testing"
)

const (
	// delay is the delay between assertion checks
	delay = 10 * time.Millisecond
	// max is the maximum time to wait for an assertion to pass
	max = time.Second
)

// poolRedisHook injects one rmap destroy failure for cleanup retry tests.
type poolRedisHook struct {
	failDestroy          atomic.Bool
	failCompletion       atomic.Bool
	failDetach           atomic.Bool
	failNodeInit         atomic.Bool
	failStartCleanup     atomic.Bool
	blockShutdownCheck   atomic.Bool
	key                  string
	detachKey            string
	completionSHA        string
	startCleanupSHA      string
	shutdownKey          string
	nodeStreamPrefix     string
	shutdownCheckStart   chan struct{}
	releaseShutdownCheck chan struct{}
	failure              error
}

// DialHook preserves normal Redis dialing.
func (h *poolRedisHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook fails the selected map's Lua destroy operation.
func (h *poolRedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.blockShutdownCheck.Load() && cmd.Name() == "hexists" {
			args := cmd.Args()
			if len(args) == 3 && args[1] == h.shutdownKey && args[2] == "shutdown" &&
				h.blockShutdownCheck.CompareAndSwap(true, false) {
				close(h.shutdownCheckStart)
				<-h.releaseShutdownCheck
			}
		}
		if h.failCompletion.Load() && cmd.Name() == "evalsha" {
			args := cmd.Args()
			if len(args) > 1 && args[1] == h.completionSHA {
				return h.failure
			}
		}
		if h.failStartCleanup.Load() && cmd.Name() == "evalsha" {
			args := cmd.Args()
			if len(args) > 1 && args[1] == h.startCleanupSHA {
				return h.failure
			}
		}
		if h.failDetach.Load() && cmd.Name() == "evalsha" {
			for _, arg := range cmd.Args() {
				if value, ok := arg.(string); ok && strings.HasPrefix(value, h.detachKey) {
					return h.failure
				}
			}
		}
		if h.failNodeInit.Load() && cmd.Name() == "evalsha" {
			for _, arg := range cmd.Args() {
				value, ok := arg.(string)
				if ok && strings.HasPrefix(value, h.nodeStreamPrefix) &&
					h.failNodeInit.CompareAndSwap(true, false) {
					return h.failure
				}
			}
		}
		if h.failDestroy.Load() && cmd.Name() == "evalsha" {
			for _, arg := range cmd.Args() {
				if key, ok := arg.(string); ok && key == h.key {
					return h.failure
				}
			}
		}
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook preserves normal Redis pipelines.
func (h *poolRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}

// generationStreamKey returns the lifecycle-selected physical stream key for a
// logical name, or the empty string before lifecycle binding.
func generationStreamKey(ctx context.Context, rdb *redis.Client, name string) string {
	key := rdb.HGet(ctx, "pulse:stream:"+name+":lifecycle", "physical_key").Val()
	if key == "" {
		return ""
	}
	if rdb.Type(ctx, key).Val() != "stream" {
		return ""
	}
	return key
}

func TestAddNodeRollsBackPostRegistrationFailure(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	poolName := strings.ReplaceAll(t.Name(), "/", "_")
	failure := errors.New("injected node stream initialization failure")
	hook := &poolRedisHook{
		nodeStreamPrefix: "pulse:stream:" + nodeStreamName(poolName, ""),
		failure:          failure,
	}
	hook.failNodeInit.Store(true)
	rdb.AddHook(hook)

	_, err := AddNode(ctx, poolName, rdb, WithJobSinkBlockDuration(50*time.Millisecond))
	require.ErrorIs(t, err, failure)
	leases, err := rdb.HKeys(ctx, rmapContentKey(nodeKeepAliveMapName(poolName))).Result()
	require.NoError(t, err)
	for _, key := range leases {
		require.True(t, key == "=rev" || key == "=kind", "leaked node registration %q", key)
	}
	keys, err := rdb.Keys(ctx, "pulse:stream:"+nodeStreamName(poolName, "")+"*").Result()
	require.NoError(t, err)
	for _, key := range keys {
		require.NotEqual(t, "stream", rdb.Type(ctx, key).Val(), "leaked node stream %q", key)
	}

	node, err := AddNode(ctx, poolName, rdb, WithJobSinkBlockDuration(50*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, node.Shutdown(ctx))
}

func TestAddNodeRejectsInvalidOptions(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	cases := []struct {
		name string
		opts []NodeOption
	}{
		{name: "worker TTL", opts: []NodeOption{WithWorkerTTL(time.Millisecond)}},
		{name: "requeue timeout", opts: []NodeOption{WithRequeueTimeout(500 * time.Microsecond)}},
		{name: "job sink block", opts: []NodeOption{WithJobSinkBlockDuration(500 * time.Microsecond)}},
		{name: "queued jobs", opts: []NodeOption{WithMaxQueuedJobs(0)}},
		{name: "dispatch timeout", opts: []NodeOption{WithDispatchTimeout(500 * time.Microsecond)}},
		{name: "recovery grace", opts: []NodeOption{WithRecoveryGrace(500 * time.Microsecond)}},
		{name: "cleanup lease", opts: []NodeOption{WithCleanupLease(500 * time.Microsecond)}},
		{
			name: "dispatch retention precision",
			opts: []NodeOption{WithDispatchResultRetention(500 * time.Microsecond)},
		},
		{
			name: "dispatch retention window",
			opts: []NodeOption{
				WithDispatchTimeout(time.Second),
				WithDispatchResultRetention(time.Second),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := AddNode(ctx, t.Name(), rdb, tc.opts...)
			require.Error(t, err)
		})
	}
}

func TestClosingImmediatelyFencesAdmission(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	node := newTestNode(t, ctx, rdb, strings.ReplaceAll(t.Name(), "/", "_"))

	closed := make(chan error, 1)
	go func() {
		closed <- node.Close(ctx)
	}()
	require.Eventually(t, func() bool {
		node.lock.RLock()
		defer node.lock.RUnlock()
		return node.closing
	}, max, delay)

	_, err := node.AddWorker(ctx, &mockHandler{})
	require.ErrorContains(t, err, "closed")
	require.ErrorContains(t, node.DispatchJob(ctx, "job", nil), "closed")
	require.ErrorContains(t, node.DispatchMessage(ctx, "message", nil), "closed")
	require.ErrorContains(t, node.StopJob(ctx, "job"), "closed")
	require.ErrorContains(t, node.NotifyWorker(ctx, "job", nil), "closed")
	require.NoError(t, <-closed)
}

func TestShutdownPublishesWhileCloseIsInProgress(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	poolName := strings.ReplaceAll(t.Name(), "/", "_")
	first := newTestNode(t, ctx, rdb, poolName)
	peer := newTestNode(t, ctx, rdb, poolName)

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- first.Close(ctx)
	}()
	require.Eventually(t, func() bool {
		first.lock.RLock()
		defer first.lock.RUnlock()
		return first.closing
	}, max, delay)
	require.NoError(t, first.Shutdown(ctx))
	require.NoError(t, <-closeResult)
	require.True(t, first.IsShutdown())
	require.True(t, peer.IsShutdown())
}

func TestAddNodeTakesOverExpiredPoolCleanup(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	poolName := strings.ReplaceAll(t.Name(), "/", "_")
	old := newTestNode(t, ctx, rdb, poolName)
	generation := old.poolStream.Generation()
	require.NoError(t, old.Close(ctx))
	require.NoError(t, rdb.HSet(
		ctx,
		poolCleanupGenerationsKey(poolName),
		"state", poolCleanupFinishingState,
		"generation", generation,
		"owner", "crashed",
		"lease_until", "0",
	).Err())
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(nodeShutdownMapName(poolName)),
		"shutdown", "crashed",
	).Err())

	next := newTestNode(t, ctx, rdb, poolName)
	require.NotEqual(t, generation, next.poolStream.Generation())
	record, err := rdb.HGetAll(ctx, poolCleanupGenerationsKey(poolName)).Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"state":      poolCleanupCompleteState,
		"generation": generation,
	}, record)
	require.NoError(t, next.Shutdown(ctx))
	record, err = rdb.HGetAll(ctx, poolCleanupGenerationsKey(poolName)).Result()
	require.NoError(t, err)
	require.Len(t, record, 2)
}

func TestExpiredCleanupOwnerCannotDeleteReusedPool(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	poolName := t.Name()
	stream, err := streaming.NewStream(poolStreamName(poolName), rdb)
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	generation := stream.Generation()
	resources, err := establishPoolResources(
		ctx,
		rdb,
		poolName,
		generation,
		1000,
		30*time.Second,
		time.Second,
		5*time.Minute,
	)
	require.NoError(t, err)
	_, err = stream.Add(ctx, evInit, nil)
	require.NoError(t, err)

	status, err := claimPoolCleanup(ctx, rdb, poolName, generation, "paused", time.Second)
	require.NoError(t, err)
	require.Equal(t, poolCleanupClaimed, status)
	require.NoError(t, rdb.HSet(ctx, poolCleanupGenerationsKey(poolName), "lease_until", "0").Err())
	status, err = claimPoolCleanup(ctx, rdb, poolName, generation, "takeover", time.Second)
	require.NoError(t, err)
	require.Equal(t, poolCleanupClaimed, status)

	require.NoError(t, destroyCleanupStream(ctx, rdb, poolName, generation, "takeover", time.Second))
	require.NoError(t, destroyPoolMap(
		ctx,
		rdb,
		poolName,
		resources.jobs,
		generation,
		"takeover",
		time.Second,
	))
	require.NoError(t, completePoolCleanupScript.Run(
		ctx,
		rdb,
		[]string{poolCleanupGenerationsKey(poolName), poolResourcesKey(poolName)},
		generation,
		"takeover",
		poolCleanupFinishingState,
		poolCleanupCompleteState,
		poolResourceStateActive,
		poolResourceStateDestroyed,
	).Err())
	require.Error(t, renewPoolCleanup(ctx, rdb, poolName, generation, "takeover", time.Second))

	recreated, err := streaming.NewStream(poolStreamName(poolName), rdb)
	require.NoError(t, err)
	_, err = recreated.Add(ctx, evInit, nil)
	require.NoError(t, err)
	require.NotEqual(t, generation, recreated.Generation())
	replacementResources, err := establishPoolResources(
		ctx,
		rdb,
		poolName,
		recreated.Generation(),
		1000,
		30*time.Second,
		30*time.Second,
		5*time.Minute,
	)
	require.NoError(t, err)
	require.NotEqual(t, resources.jobs, replacementResources.jobs)
	replacementKey := rmapContentKey(replacementResources.jobs)
	require.NoError(t, rdb.HSet(ctx, replacementKey, "replacement", "value").Err())
	for _, pair := range [][2]string{
		{resources.nodeKeepAlive, replacementResources.nodeKeepAlive},
		{resources.workers, replacementResources.workers},
		{resources.jobs, replacementResources.jobs},
		{resources.jobPending, replacementResources.jobPending},
	} {
		require.NotEqual(t, pair[0], pair[1])
		require.NoError(t, rdb.HSet(ctx, rmapContentKey(pair[0]), "stale", "old").Err())
		require.False(t, rdb.HExists(ctx, rmapContentKey(pair[1]), "stale").Val())
	}

	err = destroyPoolMap(ctx, rdb, poolName, resources.jobs, generation, "paused", time.Second)
	require.ErrorContains(t, err, "POOLCLEANUPLOST")
	require.Equal(t, "value", rdb.HGet(ctx, replacementKey, "replacement").Val())
	require.NoError(t, recreated.Destroy(ctx))
}

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
	worker1 := newTestWorker(t, ctx, node1)
	worker2 := newTestWorker(t, ctx, node2)
	defer func() {
		assert.NoError(t, node1.Shutdown(ctx))
		assert.NoError(t, node2.Shutdown(ctx))
	}()

	// Configure nodes to send jobs to specific workers
	node1.h, node2.h = &ptesting.Hasher{Index: 0}, &ptesting.Hasher{Index: 1}
	requireActiveWorkerRing(t, []*Node{node1, node2}, worker1.ID, worker2.ID)

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
				return ok && assert.Equal(t, tt.payload, payload)
			}, max, delay, fmt.Sprintf("Failed to get correct payload for job %s", tt.key))
		})
	}

	// Test non-existent job
	payload, ok := node.JobPayload("non-existent-job")
	assert.False(t, ok, "Expected false for non-existent job")
	assert.Nil(t, payload, "Expected nil payload for non-existent job")

	// Remove existing job
	assert.NoError(t, node.StopJob(ctx, "job1"))
	// Check if the payload was removed
	assert.Eventually(t, func() bool {
		_, ok := node.JobPayload("job1")
		return !ok
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

func TestStopJobRoutesToCurrentOwner(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	node.h = &ptesting.Hasher{Index: 0}
	worker1 := newTestWorker(t, ctx, node)
	worker2 := newTestWorker(t, ctx, node)

	stopped := make(chan struct{})
	var stoppedOnce sync.Once
	worker1.handler.(*mockHandler).stopFunc = func(key string) error {
		stoppedOnce.Do(func() { close(stopped) })
		return nil
	}
	worker2.handler.(*mockHandler).stopFunc = func(key string) error {
		t.Errorf("stop routed to worker without ownership: %s", key)
		return nil
	}

	payload := []byte("payload")
	require.NoError(t, node.DispatchJob(ctx, testName, payload))
	require.Eventually(t, func() bool {
		return len(worker1.Jobs()) == 1
	}, max, delay)
	require.Eventually(t, func() bool {
		return sameStrings(jobOwners(node, testName), []string{worker1.ID})
	}, max, delay)

	node.h = &ptesting.Hasher{Index: 1}
	require.NoError(t, node.StopJob(ctx, testName))
	select {
	case <-stopped:
	case <-time.After(max):
		t.Fatal("job stop was not routed to current owner")
	}
	require.Eventually(t, func() bool {
		return len(worker1.Jobs()) == 0 && len(worker2.Jobs()) == 0
	}, max, delay)
	require.Eventually(t, func() bool {
		_, ok := node.JobPayload(testName)
		return !ok && len(jobOwners(node, testName)) == 0
	}, max, delay)

	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestControlEventRoutingDuringOwnershipGaps(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	const jobKey = "handoff-job"
	_, err := node.jobPayloadMap.SetAndWait(ctx, jobKey, "payload")
	require.NoError(t, err)

	_, err = node.workerForEvent(evStopJob, jobKey)
	assert.ErrorIs(t, err, errJobAwaitingOwner)

	_, err = node.workerForEvent(evNotify, jobKey)
	assert.ErrorIs(t, err, errJobAwaitingOwner)

	_, err = node.jobPayloadMap.Delete(ctx, jobKey)
	require.NoError(t, err)

	_, err = node.workerForEvent(evStopJob, jobKey)
	assert.ErrorIs(t, err, errJobNotFound)

	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestDispatchJobRaceCondition(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	node1 := newTestNode(t, ctx, rdb, testName)
	node2 := newTestNode(t, ctx, rdb, testName)
	worker1 := newTestWorker(t, ctx, node1)
	worker2 := newTestWorker(t, ctx, node2)
	defer func() {
		assert.NoError(t, node1.Shutdown(ctx))
		assert.NoError(t, node2.Shutdown(ctx))
	}()
	requireActiveWorkerRing(t, []*Node{node1, node2}, worker1.ID, worker2.ID)

	t.Run("concurrent dispatch of same job returns error", func(t *testing.T) {
		// Start dispatching same job from both nodes concurrently
		errCh := make(chan error, 2)
		jobKey := "concurrent-job"
		payload := []byte("test payload")

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			errCh <- node1.DispatchJob(ctx, jobKey, payload)
		}()
		go func() {
			defer wg.Done()
			errCh <- node2.DispatchJob(ctx, jobKey, payload)
		}()
		wg.Wait()
		close(errCh)

		// Collect results
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}

		// Verify that exactly one dispatch succeeded and one failed
		successCount := 0
		errorCount := 0
		for _, err := range errs {
			if err == nil {
				successCount++
			} else if errors.Is(err, ErrJobExists) {
				errorCount++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}
		assert.Equal(t, 1, successCount, "Expected exactly one successful dispatch")
		assert.Equal(t, 1, errorCount, "Expected exactly one ErrJobExists error")
	})

	t.Run("dispatch after existing job returns error", func(t *testing.T) {
		jobKey := "sequential-job"
		payload := []byte("test payload")

		// First dispatch should succeed
		err := node1.DispatchJob(ctx, jobKey, payload)
		require.NoError(t, err, "First dispatch should succeed")

		// Second dispatch should fail with ErrJobExists
		err = node2.DispatchJob(ctx, jobKey, payload)
		assert.True(t, errors.Is(err, ErrJobExists), "Expected ErrJobExists, got: %v", err)
	})

	t.Run("claim rejects active pending job from redis", func(t *testing.T) {
		jobKey := "active-redis-pending-job"
		pendingHash := rmapContentKey(node2.resources.jobPending)
		const pendingNonce = "active-dispatch"
		storedGuard := pendingNonce + "\x00" + "1-0"
		require.NoError(t, rdb.HSet(ctx, pendingHash, jobKey, storedGuard).Err())
		_, localExists := node2.jobPendingMap.Get(jobKey)
		require.False(t, localExists)

		_, err := node2.publishDispatch(ctx, jobKey, "replacement", marshalJob(&Job{Key: jobKey}))
		require.True(t, errors.Is(err, ErrJobExists), "Expected ErrJobExists, got: %v", err)

		stored, err := rdb.HGet(ctx, pendingHash, jobKey).Result()
		require.NoError(t, err)
		require.Equal(t, storedGuard, stored)
	})

	t.Run("claim never replaces pending dispatch without completion", func(t *testing.T) {
		jobKey := "unknown-redis-pending-job"
		pendingHash := rmapContentKey(node2.resources.jobPending)
		const pendingNonce = "unknown-dispatch"
		storedGuard := pendingNonce + "\x00" + "1-0"
		require.NoError(t, rdb.HSet(ctx, pendingHash, jobKey, storedGuard).Err())

		_, err := node2.publishDispatch(ctx, jobKey, "replacement", marshalJob(&Job{Key: jobKey}))
		require.ErrorIs(t, err, ErrJobExists)

		stored, err := rdb.HGet(ctx, pendingHash, jobKey).Result()
		require.NoError(t, err)
		require.Equal(t, storedGuard, stored)
		require.Error(t, node2.completeDispatch(ctx, jobKey, pendingNonce))
		require.NoError(t, rdb.HDel(ctx, pendingHash, jobKey).Err())
	})

	t.Run("claim treats every persisted value as an owned nonce", func(t *testing.T) {
		jobKey := "opaque-redis-pending-job"
		pendingHash := rmapContentKey(node2.resources.jobPending)
		const opaquePending = "not-a-timestamp"
		require.NoError(t, rdb.HSet(ctx, pendingHash, jobKey, opaquePending).Err())

		_, err := node2.publishDispatch(ctx, jobKey, "replacement", marshalJob(&Job{Key: jobKey}))
		require.ErrorIs(t, err, ErrJobExists)

		stored, err := rdb.HGet(ctx, pendingHash, jobKey).Result()
		require.NoError(t, err)
		require.Equal(t, opaquePending, stored)
		require.NoError(t, rdb.HDel(ctx, pendingHash, jobKey).Err())
	})

	t.Run("concurrent atomic claims admit one dispatcher", func(t *testing.T) {
		jobKey := "concurrent-claim-job"
		errCh := make(chan error, 20)
		pendingCh := make(chan string, 20)
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				dispatchID := fmt.Sprintf("dispatch-%d", i)
				_, err := node2.publishDispatch(
					ctx,
					jobKey,
					dispatchID,
					marshalJob(&Job{Key: jobKey, dispatchID: dispatchID}),
				)
				if err == nil {
					pendingCh <- dispatchID
				}
				errCh <- err
			}(i)
		}
		wg.Wait()
		close(errCh)
		close(pendingCh)

		successCount := 0
		errorCount := 0
		for err := range errCh {
			if err == nil {
				successCount++
				continue
			}
			if errors.Is(err, ErrJobExists) {
				errorCount++
				continue
			}
			t.Errorf("unexpected error: %v", err)
		}
		require.Equal(t, 1, successCount)
		require.Equal(t, 19, errorCount)
		require.NoError(t, node2.completeDispatch(ctx, jobKey, <-pendingCh))
	})

	t.Run("dispatch checks redis when local payload replica is stale", func(t *testing.T) {
		jobKey := "stale-local-payload-job"
		payload := []byte("test payload")
		payloadHash := rmapContentKey(node2.resources.jobPayloads)
		pendingHash := rmapContentKey(node2.resources.jobPending)

		// Simulate the production race: Redis already has the live job payload,
		// but this node's local rmap replica has not applied that update yet.
		require.NoError(t, rdb.HSet(ctx, payloadHash, jobKey, string(payload)).Err())
		_, localExists := node2.jobPayloadMap.Get(jobKey)
		require.False(t, localExists)

		err := node2.DispatchJob(ctx, jobKey, []byte("new payload"))
		assert.True(t, errors.Is(err, ErrJobExists), "Expected ErrJobExists, got: %v", err)
		pendingExists, err := rdb.HExists(ctx, pendingHash, jobKey).Result()
		require.NoError(t, err)
		require.False(t, pendingExists)
	})

	t.Run("dispatch never reopens unknown pending job", func(t *testing.T) {
		jobKey := "timeout-job"
		payload := []byte("test payload")

		const nonce = "unknown-dispatch"
		_, err := node1.jobPendingMap.SetAndWait(ctx, jobKey, nonce)
		require.NoError(t, err)
		defer func() {
			_, err = node1.jobPendingMap.Delete(ctx, jobKey)
			assert.NoError(t, err)
		}()

		err = node1.DispatchJob(ctx, jobKey, payload)
		require.ErrorIs(t, err, ErrJobExists)
	})

	t.Run("dispatch cleans up pending entry on success", func(t *testing.T) {
		jobKey := "success-cleanup-job"
		payload := []byte("test payload")

		// Dispatch job
		err := node1.DispatchJob(ctx, jobKey, payload)
		require.NoError(t, err, "Dispatch should succeed")
		// Verify pending entry was cleaned up
		require.Eventually(t, func() bool {
			val, exists := node1.jobPendingMap.Get(jobKey)
			t.Logf("Got pending value: %q", val)
			return !exists
		}, max, delay, "Pending entry should be cleaned up after successful dispatch")
	})

	t.Run("dispatch treats pending guard as opaque nonce", func(t *testing.T) {
		jobKey := "invalid-timestamp-job"
		payload := []byte("test payload")

		_, err := node1.jobPendingMap.SetAndWait(ctx, jobKey, "invalid-timestamp")
		require.NoError(t, err)

		err = node1.DispatchJob(ctx, jobKey, payload)
		require.ErrorIs(t, err, ErrJobExists)
		stored, err := rdb.HGet(ctx, rmapContentKey(node2.resources.jobPending), jobKey).Result()
		require.NoError(t, err)
		require.Equal(t, "invalid-timestamp", stored)
	})

	// Keep this test last because it temporarily replaces the stream key.
	t.Run("dispatch cleans up pending entry on failure", func(t *testing.T) {
		jobKey := "cleanup-job"
		payload := []byte("test payload")

		// Replace the Redis stream with a string so XADD fails after the pending
		// dispatch guard is claimed. Deleting the stream no longer forces this
		// path because the pool sink now repairs externally deleted streams.
		streamKey := generationStreamKey(ctx, rdb, poolStreamName(node1.PoolName))
		require.NotEmpty(t, streamKey)
		require.NoError(t, rdb.Del(ctx, streamKey).Err())
		err := rdb.Set(ctx, streamKey, "wrong-type", 0).Err()
		require.NoError(t, err)
		defer func() { require.NoError(t, rdb.Del(ctx, streamKey).Err()) }()

		// Attempt dispatch (should fail)
		err = node1.DispatchJob(ctx, jobKey, payload)
		require.Error(t, err, "Expected dispatch to fail")

		// Verify pending entry was cleaned up
		require.Eventually(t, func() bool {
			_, exists := node1.jobPendingMap.Get(jobKey)
			return !exists
		}, max, delay, "Pending entry should be cleaned up after failed dispatch")
	})

}

func TestDispatchCancellationRetainsAdmissionUntilDefinitiveCompletion(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node, err := AddNode(
		ctx,
		t.Name(),
		rdb,
		WithDispatchTimeout(100*time.Millisecond),
		WithRecoveryGrace(50*time.Millisecond),
	)
	require.NoError(t, err)

	dispatchCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	err = node.DispatchJob(dispatchCtx, "job", []byte("payload"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	pendingKey := rmapContentKey(jobPendingMapName(t.Name()))
	guard, err := rdb.HGet(ctx, pendingKey, "job").Result()
	require.NoError(t, err)
	require.NotEmpty(t, guard)
	dispatchID := guard
	require.ErrorIs(t, node.DispatchJob(ctx, "job", []byte("duplicate")), ErrJobExists)
	_, waiterExists := node.pendingJobChannels.Load(dispatchID)
	require.False(t, waiterExists)

	require.NoError(t, node.completeDispatch(ctx, "job", dispatchID))
	replacement, err := node.publishDispatch(
		ctx,
		"job",
		"replacement",
		marshalJob(&Job{Key: "job", dispatchID: "replacement"}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, replacement)
	require.NoError(t, node.completeDispatch(ctx, "job", "replacement"))
	require.NoError(t, node.Close(ctx))
	require.NoError(t, node.poolStream.Destroy(ctx))
}

func TestPoolRoutingDropsMalformedEventAndContinues(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	received := make(chan string, 1)
	handler := &mockMessageHandler{
		mockHandler: newMockHandler(),
		messageFunc: func(key string, payload []byte) error {
			received <- key + ":" + string(payload)
			return nil
		},
	}
	_, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)

	_, err = node.poolStream.Add(ctx, evMessage, []byte{1, 2, 3})
	require.NoError(t, err)
	require.NoError(t, node.DispatchMessage(ctx, "valid", []byte("payload")))
	require.Equal(t, "valid:payload", <-received)
	require.Eventually(t, func() bool {
		pending, pendingErr := rdb.XPending(
			ctx,
			generationStreamKey(ctx, rdb, node.poolStream.Name),
			node.poolSink.Name,
		).Result()
		return pendingErr == nil && pending.Count == 0
	}, max, delay)

	require.NoError(t, node.Shutdown(ctx))
}

func TestWorkerAckReleasesDispatchAfterCallerNodeCrash(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	const (
		jobKey      = "job"
		dispatchID  = "crashed-dispatch"
		workerID    = "worker"
		workerEvent = "worker-event"
	)
	eventID, err := node.publishDispatch(
		ctx,
		jobKey,
		dispatchID,
		marshalJob(&Job{Key: jobKey, NodeID: "crashed-node", dispatchID: dispatchID}),
	)
	require.NoError(t, err)
	pending := &streaming.Event{
		ID:        eventID,
		EventName: evStartJob,
		Payload: marshalJob(&Job{
			Key:        jobKey,
			NodeID:     "crashed-node",
			dispatchID: dispatchID,
		}),
		Acker: &mockAcker{
			XAckFunc: func(ctx context.Context, _, _ string, _ ...string) *redis.IntCmd {
				return redis.NewIntCmd(ctx, 1)
			},
		},
	}
	node.pendingEvents.Store(pendingEventKey(workerID, workerEvent), pending)

	node.ackWorkerEvent(&streaming.Event{
		Payload: marshalEnvelope(workerID, marshalAck(&ack{EventID: workerEvent})),
	})
	require.Eventually(t, func() bool {
		return !rdb.HExists(ctx, rmapContentKey(node.resources.jobPending), jobKey).Val()
	}, max, delay)
	_, exists := node.pendingEvents.Load(pendingEventKey(workerID, workerEvent))
	require.False(t, exists)
	require.NoError(t, node.Shutdown(ctx))
}

func TestPoolScriptsRecoverAfterScriptFlush(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	worker := newTestWorker(t, ctx, node)
	requireActiveWorkerRing(t, []*Node{node}, worker.ID)
	require.NoError(t, rdb.ScriptFlush(ctx).Err())

	require.NoError(t, node.DispatchJob(ctx, "job", []byte("payload")))
	require.NoError(t, node.StopJob(ctx, "job"))
	require.NoError(t, node.Shutdown(ctx))
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
	node := newTestNodeWithLogger(t, ctx, rdb, testName, pulse.ClueLogger(ctx))
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create a worker without NotificationHandler implementation
	worker := newTestWorkerWithoutOptionalHandlers(t, ctx, node)

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

func TestDispatchMessageRoutesByHashWithoutJob(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	messageKey := "message-key"
	messagePayload := []byte("message payload")
	received := make(chan string, 2)

	node.h = &ptesting.Hasher{Index: 1}
	handler1 := &mockMessageHandler{mockHandler: newMockHandler()}
	handler2 := &mockMessageHandler{mockHandler: newMockHandler()}
	worker1, err := node.AddWorker(ctx, handler1)
	require.NoError(t, err)
	worker2, err := node.AddWorker(ctx, handler2)
	require.NoError(t, err)
	handler1.messageFunc = func(key string, payload []byte) error {
		assert.Equal(t, messageKey, key)
		assert.Equal(t, messagePayload, payload)
		received <- worker1.ID
		return nil
	}
	handler2.messageFunc = func(key string, payload []byte) error {
		assert.Equal(t, messageKey, key)
		assert.Equal(t, messagePayload, payload)
		received <- worker2.ID
		return nil
	}

	require.NoError(t, node.DispatchMessage(ctx, messageKey, messagePayload))
	select {
	case got := <-received:
		assert.Equal(t, worker2.ID, got)
	case <-time.After(max):
		t.Fatal("message was not routed to hash-ring worker")
	}

	assert.Empty(t, worker1.Jobs())
	assert.Empty(t, worker2.Jobs())
	assert.Empty(t, node.JobKeys())
	_, ok := node.JobPayload(messageKey)
	assert.False(t, ok)

	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestDispatchMessageRequiresHandler(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx, buf := ptesting.NewBufferedLogContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNodeWithLogger(t, ctx, rdb, testName, pulse.ClueLogger(ctx))
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	worker := newTestWorkerWithoutOptionalHandlers(t, ctx, node)
	assert.NoError(t, node.DispatchMessage(ctx, "message-key", []byte("message payload")))

	assert.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "handler failed: worker") &&
			strings.Contains(buf.String(), "does not implement MessageHandler")
	}, max, delay, "Expected missing MessageHandler error within the timeout period")
	assert.Empty(t, worker.Jobs())

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

func TestShutdownReapsStaleNodeWithRedisTime(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	node := newTestNode(t, ctx, rdb, testName)
	now, err := rdb.Time(ctx).Result()
	require.NoError(t, err)
	_, err = node.nodeKeepAliveMap.Set(
		ctx,
		"crashed-node",
		strconv.FormatInt(now.Add(-2*node.workerTTL).UnixNano(), 10),
	)
	require.NoError(t, err)

	require.NoError(t, node.Shutdown(ctx))
	require.True(t, node.cleanupComplete)
}

func TestShutdownRetriesPartialCleanup(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	failure := errors.New("injected pool cleanup failure")
	hook := &poolRedisHook{
		key:           rmapContentKey(workerMapName(testName)),
		completionSHA: completePoolCleanupScript.Hash(),
		failure:       failure,
	}
	rdb.AddHook(hook)
	node := newTestNode(t, ctx, rdb, testName)
	hook.failDestroy.Store(true)

	err := node.Shutdown(ctx)
	require.ErrorIs(t, err, failure)
	require.True(t, node.IsClosed())
	require.False(t, node.cleanupComplete)
	state, err := rdb.HGet(ctx, "pulse:stream:"+poolStreamName(testName)+":lifecycle", "state").Result()
	require.NoError(t, err)
	require.Equal(t, "destroyed", state)
	require.NotEqual(t, "destroy", rdb.HGet(ctx, hook.key, "=kind").Val())

	hook.failDestroy.Store(false)
	hook.failCompletion.Store(true)
	err = node.Shutdown(ctx)
	require.ErrorIs(t, err, failure)
	require.Contains(t, err.Error(), "failed to record cleanup completion")
	_, err = AddNode(ctx, testName, rdb)
	require.EqualError(t, err, `AddNode: pool "`+testName+`" is shutting down`)

	hook.failCompletion.Store(false)
	require.NoError(t, node.Shutdown(ctx))
	require.True(t, node.cleanupComplete)
	require.EqualValues(t, 0, rdb.Exists(ctx, rmapContentKey(nodeShutdownMapName(testName))).Val())
}

func TestCompletedCleanupStillClosesLocalNode(t *testing.T) {
	for _, test := range []struct {
		name  string
		close func(context.Context, *Node) error
	}{
		{name: "Shutdown", close: func(ctx context.Context, node *Node) error {
			return node.Shutdown(ctx)
		}},
		{name: "Close", close: func(ctx context.Context, node *Node) error {
			return node.Close(ctx)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := ptesting.NewTestContext(t)
			rdb := ptesting.NewRedisClient(t)
			defer ptesting.CleanupRedis(t, rdb, false, "")
			node := newTestNode(t, ctx, rdb, strings.ReplaceAll(t.Name(), "/", "_"))
			worker := newTestWorker(t, ctx, node)
			producer := newTestProducer("stale-resumed", func() (*JobPlan, error) {
				return &JobPlan{}, nil
			})
			require.NoError(t, node.Schedule(ctx, producer, time.Millisecond))
			require.NoError(t, rdb.HSet(
				ctx,
				poolCleanupGenerationsKey(node.PoolName),
				"state", poolCleanupCompleteState,
				"generation", node.resources.generation,
			).Err())
			require.NoError(t, node.poolStream.Destroy(ctx))

			require.NoError(t, test.close(ctx, node))
			require.True(t, node.IsClosed())
			require.True(t, node.IsShutdown())
			require.True(t, worker.IsStopped())
			require.True(t, node.poolSink.IsClosed())
			done := make(chan struct{})
			go func() {
				node.scheduleWG.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				require.Fail(t, "schedule goroutine was not joined")
			}
		})
	}
}

func TestCompletedCleanupPreservesSettlementErrorAfterLocalTeardown(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	node := newTestNode(t, ctx, rdb, strings.ReplaceAll(t.Name(), "/", "_"))
	worker := newTestWorker(t, ctx, node)
	settlementErr := errors.New("injected terminal settlement failure")
	node.settlements.begin(worker.ID)(settlementErr)
	require.NoError(t, rdb.HSet(
		ctx,
		poolCleanupGenerationsKey(node.PoolName),
		"state", poolCleanupCompleteState,
		"generation", node.resources.generation,
	).Err())
	require.NoError(t, node.poolStream.Destroy(ctx))

	firstErr := node.Close(ctx)
	require.ErrorIs(t, firstErr, settlementErr)
	require.True(t, node.IsClosed())
	require.True(t, node.IsShutdown())
	require.True(t, worker.IsStopped())
	require.True(t, node.poolSink.IsClosed())
	secondErr := node.Close(ctx)
	require.ErrorIs(t, secondErr, settlementErr)
	require.Equal(t, firstErr.Error(), secondErr.Error())
}

func TestLateShutdownCannotCleanNewPoolGeneration(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	oldNode := newTestNode(t, ctx, rdb, testName)
	oldGeneration := oldNode.poolStream.Generation()
	require.NoError(t, oldNode.Shutdown(ctx))

	newNode := newTestNode(t, ctx, rdb, testName)
	require.NotEqual(t, oldGeneration, newNode.poolStream.Generation())
	require.NoError(t, oldNode.Shutdown(ctx))
	require.False(t, newNode.IsClosed())
	require.NoError(t, newNode.Shutdown(ctx))
}

func TestOldGenerationMutationCannotRecreateDeletedMap(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	oldNode := newTestNode(t, ctx, rdb, testName)
	oldMapKey := rmapContentKey(oldNode.resources.workerKeepAlive)
	require.NoError(t, oldNode.Shutdown(ctx))
	require.EqualValues(t, 0, rdb.Exists(ctx, oldMapKey).Val())
	newNode := newTestNode(t, ctx, rdb, testName)

	err := oldNode.setPoolMap(ctx, oldNode.resources.workerKeepAlive, "stale", "1")
	require.ErrorIs(t, err, ErrPoolGenerationLost)
	require.EqualValues(t, 0, rdb.Exists(ctx, oldMapKey).Val())
	require.NotEqual(t, oldNode.resources.workerKeepAlive, newNode.resources.workerKeepAlive)

	require.NoError(t, newNode.Shutdown(ctx))
}

func TestAddNodePostRegistrationShutdownCheck(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	hook := &poolRedisHook{
		shutdownKey:          rmapContentKey(nodeShutdownMapName(testName)),
		shutdownCheckStart:   make(chan struct{}),
		releaseShutdownCheck: make(chan struct{}),
	}
	rdb.AddHook(hook)
	first, err := AddNode(
		ctx,
		testName,
		rdb,
		WithWorkerTTL(2*time.Second),
		WithRequeueTimeout(100*time.Millisecond),
		WithDispatchTimeout(time.Second),
		WithRecoveryGrace(500*time.Millisecond),
		WithJobSinkBlockDuration(100*time.Millisecond),
	)
	require.NoError(t, err)
	hook.blockShutdownCheck.Store(true)

	type addResult struct {
		node *Node
		err  error
	}
	added := make(chan addResult, 1)
	go func() {
		node, err := AddNode(
			ctx,
			testName,
			rdb,
			WithWorkerTTL(2*time.Second),
			WithRequeueTimeout(100*time.Millisecond),
			WithDispatchTimeout(time.Second),
			WithRecoveryGrace(500*time.Millisecond),
			WithJobSinkBlockDuration(100*time.Millisecond),
		)
		added <- addResult{node: node, err: err}
	}()

	select {
	case <-hook.shutdownCheckStart:
	case <-time.After(max):
		t.Fatal("AddNode did not reach post-registration shutdown check")
	}
	require.NoError(t, rdb.HSet(ctx, hook.shutdownKey, "shutdown", first.ID).Err())
	close(hook.releaseShutdownCheck)
	result := <-added
	require.NoError(t, result.err)
	require.Eventually(t, result.node.IsClosed, max, delay)
	require.True(t, result.node.IsShutdown())

	require.NoError(t, rdb.HDel(ctx, hook.shutdownKey, "shutdown").Err())
	require.NoError(t, first.Shutdown(ctx))
}

func TestPeerShutdownDetachFailureIsRetriedAndSurfaced(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)
	failure := errors.New("injected peer detach failure")
	hook := &poolRedisHook{
		detachKey: "map:stream:" + poolStreamName(testName) + ":generation:",
		failure:   failure,
	}
	rdb.AddHook(hook)
	first := newTestNode(t, ctx, rdb, testName)
	peer := newTestNode(t, ctx, rdb, testName)
	require.NoError(t, first.close(ctx, true))
	hook.failDetach.Store(true)
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(nodeShutdownMapName(testName)),
		"shutdown",
		first.ID,
	).Err())
	peer.ownShutdown(context.Background())

	require.Eventually(t, func() bool {
		return rdb.HExists(
			ctx,
			rmapContentKey(nodeShutdownMapName(testName)),
			shutdownErrorKey(peer.ID),
		).Val()
	}, peer.workerTTL, delay)
	start := time.Now()
	err := first.waitForPoolNodes(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to detach pool sink")
	require.Less(t, time.Since(start), peer.workerTTL)
	require.False(t, peer.IsClosed())

	hook.failDetach.Store(false)
	require.Eventually(t, peer.IsClosed, peer.workerTTL, delay)
	require.NoError(t, first.Shutdown(ctx))
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
	requireActiveWorkerRing(t, []*Node{node1, node2}, worker1.ID, worker2.ID)

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
	require.NoError(t, node2.Shutdown(ctx), "Failed to shutdown node2")
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
	requireActiveWorkerRing(t, []*Node{node1, node2}, worker1.ID, worker2.ID)

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
	case <-time.After(2 * testAckGracePeriod):
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
	node.ackWorkerEvent(mockEvent)

	// Verify that no panic occurred and the function completed successfully
	assert.True(t, true, "ackWorkerEvent should complete without panic")
}

func TestStaleNodeStreamCleanup(t *testing.T) {
	var (
		ctx      = ptesting.NewTestContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = ptesting.NewRedisClient(t)
		node1    = newFastCleanupTestNode(t, ctx, rdb, testName)
		node2    = newFastCleanupTestNode(t, ctx, rdb, testName)
		numJobs  atomic.Int64
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)

	// Configure nodes to send jobs to specific workers. The hasher is shared
	// by both nodes and called from concurrent routing and rebalance
	// goroutines, so its state must be synchronized.
	node1.h = &ptesting.Hasher{IndexFunc: func(key string, numBuckets int64) int64 {
		if numJobs.Add(1) > 2 {
			return 0 // to avoid panics on cleanup where jobs get requeued
		}
		if key == "job1" {
			return 0 // job1 goes to worker1
		}
		return 1 // job2 goes to worker2
	}}
	node2.h = node1.h

	// Create workers and dispatch jobs to both nodes to ensure streams exist
	worker1 := newTestWorker(t, ctx, node1)
	worker2 := newTestWorker(t, ctx, node2)
	requireActiveWorkerRing(t, []*Node{node1, node2}, worker1.ID, worker2.ID)

	// Dispatch jobs to both nodes
	assert.NoError(t, node1.DispatchJob(ctx, "job1", []byte("payload1")))
	assert.NoError(t, node2.DispatchJob(ctx, "job2", []byte("payload2")))

	// Verify both generation-qualified streams exist initially.
	var name1, name2 string
	assert.Eventually(t, func() bool {
		name1 = generationStreamKey(ctx, rdb, nodeStreamName(node1.PoolName, node1.ID))
		name2 = generationStreamKey(ctx, rdb, nodeStreamName(node2.PoolName, node2.ID))
		return name1 != "" && name2 != ""
	}, max, delay, "Node streams should exist initially")

	// Set node2's last seen time to a stale value
	close(node2.stop)
	_, err := node2.nodeKeepAliveMap.Set(ctx, node2.ID, "0")
	assert.NoError(t, err)
	node2.wg.Wait()
	node2.stop = make(chan struct{}) // so we can close

	// Verify node2's stream gets cleaned up
	assert.Eventually(t, func() bool {
		exists, err := rdb.Exists(ctx, name2).Result()
		return err == nil && exists == 0
	}, max, delay, "Stale node stream should have been cleaned up")

	// Verify node1's stream still exists
	assert.Eventually(t, func() bool {
		exists, err := rdb.Exists(ctx, name1).Result()
		return err == nil && exists == 1
	}, max, delay, "Active node stream should still exist")

	// Verify node2 was removed from keep-alive map
	assert.Eventually(t, func() bool {
		_, exists := node1.nodeKeepAliveMap.Get(node2.ID)
		return !exists
	}, max, delay, "Stale node should have been removed from keep-alive map")
	lifecycle, err := rdb.HGetAll(ctx, "pulse:stream:"+nodeStreamName(node2.PoolName, node2.ID)+":lifecycle").Result()
	require.NoError(t, err)
	require.Equal(t, node2.nodeStream.Generation(), lifecycle["generation"])
	require.Equal(t, "destroyed", lifecycle["state"])

	// A stale node that resumes observes the cleanup fence before mutating
	// ownership and performs complete local teardown.
	err = node2.ensureGenerationActive(ctx)
	require.ErrorIs(t, err, ErrPoolGenerationLost)
	require.Eventually(t, node2.IsClosed, max, delay)
	require.NoError(t, node2.Close(ctx))
	assert.NoError(t, node1.Shutdown(ctx))
}

func TestInactiveNodeDiscoveryRemainsUntilStreamDestroySucceeds(t *testing.T) {
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	staleID := "stale-node"
	hook := &poolRedisHook{
		failure: errors.New("destroy failed"),
		key:     "pulse:stream:" + nodeStreamName(t.Name(), staleID) + ":lifecycle",
	}
	rdb.AddHook(hook)
	node := newTestNode(t, ctx, rdb, t.Name())
	staleStream, err := streaming.NewStream(nodeStreamName(node.PoolName, staleID), rdb)
	require.NoError(t, err)
	_, err = staleStream.Add(ctx, evInit, []byte(staleID))
	require.NoError(t, err)
	now, err := rdb.Time(ctx).Result()
	require.NoError(t, err)
	_, err = node.nodeKeepAliveMap.SetAndWait(
		ctx,
		staleID,
		strconv.FormatInt(now.Add(-2*node.workerTTL).UnixNano(), 10),
	)
	require.NoError(t, err)
	hook.failDestroy.Store(true)

	node.cleanupInactiveNodes()
	_, exists := node.nodeKeepAliveMap.Get(staleID)
	require.True(t, exists)

	hook.failDestroy.Store(false)
	node.cleanupInactiveNodes()
	require.Eventually(t, func() bool {
		_, exists = node.nodeKeepAliveMap.Get(staleID)
		return !exists
	}, max, delay)
	require.NoError(t, node.Shutdown(ctx))
}

func TestShutdownStopsAllJobs(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create node and workers
	node := newTestNode(t, ctx, rdb, testName)
	worker1 := newTestWorker(t, ctx, node)
	worker2 := newTestWorker(t, ctx, node)

	// Track stopped jobs
	var stoppedJobs sync.Map
	stopHandler := func(key string) error {
		stoppedJobs.Store(key, true)
		return nil
	}
	worker1.handler.(*mockHandler).stopFunc = stopHandler
	worker2.handler.(*mockHandler).stopFunc = stopHandler

	// Dispatch multiple jobs
	jobs := []struct {
		key     string
		payload []byte
	}{
		{key: "job1", payload: []byte("payload1")},
		{key: "job2", payload: []byte("payload2")},
		{key: "job3", payload: []byte("payload3")},
		{key: "job4", payload: []byte("payload4")},
	}

	// Configure node to distribute jobs between workers
	node.h = &ptesting.Hasher{IndexFunc: func(key string, numBuckets int64) int64 {
		if strings.HasSuffix(key, "1") || strings.HasSuffix(key, "2") {
			return 0 // jobs 1 and 2 go to worker1
		}
		return 1 // jobs 3 and 4 go to worker2
	}}

	// Dispatch all jobs
	for _, job := range jobs {
		require.NoError(t, node.DispatchJob(ctx, job.key, job.payload))
	}

	// Wait for jobs to be distributed
	require.Eventually(t, func() bool {
		return len(worker1.Jobs()) == 2 && len(worker2.Jobs()) == 2
	}, max, delay, "Jobs were not distributed correctly")

	// Shutdown the node
	assert.NoError(t, node.Shutdown(ctx))

	// Verify all jobs were stopped
	for _, job := range jobs {
		_, ok := stoppedJobs.Load(job.key)
		assert.True(t, ok, "Job %s was not stopped during shutdown", job.key)
	}

	// Verify workers have no remaining jobs
	assert.Empty(t, worker1.Jobs(), "Worker1 should have no remaining jobs")
	assert.Empty(t, worker2.Jobs(), "Worker2 should have no remaining jobs")
}

func TestWorkerAckStreams(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create a worker and dispatch a job
	worker := newTestWorker(t, ctx, node)
	require.NoError(t, node.DispatchJob(ctx, testName, []byte("payload")))

	// Wait for the job to start and be acknowledged
	require.Eventually(t, func() bool {
		return len(worker.Jobs()) == 1
	}, max, delay)

	// Verify stream is created and cached
	stream1, err := node.getNodeStream(node.ID)
	require.NoError(t, err)
	stream2, err := node.getNodeStream(node.ID)
	require.NoError(t, err)
	assert.Same(t, stream1, stream2, "Expected same stream instance to be returned")

	// Verify stream exists before shutdown
	streamKey := generationStreamKey(ctx, rdb, nodeStreamName(testName, node.ID))
	require.NotEmpty(t, streamKey)
	exists, err := rdb.Exists(ctx, streamKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists, "Expected stream to exist before shutdown")

	// Shutdown node
	assert.NoError(t, node.Shutdown(ctx))

	// Verify stream is destroyed in Redis
	exists, err = rdb.Exists(ctx, streamKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists, "Expected stream to be destroyed after shutdown")
}

func TestStaleWorkerCleanupAfterJobRequeue(t *testing.T) {
	// Setup test environment
	ctx := ptesting.NewTestContext(t)
	testName := strings.Replace(t.Name(), "/", "_", -1)
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	node := newTestNode(t, ctx, rdb, testName)
	defer func() { assert.NoError(t, node.Shutdown(ctx)) }()

	// Create a worker that will become stale
	staleWorker := newTestWorker(t, ctx, node)

	// Dispatch some jobs to the worker
	for i := 0; i < 3; i++ {
		jobKey := fmt.Sprintf("%s_%d", testName, i)
		require.NoError(t, node.DispatchJob(ctx, jobKey, []byte("test-payload")))
	}

	// Wait for jobs to be assigned
	require.Eventually(t, func() bool {
		return len(staleWorker.Jobs()) == 3
	}, max, delay, "Jobs were not assigned to worker")

	// Make the worker stale by stopping it and setting an old keepalive
	staleWorker.stop(ctx)
	_, err := node.workerKeepAliveMap.Set(ctx, staleWorker.ID, "0")
	require.NoError(t, err)

	// Create a new worker to receive requeued jobs
	newWorker := newTestWorker(t, ctx, node)

	// Wait for cleanup to happen and jobs to be requeued
	require.Eventually(t, func() bool {
		return len(newWorker.Jobs()) == 3
	}, 2*node.workerTTL, delay, "Jobs were not requeued to new worker")

	// Verify stale worker was deleted
	require.Eventually(t, func() bool {
		// Check that worker is removed from all tracking maps
		workers := node.Workers()
		if len(workers) != 1 {
			t.Logf("Expected 1 worker, got %d", len(workers))
			return false
		}

		// Check worker is removed from worker map
		workerMap := node.workerMap.Map()
		if _, exists := workerMap[staleWorker.ID]; exists {
			t.Log("Worker still exists in worker map")
			return false
		}

		// Check keepalive is removed
		keepAlive := node.workerKeepAliveMap.Map()
		if _, exists := keepAlive[staleWorker.ID]; exists {
			t.Log("Worker still has keepalive entry")
			return false
		}

		// Check jobs are removed
		jobs := node.jobMap.Map()
		if _, exists := jobs[staleWorker.ID]; exists {
			t.Log("Worker still has jobs assigned")
			return false
		}

		return true
	}, max, delay, "Stale worker was not properly cleaned up")
}

func TestRemoveWorkerFromMapsDoesNotDeleteJobPayloads(t *testing.T) {
	testName := strings.Replace(t.Name(), "/", "_", -1)
	ctx := ptesting.NewTestContext(t)
	rdb := ptesting.NewRedisClient(t)
	node := newTestNode(t, ctx, rdb, testName)
	defer ptesting.CleanupRedis(t, rdb, true, testName)

	// Create a worker and dispatch jobs to it
	worker := newTestWorker(t, ctx, node)
	jobs := []struct {
		key     string
		payload []byte
	}{
		{key: "job1", payload: []byte("payload1")},
		{key: "job2", payload: []byte("payload2")},
	}

	// Dispatch jobs
	for _, job := range jobs {
		assert.NoError(t, node.DispatchJob(ctx, job.key, job.payload), "Failed to dispatch job")
	}

	// Verify jobs are received and payloads are stored
	require.Eventually(t, func() bool {
		return len(worker.Jobs()) == len(jobs)
	}, max, delay, "Worker did not receive all jobs")

	// Verify payloads are in the map
	for _, job := range jobs {
		payload, ok := node.JobPayload(job.key)
		assert.True(t, ok, "Job payload not found for key %s", job.key)
		assert.Equal(t, job.payload, payload, "Incorrect payload for job %s", job.key)
	}

	// Remove the worker maps
	node.removeWorkerFromMaps(ctx, worker.ID)

	// Verify job payloads are NOT removed (payloads are job-scoped and must remain
	// recoverable during distributed cleanup).
	for _, job := range jobs {
		payload, ok := node.JobPayload(job.key)
		assert.True(t, ok, "Expected payload to remain for job %s", job.key)
		assert.Equal(t, job.payload, payload, "Incorrect payload for job %s", job.key)
	}

	// Shutdown node
	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestRequeueOrphanedPayloads(t *testing.T) {
	type jobInfo struct {
		key     string
		payload []byte
	}

	tests := []struct {
		name        string
		setupJobs   []jobInfo
		deletedJobs []string
	}{
		{
			name: "no orphaned payloads",
			setupJobs: []jobInfo{
				{key: "job1", payload: []byte("payload1")},
				{key: "job2", payload: []byte("payload2")},
				{key: "job3", payload: []byte("payload3")},
			},
		},
		{
			name: "some orphaned payloads",
			setupJobs: []jobInfo{
				{key: "job1", payload: []byte("payload1")},
				{key: "job2", payload: []byte("payload2")},
				{key: "job3", payload: []byte("payload3")},
			},
			deletedJobs: []string{"job1", "job3"},
		},
		{
			name: "all orphaned payloads",
			setupJobs: []jobInfo{
				{key: "job1", payload: []byte("payload1")},
				{key: "job2", payload: []byte("payload2")},
			},
			deletedJobs: []string{"job1", "job2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testName := strings.Replace(t.Name(), "/", "_", -1)
			ctx := ptesting.NewTestContext(t)
			rdb := ptesting.NewRedisClient(t)
			node := newTestNode(t, ctx, rdb, testName)
			worker := newTestWorker(t, ctx, node)
			defer ptesting.CleanupRedis(t, rdb, true, testName)

			// Dispatch jobs to the worker
			for _, job := range tt.setupJobs {
				require.NoError(t, node.DispatchJob(ctx, job.key, job.payload))
			}

			// Wait for jobs to be assigned
			require.Eventually(t, func() bool {
				jobs, _ := node.jobMap.GetValues(worker.ID)
				return len(jobs) == len(tt.setupJobs)
			}, max, delay, "Jobs were not assigned, got %d jobs in jobMap, expected %d", node.jobMap.Len(), len(tt.setupJobs))

			// Delete some job keys
			for _, key := range tt.deletedJobs {
				_, _, err := node.jobMap.RemoveValues(ctx, worker.ID, key)
				assert.NoError(t, err)
			}

			assert.Eventually(t, func() bool {
				jobs, _ := node.jobMap.GetValues(worker.ID)
				return len(jobs) == len(tt.setupJobs)-len(tt.deletedJobs)
			}, max, delay, "Job keys were not deleted")

			// Requeue orphaned payloads.
			// First call records the first-seen timestamp; second call after grace requeues.
			node.requeueOrphanedPayloads(ctx)
			time.Sleep(orphanedPayloadGrace(node) + 20*time.Millisecond)
			node.requeueOrphanedPayloads(ctx)

			// Verify the previously deleted job keys reappear in the job map.
			assert.Eventually(t, func() bool {
				jobs, _ := node.jobMap.GetValues(worker.ID)
				// The requeued jobs may end up on this worker or another (if present);
				// in this test there is only one worker, so they should all reappear here.
				return len(jobs) == len(tt.setupJobs)
			}, 15*time.Second, delay, fmt.Sprintf("Orphaned payload requeue did not restore job keys; expected %d jobs in jobMap", len(tt.setupJobs)))

			assert.NoError(t, node.Shutdown(ctx))
		})
	}
}

// requireActiveWorkerRing waits until every node has replicated the same active
// worker ring the test is about to route through. Dispatch tests depend on this
// contract; without it they can race rmap propagation instead of testing pool
// behavior.
func requireActiveWorkerRing(t *testing.T, nodes []*Node, workerIDs ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if !sameStrings(node.activeWorkers(), workerIDs) {
				return false
			}
		}
		return true
	}, 5*time.Second, delay, "active worker ring did not converge")
}

// orphanedPayloadGrace mirrors the recovery grace used by
// requeueOrphanedPayloads so tests wait for the behavior's contract instead of
// an unrelated timing constant.
func orphanedPayloadGrace(node *Node) time.Duration {
	grace := 2 * node.workerTTL
	if grace < node.recoveryGrace {
		return node.recoveryGrace
	}
	return grace
}

type mockAcker struct {
	XAckFunc func(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd
}

func (m *mockAcker) XAck(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd {
	return m.XAckFunc(ctx, streamKey, sinkName, ids...)
}
