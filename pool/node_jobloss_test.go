package pool

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	ptesting "goa.design/pulse/testing"
)

// TestJobLossDuringConcurrentWorkerCleanup reproduces a historical race condition where
// jobs could be lost during cascading node failures (worker cleanup + rebalancing).
//
// To increase reproduction probability on buggy implementations, run with:
//
//	go test ./pool -run TestJobLossDuringConcurrentWorkerCleanup -count 10
//
// The test has 3 phases:
//  1. Create 3 nodes/workers, dispatch N jobs (jobs distribute across workers).
//  2. Close node1 to trigger rebalancing/requeue.
//  3. While jobs are requeuing to node2, close node2 as well (cascade),
//     then verify all jobs eventually land on node3.
func TestJobLossDuringConcurrentWorkerCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping job-loss race test in short mode")
	}

	testName := strings.ReplaceAll(t.Name(), "/", "_")
	ctx := context.Background()
	rdb := ptesting.NewRedisClient(t)
	// This repro test intentionally creates lots of churn and relies on some
	// asynchronous cleanup. We only require the DB to be flushed at the end.
	defer ptesting.CleanupRedis(t, rdb, false, testName)

	// Use aggressive TTLs to make the race easier to hit on buggy implementations.
	opts := []NodeOption{
		WithWorkerTTL(200 * time.Millisecond),
		WithWorkerShutdownTTL(200 * time.Millisecond),
		WithJobSinkBlockDuration(50 * time.Millisecond),
		WithAckGracePeriod(200 * time.Millisecond),
		// Keep logs quiet so failures are easy to see (and tool output isn't truncated).
		WithLogger(pulse.NoopLogger()),
	}

	poolName := testName + "-race-" + uuid.NewString()

	node1, err := AddNode(ctx, poolName, rdb, opts...)
	require.NoError(t, err)
	node2, err := AddNode(ctx, poolName, rdb, opts...)
	require.NoError(t, err)
	node3, err := AddNode(ctx, poolName, rdb, opts...)
	require.NoError(t, err)

	// Track job starts/stops.
	allJobsStarted := make(chan string, 1000)
	jobRequeuedToNode2 := make(chan string, 1000)
	jobRequeuedToNode3 := make(chan string, 1000)

	handler1 := &mockHandler{
		startFunc: func(job *Job) error {
			allJobsStarted <- "node1:" + job.Key
			return nil
		},
		stopFunc: func(key string) error {
			// Print is intentional: this test is a repro harness.
			fmt.Printf("job stopped on node1: %s\n", key)
			return nil
		},
		notifyFunc: func(key string, payload []byte) error { return nil },
	}
	worker1, err := node1.AddWorker(ctx, handler1)
	require.NoError(t, err)

	handler2 := &mockHandler{
		startFunc: func(job *Job) error {
			allJobsStarted <- "node2:" + job.Key
			jobRequeuedToNode2 <- job.Key
			return nil
		},
		stopFunc: func(key string) error {
			fmt.Printf("job stopped on node2: %s\n", key)
			return nil
		},
		notifyFunc: func(key string, payload []byte) error { return nil },
	}
	worker2, err := node2.AddWorker(ctx, handler2)
	require.NoError(t, err)

	handler3 := &mockHandler{
		startFunc: func(job *Job) error {
			allJobsStarted <- "node3:" + job.Key
			jobRequeuedToNode3 <- job.Key
			return nil
		},
		stopFunc: func(key string) error {
			fmt.Printf("job stopped on node3: %s\n", key)
			return nil
		},
		notifyFunc: func(key string, payload []byte) error { return nil },
	}
	_, err = node3.AddWorker(ctx, handler3)
	require.NoError(t, err)

	// Dispatch jobs (will distribute across all workers via consistent hashing).
	// More jobs => higher odds of hitting the historical race on buggy implementations.
	numJobs := 25
	jobKeys := make([]string, 0, numJobs)
	for range numJobs {
		key := "job-" + uuid.NewString()
		jobKeys = append(jobKeys, key)
		require.NoError(t, node1.DispatchJob(ctx, key, []byte("payload-"+key)))
	}

	// Phase 1: wait until all jobs started somewhere.
	var phase1Node1, phase1Node2, phase1Node3 int
	timeout := time.After(30 * time.Second)
	for phase1Node1+phase1Node2+phase1Node3 < numJobs {
		select {
		case msg := <-allJobsStarted:
			switch {
			case strings.HasPrefix(msg, "node1:"):
				phase1Node1++
			case strings.HasPrefix(msg, "node2:"):
				phase1Node2++
			case strings.HasPrefix(msg, "node3:"):
				phase1Node3++
			}
		case <-timeout:
			t.Fatalf("timeout waiting for initial job starts: %d/%d", phase1Node1+phase1Node2+phase1Node3, numJobs)
		}
	}

	// Verify initial payloads exist.
	initialPayloads := 0
	for _, key := range jobKeys {
		if _, ok := node1.JobPayload(key); ok {
			initialPayloads++
		}
	}
	require.Equal(t, numJobs, initialPayloads, "expected all job payloads to exist after dispatch")

	// Phase 2: crash node1.
	fmt.Printf("\n=== PHASE 2: closing node1 (%s) ===\n", worker1.ID)
	go func() { _ = node1.Close(ctx) }()
	time.Sleep(50 * time.Millisecond)

	// Monitor requeues to node2 and crash node2 mid-requeue.
	uniqueOnNode2 := make(map[string]bool)
	crashed2 := false
	requeueTimeout := time.After(2 * time.Second)
	for {
		select {
		case key := <-jobRequeuedToNode2:
			uniqueOnNode2[key] = true
			// Crash quickly to maximize overlap with cleanup/rebalance.
			if len(uniqueOnNode2) >= 1 && !crashed2 {
				crashed2 = true
				fmt.Printf("\n=== PHASE 3: closing node2 during requeue (%s) ===\n", worker2.ID)
				go func() { _ = node2.Close(ctx) }()
				time.Sleep(50 * time.Millisecond)
			}
		case <-requeueTimeout:
			goto phase4
		}
	}

phase4:
	// Give background cleanup/rebalancing time to progress.
	time.Sleep(1 * time.Second)

	// Drain node3 starts for a bounded time and count unique jobs that landed there.
	jobsOnNode3 := make(map[string]bool)
	drainTimeout := time.After(2 * time.Second)
	for {
		select {
		case key := <-jobRequeuedToNode3:
			jobsOnNode3[key] = true
		case <-drainTimeout:
			goto done
		}
	}

done:
	// Final check: all jobs should have landed on node3.
	var missing []string
	for _, key := range jobKeys {
		if !jobsOnNode3[key] {
			missing = append(missing, key)
		}
	}

	// Useful debugging: distinguish deleted payload vs orphaned payload.
	if len(missing) > 0 {
		fmt.Printf("\nMISSING JOBS (%d):\n", len(missing))
		for _, key := range missing {
			if _, ok := node3.JobPayload(key); ok {
				fmt.Printf("  - %s [ORPHANED payload]\n", key)
			} else {
				fmt.Printf("  - %s [DELETED payload]\n", key)
			}
		}
	}

	// Cleanup (best-effort).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = node3.Shutdown(shutdownCtx)
	_ = node1.Shutdown(shutdownCtx)
	_ = node2.Shutdown(shutdownCtx)

	assert.Equal(t, numJobs, len(jobsOnNode3),
		"BUG: Not all jobs made it to node3 after cascading failures; missing=%d", len(missing))
}
