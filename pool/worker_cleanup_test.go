package pool

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"

	ptesting "goa.design/pulse/testing"
)

func TestWorkerCleanupLeaseFencesAndDeduplicatesTakeover(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	workerID := "stale-worker"
	job := &Job{
		Key:       "job",
		Payload:   []byte("payload"),
		CreatedAt: time.Unix(1, 0),
		NodeID:    node.ID,
		Requeued:  true,
	}
	require.NoError(t, node.appendPoolMapValue(ctx, node.resources.jobs, workerID, job.Key))
	require.NoError(t, node.setPoolMap(ctx, node.resources.jobPayloads, job.Key, string(job.Payload)))

	first, err := node.acquireWorkerCleanup(ctx, workerID)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.workerCleanup),
		workerID,
		fmt.Sprintf("%s|%s|0", first.owner, first.fence),
	).Err())
	second, err := node.acquireWorkerCleanup(ctx, workerID)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NotEqual(t, first.fence, second.fence)

	_, err = node.publishWorkerRequeue(ctx, first, job)
	require.ErrorContains(t, err, "WORKERCLEANUPLOST")
	status, err := node.publishWorkerRequeue(ctx, second, job)
	require.NoError(t, err)
	require.EqualValues(t, 1, status)
	status, err = node.publishWorkerRequeue(ctx, second, job)
	require.NoError(t, err)
	require.EqualValues(t, 0, status)
	physical := rdb.HGet(
		ctx,
		fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
		"physical_key",
	).Val()
	require.EqualValues(t, 1, rdb.XLen(ctx, physical).Val())

	require.NoError(t, node.releaseWorkerCleanup(ctx, second, true))
	require.False(t, rdb.HExists(
		ctx,
		rmapContentKey(node.resources.workerCleanup),
		workerRequeueField(workerID, job.Key),
	).Val())
	require.NoError(t, node.Shutdown(context.Background()))
}

func TestWorkerCleanupAcquisitionLinearizesWithHeartbeat(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	workerID := "paused-worker"
	require.NoError(t, node.setPoolMap(
		ctx,
		node.resources.workers,
		workerID,
		strconv.FormatInt(time.Now().UnixNano(), 10),
	))

	_, err := node.updateWorkerHeartbeat(ctx, workerID)
	require.NoError(t, err)
	lease, err := node.acquireWorkerCleanup(ctx, workerID)
	require.NoError(t, err)
	require.Nil(t, lease, "a heartbeat committed before acquisition must win")

	redisNow, err := rdb.Time(ctx).Result()
	require.NoError(t, err)
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.workerKeepAlive),
		workerID,
		strconv.FormatInt(redisNow.Add(-2*node.workerTTL).UnixNano(), 10),
	).Err())
	lease, err = node.acquireWorkerCleanup(ctx, workerID)
	require.NoError(t, err)
	require.NotNil(t, lease)

	_, err = node.updateWorkerHeartbeat(ctx, workerID)
	require.ErrorContains(t, err, "WORKERCLEANUPLOST")
	require.NoError(t, node.releaseWorkerCleanup(ctx, lease, false))
	require.NoError(t, node.Shutdown(context.Background()))
}

// runClaimWorkerStart mirrors Worker.claimDispatchedStart for a synthetic
// worker so tests can drive the claim script against injected fence states.
func runClaimWorkerStart(
	ctx context.Context,
	node *Node,
	workerID, key, dispatchID string,
	payload, identity []byte,
) (int64, error) {
	return claimWorkerStartScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.jobPending),
			dispatchRecordKey(node.resources.dispatches, dispatchID),
			rmapContentKey(node.resources.jobPayloads),
			rmapContentKey(node.resources.jobs),
			rmapUpdateChannel(node.resources.jobs),
			rmapUpdateChannel(node.resources.jobPayloads),
			rmapContentKey(node.resources.workers),
			rmapContentKey(node.resources.workerCleanup),
		},
		node.resources.generation,
		"active",
		key,
		dispatchID,
		workerID,
		payload,
		identity,
	).Int64()
}

// seedPendingDispatch installs the admission guard and durable record one
// exact dispatch needs before a worker may claim its start.
func seedPendingDispatch(
	t *testing.T,
	ctx context.Context,
	node *Node,
	key, dispatchID string,
	payload []byte,
) []byte {
	t.Helper()
	identity, err := dispatchIdentity(key, payload)
	require.NoError(t, err)
	require.NoError(t, node.setPoolMap(ctx, node.resources.jobPending, key, dispatchID))
	require.NoError(t, node.rdb.HSet(
		ctx,
		dispatchRecordKey(node.resources.dispatches, dispatchID),
		"id", dispatchID,
		"identity", identity,
		"key", key,
		"state", "pending",
	).Err())
	return identity
}

func TestStaleWorkerCannotClaimAfterCleanupFence(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	workerID := "claiming-worker"
	payload := []byte("payload")
	createdAt := strconv.FormatInt(time.Now().UnixNano(), 10)
	require.NoError(t, node.setPoolMap(ctx, node.resources.workers, workerID, createdAt))

	// A live registered worker claims exactly once.
	identity := seedPendingDispatch(t, ctx, node, "job-live", "dispatch-live", payload)
	claimed, err := runClaimWorkerStart(ctx, node, workerID, "job-live", "dispatch-live", payload, identity)
	require.NoError(t, err)
	require.EqualValues(t, 1, claimed)

	// A payload that diverged from the admitted identity is rejected: the
	// claim carries the identity derived from the bytes it is about to run.
	seedPendingDispatch(t, ctx, node, "job-identity", "dispatch-identity", payload)
	mutated := []byte("mutated")
	mutatedIdentity, err := dispatchIdentity("job-identity", mutated)
	require.NoError(t, err)
	_, err = runClaimWorkerStart(
		ctx, node, workerID, "job-identity", "dispatch-identity", mutated, mutatedIdentity,
	)
	require.ErrorContains(t, err, "DISPATCHIDENTITYMISMATCH")

	// The worker classifies the mismatch as a terminal invariant violation,
	// not a requeue: retrying immutable bytes can never repair it.
	mismatchWorker := &Worker{ID: workerID, node: node, logger: pulse.NoopLogger()}
	_, err = mismatchWorker.claimDispatchedStart(ctx, &Job{
		Key:        "job-identity",
		Payload:    mutated,
		dispatchID: "dispatch-identity",
	})
	require.ErrorIs(t, err, errDispatchIdentityMismatch)

	// An installed cleanup fence rejects the claim atomically.
	redisNow, err := rdb.Time(ctx).Result()
	require.NoError(t, err)
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.workerCleanup),
		workerID,
		fmt.Sprintf("owner|1|%d", redisNow.Add(time.Minute).UnixMilli()),
	).Err())
	identity = seedPendingDispatch(t, ctx, node, "job-fenced", "dispatch-fenced", payload)
	_, err = runClaimWorkerStart(ctx, node, workerID, "job-fenced", "dispatch-fenced", payload, identity)
	require.ErrorContains(t, err, "WORKERCLEANUPLOST")

	// A deregistered worker cannot claim even after the fence is gone.
	require.NoError(t, rdb.HDel(ctx, rmapContentKey(node.resources.workerCleanup), workerID).Err())
	require.NoError(t, node.setPoolMap(ctx, node.resources.workers, workerID, "-"))
	_, err = runClaimWorkerStart(ctx, node, workerID, "job-fenced", "dispatch-fenced", payload, identity)
	require.ErrorContains(t, err, "WORKERCLEANUPLOST")

	require.NoError(t, node.Shutdown(context.Background()))
}

func TestGracefulRequeueLeaseArbitratesWithCleanup(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	workerID := "graceful-worker"
	createdAt := strconv.FormatInt(time.Now().UnixNano(), 10)
	require.NoError(t, node.setPoolMap(ctx, node.resources.workers, workerID, createdAt))

	// Graceful self-acquisition wins the lease despite a live heartbeat and
	// atomically deactivates the registration.
	_, err := node.updateWorkerHeartbeat(ctx, workerID)
	require.NoError(t, err)
	lease, err := node.acquireGracefulRequeue(ctx, workerID)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, "-", rdb.HGet(ctx, rmapContentKey(node.resources.workers), workerID).Val())

	// While the graceful lease is live, neither a foreign cleanup owner nor a
	// second graceful attempt can win.
	foreign, err := node.acquireWorkerCleanup(ctx, workerID)
	require.NoError(t, err)
	require.Nil(t, foreign, "foreign cleanup must not steal a live graceful lease")
	second, err := node.acquireGracefulRequeue(ctx, workerID)
	require.NoError(t, err)
	require.Nil(t, second, "a second graceful attempt must not win a live lease")

	// After the graceful owner disappears (lease expired, heartbeat stale),
	// foreign cleanup recovers the half-requeued worker: the deactivated
	// registration is not a shield against takeover.
	redisNow, err := rdb.Time(ctx).Result()
	require.NoError(t, err)
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.workerKeepAlive),
		workerID,
		strconv.FormatInt(redisNow.Add(-2*node.workerTTL).UnixNano(), 10),
	).Err())
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.workerCleanup),
		workerID,
		fmt.Sprintf("%s|%s|0", lease.owner, lease.fence),
	).Err())
	foreign, err = node.acquireWorkerCleanup(ctx, workerID)
	require.NoError(t, err)
	require.NotNil(t, foreign, "expired graceful lease must be recoverable by cleanup")

	// Once cleanup deleted the registration, graceful acquisition refuses and
	// never recreates it.
	require.NoError(t, node.releaseWorkerCleanup(ctx, foreign, true))
	require.NoError(t, rdb.HDel(ctx, rmapContentKey(node.resources.workers), workerID).Err())
	late, err := node.acquireGracefulRequeue(ctx, workerID)
	require.NoError(t, err)
	require.Nil(t, late)
	require.False(t, rdb.HExists(ctx, rmapContentKey(node.resources.workers), workerID).Val())

	require.NoError(t, node.Shutdown(context.Background()))
}
