// Node cleanup tests prove Redis heartbeat/fence linearization.
package pool

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming"
	ptesting "goa.design/pulse/testing"
)

func TestNodeCleanupAcquisitionFencesResumedNode(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())
	staleID := "paused-node"
	staleStream, err := streaming.NewStream(nodeStreamName(node.PoolName, staleID), rdb)
	require.NoError(t, err)
	_, err = staleStream.Add(ctx, evInit, []byte(staleID))
	require.NoError(t, err)

	registered, _, err := registerPoolNode(ctx, rdb, node.resources, staleID)
	require.NoError(t, err)
	require.True(t, registered)
	lease, err := acquireNodeCleanup(
		ctx,
		rdb,
		node.resources,
		staleID,
		newNodeCleanupOwner(node.ID),
	)
	require.NoError(t, err)
	require.Nil(t, lease, "a heartbeat committed before acquisition must win")

	redisNow, err := rdb.Time(ctx).Result()
	require.NoError(t, err)
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.nodeKeepAlive),
		staleID,
		strconv.FormatInt(redisNow.Add(-2*node.workerTTL).UnixNano(), 10),
	).Err())
	lease, err = acquireNodeCleanup(
		ctx,
		rdb,
		node.resources,
		staleID,
		newNodeCleanupOwner(node.ID),
	)
	require.NoError(t, err)
	require.NotNil(t, lease)

	_, _, err = registerPoolNode(ctx, rdb, node.resources, staleID)
	require.ErrorContains(t, err, "NODECLEANUPLOST")
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.nodeKeepAlive),
		nodeCleanupField(staleID),
		fmt.Sprintf("%s|%s|0", lease.owner, lease.fence),
	).Err())
	cleaned, err := cleanupStalePoolNode(
		ctx,
		rdb,
		node.resources,
		staleID,
		newNodeCleanupOwner(node.ID),
	)
	require.NoError(t, err)
	require.True(t, cleaned)
	require.False(t, rdb.HExists(
		ctx,
		rmapContentKey(node.resources.nodeKeepAlive),
		staleID,
	).Val())
	_, err = staleStream.Add(ctx, evInit, []byte("resumed"))
	require.ErrorIs(t, err, streaming.ErrStreamDestroyed)
	require.NoError(t, node.Shutdown(context.Background()))
}

func TestStaleNodeCannotDispatchOrScheduleAfterCleanup(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	node := newTestNode(t, ctx, rdb, t.Name())

	// A live registered node admits dispatches.
	record, err := node.publishDispatchRecord(
		ctx, "job-live", "dispatch-live", []byte("payload"), marshalJob(&Job{
			Key:       "job-live",
			Payload:   []byte("payload"),
			CreatedAt: time.Now(),
			NodeID:    node.ID,
		}),
	)
	require.NoError(t, err)
	require.Equal(t, dispatchClaimed, record.status)

	sched := &scheduler{
		name:             t.Name(),
		interval:         time.Second,
		node:             node,
		transitionPrefix: "=transition:" + hex.EncodeToString([]byte(t.Name())) + ":",
		owner:            "test-transition-owner",
		lease:            node.workerTTL,
		logger:           pulse.NoopLogger(),
	}
	_, err = sched.claimTransition(ctx)
	require.NoError(t, err)

	// An installed node-cleanup fence rejects dispatch and scheduling.
	require.NoError(t, rdb.HSet(
		ctx,
		rmapContentKey(node.resources.nodeKeepAlive),
		nodeCleanupField(node.ID),
		"owner|1|0",
	).Err())
	_, err = node.publishDispatchRecord(
		ctx, "job-fenced", "dispatch-fenced", []byte("payload"), []byte("event"),
	)
	require.ErrorContains(t, err, "NODECLEANUPLOST")
	_, err = sched.claimTransition(ctx)
	require.ErrorContains(t, err, "NODECLEANUPLOST")

	// A node whose heartbeat registration was removed is equally fenced.
	require.NoError(t, rdb.HDel(ctx, rmapContentKey(node.resources.nodeKeepAlive), nodeCleanupField(node.ID)).Err())
	heartbeat := rdb.HGet(ctx, rmapContentKey(node.resources.nodeKeepAlive), node.ID).Val()
	require.NotEmpty(t, heartbeat)
	require.NoError(t, rdb.HDel(ctx, rmapContentKey(node.resources.nodeKeepAlive), node.ID).Err())
	_, err = node.publishDispatchRecord(
		ctx, "job-removed", "dispatch-removed", []byte("payload"), []byte("event"),
	)
	require.ErrorContains(t, err, "NODECLEANUPLOST")
	_, err = sched.claimTransition(ctx)
	require.ErrorContains(t, err, "NODECLEANUPLOST")

	// Restore liveness so shutdown completes normally.
	require.NoError(t, rdb.HSet(ctx, rmapContentKey(node.resources.nodeKeepAlive), node.ID, heartbeat).Err())
	require.NoError(t, node.Shutdown(context.Background()))
}
