// Stream lifecycle tests prove generation identity survives Redis state loss
// for the genesis handle while intentional destruction stays terminal.
package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming/options"
	ptesting "goa.design/pulse/testing"
)

func TestGenesisLifecycleReestablishedAfterStateLoss(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	sink, err := stream.NewSink(ctx, "sink", options.WithSinkBlockDuration(testBlockDuration))
	require.NoError(t, err)
	defer cleanupSink(t, ctx, stream, sink)
	events := sink.Subscribe()

	_, err = stream.Add(ctx, "before", []byte("before"))
	require.NoError(t, err)
	require.NoError(t, sink.Ack(ctx, receiveSinkEvent(t, events)))

	// Redis loses every key: lifecycle, stream data, groups, and cursors.
	// The bound genesis handles must re-assert their identity and resume.
	require.NoError(t, rdb.FlushDB(ctx).Err())

	// The sink's read loop re-establishes the genesis lifecycle and recreates
	// its consumer group; the flushed recovery cursor is gone, so the group
	// restarts at the tail and only later events are deliverable.
	require.Eventually(t, func() bool {
		return consumerGroupExists(ctx, rdb, stream.key, sink.Name)
	}, max, delay, "sink should re-establish the lost genesis lifecycle and group")

	afterID, err := stream.Add(ctx, "after", []byte("after"))
	require.NoError(t, err, "publisher should re-establish the lost genesis lifecycle")
	require.Equal(t, "1", stream.Generation())

	recovered := receiveSinkEvent(t, events)
	require.Equal(t, afterID, recovered.ID)
	require.Equal(t, "after", recovered.EventName)
	require.NoError(t, sink.Ack(ctx, recovered))
}

func TestDestroyedGenesisStaysTerminal(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, "")
	ctx := ptesting.NewTestContext(t)
	stream, err := NewStream(t.Name(), rdb, options.WithStreamLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	require.NoError(t, stream.Open(ctx))
	require.NoError(t, stream.Destroy(ctx))

	// A destroy tombstone is intentional termination, not state loss: the
	// bound handle must not resurrect the generation.
	_, err = stream.Add(ctx, "after", []byte("after"))
	require.ErrorIs(t, err, ErrStreamDestroyed)
	require.ErrorIs(t, stream.verifyGeneration(ctx), ErrStreamDestroyed)
}
