package rmap

import (
	"context"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ptesting "goa.design/pulse/testing"
)

// TestSyncWaitsForRevisionCommittedElsewhere commits a write in Redis the way
// another replica's script would, but holds back its notification, so the
// local copy is behind on purpose. Sync must block until the notification is
// delivered and then return with the write visible.
func TestSyncWaitsForRevisionCommittedElsewhere(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	ctx := context.Background()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)
	require.NoError(t, m.Reset(ctx))
	_, err = m.SetAndWait(ctx, "k", "v1")
	require.NoError(t, err)

	require.NoError(t, rdb.HSet(ctx, m.hashkey, "k", "v2").Err())
	rev, err := rdb.HIncrBy(ctx, m.hashkey, revField, 1).Result()
	require.NoError(t, err)

	done := make(chan error, 1)
	go syncInto(ctx, m, done)
	select {
	case err := <-done:
		t.Fatalf("Sync returned before the local copy applied revision %d: %v", rev, err)
	case <-time.After(100 * time.Millisecond):
	}
	local, _ := m.Get("k")
	assert.Equal(t, "v1", local)

	payload := "set:" + packStrings("k", "v2", formatRevision(uint64(rev)))
	require.NoError(t, rdb.Publish(ctx, m.chankey, payload).Err())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(wf):
		t.Fatal("Sync did not return after the revision was applied")
	}
	local, _ = m.Get("k")
	assert.Equal(t, "v2", local)
}

// TestSyncReturnsWhenCaughtUp covers the calls that need no wait: a map that
// was never written and a copy that already holds the latest revision.
func TestSyncReturnsWhenCaughtUp(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	ctx := context.Background()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)

	require.NoError(t, rdb.Del(ctx, m.hashkey).Err())
	assert.NoError(t, m.Sync(ctx))

	_, err = m.SetAndWait(ctx, "k", "v1")
	require.NoError(t, err)
	assert.NoError(t, m.Sync(ctx))
}

// TestSyncSeesWritesFromAnotherReplica checks the cross-replica guarantee
// through the public API: after a write returns on one replica, Sync on
// another makes the write visible to its local reads.
func TestSyncSeesWritesFromAnotherReplica(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	ctx := context.Background()
	writer, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, writer)
	reader, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, reader)
	require.NoError(t, writer.Reset(ctx))

	for i := range 200 {
		value := formatRevision(uint64(i))
		_, err := writer.Set(ctx, "k", value)
		require.NoError(t, err)
		require.NoError(t, reader.Sync(ctx))
		got, _ := reader.Get("k")
		require.Equal(t, value, got)
	}
}

// TestSyncErrors covers a context that ends while waiting, a stopped map,
// and a map stopped while Sync waits.
func TestSyncErrors(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	ctx := context.Background()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	require.NoError(t, m.Reset(ctx))

	// Commit a revision with no notification so Sync has to wait.
	_, err = rdb.HIncrBy(ctx, m.hashkey, revField, 1).Result()
	require.NoError(t, err)
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, m.Sync(short), context.DeadlineExceeded)

	done := make(chan error, 1)
	go syncInto(ctx, m, done)
	time.Sleep(50 * time.Millisecond)
	m.Close()
	select {
	case err := <-done:
		assert.ErrorContains(t, err, "is stopped")
	case <-time.After(wf):
		t.Fatal("Sync did not return after Close")
	}
	assert.ErrorContains(t, m.Sync(ctx), "is stopped")
	require.NoError(t, rdb.Del(ctx, m.hashkey).Err())
}

// syncInto runs m.Sync and sends its result to done.
func syncInto(ctx context.Context, m *Map, done chan<- error) {
	done <- m.Sync(ctx)
}

// packStrings encodes strings the way the Lua scripts' struct.pack("ic0...")
// does: each as a little-endian uint32 length followed by its bytes.
func packStrings(values ...string) string {
	var out []byte
	for _, v := range values {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(v)))
		out = append(out, v...)
	}
	return string(out)
}

// formatRevision renders a revision the way the Lua scripts publish it.
func formatRevision(rev uint64) string {
	return strconv.FormatUint(rev, 10)
}
