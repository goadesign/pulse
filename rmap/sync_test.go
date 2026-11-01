package rmap

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ptesting "goa.design/pulse/testing"
)

type (
	// dialGate lets a test refuse new connections from one Redis client, to
	// hold a replica disconnected while other replicas write.
	dialGate struct {
		closed atomic.Bool
	}
)

// TestSyncSeesWritesFromAnotherReplica checks the cross-replica guarantee
// through the public API: after a write returns on one replica, Sync on
// another makes the write visible to its local reads.
func TestSyncSeesWritesFromAnotherReplica(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	// Each test is bounded so a regression fails here instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, writer)
	reader, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, reader)
	require.NoError(t, writer.Reset(ctx))

	for i := range 200 {
		value := string(rune('a' + i%26))
		_, err := writer.Set(ctx, "k", value)
		require.NoError(t, err)
		require.NoError(t, reader.Sync(ctx))
		got, _ := reader.Get("k")
		require.Equal(t, value, got)
	}
}

// TestSyncIncludesWriteWhoseNotificationWasLost commits a change in Redis the
// way a write script does but never publishes it, as when the notification is
// lost. Sync must still return with the change visible, because it reads the
// map instead of waiting for notifications.
func TestSyncIncludesWriteWhoseNotificationWasLost(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	// Each test is bounded so a regression fails here instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)
	require.NoError(t, m.Reset(ctx))
	_, err = m.SetAndWait(ctx, "k", "v1")
	require.NoError(t, err)

	require.NoError(t, rdb.HSet(ctx, m.hashkey, "k", "v2").Err())
	require.NoError(t, rdb.HIncrBy(ctx, m.hashkey, revField, 1).Err())
	local, _ := m.Get("k")
	require.Equal(t, "v1", local)

	require.NoError(t, m.Sync(ctx))
	local, _ = m.Get("k")
	assert.Equal(t, "v2", local)
}

// TestSkippedRevisionReloads loses one change's notification while the
// replica stays connected (the change is committed in Redis but never
// published), then publishes a later change normally. The later change skips
// a revision, so the replica must reload and hold both changes, without any
// call to Sync.
func TestSkippedRevisionReloads(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	// Each test is bounded so a regression fails here instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, writer)
	reader, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, reader)
	require.NoError(t, writer.Reset(ctx))
	_, err = writer.SetAndWait(ctx, "a", "1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, ok := reader.Get("a"); return ok }, wf, tck)

	require.NoError(t, rdb.HSet(ctx, writer.hashkey, "lost", "x").Err())
	require.NoError(t, rdb.HIncrBy(ctx, writer.hashkey, revField, 1).Err())
	_, err = writer.Set(ctx, "b", "2")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		lost, okLost := reader.Get("lost")
		b, okB := reader.Get("b")
		return okLost && lost == "x" && okB && b == "2"
	}, wf, tck)
}

// TestExpiredMapStartsNewGeneration lets the map's data expire in Redis while
// two replicas hold it. Sync must empty the copy, and a later write (whose
// revisions restart in a new generation) must reach a replica that never
// called Sync, without being mistaken for an old change.
func TestExpiredMapStartsNewGeneration(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	// Each test is bounded so a regression fails here instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, writer)
	syncer, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, syncer)
	bystander, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, bystander)
	require.NoError(t, writer.Reset(ctx))
	for _, v := range []string{"v1", "v2", "v3"} {
		_, err := writer.SetAndWait(ctx, "k", v)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { v, _ := bystander.Get("k"); return v == "v3" }, wf, tck)
	require.NoError(t, syncer.Sync(ctx))

	require.NoError(t, rdb.PExpire(ctx, writer.hashkey, 50*time.Millisecond).Err())
	require.Eventually(t, func() bool { return rdb.Exists(ctx, writer.hashkey).Val() == 0 }, wf, tck)

	require.NoError(t, syncer.Sync(ctx))
	_, ok := syncer.Get("k")
	assert.False(t, ok, "Sync after expiry must empty the copy")

	_, err = writer.Set(ctx, "k", "v4")
	require.NoError(t, err)
	assert.Eventually(t, func() bool { v, _ := bystander.Get("k"); return v == "v4" }, wf, tck)
	assert.Eventually(t, func() bool { v, _ := syncer.Get("k"); return v == "v4" }, wf, tck)
	require.NoError(t, syncer.Sync(ctx))
	v, _ := syncer.Get("k")
	assert.Equal(t, "v4", v)
}

// TestStaleGenerationChangeDoesNotResurrectExpiredData delivers a queued
// change from an expired generation after Sync has seen the map gone. The
// replica must check Redis rather than adopt the change, so the expired value
// stays gone.
func TestStaleGenerationChangeDoesNotResurrectExpiredData(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	// Each test is bounded so a regression fails here instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)
	require.NoError(t, rdb.Del(ctx, m.hashkey).Err())
	require.NoError(t, m.Sync(ctx))
	_, ok := m.Get("k")
	require.False(t, ok)

	stale := "set:" + packStrings("k", "expired", "1", "old-generation")
	require.NoError(t, rdb.Publish(ctx, m.chankey, stale).Err())
	time.Sleep(200 * time.Millisecond)
	_, ok = m.Get("k")
	assert.False(t, ok, "a change from an expired generation must not be applied")
}

// TestResubscribeReloadsMissedChanges drops a replica's Redis connections and
// holds it disconnected while another replica writes, so the notifications
// are lost. Once go-redis reconnects and resubscribes, the replica must reload
// and hold the writes, and subscribers must see the reset.
func TestResubscribeReloadsMissedChanges(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	// Each test is bounded so a regression fails here instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gate := &dialGate{}
	opts := *rdb.Options()
	opts.Dialer = gate.dial
	// A unique name lets the test drop only this client's subscription.
	opts.ClientName = "rmap-gated-" + strings.ToLower(rand.Text())
	gated := redis.NewClient(&opts)
	defer func() { assert.NoError(t, gated.Close()) }()

	writer, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, writer)
	reader, err := Join(ctx, "test", gated)
	require.NoError(t, err)
	defer reader.Close()
	_, err = writer.SetAndWait(ctx, "before", "1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, ok := reader.Get("before"); return ok }, wf, tck)
	events := reader.Subscribe()

	gate.closed.Store(true)
	killSubscriber(t, rdb, opts.ClientName)
	require.NoError(t, writer.Reset(ctx))
	_, err = writer.Set(ctx, "during", "2")
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	_, ok := reader.Get("during")
	require.False(t, ok, "the reader must still be disconnected")

	gate.closed.Store(false)
	require.Eventually(t, func() bool {
		v, ok := reader.Get("during")
		_, stale := reader.Get("before")
		return ok && v == "2" && !stale
	}, 5*time.Second, tck)
	select {
	case ev := <-events:
		assert.Equal(t, EventReset, ev)
	case <-time.After(wf):
		t.Fatal("expected a reset notification after the reload")
	}
}

// TestSyncErrors covers a canceled context and a stopped map.
func TestSyncErrors(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	// Each test is bounded so a regression fails here instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	require.NoError(t, m.Reset(ctx))

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	assert.ErrorIs(t, m.Sync(canceled), context.Canceled)

	m.Close()
	assert.ErrorContains(t, m.Sync(ctx), "is stopped")
	require.NoError(t, rdb.Del(ctx, m.hashkey).Err())
}

// dial opens a connection unless the gate is closed.
func (g *dialGate) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if g.closed.Load() {
		return nil, net.ErrClosed
	}
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// killSubscriber closes the subscriber connection of the client named name,
// as a network failure would, without touching any other Redis client.
func killSubscriber(t *testing.T, rdb *redis.Client, name string) {
	t.Helper()
	ctx := context.Background()
	list, err := rdb.Do(ctx, "CLIENT", "LIST", "TYPE", "pubsub").Text()
	require.NoError(t, err)
	var ids []string
	for _, line := range strings.Split(list, "\n") {
		fields := make(map[string]string)
		for _, field := range strings.Fields(line) {
			k, v, _ := strings.Cut(field, "=")
			fields[k] = v
		}
		if fields["name"] == name {
			ids = append(ids, fields["id"])
		}
	}
	require.Len(t, ids, 1, "expected exactly one subscriber connection named %s", name)
	require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "ID", ids[0]).Err())
}

// packStrings encodes strings the way the replicated map's Lua scripts
// publish them: each as a little-endian uint32 length followed by its bytes.
func packStrings(values ...string) string {
	var out []byte
	for _, v := range values {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(v)))
		out = append(out, v...)
	}
	return string(out)
}
