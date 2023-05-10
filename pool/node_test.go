package pool

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"

	"goa.design/ponos/ponos"
)

var redisPwd = "redispassword"

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
}

func TestDispatchJobOneWorker(t *testing.T) {
	var (
		key     = "testDispatchJobOneWorker"
		payload = []byte("payload")
		ctx     = testContext(t)
		rdb     = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		node    = newTestNode(t, ctx, rdb)
		worker  = newTestWorker(t, ctx, node)
	)
	err := node.DispatchJob(ctx, key, payload)
	assert.NoError(t, err)
	job := readOneWorkerJob(t, worker)
	if job != nil {
		job.Ack(ctx)
		assert.Equal(t, payload, job.Payload)
		assert.Equal(t, key, job.Key)
	}
	assert.NoError(t, node.Shutdown(ctx))
	ln, err := rdb.Keys(ctx, "*").Result()
	assert.NoError(t, err)
	assert.Len(t, ln, 0)
}

func TestStopThenShutdown(t *testing.T) {
	var (
		ctx    = testContext(t)
		rdb    = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		node   = newTestNode(t, ctx, rdb)
		worker = newTestWorker(t, ctx, node)
	)
	worker.Stop(ctx)
	assert.NoError(t, node.Shutdown(ctx))
}

func newTestNode(t *testing.T, ctx context.Context, rdb *redis.Client) *Node {
	t.Helper()
	node, err := AddNode(ctx, "testNode", rdb,
		WithLogger(ponos.ClueLogger(ctx)),
		WithMaxShutdownDuration(100*time.Millisecond),
		WithJobSinkBlockDuration(50*time.Millisecond),
		WithWorkerTTL(50*time.Millisecond))
	require.NoError(t, err)
	return node
}

func newTestWorker(t *testing.T, ctx context.Context, node *Node) *Worker {
	t.Helper()
	worker, err := node.AddWorker(ctx)
	require.NoError(t, err)
	return worker
}

func readOneWorkerJob(t *testing.T, worker *Worker) *Job {
	t.Helper()
	select {
	case job := <-worker.C:
		return job
	case <-time.After(10 * time.Second):
		t.Error("timeout")
		return nil
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	return log.Context(context.Background(), log.WithDebug())
}
