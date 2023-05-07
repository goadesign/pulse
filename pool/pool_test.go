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
	key := "testDispatchJobOneWorker"
	payload := []byte("payload")
	ctx := testContext(t)
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	pool, err := AddNode(ctx, "testPool", rdb, WithLogger(ponos.ClueLogger(ctx)))
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, pool.Shutdown(ctx))
	}()
	worker, err := pool.AddWorker(ctx)
	assert.NoError(t, err)
	err = pool.DispatchJob(ctx, key, payload)
	assert.NoError(t, err)
	job := readOneWorkerJob(t, worker)
	assert.Equal(t, payload, job.Payload)
	assert.Equal(t, key, job.Key)
}

func TestStopThenShutdown(t *testing.T) {
	ctx := testContext(t)
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	pool, err := AddNode(ctx, "testPool", rdb, WithLogger(ponos.ClueLogger(ctx)))
	require.NoError(t, err)
	worker, err := pool.AddWorker(ctx)
	assert.NoError(t, err)
	assert.NoError(t, worker.Stop(ctx))
	assert.NoError(t, pool.Shutdown(ctx))
}

func readOneWorkerJob(t *testing.T, worker *Worker) *Job {
	t.Helper()
	select {
	case job := <-worker.C:
		return job
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout")
		return nil
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	return log.Context(context.Background(), log.WithDebug())
}
