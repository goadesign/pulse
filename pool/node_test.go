package pool

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"

	"goa.design/ponos/ponos"
)

var redisPwd = "redispassword"

const delay = 10 * time.Millisecond
const max = 1000 * time.Millisecond

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
}

type workerMock struct {
	startFunc  func(ctx context.Context, job *Job) error
	stopFunc   func(ctx context.Context, key string) error
	notifyFunc func(ctx context.Context, payload []byte) error
	jobs       map[string]*Job
}

func (w *workerMock) Start(ctx context.Context, job *Job) error  { return w.startFunc(ctx, job) }
func (w *workerMock) Stop(ctx context.Context, key string) error { return w.stopFunc(ctx, key) }
func (w *workerMock) Notify(ctx context.Context, p []byte) error { return w.notifyFunc(ctx, p) }

func TestDispatchJobOneWorker(t *testing.T) {
	var (
		testName = strings.Replace(t.Name(), "/", "_", -1)
		payload  = []byte("payload")
		ctx      = testContext(t)
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		node     = newTestNode(t, ctx, rdb, testName)
		worker   = newTestWorker(t, ctx, node)
	)
	defer cleanup(t, rdb, true, testName)
	err := node.DispatchJob(ctx, testName, payload)
	assert.NoError(t, err)
	require.Eventually(t, func() bool { return numJobs(t, worker) == 1 }, max, delay)
	assert.Equal(t, payload, worker.jobs[testName].Payload)
	assert.NoError(t, node.Shutdown(ctx))
}

func TestDispatchJobTwoWorkers(t *testing.T) {
	var (
		testName = strings.Replace(t.Name(), "/", "_", -1)
		key      = "differentHash"
		key2     = "testDispatchJobTwoWorkers"
		payload  = []byte("payload")
		ctx      = testContext(t)
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		node     = newTestNode(t, ctx, rdb, testName)
		worker1  = newTestWorker(t, ctx, node)
		worker2  = newTestWorker(t, ctx, node)
	)
	defer cleanup(t, rdb, true, testName)
	err := node.DispatchJob(ctx, key, payload)
	assert.NoError(t, err)
	err = node.DispatchJob(ctx, key2, payload)
	assert.NoError(t, err)
	require.Eventually(t, func() bool { return numJobs(t, worker1) == 1 }, max, delay)
	require.Eventually(t, func() bool { return numJobs(t, worker2) == 1 }, max, delay)
	assert.Equal(t, payload, worker1.jobs[key].Payload)
	assert.Equal(t, payload, worker2.jobs[key2].Payload)
	assert.NoError(t, node.Shutdown(ctx))
}

func TestRemoveWorkerThenShutdown(t *testing.T) {
	var (
		ctx      = testContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		node     = newTestNode(t, ctx, rdb, testName)
		worker   = newTestWorker(t, ctx, node)
	)
	defer cleanup(t, rdb, true, testName)
	assert.NoError(t, node.RemoveWorker(ctx, worker))
	assert.NoError(t, node.Shutdown(ctx))
}

func TestClose(t *testing.T) {
	var (
		ctx      = testContext(t)
		testName = strings.Replace(t.Name(), "/", "_", -1)
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		node     = newTestNode(t, ctx, rdb, testName)
	)
	defer cleanup(t, rdb, false, testName)
	assert.NoError(t, node.Close(ctx))
}

func newTestNode(t *testing.T, ctx context.Context, rdb *redis.Client, name string) *Node {
	t.Helper()
	node, err := AddNode(ctx, name, rdb,
		WithLogger(ponos.ClueLogger(ctx)),
		WithWorkerShutdownTTL(100*time.Millisecond),
		WithJobSinkBlockDuration(50*time.Millisecond),
		WithWorkerTTL(50*time.Millisecond))
	require.NoError(t, err)
	return node
}

func newTestWorker(t *testing.T, ctx context.Context, node *Node) *Worker {
	t.Helper()
	wm := &workerMock{jobs: make(map[string]*Job)}
	wm.startFunc = func(_ context.Context, job *Job) error { wm.jobs[job.Key] = job; return nil }
	wm.stopFunc = func(_ context.Context, key string) error { delete(wm.jobs, key); return nil }
	wm.notifyFunc = func(_ context.Context, payload []byte) error { return nil }
	worker, err := node.AddWorker(ctx, wm)
	require.NoError(t, err)
	return worker
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	return log.Context(context.Background(), log.WithDebug())
}

func numJobs(t *testing.T, w *Worker) int {
	t.Helper()
	w.lock.Lock()
	defer w.lock.Unlock()
	return len(w.jobs)
}
func cleanup(t *testing.T, rdb *redis.Client, checkClean bool, testName string) {
	t.Helper()
	ctx := context.Background()
	keys, err := rdb.Keys(ctx, "*").Result()
	require.NoError(t, err)
	var filtered []string
	for _, k := range keys {
		if strings.Contains(k, testName) {
			filtered = append(filtered, k)
		}
	}
	if checkClean {
		assert.Len(t, filtered, 0)
	}
	assert.NoError(t, rdb.FlushDB(ctx).Err())
}
