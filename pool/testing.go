package pool

import (
	"context"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"goa.design/pulse/pulse"
)

// mockHandler is a mock worker mockHandler for testing
type mockHandler struct {
	startFunc  func(job *Job) error
	stopFunc   func(key string) error
	notifyFunc func(payload []byte) error
}

const (
	testWorkerShutdownTTL    = 100 * time.Millisecond
	testJobSinkBlockDuration = 100 * time.Millisecond
	testWorkerTTL            = 100 * time.Millisecond
	testAckGracePeriod       = 50 * time.Millisecond
)

// newTestNode creates a new Node instance for testing purposes.
// It configures the node with specific TTL and block duration settings
// suitable for testing, and uses the provided Redis client and name.
func newTestNode(t *testing.T, ctx context.Context, rdb *redis.Client, name string) *Node {
	t.Helper()
	node, err := AddNode(ctx, name, rdb,
		WithLogger(pulse.ClueLogger(ctx)),
		WithWorkerShutdownTTL(testWorkerShutdownTTL),
		WithJobSinkBlockDuration(testJobSinkBlockDuration),
		WithWorkerTTL(testWorkerTTL),
		WithAckGracePeriod(testAckGracePeriod))
	require.NoError(t, err)
	return node
}

// newTestWorker creates a new Worker instance for testing purposes.
// It sets up a mock handler with basic job management functions and adds the
// worker to the given node.
func newTestWorker(t *testing.T, ctx context.Context, node *Node) *Worker {
	t.Helper()
	handler := &mockHandler{
		startFunc:  func(job *Job) error { return nil },
		stopFunc:   func(key string) error { return nil },
		notifyFunc: func(payload []byte) error { return nil },
	}
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	return worker
}

func (w *mockHandler) Start(job *Job) error  { return w.startFunc(job) }
func (w *mockHandler) Stop(key string) error { return w.stopFunc(key) }
func (w *mockHandler) Notify(p []byte) error { return w.notifyFunc(p) }
