package pool

import (
	"context"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"goa.design/pulse/pulse"
)

// mockHandler implements the regular test worker job and notification contract.
type mockHandler struct {
	startFunc  func(job *Job) error
	stopFunc   func(key string) error
	notifyFunc func(key string, payload []byte) error
}

// mockJobHandler implements only the required job handler contract.
type mockJobHandler struct {
	startFunc func(job *Job) error
	stopFunc  func(key string) error
}

// mockMessageHandler extends the regular test job handler with explicit message
// support so tests only opt into the message contract when they need it.
type mockMessageHandler struct {
	*mockHandler
	messageFunc func(key string, payload []byte) error
}

const (
	testRequeueTimeout       = 100 * time.Millisecond
	testJobSinkBlockDuration = 100 * time.Millisecond
	testWorkerTTL            = 2 * time.Second
	testFastWorkerTTL        = 500 * time.Millisecond
	testFastRequeueTimeout   = 100 * time.Millisecond
	// testAckGracePeriod should cover scheduler jitter under -race; tests that
	// need fast stale-worker detection use the worker TTLs above instead.
	testAckGracePeriod = 4 * time.Second
)

// newTestNode creates a new Node instance for testing purposes.
// It configures the node with specific TTL and block duration settings
// suitable for testing, and uses the provided Redis client and name.
func newTestNode(t *testing.T, ctx context.Context, rdb *redis.Client, name string) *Node {
	t.Helper()
	return newTestNodeWithLogger(t, ctx, rdb, name, pulse.NoopLogger())
}

// newTestNodeWithLogger creates a regular test node with the supplied logger.
func newTestNodeWithLogger(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
	name string,
	logger pulse.Logger,
) *Node {
	t.Helper()
	node, err := AddNode(ctx, name, rdb,
		WithLogger(logger),
		WithRequeueTimeout(testRequeueTimeout),
		WithJobSinkBlockDuration(testJobSinkBlockDuration),
		WithWorkerTTL(testWorkerTTL),
		WithDispatchTimeout(2*testAckGracePeriod),
		WithRecoveryGrace(testAckGracePeriod))
	require.NoError(t, err)
	return node
}

// newFastCleanupTestNode creates a node with deliberately short liveness TTLs
// for tests that exercise stale node or worker cleanup. Regular dispatch tests
// use newTestNode so a busy race-enabled scheduler cannot make healthy nodes
// look dead.
func newFastCleanupTestNode(t *testing.T, ctx context.Context, rdb *redis.Client, name string) *Node {
	t.Helper()
	node, err := AddNode(ctx, name, rdb,
		WithLogger(pulse.NoopLogger()),
		WithRequeueTimeout(testFastRequeueTimeout),
		WithJobSinkBlockDuration(testJobSinkBlockDuration),
		WithWorkerTTL(testFastWorkerTTL),
		WithDispatchTimeout(2*testAckGracePeriod),
		WithRecoveryGrace(testAckGracePeriod))
	require.NoError(t, err)
	return node
}

// newTestWorker creates a new Worker instance for testing purposes.
// It sets up a mock handler with basic job management functions and adds the
// worker to the given node.
func newTestWorker(t *testing.T, ctx context.Context, node *Node) *Worker {
	t.Helper()
	handler := newMockHandler()
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	return worker
}

// newTestWorkerWithoutOptionalHandlers creates a worker whose handler only
// implements the required job lifecycle methods.
func newTestWorkerWithoutOptionalHandlers(t *testing.T, ctx context.Context, node *Node) *Worker {
	t.Helper()
	handler := newMockJobHandler()
	worker, err := node.AddWorker(ctx, handler)
	require.NoError(t, err)
	return worker
}

func newMockHandler() *mockHandler {
	return &mockHandler{
		startFunc:  func(job *Job) error { return nil },
		stopFunc:   func(key string) error { return nil },
		notifyFunc: func(key string, payload []byte) error { return nil },
	}
}

func newMockJobHandler() *mockJobHandler {
	return &mockJobHandler{
		startFunc: func(job *Job) error { return nil },
		stopFunc:  func(key string) error { return nil },
	}
}

func (w *mockHandler) Start(job *Job) error  { return w.startFunc(job) }
func (w *mockHandler) Stop(key string) error { return w.stopFunc(key) }
func (w *mockHandler) HandleNotification(key string, payload []byte) error {
	return w.notifyFunc(key, payload)
}
func (w *mockMessageHandler) HandleMessage(key string, payload []byte) error {
	return w.messageFunc(key, payload)
}

func (h *mockJobHandler) Start(job *Job) error  { return h.startFunc(job) }
func (h *mockJobHandler) Stop(key string) error { return h.stopFunc(key) }
