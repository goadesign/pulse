// These tests interrupt AddWorker at its Redis registration command. Failed
// replies must leave unfinished cleanup discoverable. Each saved stream version,
// called its generation, must reject an old constructor after that version ends.
package pool

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming"
	ptesting "goa.design/pulse/testing"
)

type (
	workerLifecycleContextKey struct{}

	workerLifecycleRegistration struct {
		workerID     string
		createdAt    string
		generation   string
		lifecycleKey string
	}

	workerLifecycleHook struct {
		workersKey      string
		afterSave       bool
		replyError      error
		destroyError    error
		observed        chan workerLifecycleRegistration
		resume          chan struct{}
		releaseOnce     sync.Once
		intercepted     atomic.Bool
		destroyAttempts atomic.Int32
		registration    workerLifecycleRegistration
	}

	workerLifecycleResult struct {
		worker *Worker
		err    error
	}

	workerLifecycleStreamSnapshot struct {
		lifecycle map[string]string
		events    []redis.XMessage
	}
)

func TestWorkerRegistrationFailedDestroyPreservesDiscovery(t *testing.T) {
	ctx, rdb := newWorkerLifecycleRedis(t)
	node := newWorkerLifecycleNode(t, ctx, rdb)
	replyError := errors.New("synthetic registration reply failure")
	destroyError := errors.New("synthetic exact stream destruction failure")
	hook := newWorkerLifecycleHook(t, rdb, node)
	hook.afterSave = true
	hook.replyError = replyError
	hook.destroyError = destroyError
	result := startWorkerLifecycleConstructor(ctx, node)
	registration := awaitWorkerLifecycleRegistration(t, ctx, hook)
	original := openWorkerLifecycleStream(t, ctx, rdb, registration)
	before := workerLifecycleMaps(t, ctx, rdb, node)
	streamBefore := readWorkerLifecycleStream(t, ctx, rdb, registration.lifecycleKey)
	if before[0][registration.workerID] != registration.createdAt {
		t.Fatalf("saved worker creation = %q, want %q",
			before[0][registration.workerID], registration.createdAt)
	}
	heartbeat, err := strconv.ParseInt(before[1][registration.workerID], 10, 64)
	if err != nil || heartbeat <= 0 {
		t.Fatalf("first heartbeat = %q, want a positive Redis timestamp: %v",
			before[1][registration.workerID], err)
	}
	creation, err := strconv.ParseInt(registration.createdAt, 10, 64)
	if err != nil || heartbeat < creation {
		t.Fatalf("heartbeat %d precedes worker creation %q: %v", heartbeat, registration.createdAt, err)
	}

	hook.release()
	got := awaitWorkerLifecycleResult(t, ctx, result)
	if got.worker != nil || !errors.Is(got.err, replyError) || !errors.Is(got.err, destroyError) {
		t.Fatalf("AddWorker = (%v, %v), want no worker and both injected errors", got.worker, got.err)
	}
	if attempts := hook.destroyAttempts.Load(); attempts != 1 {
		t.Errorf("exact stream destruction attempts = %d, want 1", attempts)
	}
	if after := workerLifecycleMaps(t, ctx, rdb, node); !reflect.DeepEqual(before, after) {
		t.Errorf("failed destruction changed discovery or revisions: before %v, after %v", before, after)
	}
	if streamAfter := readWorkerLifecycleStream(t, ctx, rdb, registration.lifecycleKey); !reflect.DeepEqual(streamBefore, streamAfter) {
		t.Errorf("failed destruction changed the active stream: before %v, after %v", streamBefore, streamAfter)
	}
	requireWorkerLifecycleState(t, ctx, rdb, registration.lifecycleKey, original.Generation(), "active")
	requireNoWorkerLifecycleAdmission(t, node, registration.workerID)

	// The failed constructor has no heartbeat writer. Make its saved heartbeat
	// stale, then let the existing cleanup owner destroy the stream and records.
	now, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatalf("read Redis cleanup time: %v", err)
	}
	if err := rdb.HSet(ctx, rmapContentKey(node.resources.workerKeepAlive),
		registration.workerID, strconv.FormatInt(now.Add(-2*node.workerTTL).UnixNano(), 10)).Err(); err != nil {
		t.Fatalf("set synthetic stale worker heartbeat: %v", err)
	}
	lease, err := node.acquireWorkerCleanup(ctx, registration.workerID)
	if err != nil || lease == nil {
		t.Fatalf("acquire worker cleanup = (%v, %v), want an owned lease", lease, err)
	}
	complete := node.requeueWorkerJobs(ctx, lease)
	if err := node.releaseWorkerCleanup(ctx, lease, complete); err != nil {
		t.Fatalf("release worker cleanup: %v", err)
	}
	if !complete {
		t.Fatal("existing cleanup did not finish the failed constructor's worker")
	}
	for index, entries := range workerLifecycleMaps(t, ctx, rdb, node) {
		if _, exists := entries[registration.workerID]; exists {
			t.Errorf("cleanup retained worker in map %d", index)
		}
	}
	requireWorkerLifecycleState(t, ctx, rdb, registration.lifecycleKey, original.Generation(), "destroyed")
	if err := original.Open(ctx); !errors.Is(err, streaming.ErrStreamDestroyed) {
		t.Errorf("cleaned original stream Open = %v, want ErrStreamDestroyed", err)
	}
}

func TestWorkerRegistrationLifecycleLossRetiresNode(t *testing.T) {
	for _, loss := range []string{"pool generation", "node cleanup"} {
		t.Run(loss, func(t *testing.T) {
			ctx, rdb := newWorkerLifecycleRedis(t)
			node := newWorkerLifecycleNode(t, ctx, rdb)
			hook := newWorkerLifecycleHook(t, rdb, node)
			result := startWorkerLifecycleConstructor(ctx, node)
			registration := awaitWorkerLifecycleRegistration(t, ctx, hook)
			original := openWorkerLifecycleStream(t, ctx, rdb, registration)
			before := workerLifecycleMaps(t, ctx, rdb, node)
			for index, entries := range before {
				if _, exists := entries[registration.workerID]; exists {
					t.Fatalf("worker already registered in map %d before the paused command", index)
				}
			}

			// AddWorker already passed its pool and node checks. End the actual
			// generation or clean up the stale node before registration executes.
			switch loss {
			case "pool generation":
				if err := node.poolStream.Destroy(ctx); err != nil {
					t.Fatalf("destroy original pool generation: %v", err)
				}
			case "node cleanup":
				now, err := rdb.Time(ctx).Result()
				if err != nil {
					t.Fatalf("read Redis cleanup time: %v", err)
				}
				if err := rdb.HSet(ctx, rmapContentKey(node.resources.nodeKeepAlive),
					node.ID, strconv.FormatInt(now.Add(-2*node.workerTTL).UnixNano(), 10)).Err(); err != nil {
					t.Fatalf("set synthetic stale node heartbeat: %v", err)
				}
				cleaned, err := cleanupStalePoolNode(ctx, rdb, node.resources,
					node.ID, newNodeCleanupOwner("synthetic-cleanup"))
				if err != nil || !cleaned {
					t.Fatalf("cleanup stale node = (%v, %v), want completed cleanup", cleaned, err)
				}
				requireWorkerLifecycleState(t, ctx, rdb,
					"pulse:stream:"+node.nodeStream.Name+":lifecycle", node.nodeStream.Generation(), "destroyed")
			}
			hook.release()
			got := awaitWorkerLifecycleResult(t, ctx, result)
			if got.worker != nil || !errors.Is(got.err, ErrPoolGenerationLost) {
				t.Fatalf("AddWorker = (%v, %v), want no worker and ErrPoolGenerationLost", got.worker, got.err)
			}
			select {
			case <-node.closed:
			case <-ctx.Done():
				t.Fatalf("node did not retire automatically after %s: %v", loss, ctx.Err())
			}
			if !node.IsClosed() {
				t.Error("node closure channel completed without IsClosed")
			}
			requireNoWorkerLifecycleAdmission(t, node, registration.workerID)
			if after := workerLifecycleMaps(t, ctx, rdb, node); !reflect.DeepEqual(before, after) {
				t.Errorf("rejected registration changed worker maps or revisions: before %v, after %v", before, after)
			}
			requireWorkerLifecycleState(t, ctx, rdb, registration.lifecycleKey, original.Generation(), "destroyed")
		})
	}
}

func TestWorkerRegistrationRejectsReplacedStreamGeneration(t *testing.T) {
	ctx, rdb := newWorkerLifecycleRedis(t)
	node := newWorkerLifecycleNode(t, ctx, rdb)
	hook := newWorkerLifecycleHook(t, rdb, node)
	result := startWorkerLifecycleConstructor(ctx, node)
	registration := awaitWorkerLifecycleRegistration(t, ctx, hook)
	original := openWorkerLifecycleStream(t, ctx, rdb, registration)
	if err := original.Destroy(ctx); err != nil {
		t.Fatalf("destroy original worker stream: %v", err)
	}
	successor, err := streaming.NewStream(original.Name, rdb)
	if err != nil {
		t.Fatalf("create successor stream handle: %v", err)
	}
	if _, err := successor.Add(ctx, "synthetic-successor", []byte("synthetic payload")); err != nil {
		t.Fatalf("activate successor worker stream: %v", err)
	}
	if successor.Generation() == original.Generation() {
		t.Fatalf("successor generation = %q, want a new generation", successor.Generation())
	}
	before := workerLifecycleMaps(t, ctx, rdb, node)
	for index, entries := range before {
		if _, exists := entries[registration.workerID]; exists {
			t.Fatalf("worker already registered in map %d before the paused command", index)
		}
	}
	streamBefore := readWorkerLifecycleStream(t, ctx, rdb, registration.lifecycleKey)
	requireWorkerLifecycleState(t, ctx, rdb, registration.lifecycleKey, successor.Generation(), "active")

	hook.release()
	got := awaitWorkerLifecycleResult(t, ctx, result)
	if got.worker != nil || !redis.HasErrorPrefix(got.err, "STREAMDESTROYED") {
		t.Fatalf("AddWorker = (%v, %v), want the Redis STREAMDESTROYED registration rejection", got.worker, got.err)
	}
	if !errors.Is(got.err, streaming.ErrStreamDestroyed) {
		t.Errorf("old stream rollback = %v, want ErrStreamDestroyed", got.err)
	}
	requireNoWorkerLifecycleAdmission(t, node, registration.workerID)
	if after := workerLifecycleMaps(t, ctx, rdb, node); !reflect.DeepEqual(before, after) {
		t.Errorf("old registration changed worker maps or revisions: before %v, after %v", before, after)
	}
	if streamAfter := readWorkerLifecycleStream(t, ctx, rdb, registration.lifecycleKey); !reflect.DeepEqual(streamBefore, streamAfter) {
		t.Errorf("old constructor changed successor lifecycle or events: before %v, after %v", streamBefore, streamAfter)
	}
	if err := successor.Open(ctx); err != nil {
		t.Errorf("successor is no longer active: %v", err)
	}
	if err := successor.Destroy(ctx); err != nil {
		t.Errorf("destroy synthetic successor: %v", err)
	}
}

// newWorkerLifecycleRedis leases one database through the shared test helper.
// Each test has a deadline and flushes only the data in its leased database.
func newWorkerLifecycleRedis(t *testing.T) (context.Context, *redis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	rdb := ptesting.NewRedisClient(t)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := rdb.FlushDB(cleanupCtx).Err(); err != nil {
			t.Errorf("flush leased test database: %v", err)
		}
		if err := rdb.Close(); err != nil {
			t.Errorf("close leased test client: %v", err)
		}
	})
	return ctx, rdb
}

// newWorkerLifecycleNode starts a public pool node with heartbeat intervals
// longer than the test deadline, so only the test changes synthetic liveness.
func newWorkerLifecycleNode(t *testing.T, ctx context.Context, rdb *redis.Client) *Node {
	t.Helper()
	node, err := AddNode(ctx, t.Name(), rdb, WithLogger(pulse.NoopLogger()),
		WithWorkerTTL(time.Minute), WithJobSinkBlockDuration(testJobSinkBlockDuration))
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	t.Cleanup(func() {
		select {
		case <-node.closed:
			return
		default:
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- node.Shutdown(cleanupCtx)
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("shutdown test node: %v", err)
			}
		case <-cleanupCtx.Done():
			t.Errorf("shutdown test node did not finish: %v", cleanupCtx.Err())
		}
	})
	return node
}

// newWorkerLifecycleHook marks one constructor command for interruption and
// releases it during cleanup, including when an assertion stops the test.
func newWorkerLifecycleHook(t *testing.T, rdb *redis.Client, node *Node) *workerLifecycleHook {
	t.Helper()
	hook := &workerLifecycleHook{
		workersKey: rmapContentKey(node.resources.workers),
		observed:   make(chan workerLifecycleRegistration, 1),
		resume:     make(chan struct{}),
	}
	t.Cleanup(hook.release)
	rdb.AddHook(hook)
	return hook
}

// startWorkerLifecycleConstructor calls the public AddWorker method with a
// marked context and returns its result through a buffered channel.
func startWorkerLifecycleConstructor(ctx context.Context, node *Node) <-chan workerLifecycleResult {
	result := make(chan workerLifecycleResult, 1)
	go func() {
		worker, err := node.AddWorker(
			context.WithValue(ctx, workerLifecycleContextKey{}, true), newMockJobHandler())
		result <- workerLifecycleResult{worker: worker, err: err}
	}()
	return result
}

// awaitWorkerLifecycleRegistration returns the actual arguments intercepted
// during construction, or fails the test when its deadline expires.
func awaitWorkerLifecycleRegistration(
	t *testing.T, ctx context.Context, hook *workerLifecycleHook,
) workerLifecycleRegistration {
	t.Helper()
	select {
	case registration := <-hook.observed:
		return registration
	case <-ctx.Done():
		t.Fatalf("constructor did not reach registration: %v", ctx.Err())
		return workerLifecycleRegistration{}
	}
}

// awaitWorkerLifecycleResult returns the public constructor's outcome, or
// fails the test when construction does not finish before its deadline.
func awaitWorkerLifecycleResult(
	t *testing.T, ctx context.Context, result <-chan workerLifecycleResult,
) workerLifecycleResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-ctx.Done():
		t.Fatalf("AddWorker did not return before its deadline: %v", ctx.Err())
		return workerLifecycleResult{}
	}
}

// openWorkerLifecycleStream opens the stream named by the paused constructor
// and checks that its actual generation matches the registration arguments.
func openWorkerLifecycleStream(
	t *testing.T, ctx context.Context, rdb *redis.Client, registration workerLifecycleRegistration,
) *streaming.Stream {
	t.Helper()
	stream, err := streaming.NewStream(workerStreamName(registration.workerID), rdb)
	if err != nil {
		t.Fatalf("create original worker stream handle: %v", err)
	}
	if err := stream.Open(ctx); err != nil {
		t.Fatalf("open original worker stream: %v", err)
	}
	if stream.Generation() != registration.generation {
		t.Fatalf("original generation = %q, registration names %q", stream.Generation(), registration.generation)
	}
	return stream
}

// workerLifecycleMaps reads both complete worker hashes, including their
// revision fields, so a rejected constructor cannot hide a write and deletion.
func workerLifecycleMaps(
	t *testing.T, ctx context.Context, rdb *redis.Client, node *Node,
) [2]map[string]string {
	t.Helper()
	var snapshot [2]map[string]string
	for index, name := range []string{node.resources.workers, node.resources.workerKeepAlive} {
		entries, err := rdb.HGetAll(ctx, rmapContentKey(name)).Result()
		if err != nil {
			t.Fatalf("read worker map %q: %v", name, err)
		}
		snapshot[index] = entries
	}
	return snapshot
}

// readWorkerLifecycleStream reads the actual lifecycle and at most two events.
// The tests create one event, so missing or additional events fail immediately.
func readWorkerLifecycleStream(
	t *testing.T, ctx context.Context, rdb *redis.Client, key string,
) workerLifecycleStreamSnapshot {
	t.Helper()
	lifecycle, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("read stream lifecycle %q: %v", key, err)
	}
	physical := lifecycle["physical_key"]
	if physical == "" {
		t.Fatalf("stream lifecycle %q has no physical stream key", key)
	}
	events, err := rdb.XRangeN(ctx, physical, "-", "+", 2).Result()
	if err != nil || len(events) != 1 {
		t.Fatalf("read stream %q = (%v, %v), want one synthetic event", physical, events, err)
	}
	return workerLifecycleStreamSnapshot{lifecycle: lifecycle, events: events}
}

// requireWorkerLifecycleState checks the saved stream version and state so a
// failed old constructor cannot silently destroy a newer stream.
func requireWorkerLifecycleState(
	t *testing.T, ctx context.Context, rdb *redis.Client, key, generation, state string,
) {
	t.Helper()
	lifecycle, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("read stream lifecycle %q: %v", key, err)
	}
	if lifecycle["generation"] != generation || lifecycle["state"] != state {
		t.Errorf("stream lifecycle %q = %v, want generation %q and state %q", key, lifecycle, generation, state)
	}
}

// requireNoWorkerLifecycleAdmission checks both local records and the public
// worker list so failed construction cannot leave a worker available for jobs.
func requireNoWorkerLifecycleAdmission(t *testing.T, node *Node, workerID string) {
	t.Helper()
	if _, exists := node.localWorkers.Load(workerID); exists {
		t.Error("failed constructor admitted a local worker")
	}
	if _, exists := node.workerStreams.Load(workerID); exists {
		t.Error("failed constructor published a local worker stream")
	}
	if workers := node.Workers(); len(workers) != 0 {
		t.Errorf("public Workers returned %d workers after failed construction", len(workers))
	}
}

func (h *workerLifecycleHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook interrupts only the marked constructor's registration. A failed
// destroy matches that constructor's exact stream and generation; background
// Redis work and subsequent cleanup run normally.
func (h *workerLifecycleHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if ctx.Value(workerLifecycleContextKey{}) != true ||
			(cmd.Name() != "evalsha" && cmd.Name() != "eval") {
			return next(ctx, cmd)
		}
		args := cmd.Args()
		if h.destroyError != nil && len(args) == 13 && args[2] == 6 &&
			args[3] == h.registration.lifecycleKey && args[9] == "active" &&
			args[10] == h.registration.generation && args[11] == "destroyed" &&
			args[12] == "physical_key" {
			h.destroyAttempts.Add(1)
			return h.destroyError
		}
		if len(args) != 17 || args[2] != 8 || args[4] != h.workersKey {
			return next(ctx, cmd)
		}
		var err error
		if h.afterSave {
			err = next(ctx, cmd)
			if err != nil {
				return err
			}
		}
		if h.intercepted.CompareAndSwap(false, true) {
			workerID, workerOK := args[13].(string)
			createdAt, creationOK := args[14].(string)
			generation, generationOK := args[16].(string)
			lifecycleKey, keyOK := args[10].(string)
			if !workerOK || !creationOK || !generationOK || !keyOK {
				return fmt.Errorf("registration command contains unexpected argument types")
			}
			h.registration = workerLifecycleRegistration{
				workerID: workerID, createdAt: createdAt,
				generation: generation, lifecycleKey: lifecycleKey,
			}
			h.observed <- h.registration
			select {
			case <-h.resume:
			case <-ctx.Done():
				return ctx.Err()
			}
			if h.afterSave {
				return h.replyError
			}
		}
		if h.afterSave {
			return err
		}
		return next(ctx, cmd)
	}
}

func (h *workerLifecycleHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *workerLifecycleHook) release() {
	h.releaseOnce.Do(func() {
		close(h.resume)
	})
}
