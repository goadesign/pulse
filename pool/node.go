package pool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"
	"goa.design/clue/log"

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming"
	"goa.design/pulse/streaming/options"
)

type (
	// Node is a pool of workers.
	Node struct {
		ID                      string
		PoolName                string
		poolStream              *streaming.Stream // pool event stream for dispatching jobs
		poolSink                *streaming.Sink   // pool event sink
		nodeStream              *streaming.Stream // node event stream for receiving worker events
		nodeReader              *streaming.Reader // node event reader
		nodeKeepAliveMap        *rmap.Map         // node keep-alive timestamps indexed by ID
		nodeShutdownMap         *rmap.Map         // key is node ID that requested shutdown
		workerMap               *rmap.Map         // worker creation times by ID
		workerKeepAliveMap      *rmap.Map         // worker keep-alive timestamps indexed by ID
		workerCleanupMap        *rmap.Map         // key is stale worker ID that needs cleanup
		jobMap                  *rmap.Map         // jobs by worker ID
		jobPendingMap           *rmap.Map         // pending jobs by job key
		jobPayloadMap           *rmap.Map         // job payloads by job key
		tickerMap               *rmap.Map         // ticker next tick time indexed by name
		schedulerJobMap         *rmap.Map         // generation-fenced scheduler-owned jobs
		workerTTL               time.Duration     // Worker considered dead if keep-alive not updated after this duration
		requeueTimeout          time.Duration     // Bounds one local worker requeue handoff attempt
		dispatchTimeout         time.Duration
		dispatchResultRetention time.Duration
		recoveryGrace           time.Duration
		cleanupLease            time.Duration
		maxQueuedJobs           int
		clientOnly              bool
		logger                  pulse.Logger
		h                       hasher
		resources               poolResources
		stop                    chan struct{}  // closed when node is stopped
		closed                  chan struct{}  // closed when node is closed
		wg                      sync.WaitGroup // allows to wait until all goroutines exit
		scheduleCtx             context.Context
		scheduleCancel          context.CancelFunc
		scheduleWG              sync.WaitGroup
		settlements             *dispatchSettlements
		rdb                     *redis.Client

		localWorkers       sync.Map // workers created by this node
		workerStreams      sync.Map // worker streams indexed by ID
		nodeStreams        sync.Map // streams for worker acks indexed by ID
		pendingJobChannels sync.Map // dispatch nonce -> *dispatchWaiter
		pendingEvents      sync.Map // pending events indexed by sender and event IDs
		orphanedPayloads   sync.Map // job key -> first time observed orphaned payload (unix nanos)

		dispatchWaitersLock  sync.Mutex
		closeLock            sync.Mutex
		stopOnce             sync.Once
		shutdownOnce         sync.Once
		terminalOnce         sync.Once
		lock                 sync.RWMutex
		closing              bool
		closedState          bool
		shutdown             bool
		cleanupComplete      bool
		closeAfterCleanupErr error
	}

	// dispatchWaiter owns one admitted dispatch until its worker result arrives
	// or its persisted guard expires.
	dispatchWaiter struct {
		done chan struct{}
		once sync.Once
		refs atomic.Int64
	}

	// dispatchRecord is the Redis-owned publication and terminal outcome for
	// one globally unique dispatch ID.
	dispatchRecord struct {
		status  int64
		eventID string
		result  string
		err     string
	}

	// hasher is the interface implemented by types that can hash keys.
	hasher interface {
		Hash(key string, numBuckets int64) int64
	}

	// jumpHash implement Jump Consistent Hash.
	jumpHash struct {
		mu sync.Mutex
		h  hash.Hash64
	}
)

const (
	// shutdownErrorPoll bounds how quickly the initiator observes a peer's
	// authoritative close failure.
	shutdownErrorPoll = 100 * time.Millisecond
	// evInit is the event used to initialize a node or worker stream.
	evInit string = "i"
	// evStartJob is the event used to send new job to workers.
	evStartJob string = "j"
	// evMessage is the event used to send a keyed message to a hash-ring worker.
	evMessage string = "m"
	// evNotify is the event used to notify a worker running a specific job.
	evNotify string = "n"
	// evStopJob is the event used to stop a job.
	evStopJob string = "s"
	// evAck is the worker event used to ack a pool event.
	evAck string = "a"
)

// pendingEventTTL is the TTL for pending events.
var pendingEventTTL = 2 * time.Minute

var (
	// ErrJobExists is returned when attempting to dispatch a job with a key that already exists.
	ErrJobExists = errors.New("job already exists")
	// ErrPoolCapacity is returned when active pending dispatches have reached
	// the generation's immutable MaxQueuedJobs contract.
	ErrPoolCapacity = errors.New("pool dispatch capacity reached")
	// ErrDispatchConflict is returned when a dispatch ID is reused with
	// different exact job key or payload bytes.
	ErrDispatchConflict = errors.New("pool dispatch idempotency conflict")
	// ErrPoolGenerationLost is returned when a node mutates an inactive pool.
	ErrPoolGenerationLost = errors.New("pool generation lost")
	// ErrPoolConfigMismatch is returned when node configuration differs from
	// the active generation's immutable configuration.
	ErrPoolConfigMismatch = errors.New("pool configuration mismatch")
	// ErrQuiescenceRequired is returned when legacy resources prove that old
	// pool writers are still active during a hard upgrade.
	ErrQuiescenceRequired = errors.New("pool quiescence required")

	errJobAwaitingOwner = errors.New("job awaiting active owner")
	errJobNotFound      = errors.New("job not found")

	// registerPoolNodeScript linearizes node registration against shutdown and
	// final cleanup, verifies the exact pool incarnation, and publishes the
	// keep-alive map update in the same operation.
	registerPoolNodeScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3] or
   redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
if redis.call("HGET", KEYS[3], ARGV[4]) then
    return {2}
end
for _, state in ipairs(redis.call("HVALS", KEYS[5])) do
    if state == "finishing" then
        return {0}
    end
end
if redis.call("HEXISTS", KEYS[2], "shutdown") == 1 then
    return {0}
end
local now = redis.call("TIME")
local timestamp = now[1] .. string.format("%06d", now[2]) .. "000"
redis.call("HSET", KEYS[3], ARGV[1], timestamp)
local rev = tostring(redis.call("HINCRBY", KEYS[3], "=rev", 1))
redis.call("HSET", KEYS[3], "=kind", "set")
local msg = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[1]), ARGV[1],
    string.len(timestamp), timestamp,
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[4], "set:" .. msg)
return {1, timestamp}
`)

	// refreshPoolNodeScript renews an existing node through shutdown but
	// rejects a stale-node cleanup fence or removed registration.
	refreshPoolNodeScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3] or
   redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
if redis.call("HGET", KEYS[2], ARGV[4])
or not redis.call("HGET", KEYS[2], ARGV[1]) then
    return redis.error_reply("NODECLEANUPLOST")
end
local now = redis.call("TIME")
local timestamp = now[1] .. string.format("%06d", now[2]) .. "000"
redis.call("HSET", KEYS[2], ARGV[1], timestamp)
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "set")
local msg = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[1]), ARGV[1],
    string.len(timestamp), timestamp,
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[3], "set:" .. msg)
return timestamp
`)

	// publishPoolShutdownScript records the distributed shutdown obligation and
	// emits the rmap wire notification without depending on a local map handle
	// that a concurrent Close may already have closed.
	publishPoolShutdownScript = redis.NewScript(`
local key = "shutdown"
local value = ARGV[1]
redis.call("HSET", KEYS[1], key, value)
local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
redis.call("HSET", KEYS[1], "=kind", "set")
local msg = struct.pack("ic0ic0ic0", string.len(key), key, string.len(value), value, string.len(rev), rev)
redis.call("PUBLISH", KEYS[2], "set:" .. msg)
return rev
`)
)

// AddNode adds a new node to the pool with the given name and returns it. The
// node can be used to dispatch jobs and add new workers. A node also routes
// dispatched jobs to the proper worker and acks the corresponding events once
// the worker acks the job.
//
// The options WithClientOnly can be used to create a node that can only be used
// to dispatch jobs. Such a node does not route or process jobs in the
// background.
func AddNode(ctx context.Context, poolName string, rdb *redis.Client, opts ...NodeOption) (_ *Node, resultErr error) {
	o := parseOptions(opts...)
	if err := validateNodeOptions(o); err != nil {
		return nil, fmt.Errorf("AddNode: %w", err)
	}
	logger := o.logger
	nodeID := ulid.Make().String()
	if logger == nil {
		logger = pulse.NoopLogger()
	} else {
		logger = logger.WithPrefix("pool", poolName, "node", nodeID)
	}
	logger.Info("options",
		"client_only", o.clientOnly,
		"max_queued_jobs", o.maxQueuedJobs,
		"worker_ttl", o.workerTTL,
		"worker_requeue_timeout", o.requeueTimeout,
		"dispatch_timeout", o.dispatchTimeout,
		"dispatch_result_retention", o.dispatchResultRetention,
		"recovery_grace", o.recoveryGrace,
		"cleanup_lease", o.cleanupLease)

	poolStream, err := streaming.NewStream(poolStreamName(poolName), rdb,
		options.WithUnboundedStream(),
		options.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to create pool job stream %q: %w", poolStreamName(poolName), err)
	}
	if err := resumeExpiredPoolCleanup(ctx, poolName, nodeID, o.cleanupLease, rdb); err != nil {
		return nil, fmt.Errorf("AddNode: %w", err)
	}
	if err := poolStream.Open(ctx); err != nil {
		return nil, fmt.Errorf("AddNode: failed to open pool job stream %q: %w", poolStreamName(poolName), err)
	}
	resources, err := establishPoolResources(
		ctx,
		rdb,
		poolName,
		poolStream.Generation(),
		o.maxQueuedJobs,
		o.workerTTL,
		o.cleanupLease,
		o.dispatchResultRetention,
	)
	if err != nil {
		return nil, fmt.Errorf("AddNode: %w", err)
	}
	nsm, err := rmap.Join(ctx, resources.nodeShutdown, rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to join shutdown replicated map %q: %w", resources.nodeShutdown, err)
	}
	shutdownUpdates := nsm.Subscribe()
	if nsm.Len() > 0 {
		nsm.Unsubscribe(shutdownUpdates)
		nsm.Close()
		return nil, fmt.Errorf("AddNode: pool %q is shutting down", poolName)
	}

	nkm, err := rmap.Join(ctx, resources.nodeKeepAlive, rdb, rmap.WithLogger(logger))
	if err != nil {
		nsm.Unsubscribe(shutdownUpdates)
		nsm.Close()
		return nil, fmt.Errorf("AddNode: failed to join node keep-alive map %q: %w", resources.nodeKeepAlive, err)
	}
	registered, _, err := registerPoolNode(ctx, rdb, resources, nodeID)
	if err != nil {
		nkm.Close()
		nsm.Unsubscribe(shutdownUpdates)
		nsm.Close()
		return nil, fmt.Errorf("AddNode: failed to register node: %w", err)
	}
	if !registered {
		nkm.Close()
		nsm.Unsubscribe(shutdownUpdates)
		nsm.Close()
		return nil, fmt.Errorf("AddNode: pool %q is shutting down", poolName)
	}
	var (
		wm   *rmap.Map
		jm   *rmap.Map
		jpm  *rmap.Map
		jpem *rmap.Map
		wkm  *rmap.Map
		tm   *rmap.Map
		sjm  *rmap.Map
		wcm  *rmap.Map

		poolSink         *streaming.Sink
		nodeStream       *streaming.Stream
		nodeReader       *streaming.Reader
		nodeStreamActive bool
		setupComplete    bool
	)
	// Registration is the first externally visible setup step. Every later
	// failure unwinds local resources and that registration in reverse order.
	defer func() {
		if setupComplete {
			return
		}
		var rollbackErr error
		if nodeReader != nil {
			nodeReader.Close()
		}
		if nodeStreamActive {
			if err := nodeStream.Destroy(context.WithoutCancel(ctx)); err != nil {
				rollbackErr = errors.Join(rollbackErr, fmt.Errorf("destroy node stream: %w", err))
			}
		}
		if poolSink != nil {
			if err := poolSink.Close(context.WithoutCancel(ctx)); err != nil {
				rollbackErr = errors.Join(rollbackErr, fmt.Errorf("close pool sink: %w", err))
			}
		}
		for _, m := range []*rmap.Map{jpem, wcm, sjm, tm, wkm, jpm, jm, wm} {
			if m != nil {
				m.Close()
			}
		}
		if _, err := nkm.Delete(context.WithoutCancel(ctx), nodeID); err != nil {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("remove node registration: %w", err))
		}
		nkm.Close()
		nsm.Unsubscribe(shutdownUpdates)
		nsm.Close()
		if rollbackErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("AddNode: rollback failed: %w", rollbackErr))
		}
	}()
	registrationCtx, stopRegistrationLease := context.WithCancel(ctx)
	registrationLeaseDone := make(chan struct{})
	pulse.Go(logger, func() {
		maintainNodeRegistrationLease(registrationCtx, rdb, resources, nodeID, o.workerTTL, logger)
		close(registrationLeaseDone)
	})
	defer func() {
		stopRegistrationLease()
		<-registrationLeaseDone
	}()

	if !o.clientOnly {
		wm, err = rmap.Join(ctx, resources.workers, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool workers replicated map %q: %w", resources.workers, err)
		}
		workerIDs := wm.Keys()
		logger.Info("joined", "workers", workerIDs)

		jm, err = rmap.Join(ctx, resources.jobs, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool jobs replicated map %q: %w", resources.jobs, err)
		}

		jpm, err = rmap.Join(ctx, resources.jobPayloads, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool job payloads replicated map %q: %w", resources.jobPayloads, err)
		}

		wkm, err = rmap.Join(ctx, resources.workerKeepAlive, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join worker keep-alive replicated map %q: %w", resources.workerKeepAlive, err)
		}

		tm, err = rmap.Join(ctx, resources.tickers, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool ticker replicated map %q: %w", resources.tickers, err)
		}

		sjm, err = rmap.Join(ctx, resources.schedulerJobs, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join scheduler jobs replicated map %q: %w", resources.schedulerJobs, err)
		}

		wcm, err = rmap.Join(ctx, resources.workerCleanup, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool cleanup replicated map %q: %w", resources.workerCleanup, err)
		}

		// Initialize and join pending jobs map
		jpem, err = rmap.Join(ctx, resources.jobPending, rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pending jobs replicated map %q: %w", resources.jobPending, err)
		}

		poolSink, err = poolStream.NewSink(ctx, "events",
			options.WithSinkBlockDuration(o.jobSinkBlockDuration),
			options.WithSinkAckGracePeriod(o.recoveryGrace))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to create events sink for stream %q: %w", poolStreamName(poolName), err)
		}
	}

	nodeStream, err = streaming.NewStream(nodeStreamName(poolName, nodeID), rdb, options.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to create node event stream %q: %w", nodeStreamName(poolName, nodeID), err)
	}
	nodeStreamActive = true
	if _, err = nodeStream.Add(ctx, evInit, []byte(nodeID)); err != nil {
		return nil, fmt.Errorf("AddNode: failed to add init event to node event stream %q: %w", nodeStreamName(poolName, nodeID), err)
	}

	nodeReader, err = nodeStream.NewReader(ctx, options.WithReaderBlockDuration(o.jobSinkBlockDuration), options.WithReaderStartAtOldest())
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to create node event reader for stream %q: %w", nodeStreamName(poolName, nodeID), err)
	}

	scheduleCtx, scheduleCancel := context.WithCancel(context.Background())
	p := &Node{
		ID:                      nodeID,
		PoolName:                poolName,
		nodeKeepAliveMap:        nkm,
		nodeShutdownMap:         nsm,
		workerMap:               wm,
		workerKeepAliveMap:      wkm,
		workerCleanupMap:        wcm,
		jobMap:                  jm,
		jobPayloadMap:           jpm,
		jobPendingMap:           jpem,
		tickerMap:               tm,
		schedulerJobMap:         sjm,
		workerStreams:           sync.Map{},
		nodeStreams:             sync.Map{},
		pendingJobChannels:      sync.Map{},
		pendingEvents:           sync.Map{},
		poolStream:              poolStream,
		poolSink:                poolSink,
		nodeStream:              nodeStream,
		nodeReader:              nodeReader,
		clientOnly:              o.clientOnly,
		workerTTL:               resources.workerTTL,
		requeueTimeout:          o.requeueTimeout,
		dispatchTimeout:         o.dispatchTimeout,
		dispatchResultRetention: o.dispatchResultRetention,
		recoveryGrace:           o.recoveryGrace,
		cleanupLease:            o.cleanupLease,
		maxQueuedJobs:           o.maxQueuedJobs,
		resources:               resources,
		h:                       &jumpHash{h: crc64.New(crc64.MakeTable(crc64.ECMA))},
		stop:                    make(chan struct{}),
		closed:                  make(chan struct{}),
		scheduleCtx:             scheduleCtx,
		scheduleCancel:          scheduleCancel,
		settlements:             newDispatchSettlements(),
		rdb:                     rdb,
		logger:                  logger,
	}

	nch := nodeReader.Subscribe()

	// Preserve the caller's logging context for background goroutines.
	logCtx := context.Background()
	logCtx = log.WithContext(logCtx, ctx)

	if o.clientOnly {
		logger.Info("client-only")
		p.wg.Add(4)
		pulse.Go(logger, func() { p.handleNodeEvents(nch) }) // to handle job acks
		pulse.Go(logger, func() { p.watchShutdown(logCtx, shutdownUpdates) })
		pulse.Go(logger, func() { p.processInactiveNodes() })
		pulse.Go(logger, func() { p.updateNodeKeepAlive() })
	} else {
		p.wg.Add(7) // Increment for all background goroutines
		pulse.Go(logger, func() { p.handlePoolEvents(poolSink.Subscribe()) })
		pulse.Go(logger, func() { p.handleNodeEvents(nch) })
		pulse.Go(logger, func() { p.watchWorkers(logCtx) })
		pulse.Go(logger, func() { p.watchShutdown(logCtx, shutdownUpdates) })
		pulse.Go(logger, func() { p.processInactiveNodes() })
		pulse.Go(logger, func() { p.processInactiveWorkers(logCtx) })
		pulse.Go(logger, func() { p.updateNodeKeepAlive() })
	}

	shuttingDown, err := rdb.HExists(ctx, rmapContentKey(resources.nodeShutdown), "shutdown").Result()
	if err != nil {
		if closeErr := p.close(ctx, true); closeErr != nil {
			return nil, errors.Join(
				fmt.Errorf("AddNode: failed post-registration shutdown check: %w", err),
				fmt.Errorf("AddNode: failed to close node after shutdown check: %w", closeErr),
			)
		}
		return nil, fmt.Errorf("AddNode: failed post-registration shutdown check: %w", err)
	}
	if shuttingDown {
		p.ownShutdown(logCtx)
	}

	setupComplete = true
	return p, nil
}

// AddWorker adds a new worker to the pool and returns it. The worker starts
// processing jobs immediately. handler can optionally implement the
// NotificationHandler and MessageHandler interfaces to handle job-scoped
// notifications and hash-routed messages.
func (node *Node) AddWorker(ctx context.Context, handler JobHandler) (*Worker, error) {
	node.lock.RLock()
	defer node.lock.RUnlock()
	if node.closing {
		return nil, fmt.Errorf("AddWorker: pool %q is closed", node.PoolName)
	}
	if node.clientOnly {
		return nil, fmt.Errorf("AddWorker: pool %q is client-only", node.PoolName)
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		return nil, fmt.Errorf("AddWorker: %w", err)
	}
	w, err := newWorker(ctx, node, handler)
	if err != nil {
		return nil, err
	}
	node.localWorkers.Store(w.ID, w)
	node.workerStreams.Store(w.ID, w.stream)
	return w, nil
}

// RemoveWorker stops the worker, removes it from the pool and requeues all its
// jobs.
func (node *Node) RemoveWorker(ctx context.Context, w *Worker) error {
	node.lock.RLock()
	defer node.lock.RUnlock()
	if node.closing {
		return fmt.Errorf("RemoveWorker: pool %q is closed", node.PoolName)
	}
	if err := w.stop(ctx); err != nil {
		return fmt.Errorf("RemoveWorker: failed to stop worker %q: %w", w.ID, err)
	}
	if err := node.settlements.waitWorker(ctx, w.ID); err != nil {
		return fmt.Errorf("RemoveWorker: terminal outcomes for worker %q remain unsettled: %w", w.ID, err)
	}
	if err := w.requeueJobs(ctx); err != nil {
		return fmt.Errorf("RemoveWorker: failed to requeue jobs for worker %q: %w", w.ID, err)
	}
	if err := node.removeWorker(ctx, w.ID); err != nil {
		return fmt.Errorf("RemoveWorker: failed to remove worker %q: %w", w.ID, err)
	}
	node.localWorkers.Delete(w.ID)
	node.logger.Info("removed worker", "worker", w.ID)
	return nil
}

// Workers returns the list of workers running in the local node.
func (node *Node) Workers() []*Worker {
	var workers []*Worker
	node.localWorkers.Range(func(key, value any) bool {
		w := value.(*Worker)
		workers = append(workers, &Worker{
			ID:        w.ID,
			CreatedAt: w.CreatedAt,
		})
		return true
	})
	return workers
}

// PoolWorkers returns the list of workers running in the entire pool.
func (node *Node) PoolWorkers() []*Worker {
	workers := node.workerMap.Map()
	poolWorkers := make([]*Worker, 0, len(workers))
	for id, createdAt := range workers {
		cat, err := strconv.ParseInt(createdAt, 10, 64)
		if err != nil {
			node.logger.Error(fmt.Errorf("PoolWorkers: failed to parse createdAt %q for worker %q: %w", createdAt, id, err))
			continue
		}
		poolWorkers = append(poolWorkers, &Worker{ID: id, CreatedAt: time.Unix(0, cat)})
	}
	return poolWorkers
}

// DispatchJob dispatches a job to the worker in the pool that is assigned to
// the job key using consistent hashing.
// It returns:
// - nil if the job is successfully dispatched and started by a worker
// - ErrJobExists if a job with the same key already exists in the pool
// - an error returned by the worker's start handler if the job fails to start
// - an error if the pool is closed or if there's a failure in adding the job
//
// The method blocks until one of the above conditions is met.
func (node *Node) DispatchJob(ctx context.Context, key string, payload []byte) error {
	_, err := node.DispatchJobOnce(ctx, ulid.Make().String(), key, payload)
	return err
}

// DispatchJobOnce atomically publishes one start event for dispatchID and
// waits for its worker result. Retrying the same dispatchID returns the same
// Redis event ID and never starts a duplicate handler.
func (node *Node) DispatchJobOnce(
	ctx context.Context,
	dispatchID, key string,
	payload []byte,
) (string, error) {
	node.lock.RLock()
	defer node.lock.RUnlock()
	if node.closing {
		return "", fmt.Errorf("DispatchJob: pool %q is closed", node.PoolName)
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		return "", fmt.Errorf("DispatchJob: %w", err)
	}
	if dispatchID == "" {
		return "", fmt.Errorf("DispatchJob: dispatch ID cannot be empty")
	}
	job := &Job{
		Key:        key,
		Payload:    payload,
		CreatedAt:  time.Now(),
		NodeID:     node.ID,
		dispatchID: dispatchID,
	}
	return node.dispatchJob(ctx, dispatchID, key, job)
}

// DispatchMessage sends a keyed message to the worker currently assigned by the
// pool hash ring. Messages do not create job ownership and are intended for
// fire-and-forget work that should be load-balanced by key.
func (node *Node) DispatchMessage(ctx context.Context, key string, payload []byte) error {
	node.lock.RLock()
	defer node.lock.RUnlock()
	if node.closing {
		return fmt.Errorf("DispatchMessage: pool %q is closed", node.PoolName)
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		return fmt.Errorf("DispatchMessage: %w", err)
	}
	if _, err := node.poolStream.Add(ctx, evMessage, marshalKeyedPayload(key, payload)); err != nil {
		return fmt.Errorf("DispatchMessage: failed to add message to stream %q: %w", node.poolStream.Name, err)
	}
	node.logger.Info("message dispatched", "key", key)
	return nil
}

func (node *Node) dispatchJob(ctx context.Context, dispatchID, key string, job *Job) (string, error) {
	waiter := node.acquireDispatchWaiter(dispatchID)
	defer node.releaseDispatchWaiter(dispatchID, waiter)

	record, err := node.publishDispatchRecord(ctx, key, dispatchID, job.Payload, marshalJob(job))
	if err != nil {
		return "", err
	}
	if record.status == dispatchTerminal {
		return record.eventID, dispatchTerminalError(record)
	}
	identity, err := dispatchIdentity(key, job.Payload)
	if err != nil {
		return record.eventID, err
	}
	record, err = node.awaitDispatch(ctx, waiter, dispatchID, identity, record)
	if err != nil {
		node.logger.Error(fmt.Errorf("DispatchJob: failed to dispatch job: %w", err), "key", key)
		return record.eventID, err
	}
	node.logger.Info("dispatched", "key", key)
	return record.eventID, dispatchTerminalError(record)
}

// awaitDispatch treats local completion as a wake-up hint and Redis as the
// authoritative terminal state. Polling is bounded by DispatchTimeout and each
// wake, cancellation, or timeout edge performs one final durable read.
func (node *Node) awaitDispatch(
	ctx context.Context,
	waiter *dispatchWaiter,
	dispatchID string,
	identity []byte,
	record dispatchRecord,
) (dispatchRecord, error) {
	poll := min(50*time.Millisecond, node.dispatchTimeout)
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	timer := time.NewTimer(node.dispatchTimeout)
	defer timer.Stop()
	for {
		var edgeErr error
		select {
		case <-waiter.done:
		case <-ticker.C:
		case <-timer.C:
			edgeErr = fmt.Errorf(
				"DispatchJob: dispatch %q timed out after %v",
				dispatchID,
				node.dispatchTimeout,
			)
		case <-ctx.Done():
			edgeErr = ctx.Err()
		}
		var (
			current dispatchRecord
			err     error
		)
		if edgeErr != nil {
			current, err = node.readDispatchRecordAfterEdge(ctx, dispatchID, identity)
		} else {
			current, err = node.readDispatchRecord(ctx, dispatchID, identity)
		}
		if err != nil {
			return record, err
		}
		record = current
		if record.status == dispatchTerminal {
			return record, nil
		}
		if edgeErr != nil {
			return record, edgeErr
		}
	}
}

// readDispatchRecordAfterEdge gives cancellation one bounded authoritative
// Redis read so a concurrently committed terminal result wins the race.
func (node *Node) readDispatchRecordAfterEdge(
	ctx context.Context,
	dispatchID string,
	identity []byte,
) (dispatchRecord, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
	defer cancel()
	return node.readDispatchRecord(readCtx, dispatchID, identity)
}

// acquireDispatchWaiter joins all local callers for one dispatch ID to the
// same completion broadcast without racing the final caller's removal.
func (node *Node) acquireDispatchWaiter(dispatchID string) *dispatchWaiter {
	node.dispatchWaitersLock.Lock()
	defer node.dispatchWaitersLock.Unlock()
	candidate := &dispatchWaiter{done: make(chan struct{})}
	value, _ := node.pendingJobChannels.LoadOrStore(dispatchID, candidate)
	waiter := value.(*dispatchWaiter)
	waiter.refs.Add(1)
	return waiter
}

// releaseDispatchWaiter removes the local completion broadcast only after the
// final joined caller has stopped waiting.
func (node *Node) releaseDispatchWaiter(dispatchID string, waiter *dispatchWaiter) {
	node.dispatchWaitersLock.Lock()
	defer node.dispatchWaitersLock.Unlock()
	if waiter.refs.Add(-1) == 0 {
		node.pendingJobChannels.CompareAndDelete(dispatchID, waiter)
	}
}

// publishDispatch atomically admits and appends one generation-fenced start
// event and durable exact-identity record.
func (node *Node) publishDispatchRecord(
	ctx context.Context,
	key, dispatchID string,
	jobPayload, eventPayload []byte,
) (dispatchRecord, error) {
	if key == "" {
		return dispatchRecord{}, fmt.Errorf("DispatchJob: job key cannot be empty")
	}
	if strings.Contains(key, "=") {
		return dispatchRecord{}, fmt.Errorf("DispatchJob: job key %q cannot contain '='", key)
	}
	identity, err := dispatchIdentity(key, jobPayload)
	if err != nil {
		return dispatchRecord{}, err
	}
	raw, err := luaDispatchJob.Run(ctx, node.rdb, []string{
		fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
		rmapContentKey(node.resources.jobPayloads),
		rmapContentKey(node.resources.jobPending),
		rmapUpdateChannel(node.resources.jobPending),
		dispatchRecordKey(node.resources.dispatches, dispatchID),
		dispatchActiveKey(node.resources.dispatches),
		rmapContentKey(node.resources.nodeKeepAlive),
	},
		key,
		dispatchID,
		"active",
		node.resources.generation,
		node.maxQueuedJobs,
		evStartJob,
		eventPayload,
		"physical_key",
		identity,
		node.ID,
		nodeCleanupField(node.ID),
	).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "DISPATCHIDEMPOTENCYCONFLICT") {
			return dispatchRecord{}, fmt.Errorf("%w: dispatch %q", ErrDispatchConflict, dispatchID)
		}
		return dispatchRecord{}, fmt.Errorf("DispatchJob: failed to claim job %q: %w", key, poolBoundaryError(err))
	}
	record, err := parseDispatchRecord(raw)
	if err != nil {
		return dispatchRecord{}, fmt.Errorf("DispatchJob: failed to parse claim result for job %q: %w", key, err)
	}
	switch record.status {
	case dispatchClaimed, dispatchTerminal:
		return record, nil
	case dispatchAlreadyPending:
		node.logger.Info("DispatchJob: job already dispatched", "key", key)
		return dispatchRecord{}, fmt.Errorf("%w: job %q is already dispatched", ErrJobExists, key)
	case dispatchAlreadyRunning:
		node.logger.Info("DispatchJob: job already exists", "key", key)
		return dispatchRecord{}, fmt.Errorf("%w: job %q", ErrJobExists, key)
	case dispatchCapacityReached:
		return dispatchRecord{}, fmt.Errorf("%w: maximum %d pending jobs", ErrPoolCapacity, node.maxQueuedJobs)
	default:
		return dispatchRecord{}, fmt.Errorf("DispatchJob: unexpected claim status %d for job %q", record.status, key)
	}
}

// publishDispatch admits a marshaled job for focused package tests.
func (node *Node) publishDispatch(
	ctx context.Context,
	key, dispatchID string,
	eventPayload []byte,
) (string, error) {
	job, err := unmarshalJob(eventPayload)
	if err != nil {
		return "", err
	}
	record, err := node.publishDispatchRecord(ctx, key, dispatchID, job.Payload, eventPayload)
	return record.eventID, err
}

// settleDispatch durably records one terminal result and settles its exact
// stream event. Exact retries return the original immutable outcome.
func (node *Node) settleDispatch(ctx context.Context, key, dispatchID string, resultErr error) (dispatchRecord, error) {
	errorText := ""
	if resultErr != nil {
		errorText = resultErr.Error()
	}
	raw, err := luaSettleDispatch.Run(ctx, node.rdb, []string{
		fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
		rmapContentKey(node.resources.jobPending),
		rmapUpdateChannel(node.resources.jobPending),
		dispatchRecordKey(node.resources.dispatches, dispatchID),
		dispatchActiveKey(node.resources.dispatches),
	}, key, dispatchID, "active", node.resources.generation, "physical_key",
		"", errorText, "events", node.dispatchResultRetention.Milliseconds()).Result()
	if err != nil {
		return dispatchRecord{}, fmt.Errorf("settle dispatch %q: %w", key, poolBoundaryError(err))
	}
	record, err := parseTerminalDispatch(raw)
	if err != nil {
		return dispatchRecord{}, fmt.Errorf("settle dispatch %q: %w", key, err)
	}
	node.dispatchWaitersLock.Lock()
	if value, ok := node.pendingJobChannels.Load(dispatchID); ok {
		waiter := value.(*dispatchWaiter)
		waiter.once.Do(func() {
			close(waiter.done)
		})
	}
	node.dispatchWaitersLock.Unlock()
	return record, nil
}

// readDispatchRecord returns the exact Redis-owned status for one dispatch.
func (node *Node) readDispatchRecord(
	ctx context.Context,
	dispatchID string,
	identity []byte,
) (dispatchRecord, error) {
	raw, err := readDispatchRecordScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			dispatchRecordKey(node.resources.dispatches, dispatchID),
		},
		"active",
		node.resources.generation,
		identity,
	).Result()
	if err != nil {
		switch {
		case redis.HasErrorPrefix(err, "DISPATCHIDEMPOTENCYCONFLICT"):
			return dispatchRecord{}, fmt.Errorf("%w: dispatch %q", ErrDispatchConflict, dispatchID)
		case redis.HasErrorPrefix(err, "DISPATCHRECORDNOTFOUND"):
			return dispatchRecord{}, fmt.Errorf(
				"DispatchJob: durable record for dispatch %q is unavailable",
				dispatchID,
			)
		default:
			return dispatchRecord{}, fmt.Errorf(
				"DispatchJob: read durable dispatch %q: %w",
				dispatchID,
				poolBoundaryError(err),
			)
		}
	}
	record, err := parseDispatchRecord(raw)
	if err != nil {
		return dispatchRecord{}, fmt.Errorf("DispatchJob: parse durable dispatch %q: %w", dispatchID, err)
	}
	return record, nil
}

// completeDispatch settles a successful dispatch for focused package tests.
func (node *Node) completeDispatch(ctx context.Context, key, dispatchID string) error {
	_, err := node.settleDispatch(ctx, key, dispatchID, nil)
	return err
}

// parseDispatchRecord validates the durable admission result boundary.
func parseDispatchRecord(raw any) (dispatchRecord, error) {
	values, ok := raw.([]any)
	if !ok || len(values) != 4 {
		return dispatchRecord{}, fmt.Errorf("invalid dispatch result %T", raw)
	}
	status, ok := values[0].(int64)
	if !ok {
		return dispatchRecord{}, fmt.Errorf("invalid dispatch status %T", values[0])
	}
	decoded := make([]string, 3)
	for i := range decoded {
		value, ok := values[i+1].(string)
		if !ok {
			return dispatchRecord{}, fmt.Errorf("invalid dispatch field %d type %T", i, values[i+1])
		}
		decoded[i] = value
	}
	return dispatchRecord{status: status, eventID: decoded[0], result: decoded[1], err: decoded[2]}, nil
}

// parseTerminalDispatch validates an atomic settlement result.
func parseTerminalDispatch(raw any) (dispatchRecord, error) {
	values, ok := raw.([]any)
	if !ok || len(values) != 3 {
		return dispatchRecord{}, fmt.Errorf("invalid terminal dispatch result %T", raw)
	}
	decoded := make([]string, len(values))
	for i, value := range values {
		text, ok := value.(string)
		if !ok {
			return dispatchRecord{}, fmt.Errorf("invalid terminal dispatch field %d type %T", i, value)
		}
		decoded[i] = text
	}
	return dispatchRecord{
		status:  dispatchTerminal,
		eventID: decoded[0],
		result:  decoded[1],
		err:     decoded[2],
	}, nil
}

// dispatchTerminalError reconstructs the public terminal error text persisted
// by the worker settlement owner.
func dispatchTerminalError(record dispatchRecord) error {
	if record.err == "" {
		return nil
	}
	return errors.New(record.err)
}

// dispatchIdentity length-prefixes exact key and payload bytes.
func dispatchIdentity(key string, payload []byte) ([]byte, error) {
	var identity bytes.Buffer
	for _, field := range [][]byte{[]byte(key), payload} {
		if err := binary.Write(&identity, binary.BigEndian, uint64(len(field))); err != nil {
			return nil, fmt.Errorf("encode dispatch identity: %w", err)
		}
		if _, err := identity.Write(field); err != nil {
			return nil, fmt.Errorf("encode dispatch identity: %w", err)
		}
	}
	return identity.Bytes(), nil
}

// dispatchRecordToken produces a collision-free Redis hash field namespace.
func dispatchRecordToken(dispatchID string) string {
	return hex.EncodeToString([]byte(dispatchID))
}

// dispatchRecordKey returns one generation-qualified per-dispatch Redis hash.
func dispatchRecordKey(resource, dispatchID string) string {
	return fmt.Sprintf(
		"pulse:pool:dispatch:%s:%s",
		hex.EncodeToString([]byte(resource)),
		dispatchRecordToken(dispatchID),
	)
}

// dispatchActiveKey indexes only unsettled records for exact cleanup.
func dispatchActiveKey(resource string) string {
	return fmt.Sprintf("pulse:pool:dispatch:%s:active", hex.EncodeToString([]byte(resource)))
}

// poolBoundaryError maps Redis-owned pool contracts to typed public errors.
func poolBoundaryError(err error) error {
	switch {
	case redis.HasErrorPrefix(err, "POOLGENERATIONLOST"):
		return fmt.Errorf("%w: %v", ErrPoolGenerationLost, err)
	case redis.HasErrorPrefix(err, "NODECLEANUPLOST"):
		return fmt.Errorf("%w: %v", ErrPoolGenerationLost, err)
	case redis.HasErrorPrefix(err, "POOLCONFIGMISMATCH"):
		return fmt.Errorf("%w: %v", ErrPoolConfigMismatch, err)
	case redis.HasErrorPrefix(err, "POOLQUIESCENCEREQUIRED"):
		return fmt.Errorf("%w: %v", ErrQuiescenceRequired, err)
	default:
		return err
	}
}

// StopJob durably publishes a stop request for the job with the given key.
// Success means Redis accepted the request, not that the handler has completed.
func (node *Node) StopJob(ctx context.Context, key string) error {
	node.lock.RLock()
	defer node.lock.RUnlock()
	if node.closing {
		return fmt.Errorf("StopJob: pool %q is closed", node.PoolName)
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		return fmt.Errorf("StopJob: %w", err)
	}
	if _, err := node.poolStream.Add(ctx, evStopJob, marshalJobKey(key)); err != nil {
		return fmt.Errorf("StopJob: failed to add stop job to stream %q: %w", node.poolStream.Name, err)
	}
	node.logger.Info("stop requested", "key", key)
	return nil
}

// JobKeys returns the list of keys of the jobs running in the pool.
func (node *Node) JobKeys() []string {
	var jobKeys []string
	for workerID := range node.jobMap.Map() {
		keys, ok := node.jobMap.GetValues(workerID)
		if !ok {
			continue
		}
		jobKeys = append(jobKeys, keys...)
	}
	return jobKeys
}

// JobPayload returns the payload of the job with the given key.
// It returns:
// - (payload, true) if the job exists and has a payload
// - (nil, true) if the job exists but has an empty payload
// - (nil, false) if the job does not exist
func (node *Node) JobPayload(key string) ([]byte, bool) {
	payload, ok := node.jobPayloadMap.Get(key)
	if !ok {
		return nil, false
	}
	if payload == "" {
		return nil, true
	}
	return []byte(payload), true
}

// NotifyWorker notifies the worker that currently owns the job with the given
// key.
func (node *Node) NotifyWorker(ctx context.Context, key string, payload []byte) error {
	node.lock.RLock()
	defer node.lock.RUnlock()
	if node.closing {
		return fmt.Errorf("NotifyWorker: pool %q is closed", node.PoolName)
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		return fmt.Errorf("NotifyWorker: %w", err)
	}
	if _, err := node.poolStream.Add(ctx, evNotify, marshalKeyedPayload(key, payload)); err != nil {
		return fmt.Errorf("NotifyWorker: failed to add notification to stream %q: %w", node.poolStream.Name, err)
	}
	node.logger.Info("notification sent", "key", key)
	return nil
}

// ensureGenerationActive fences every node-owned mutation against the exact
// pool stream incarnation. Cleanup invalidates the stream before deleting its
// maps, so a successful preflight can only race with deletion that follows it;
// once deletion completes, stale nodes fail before recreating any old key.
func (node *Node) ensureGenerationActive(ctx context.Context) error {
	err := node.poolStream.Open(ctx)
	if err == nil {
		_, refreshErr := refreshPoolNode(ctx, node.rdb, node.resources, node.ID)
		if refreshErr == nil {
			return nil
		}
		if strings.Contains(refreshErr.Error(), "NODECLEANUPLOST") {
			node.stopAfterLifecycleLoss("stale node cleanup fence", false)
		}
		return fmt.Errorf("%w: %v", ErrPoolGenerationLost, refreshErr)
	}
	if errors.Is(err, streaming.ErrStreamDestroyed) {
		node.stopAfterLifecycleLoss("pool generation ended", true)
	}
	return fmt.Errorf("%w: %v", ErrPoolGenerationLost, err)
}

// stopAfterLifecycleLoss immediately fences local admission and asynchronously
// closes the node after a Redis-owned terminal lifecycle transition.
func (node *Node) stopAfterLifecycleLoss(reason string, shutdown bool) {
	node.lock.Lock()
	closing := node.closing
	node.closing = true
	node.lock.Unlock()
	if closing {
		return
	}
	node.terminalOnce.Do(func() {
		pulse.Go(node.logger, func() {
			if closeErr := node.closeAfterDistributedLoss(context.Background(), shutdown); closeErr != nil {
				node.logger.Error(fmt.Errorf("stop node after %s: %w", reason, closeErr))
			}
		})
	})
}

// Shutdown stops the pool workers gracefully across all nodes. It notifies all
// workers and waits until they are completed. Shutdown prevents the pool nodes
// from creating new workers and the pool workers from accepting new jobs. After
// Shutdown returns, the node object cannot be used anymore and should be
// discarded. One of Shutdown or Close should be called before the node is
// garbage collected unless it is client-only.
func (node *Node) Shutdown(ctx context.Context) error {
	if node.clientOnly {
		return fmt.Errorf("Shutdown: client-only node cannot shutdown worker pool")
	}
	node.lock.RLock()
	cleanupComplete := node.cleanupComplete
	node.lock.RUnlock()
	if cleanupComplete {
		return node.closeAfterCleanup(ctx)
	}
	cleanupComplete, err := node.poolCleanupComplete(ctx)
	if err != nil {
		return err
	}
	if cleanupComplete {
		node.lock.Lock()
		node.cleanupComplete = true
		node.lock.Unlock()
		return node.closeAfterCleanup(ctx)
	}
	// Publish through Redis directly because concurrent Close may already have
	// closed the local shutdown-map replica. The obligation must exist before
	// this caller joins or resumes the distributed barrier.
	if err := node.publishShutdown(ctx); err != nil {
		return err
	}
	if err := node.close(ctx, true); err != nil {
		return fmt.Errorf("Shutdown: failed to close local node: %w", err)
	}
	if err := node.waitForPoolNodes(ctx); err != nil {
		return err
	}
	if err := node.cleanupPool(ctx); err != nil {
		return err
	}

	node.lock.Lock()
	node.cleanupComplete = true
	node.shutdown = true
	node.lock.Unlock()
	node.logger.Info("shutdown")
	return nil
}

// publishShutdown durably records this node's pool-wide shutdown obligation.
func (node *Node) publishShutdown(ctx context.Context) error {
	err := publishPoolShutdownScript.Run(
		ctx,
		node.rdb,
		[]string{
			rmapContentKey(node.resources.nodeShutdown),
			rmapUpdateChannel(node.resources.nodeShutdown),
		},
		node.ID,
	).Err()
	if err != nil {
		return fmt.Errorf("Shutdown: failed to publish distributed shutdown obligation: %w", err)
	}
	return nil
}

// poolCleanupComplete reads this pool-stream generation's durable completion
// marker. Local closure and distributed cleanup completion are separate states.
func (node *Node) poolCleanupComplete(ctx context.Context) (bool, error) {
	values, err := node.rdb.HMGet(
		ctx,
		poolCleanupGenerationsKey(node.PoolName),
		"state",
		"generation",
	).Result()
	if err != nil {
		return false, fmt.Errorf("Shutdown: failed to read pool cleanup completion: %w", err)
	}
	state, _ := values[0].(string)
	generation, _ := values[1].(string)
	return state == poolCleanupCompleteState && generation == node.poolStream.Generation(), nil
}

// registerPoolNode reserves and publishes the node's authoritative lease only
// while its exact pool generation is active and shutdown/final cleanup are
// absent.
func registerPoolNode(
	ctx context.Context,
	rdb *redis.Client,
	resources poolResources,
	nodeID string,
) (bool, string, error) {
	result, err := registerPoolNodeScript.Run(
		ctx,
		rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", poolStreamName(resources.pool)),
			rmapContentKey(resources.nodeShutdown),
			rmapContentKey(resources.nodeKeepAlive),
			rmapUpdateChannel(resources.nodeKeepAlive),
			poolCleanupGenerationsKey(resources.pool),
		},
		nodeID,
		resources.generation,
		"active",
		nodeCleanupField(nodeID),
	).Slice()
	if err != nil {
		return false, "", err
	}
	if len(result) == 0 {
		return false, "", fmt.Errorf("registration script returned no status")
	}
	status, ok := result[0].(int64)
	if !ok {
		return false, "", fmt.Errorf("registration script returned invalid status %T", result[0])
	}
	if status == 0 {
		return false, "", nil
	}
	if status == 2 {
		return false, "", errors.New("NODECLEANUPLOST")
	}
	if len(result) != 2 {
		return false, "", fmt.Errorf("registration script returned %d values", len(result))
	}
	timestamp, ok := result[1].(string)
	if !ok {
		return false, "", fmt.Errorf("registration script returned invalid timestamp %T", result[1])
	}
	return true, timestamp, nil
}

// refreshPoolNode renews an existing node heartbeat without reopening
// admission during shutdown.
func refreshPoolNode(
	ctx context.Context,
	rdb *redis.Client,
	resources poolResources,
	nodeID string,
) (string, error) {
	timestamp, err := refreshPoolNodeScript.Run(
		ctx,
		rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", poolStreamName(resources.pool)),
			rmapContentKey(resources.nodeKeepAlive),
			rmapUpdateChannel(resources.nodeKeepAlive),
		},
		nodeID,
		resources.generation,
		"active",
		nodeCleanupField(nodeID),
	).Text()
	return timestamp, err
}

// maintainNodeRegistrationLease keeps a node visible to the shutdown barrier
// while AddNode constructs its streams and maps. The regular node heartbeat is
// running before this temporary lease owner is stopped.
func maintainNodeRegistrationLease(
	ctx context.Context,
	rdb *redis.Client,
	resources poolResources,
	nodeID string,
	ttl time.Duration,
	logger pulse.Logger,
) {
	ticker := time.NewTicker(ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			registered, _, err := registerPoolNode(ctx, rdb, resources, nodeID)
			if err != nil {
				logger.Error(fmt.Errorf("AddNode: failed to refresh registration time: %w", err))
				continue
			}
			if !registered {
				logger.Error(fmt.Errorf("AddNode: pool generation stopped accepting registrations"))
				return
			}
		}
	}
}

// Close immediately rejects new node work, stops local workers, requeues their
// jobs, and detaches the node's Redis-owned resources. It does not close the
// caller-owned Redis client or stop workers in other nodes. A distributed
// detach failure is returned without marking the node closed, so Close may be
// retried with a fresh context. One of Shutdown or Close should be called
// before the node is garbage collected unless it is client-only.
func (node *Node) Close(ctx context.Context) error {
	node.lock.RLock()
	cleanupComplete := node.cleanupComplete
	node.lock.RUnlock()
	if cleanupComplete {
		return node.closeAfterCleanup(ctx)
	}
	cleanupComplete, err := node.poolCleanupComplete(ctx)
	if err != nil {
		return err
	}
	if cleanupComplete {
		node.lock.Lock()
		node.cleanupComplete = true
		node.lock.Unlock()
		return node.closeAfterCleanup(ctx)
	}
	return node.close(ctx, false)
}

// IsShutdown returns true if the pool is shutdown.
func (node *Node) IsShutdown() bool {
	node.lock.RLock()
	defer node.lock.RUnlock()
	return node.shutdown
}

// IsClosed returns true if the node is closed.
func (node *Node) IsClosed() bool {
	node.lock.RLock()
	defer node.lock.RUnlock()
	return node.closedState
}

// close stops the node and its workers, optionally requeuing jobs. If shutdown
// is true, jobs are not requeued as the pool is being shutdown. Otherwise, jobs
// are requeued to be picked up by other nodes. The method stops all workers,
// waits for background goroutines to complete, cleans up resources and closes
// connections. It is idempotent and can be called multiple times safely.
func (node *Node) close(ctx context.Context, shutdown bool) error {
	node.closeLock.Lock()
	defer node.closeLock.Unlock()

	node.lock.Lock()
	if node.closedState {
		node.lock.Unlock()
		return nil
	}
	if !node.closing {
		node.closing = true
	}
	node.lock.Unlock()

	node.scheduleCancel()
	node.scheduleWG.Wait()

	var stopJobsErr error
	if shutdown {
		stopJobsErr = node.stopAllJobs(ctx)
	}

	var workerStopErr error
	var workerStopLock sync.Mutex
	var workerStopWait sync.WaitGroup
	node.localWorkers.Range(func(_, value any) bool {
		worker := value.(*Worker)
		workerStopWait.Add(1)
		pulse.Go(node.logger, func() {
			defer workerStopWait.Done()
			if err := worker.stop(ctx); err != nil {
				workerStopLock.Lock()
				workerStopErr = errors.Join(workerStopErr, err)
				workerStopLock.Unlock()
			}
		})
		return true
	})
	workerStopWait.Wait()

	node.stopOnce.Do(func() {
		close(node.stop)
	})
	node.wg.Wait()
	var localTeardownErr error
	if stopJobsErr != nil {
		localTeardownErr = errors.Join(
			localTeardownErr,
			fmt.Errorf("close: failed to stop jobs: %w", stopJobsErr),
		)
	}
	if workerStopErr != nil {
		localTeardownErr = errors.Join(
			localTeardownErr,
			fmt.Errorf("close: failed to stop workers: %w", workerStopErr),
		)
	}
	if err := node.settlements.waitAll(ctx); err != nil {
		localTeardownErr = errors.Join(
			localTeardownErr,
			fmt.Errorf("close: terminal dispatch outcomes remain unsettled: %w", err),
		)
	}
	if localTeardownErr != nil {
		return localTeardownErr
	}

	// Requeue and distributed worker cleanup are retried on every Close attempt.
	// A worker remains locally discoverable until all of its map records are
	// removed, so a failed attempt cannot hide incomplete cleanup.
	if !shutdown {
		if err := node.requeueAllJobs(ctx); err != nil {
			return fmt.Errorf("close: failed to requeue jobs: %w", err)
		}
	}
	var workerCleanupErr error
	node.localWorkers.Range(func(key, value any) bool {
		worker := value.(*Worker)
		if err := node.removeWorker(ctx, worker.ID); err != nil {
			workerCleanupErr = errors.Join(workerCleanupErr, err)
			return true
		}
		node.localWorkers.Delete(key)
		return true
	})
	if workerCleanupErr != nil {
		return fmt.Errorf("close: failed to remove local workers: %w", workerCleanupErr)
	}

	// Detach distributed membership before closing the maps needed to retry it.
	// Local goroutines were stopped above on the first attempt, so subsequent
	// calls repeat only these idempotent distributed side effects.
	if node.poolSink != nil {
		if err := node.poolSink.Close(ctx); err != nil {
			return fmt.Errorf("close: pending distributed cleanup: failed to detach pool sink: %w", err)
		}
	}
	node.pendingEvents.Range(func(key, _ any) bool {
		node.pendingEvents.Delete(key)
		return true
	})
	node.pendingJobChannels.Range(func(key, _ any) bool {
		node.pendingJobChannels.Delete(key)
		return true
	})
	if _, err := node.nodeKeepAliveMap.Delete(ctx, node.ID); err != nil {
		return fmt.Errorf("close: pending distributed cleanup: failed to detach node from pool: %w", err)
	}

	// Local stream destruction is part of distributed cleanup. Keep the maps
	// open until it succeeds so Close can retry truthfully.
	if err := node.cleanupNode(ctx); err != nil {
		return fmt.Errorf("close: pending distributed cleanup: %w", err)
	}

	// Publish closure and shutdown ownership atomically after all node-owned
	// side effects complete.
	node.lock.Lock()
	node.closedState = true
	if shutdown {
		node.shutdown = true
	}
	node.lock.Unlock()
	close(node.closed)
	node.logger.Info("closed")
	return nil
}

// closeAfterCleanup performs every local teardown obligation after another
// process has already destroyed the distributed pool generation. No Redis
// resource deletion is attempted, but local intake, schedules, workers,
// readers, settlement goroutines, maps, and public closure state are joined.
func (node *Node) closeAfterCleanup(ctx context.Context) error {
	return node.closeAfterDistributedLoss(ctx, true)
}

// closeAfterDistributedLoss performs complete local teardown after Redis has
// fenced this node or destroyed its pool generation.
func (node *Node) closeAfterDistributedLoss(ctx context.Context, shutdown bool) error {
	node.closeLock.Lock()
	defer node.closeLock.Unlock()

	node.lock.Lock()
	if node.closedState {
		node.shutdown = node.shutdown || shutdown
		err := node.closeAfterCleanupErr
		node.lock.Unlock()
		return err
	}
	node.closing = true
	node.lock.Unlock()

	node.scheduleCancel()
	node.scheduleWG.Wait()
	node.localWorkers.Range(func(key, value any) bool {
		value.(*Worker).stopLocal()
		node.localWorkers.Delete(key)
		return true
	})
	node.stopOnce.Do(func() {
		close(node.stop)
	})
	if node.nodeReader != nil {
		node.nodeReader.Close()
	}
	node.wg.Wait()

	var teardownErr error
	if err := node.settlements.waitAll(ctx); err != nil {
		teardownErr = errors.Join(
			teardownErr,
			fmt.Errorf("close after distributed cleanup: local terminal settlements: %w", err),
		)
	}
	if node.poolSink != nil {
		if err := node.poolSink.Close(ctx); err != nil {
			teardownErr = errors.Join(teardownErr, fmt.Errorf("close local pool sink: %w", err))
		}
	}
	for _, m := range node.maps() {
		if m != nil {
			m.Close()
		}
	}
	node.pendingEvents.Range(func(key, _ any) bool {
		node.pendingEvents.Delete(key)
		return true
	})
	node.pendingJobChannels.Range(func(key, _ any) bool {
		node.pendingJobChannels.Delete(key)
		return true
	})

	node.lock.Lock()
	node.closedState = true
	node.shutdown = shutdown
	node.closeAfterCleanupErr = teardownErr
	node.lock.Unlock()
	close(node.closed)
	node.logger.Info("closed after distributed lifecycle loss")
	return teardownErr
}

// stopAllJobs stops all jobs running on the node.
func (node *Node) stopAllJobs(ctx context.Context) error {
	var wg sync.WaitGroup
	var total atomic.Int32
	var stopErr error
	var errLock sync.Mutex
	node.localWorkers.Range(func(key, value any) bool {
		wg.Add(1)
		worker := value.(*Worker)
		pulse.Go(node.logger, func() {
			defer wg.Done()
			for _, job := range worker.Jobs() {
				if err := worker.stopJob(ctx, job.Key); err != nil {
					node.logger.Error(fmt.Errorf("Close: failed to stop job %q for worker %q: %w", job.Key, worker.ID, err))
					errLock.Lock()
					stopErr = errors.Join(stopErr, err)
					errLock.Unlock()
				}
				total.Add(1)
			}
		})
		return true
	})
	wg.Wait()
	node.logger.Info("stopped all jobs", "total", total.Load())
	return stopErr
}

// handlePoolEvents reads events from the pool job stream.
func (node *Node) handlePoolEvents(c <-chan *streaming.Event) {
	defer node.wg.Done()

	for {
		select {
		case ev, ok := <-c:
			if !ok {
				return
			}
			if err := node.routeWorkerEvent(ev); err != nil {
				node.logger.Error(fmt.Errorf("handlePoolEvents: failed to route event: %w", err))
			}
		case <-node.stop:
			return
		}
	}
}

// routeWorkerEvent routes a dispatched event to the proper worker.
func (node *Node) routeWorkerEvent(ev *streaming.Event) error {
	// Filter out stale events
	now, err := node.rdb.Time(context.Background()).Result()
	if err != nil {
		return fmt.Errorf("routeWorkerEvent: read Redis time: %w", err)
	}
	age := now.Sub(ev.CreatedAt())
	if age > pendingEventTTL && ev.EventName != evStartJob {
		node.logger.Debug("routeWorkerEvent: stale event, not routing", "event", ev.EventName, "id", ev.ID, "since", age, "TTL", pendingEventTTL)
		settled, err := node.releaseTerminalDispatch(ev, errors.New("pool event expired before routing"))
		if err != nil {
			node.logger.Error(err, "event", ev.EventName, "id", ev.ID)
		}
		// Ack the sink event so it does not get redelivered.
		if !settled {
			err = node.settlePoolEvent(context.Background(), ev)
		}
		if err != nil {
			node.logger.Error(fmt.Errorf("routeWorkerEvent: failed to ack event: %w", err), "event", ev.EventName, "id", ev.ID)
		}
		return nil
	}

	// Compute the worker ID that will handle the event key.
	key, err := poolEventKey(ev)
	if err != nil {
		node.logger.Error(fmt.Errorf("routeWorkerEvent: malformed event: %w", err), "event", ev.EventName, "id", ev.ID)
		if ackErr := node.settlePoolEvent(context.Background(), ev); ackErr != nil {
			return fmt.Errorf("routeWorkerEvent: acknowledge malformed event %s: %w", ev.ID, ackErr)
		}
		return nil
	}
	wid, err := node.workerForEvent(ev.EventName, key)
	if err != nil {
		if errors.Is(err, errJobAwaitingOwner) {
			node.logger.Debug("routeWorkerEvent: job has no active owner yet", "event", ev.EventName, "id", ev.ID, "key", key)
			return nil
		}
		if errors.Is(err, errJobNotFound) {
			if ackErr := node.settlePoolEvent(context.Background(), ev); ackErr != nil {
				node.logger.Error(fmt.Errorf("routeWorkerEvent: failed to ack event for missing job: %w", ackErr), "event", ev.EventName, "id", ev.ID)
			}
			return nil
		}
		return err
	}

	// Stream the event to the worker corresponding to the key hash.
	stream, err := node.getWorkerStream(wid)
	if err != nil {
		return err
	}
	eventID, err := stream.Add(context.Background(), ev.EventName, marshalEnvelope(node.ID, ev.Payload), options.WithOnlyIfStreamExists())
	if err != nil {
		return fmt.Errorf("routeWorkerEvent: failed to add event %s to worker stream %q: %w", ev.EventName, workerStreamName(wid), err)
	}
	if eventID == "" {
		settled, settleErr := node.releaseTerminalDispatch(ev, errors.New("no worker accepted dispatch"))
		if settleErr != nil {
			return settleErr
		}
		if !settled {
			if err := node.settlePoolEvent(context.Background(), ev); err != nil {
				return fmt.Errorf("routeWorkerEvent: failed to acknowledge unroutable event %s: %w", ev.ID, err)
			}
		}
		return nil
	}
	node.logger.Debug("routed", "event", ev.EventName, "id", ev.ID, "worker", wid, "worker-event-id", eventID)

	// Record the event in the pending events map for future ack.
	node.pendingEvents.Store(pendingEventKey(wid, eventID), ev)

	return nil
}

// releaseTerminalDispatch clears singleton admission when routing has
// definitively discarded a start event before any worker could process it.
func (node *Node) releaseTerminalDispatch(event *streaming.Event, cause error) (bool, error) {
	if event.EventName != evStartJob {
		return false, nil
	}
	job, err := unmarshalJob(event.Payload)
	if err != nil {
		return false, fmt.Errorf("release terminal dispatch: decode start job: %w", err)
	}
	if job.dispatchID != "" {
		_, err := node.settleDispatch(context.Background(), job.Key, job.dispatchID, cause)
		return err == nil, err
	}
	return false, nil
}

// poolEventKey decodes the exact wire shape selected by the event kind and
// rejects trailing or incompatible data before routing.
func poolEventKey(event *streaming.Event) (string, error) {
	switch event.EventName {
	case evStartJob:
		job, err := unmarshalJob(event.Payload)
		if err != nil {
			return "", fmt.Errorf("decode start job: %w", err)
		}
		return job.Key, nil
	case evMessage, evNotify:
		key, _, err := unmarshalKeyedPayload(event.Payload)
		if err != nil {
			return "", fmt.Errorf("decode keyed event: %w", err)
		}
		return key, nil
	case evStopJob:
		key, err := unmarshalJobKey(event.Payload)
		if err != nil {
			return "", fmt.Errorf("decode stop job: %w", err)
		}
		return key, nil
	default:
		return "", fmt.Errorf("unknown pool event %q", event.EventName)
	}
}

// handleNodeEvents reads events from the node event stream and acks the pending
// events that correspond to jobs that are now running or done.
func (node *Node) handleNodeEvents(c <-chan *streaming.Event) {
	defer node.wg.Done()

	for {
		select {
		case ev, ok := <-c:
			if !ok {
				return
			}
			node.processNodeEvent(ev)
		case <-node.stop:
			node.nodeReader.Close()
			return
		}
	}
}

// processNodeEvent processes a node event.
func (node *Node) processNodeEvent(ev *streaming.Event) {
	switch ev.EventName {
	case evInit:
		// Event sent by pool node to initialize the node event stream.
		node.logger.Debug("handleNodeEvents: received init node", "event", ev.EventName, "id", ev.ID)
	case evAck:
		// Event sent by worker to ack a dispatched job.
		node.logger.Debug("handleNodeEvents: received ack", "event", ev.EventName, "id", ev.ID)
		node.ackWorkerEvent(ev)
	}
}

// ackWorkerEvent removes the routing node's local tracking after the worker has
// durably settled the corresponding pool event.
func (node *Node) ackWorkerEvent(ev *streaming.Event) {
	workerID, payload, err := unmarshalEnvelope(ev.Payload)
	if err != nil {
		node.dropMalformedNodeEvent(ev, fmt.Errorf("decode worker acknowledgement envelope: %w", err))
		return
	}
	ack, err := unmarshalAck(payload)
	if err != nil {
		node.dropMalformedNodeEvent(ev, fmt.Errorf("decode worker acknowledgement: %w", err))
		return
	}
	key := pendingEventKey(workerID, ack.EventID)
	val, ok := node.pendingEvents.Load(key)
	if !ok {
		node.logger.Error(fmt.Errorf("ackWorkerEvent: received unknown event %s from worker %s", ack.EventID, workerID))
		return
	}
	pending := val.(*streaming.Event)
	ctx := context.Background()
	dispatchSettled := false

	// If a dispatched job then send a return event to the node that
	// dispatched the job.
	if pending.EventName == evStartJob {
		job, err := unmarshalJob(pending.Payload)
		if err != nil {
			node.logger.Error(fmt.Errorf(
				"ackWorkerEvent: decode pending start event %s: %w",
				pending.ID,
				err,
			))
			if ackErr := node.settlePoolEvent(context.Background(), pending); ackErr != nil {
				node.logger.Error(fmt.Errorf("ackWorkerEvent: drop malformed pending event: %w", ackErr))
				return
			}
			node.pendingEvents.Delete(key)
			return
		}
		if !job.Requeued {
			ack.JobKey = job.Key
			if job.dispatchID != "" {
				var resultErr error
				if ack.Error != "" {
					resultErr = errors.New(ack.Error)
				}
				if _, err := node.settleDispatch(ctx, job.Key, job.dispatchID, resultErr); err != nil {
					node.logger.Error(err)
					return
				}
				dispatchSettled = true
			}
		}
	}

	// Ack the sink event so it does not get redelivered.
	if !dispatchSettled {
		err = node.settlePoolEvent(ctx, pending)
	}
	if err != nil {
		node.logger.Error(fmt.Errorf("ackWorkerEvent: failed to ack event: %w", err), "event", pending.EventName, "id", pending.ID)
		return
	}
	node.pendingEvents.Delete(key)
}

// settlePoolEvent acknowledges and deletes one terminal pool-stream event.
// The unbounded pool stream therefore retains only unsettled work.
func (node *Node) settlePoolEvent(ctx context.Context, event *streaming.Event) error {
	if err := node.poolSink.Ack(ctx, event); err != nil {
		return err
	}
	if err := node.poolStream.Remove(ctx, event.ID); err != nil {
		return fmt.Errorf("delete settled pool event %s: %w", event.ID, err)
	}
	return nil
}

// dropMalformedNodeEvent logs and removes a poison entry so the permanent node
// reader remains live across restarts.
func (node *Node) dropMalformedNodeEvent(event *streaming.Event, decodeErr error) {
	node.logger.Error(decodeErr, "event", event.EventName, "id", event.ID)
	if err := node.nodeStream.Remove(context.Background(), event.ID); err != nil {
		node.logger.Error(fmt.Errorf("drop malformed node event %s: %w", event.ID, err))
	}
}

// workerForEvent returns the worker that should receive a pool event. Start and
// message events are routed by the current consistent hash ring; stop and
// notification events target the worker that currently owns the job.
func (node *Node) workerForEvent(eventName, key string) (string, error) {
	if eventName == evStartJob || eventName == evMessage {
		activeWorkers := node.activeWorkers()
		if len(activeWorkers) == 0 {
			return "", fmt.Errorf("routeWorkerEvent: no active worker in pool %q", node.PoolName)
		}
		return activeWorkers[node.h.Hash(key, int64(len(activeWorkers)))], nil
	}
	if eventName == evStopJob || eventName == evNotify {
		owner, ok, err := node.activeJobOwner(key)
		if err != nil {
			return "", err
		}
		if !ok {
			exists, err := node.jobPayloadExists(context.Background(), key)
			if err != nil {
				return "", err
			}
			if exists {
				return "", fmt.Errorf("%w: %q", errJobAwaitingOwner, key)
			}
			return "", fmt.Errorf("%w: %q", errJobNotFound, key)
		}
		return owner, nil
	}
	return "", fmt.Errorf("routeWorkerEvent: unknown worker event %q", eventName)
}

// jobPayloadExists reads the durable job record from Redis, which is the source
// of truth when the local ownership map has no active owner during handoff.
func (node *Node) jobPayloadExists(ctx context.Context, key string) (bool, error) {
	exists, err := node.rdb.HExists(ctx, rmapContentKey(node.resources.jobPayloads), key).Result()
	if err != nil {
		return false, fmt.Errorf("routeWorkerEvent: failed to check job payload %q: %w", key, err)
	}
	return exists, nil
}

// activeJobOwner returns the single active worker that owns a job key according
// to the replicated ownership map.
func (node *Node) activeJobOwner(key string) (string, bool, error) {
	activeWorkers := node.activeWorkers()
	active := make(map[string]struct{}, len(activeWorkers))
	for _, workerID := range activeWorkers {
		active[workerID] = struct{}{}
	}

	var owner string
	for workerID := range node.jobMap.Map() {
		if _, ok := active[workerID]; !ok {
			continue
		}
		keys, ok := node.jobMap.GetValues(workerID)
		if !ok {
			continue
		}
		for _, ownedKey := range keys {
			if ownedKey != key {
				continue
			}
			if owner != "" {
				return "", false, fmt.Errorf("routeWorkerEvent: job %q has multiple active owners", key)
			}
			owner = workerID
			break
		}
	}
	if owner == "" {
		return "", false, nil
	}
	return owner, true, nil
}

// watches monitors the workers replicated map and triggers job rebalancing
// when workers are added or removed from the pool.
func (node *Node) watchWorkers(ctx context.Context) {
	defer node.wg.Done()
	for {
		select {
		case <-node.stop:
			return
		case <-node.workerMap.Subscribe():
			node.logger.Debug("watchWorkers: worker map updated")
			node.handleWorkerMapUpdate(ctx)
		}
	}
}

// handleWorkerMapUpdate is called when the worker map is updated.
func (node *Node) handleWorkerMapUpdate(ctx context.Context) {
	if node.IsClosed() {
		return
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		node.logger.Error(err)
		return
	}
	// First cleanup the local workers that are no longer active.
	node.localWorkers.Range(func(key, value any) bool {
		worker := value.(*Worker)
		if _, ok := node.workerMap.Get(worker.ID); !ok {
			// If it's not in the worker map, then it's not active and its jobs
			// have already been requeued.
			node.logger.Info("handleWorkerMapUpdate: removing inactive local worker", "worker", worker.ID)
			if err := node.deleteWorker(worker.ID); err != nil {
				node.logger.Error(fmt.Errorf("handleWorkerMapUpdate: failed to delete inactive worker %q: %w", worker.ID, err), "worker", worker.ID)
			}
			if err := worker.stop(ctx); err != nil {
				node.logger.Error(fmt.Errorf("handleWorkerMapUpdate: failed to stop inactive worker %q: %w", worker.ID, err))
				return true
			}
			node.localWorkers.Delete(key)
			return true
		}
		return true
	})

	// Then rebalance the jobs across the remaining active workers.
	activeWorkers := node.activeWorkers()
	if len(activeWorkers) == 0 {
		return
	}
	node.localWorkers.Range(func(key, value any) bool {
		worker := value.(*Worker)
		worker.rebalance(ctx, activeWorkers)
		return true
	})
}

// watchShutdown monitors the subscription established before node registration,
// so a shutdown concurrent with AddNode cannot be missed.
func (node *Node) watchShutdown(ctx context.Context, updates <-chan rmap.EventKind) {
	defer node.wg.Done()
	defer node.nodeShutdownMap.Unsubscribe(updates)
	for {
		select {
		case <-node.stop:
			return
		case _, ok := <-updates:
			if !ok {
				return
			}
			if _, shutdown := node.nodeShutdownMap.Get("shutdown"); !shutdown {
				continue
			}
			node.logger.Debug("watchShutdown: shutdown map updated")
			node.ownShutdown(ctx)
		}
	}
}

// ownShutdown starts exactly one peer-shutdown owner.
func (node *Node) ownShutdown(ctx context.Context) {
	node.shutdownOnce.Do(func() {
		pulse.Go(node.logger, func() { node.handleShutdown(ctx) })
	})
}

// handleShutdown retries local cleanup within one node lease and publishes each
// failure in the shutdown map so the initiating node can return it immediately.
func (node *Node) handleShutdown(ctx context.Context) {
	requestingNode, _ := node.nodeShutdownMap.Get("shutdown")
	node.logger.Debug("handleShutdown: shutting down", "requested-by", requestingNode)
	failureKey := shutdownErrorKey(node.ID)
	deadline := time.NewTimer(node.workerTTL)
	defer deadline.Stop()
	for {
		err := node.close(ctx, true)
		if err == nil {
			node.lock.Lock()
			node.shutdown = true
			node.lock.Unlock()
			if err := node.rdb.HDel(
				ctx,
				rmapContentKey(node.resources.nodeShutdown),
				failureKey,
			).Err(); err != nil {
				node.logger.Error(fmt.Errorf("handleShutdown: failed to clear shutdown error: %w", err))
				if waitForShutdownRetry(ctx, deadline.C, node.workerTTL) {
					continue
				}
				return
			}
			node.logger.Info("shutdown", "requested-by", requestingNode)
			return
		}
		if setErr := node.setPoolMap(ctx, node.resources.nodeShutdown, failureKey, err.Error()); setErr != nil {
			node.logger.Error(fmt.Errorf("handleShutdown: failed to publish shutdown error: %w", setErr))
		}
		node.logger.Error(fmt.Errorf("handleShutdown: failed to close node: %w", err))
		if !waitForShutdownRetry(ctx, deadline.C, node.workerTTL) {
			return
		}
	}
}

// waitForShutdownRetry spaces peer cleanup attempts while respecting the
// caller context and the node lease deadline.
func waitForShutdownRetry(ctx context.Context, deadline <-chan time.Time, ttl time.Duration) bool {
	retry := time.NewTimer(min(100*time.Millisecond, ttl))
	defer retry.Stop()
	select {
	case <-retry.C:
		return true
	case <-deadline:
		return false
	case <-ctx.Done():
		return false
	}
}

// processInactiveNodes periodically checks for inactive nodes and destroys their streams.
func (node *Node) processInactiveNodes() {
	defer node.wg.Done()
	ticker := time.NewTicker(node.workerTTL)
	defer ticker.Stop()

	for {
		select {
		case <-node.stop:
			return
		case <-ticker.C:
			node.cleanupInactiveNodes()
		}
	}
}

// cleanupInactiveNodes checks for inactive nodes, destroys their streams and
// removes them from the keep-alive map.
func (node *Node) cleanupInactiveNodes() {
	if err := node.ensureGenerationActive(context.Background()); err != nil {
		node.logger.Error(err)
		return
	}
	for nodeID := range node.nodeKeepAliveMap.Map() {
		if nodeID == node.ID || strings.HasPrefix(nodeID, "=") {
			continue
		}
		ctx := context.Background()
		cleaned, err := cleanupStalePoolNode(
			ctx,
			node.rdb,
			node.resources,
			nodeID,
			node.ID,
		)
		if err != nil {
			node.logger.Error(fmt.Errorf("cleanupInactiveNodes: failed to clean node: %w", err))
			continue
		}
		if cleaned {
			node.logger.Info("cleaned up inactive node", "node", nodeID)
		}
	}
}

// processInactiveWorkers periodically cleans up inactive workers.
func (node *Node) processInactiveWorkers(ctx context.Context) {
	defer node.wg.Done()
	ticker := time.NewTicker(node.workerTTL)
	defer ticker.Stop()

	for {
		select {
		case <-node.stop:
			return
		case <-ticker.C:
			node.cleanupInactiveWorkers(ctx)
		}
	}
}

// cleanupInactiveWorkers ensures all jobs are assigned to active workers by performing
// two types of cleanup:
//  1. Orphaned jobs: finds and requeues jobs assigned to workers that no longer exist
//     in the keep-alive map, which can happen if a worker was improperly terminated
//  2. Inactive workers: finds workers that haven't updated their keep-alive timestamp
//     within workerTTL duration and requeues their jobs
//
// The cleanup process is distributed and idempotent - multiple nodes can attempt
// cleanup concurrently, but only one will succeed for each worker due to cleanup
// lock acquisition. Jobs are requeued and will be reassigned to active workers
// through consistent hashing.
func (node *Node) cleanupInactiveWorkers(ctx context.Context) {
	if err := node.ensureGenerationActive(ctx); err != nil {
		node.logger.Error(err)
		return
	}
	// Discovery may be eventually replicated, but the cleanup decision is made
	// only by acquireWorkerCleanup against the authoritative Redis heartbeat.
	workersToCheck := make(map[string]struct{})
	for _, workerID := range node.jobMap.Keys() {
		workersToCheck[workerID] = struct{}{}
	}
	for _, workerID := range node.workerMap.Keys() {
		workersToCheck[workerID] = struct{}{}
	}

	for workerID := range workersToCheck {
		node.cleanupWorker(ctx, workerID)
	}

	// Also recover any jobs that still have payloads but are missing from the job map.
	// This can happen transiently during cascading failures and is preferable to leaving
	// jobs "stuck" (payload exists, but no worker owns the job).
	node.requeueOrphanedPayloads(ctx)
}

// requeueOrphanedPayloads detects payloads for job keys that are not present in
// the job map and requeues them after a short grace period.
func (node *Node) requeueOrphanedPayloads(ctx context.Context) {
	// Build a set of all job keys referenced by the job map.
	existingJobs := make(map[string]struct{})
	for workerID := range node.jobMap.Map() {
		keys, ok := node.jobMap.GetValues(workerID)
		if !ok {
			continue
		}
		for _, key := range keys {
			if key == "" {
				continue
			}
			existingJobs[key] = struct{}{}
		}
	}

	// Use a short grace period: we want recovery to be fast under churn,
	// but still avoid requeuing during brief map inconsistencies.
	grace := 2 * node.workerTTL
	if grace < node.recoveryGrace {
		grace = node.recoveryGrace
	}

	now := time.Now()
	for key := range node.jobPayloadMap.Map() {
		if _, ok := existingJobs[key]; ok {
			node.orphanedPayloads.Delete(key)
			continue
		}
		dispatchID, err := node.activeDispatchID(ctx, key)
		if err != nil {
			node.logger.Error(err, "key", key)
			continue
		}
		if dispatchID != "" {
			released, err := node.releaseCrashedDispatchStart(ctx, nil, key, dispatchID)
			if err != nil {
				node.logger.Error(err, "key", key, "dispatch", dispatchID)
				continue
			}
			if released {
				node.orphanedPayloads.Delete(key)
				node.logger.Info(
					"released orphaned exact dispatch for stream recovery",
					"key",
					key,
					"dispatch",
					dispatchID,
				)
				continue
			}
		}

		firstAny, ok := node.orphanedPayloads.Load(key)
		if !ok {
			node.orphanedPayloads.Store(key, now.UnixNano())
			continue
		}
		firstNS, _ := firstAny.(int64)
		if firstNS == 0 || now.Sub(time.Unix(0, firstNS)) < grace {
			continue
		}

		payload, ok := node.JobPayload(key)
		if !ok {
			node.orphanedPayloads.Delete(key)
			continue
		}
		job := &Job{Key: key, Payload: payload, CreatedAt: now, NodeID: node.ID, Requeued: true}
		if _, err := node.poolStream.Add(ctx, evStartJob, marshalJob(job)); err != nil {
			node.logger.Error(fmt.Errorf("requeueOrphanedPayloads: failed to requeue orphaned job: %w", err), "key", key)
			continue
		}

		node.orphanedPayloads.Delete(key)
		node.logger.Info("requeueOrphanedPayloads: requeued orphaned job", "key", key, "grace", grace)
	}
}

// cleanupWorker requeues the jobs assigned to the worker and deletes it from
// the pool.
func (node *Node) cleanupWorker(ctx context.Context, workerID string) {
	if err := node.ensureGenerationActive(ctx); err != nil {
		node.logger.Error(err)
		return
	}
	lease, err := node.acquireWorkerCleanup(ctx, workerID)
	if err != nil {
		node.logger.Error(fmt.Errorf("cleanupWorker: acquire lease: %w", err), "worker", workerID)
		return
	}
	if lease == nil {
		return
	}
	complete := false
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if err := node.releaseWorkerCleanup(releaseCtx, lease, complete); err != nil {
			node.logger.Error(fmt.Errorf("cleanupWorker: release lease: %w", err), "worker", workerID)
		}
	}()
	complete = node.requeueWorkerJobs(ctx, lease, nil)
}

// requeueWorkerJobs republishes every job owned by the leased worker through
// the lease-fenced stable publication records and deletes the worker once all
// jobs are processed. onProcessed, when non-nil, observes each job key that
// left the worker's ownership so a gracefully stopping worker can stop its
// local handler; an onProcessed error leaves the job for the next attempt.
// It returns true when the worker was completely requeued and deleted.
func (node *Node) requeueWorkerJobs(
	ctx context.Context,
	lease *workerCleanupLease,
	onProcessed func(key string) error,
) bool {
	workerID := lease.workerID
	processKey := func(key string) bool {
		if onProcessed == nil {
			return true
		}
		if err := onProcessed(key); err != nil {
			node.logger.Error(fmt.Errorf("requeueWorkerJobs: local stop failed: %w", err), "job", key, "worker", workerID)
			return false
		}
		return true
	}

	// Get the worker's jobs
	keys, ok := node.jobMap.GetValues(workerID)
	if !ok || len(keys) == 0 {
		if err := node.deleteStaleWorker(ctx, lease); err != nil {
			node.logger.Error(fmt.Errorf("requeueWorkerJobs: failed to delete worker: %w", err), "worker", workerID)
			return false
		}
		node.logger.Info("cleaned up worker with no jobs", "worker", workerID)
		return true
	}

	// Requeue jobs and process them
	var (
		requeued  int // jobs successfully requeued
		processed int // jobs that were either requeued or cleaned up as stale
	)
	for _, key := range keys {
		dispatchID, err := node.activeDispatchID(ctx, key)
		if err != nil {
			node.logger.Error(err, "job", key, "worker", workerID)
			continue
		}
		if dispatchID != "" {
			released, err := node.releaseCrashedDispatchStart(ctx, lease, key, dispatchID)
			if err != nil {
				node.logger.Error(err, "job", key, "worker", workerID, "dispatch", dispatchID)
				continue
			}
			if released {
				node.logger.Info(
					"released crashed exact dispatch for stream recovery",
					"job",
					key,
					"worker",
					workerID,
					"dispatch",
					dispatchID,
				)
				if !processKey(key) {
					continue
				}
				processed++
				continue
			}
		}
		payload, ok := node.JobPayload(key)
		if !ok {
			removed, err := node.removeStaleWorkerJob(ctx, lease, key)
			if err != nil {
				node.logger.Error(fmt.Errorf("requeueWorkerJobs: failed to remove stale job from jobs map: %w", err), "job", key, "worker", workerID)
				continue
			}
			if !removed {
				continue
			}
			node.logger.Info("requeueWorkerJobs: removed stale job key with missing payload", "job", key, "worker", workerID)
			if !processKey(key) {
				continue
			}
			processed++
			continue
		}
		job := &Job{Key: key, Payload: payload, CreatedAt: time.Now(), NodeID: node.ID, Requeued: true}
		// Requeue by adding an event back to the pool stream.
		// We intentionally do not wait for the job to start (which can time out
		// under heavy churn) - the pool sink will retry routing until it is acked.
		status, err := node.publishWorkerRequeue(ctx, lease, job)
		if err != nil {
			node.logger.Error(fmt.Errorf("requeueWorkerJobs: failed to requeue job: %w", err), "job", job.Key, "worker", workerID)
			continue
		}
		if status == 2 {
			if !processKey(key) {
				continue
			}
			processed++
			continue
		}
		if status == 3 {
			removed, err := node.removeStaleWorkerJob(ctx, lease, key)
			if err != nil {
				node.logger.Error(fmt.Errorf("requeueWorkerJobs: failed to remove stale job from jobs map: %w", err), "job", key, "worker", workerID)
				continue
			}
			if !removed {
				continue
			}
			if !processKey(key) {
				continue
			}
			processed++
			continue
		}
		if !processKey(key) {
			continue
		}
		requeued++
		processed++
	}
	if len(keys) != processed {
		node.logger.Info("partially processed stale worker jobs", "requeued", requeued, "processed", processed, "jobs", len(keys), "worker", workerID)
		return false
	}

	// Delete worker
	node.logger.Info("cleaned up worker", "worker", workerID, "requeued", requeued)
	if err := node.deleteStaleWorker(ctx, lease); err != nil {
		node.logger.Error(fmt.Errorf("requeueWorkerJobs: failed to delete worker: %w", err), "worker", workerID)
		return false
	}
	return true
}

// isWithinTTL checks if a timestamp is within a TTL. If lastSeen is not a valid
// timestamp, false is returned. lastSeen is a string representation of a unix
// timestamp in nanoseconds.
func (node *Node) isWithinTTL(lastSeen string, ttl time.Duration) bool {
	now, err := node.rdb.Time(context.Background()).Result()
	if err != nil {
		node.logger.Error(fmt.Errorf("isWithinTTL: failed to read Redis time: %w", err))
		return false
	}
	return node.isWithinTTLAt(lastSeen, ttl, now)
}

// isWithinTTLAt compares one persisted timestamp to an already-read Redis
// clock value so callers evaluating a set do not issue one TIME command per
// member.
func (node *Node) isWithinTTLAt(lastSeen string, ttl time.Duration, now time.Time) bool {
	lsi, err := strconv.ParseInt(lastSeen, 10, 64)
	if err != nil {
		node.logger.Error(fmt.Errorf("isWithinTTL: failed to parse last seen timestamp: %w", err))
		return false
	}
	return now.Sub(time.Unix(0, lsi)) <= ttl
}

// Keep node alive
func (node *Node) updateNodeKeepAlive() {
	defer node.wg.Done()
	ticker := time.NewTicker(node.workerTTL / 2)
	defer ticker.Stop()

	ctx := context.Background()
	for {
		select {
		case <-node.stop:
			return
		case <-ticker.C:
			if err := node.ensureGenerationActive(ctx); err != nil {
				node.logger.Error(err)
				return
			}
			_, err := refreshPoolNode(ctx, node.rdb, node.resources, node.ID)
			if err != nil {
				node.logger.Error(fmt.Errorf("updateNodeKeepAlive: failed to update timestamp: %w", err))
				if strings.Contains(err.Error(), "NODECLEANUPLOST") {
					node.stopAfterLifecycleLoss("stale node cleanup fence", false)
					return
				}
				continue
			}
		}
	}
}

// activeWorkers returns the IDs of the active workers in the pool.
func (node *Node) activeWorkers() []string {
	now, err := node.rdb.Time(context.Background()).Result()
	if err != nil {
		node.logger.Error(fmt.Errorf("activeWorkers: failed to read Redis time: %w", err))
		return nil
	}
	workers := node.workerMap.Map()
	workerCreatedAtByID := make(map[string]int64)
	var sortedIDs []string
	for id, createdAt := range workers {
		if createdAt == "-" {
			continue // worker is in the process of being removed
		}

		// Skip workers under an exact unexpired Redis-time cleanup lease.
		if cleanupLease, exists := node.workerCleanupMap.Get(id); exists {
			active, err := workerCleanupLeaseActive(cleanupLease, now)
			if err != nil {
				node.logger.Error(err, "worker", id)
				continue
			}
			if active {
				continue
			}
		}
		cai, err := strconv.ParseInt(createdAt, 10, 64)
		if err != nil {
			node.logger.Error(fmt.Errorf("activeWorkers: failed to parse created at timestamp: %w", err), "worker", id)
			continue
		}
		workerCreatedAtByID[id] = cai
		sortedIDs = append(sortedIDs, id)
	}
	sort.Slice(sortedIDs, func(i, j int) bool {
		return workerCreatedAtByID[sortedIDs[i]] < workerCreatedAtByID[sortedIDs[j]]
	})

	// Then filter out workers that have not been seen for more than workerTTL.
	alive := node.workerKeepAliveMap.Map()
	var activeIDs []string
	for _, id := range sortedIDs {
		ls, ok := alive[id]
		if !ok {
			// This could happen if a worker is removed from the
			// pool and the last seen map deletion replicates before
			// the workers map deletion.
			continue
		}
		if !node.isWithinTTLAt(ls, node.workerTTL, now) {
			continue
		}
		activeIDs = append(activeIDs, id)
	}

	return activeIDs
}

// deleteWorker removes a remote worker from the pool deleting the worker stream.
func (node *Node) deleteWorker(id string) error {
	ctx := context.Background()
	node.logger.Debug("deleteWorker: deleting worker", "worker", id)

	// Destroy before removing the records that make failed cleanup discoverable.
	stream, err := node.getWorkerStream(id)
	if err != nil {
		return fmt.Errorf("deleteWorker: failed to retrieve worker stream for %q: %w", id, err)
	}
	if err := stream.Destroy(ctx); err != nil {
		return fmt.Errorf("deleteWorker: failed to delete worker stream: %w", err)
	}
	return node.removeWorkerFromMaps(ctx, id)
}

// removeWorker removes a worker that was created by this node.
// This is used during graceful shutdown or explicit worker removal.
func (node *Node) removeWorker(ctx context.Context, id string) error {
	if err := node.removeWorkerFromMaps(ctx, id); err != nil {
		return err
	}
	node.workerStreams.Delete(id)
	return nil
}

// removeWorkerFromMaps removes the worker from all tracking maps.
// This is the common cleanup needed for both local and remote worker removal.
func (node *Node) removeWorkerFromMaps(ctx context.Context, id string) error {
	var cleanupErr error
	if err := node.deletePoolMap(ctx, node.resources.workers, id); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove worker %s from worker map: %w", id, err))
	}
	if err := node.deletePoolMap(ctx, node.resources.workerKeepAlive, id); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove worker %s from keep-alive map: %w", id, err))
	}
	if err := node.deletePoolMap(ctx, node.resources.workerCleanup, id); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove worker %s cleanup timestamp: %w", id, err))
	}
	// NOTE: Do not delete job payloads here.
	//
	// Payload entries are job-scoped (not worker-scoped) and are required to
	// safely requeue jobs from a stale worker during distributed cleanup. Deleting
	// payloads during worker removal can race with another node performing
	// cleanup/requeue and lead to permanent job loss.
	//
	// Payloads are deleted when jobs stop (see Worker.stopJob) and any remaining
	// orphaned payloads are eventually collected by cleanupOrphanedJobPayloads.
	if err := node.deletePoolMap(ctx, node.resources.jobs, id); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove worker %s from jobs map: %w", id, err))
	}
	return cleanupErr
}

// getWorkerStream retrieves the stream for a worker. It caches the result in the
// workerStreams map.
func (node *Node) getWorkerStream(id string) (*streaming.Stream, error) {
	val, ok := node.workerStreams.Load(id)
	if !ok {
		s, err := streaming.NewStream(workerStreamName(id), node.rdb, options.WithStreamLogger(node.logger))
		if err != nil {
			return nil, fmt.Errorf("workerStream: failed to retrieve stream for worker %q: %w", id, err)
		}
		node.workerStreams.Store(id, s)
		return s, nil
	}
	return val.(*streaming.Stream), nil
}

// getNodeStream retrieves the given node stream.
func (node *Node) getNodeStream(nodeID string) (*streaming.Stream, error) {
	if nodeID == node.ID {
		return node.nodeStream, nil
	}
	val, ok := node.nodeStreams.Load(nodeID)
	if !ok {
		s, err := streaming.NewStream(nodeStreamName(node.PoolName, nodeID), node.rdb, options.WithStreamLogger(node.logger))
		if err != nil {
			return nil, fmt.Errorf("getNodeStream: failed to create node stream %q: %w", nodeStreamName(node.PoolName, nodeID), err)
		}
		node.nodeStreams.Store(nodeID, s)
		return s, nil
	}
	return val.(*streaming.Stream), nil
}

// requeueAllJobs requeues all jobs from all local workers in parallel. It waits for all
// requeue operations to complete before returning. If any requeue operations fail, it
// collects all errors and returns them as a single error. This is typically called
// during node close to ensure no jobs are lost.
func (node *Node) requeueAllJobs(ctx context.Context) error {
	var wg sync.WaitGroup
	var errs []error
	var errLock sync.Mutex

	node.localWorkers.Range(func(key, value any) bool {
		wg.Add(1)
		pulse.Go(node.logger, func() {
			defer wg.Done()
			if err := value.(*Worker).requeueJobs(ctx); err != nil {
				errLock.Lock()
				errs = append(errs, err)
				errLock.Unlock()
			}
		})
		return true
	})
	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("failed to requeue %d jobs: %v", len(errs), errs)
	}
	return nil
}

// cleanupPool completes the takeover-owned cleanup claimed by
// waitForPoolNodes. The persisted owner and lease make every destructive step
// retryable by another process after interruption.
func (node *Node) cleanupPool(ctx context.Context) error {
	complete, err := node.poolCleanupComplete(ctx)
	if err != nil {
		return err
	}
	if complete {
		return nil
	}
	err = cleanupPoolResources(
		ctx,
		node.rdb,
		node.PoolName,
		node.poolStream.Generation(),
		node.ID,
		node.cleanupLease,
	)
	if err != nil {
		return fmt.Errorf("cleanupPool: %w", err)
	}
	return nil
}

// waitForPoolNodes reaps crashed-node leases and blocks until every live node
// detaches. Redis TIME is the sole lease clock, so host clock skew cannot hold
// or prematurely pass the destructive cleanup barrier.
func (node *Node) waitForPoolNodes(ctx context.Context) error {
	nodes, err := rmap.Join(
		ctx,
		node.resources.nodeKeepAlive,
		node.rdb,
		rmap.WithLogger(node.logger),
	)
	if err != nil {
		return fmt.Errorf("Shutdown: failed to join node shutdown barrier: %w", err)
	}
	defer nodes.Close()
	updates := nodes.Subscribe()
	defer nodes.Unsubscribe(updates)
	for {
		now, err := node.rdb.Time(ctx).Result()
		if err != nil {
			return fmt.Errorf("Shutdown: failed to read Redis time for node barrier: %w", err)
		}
		shutdownState, err := node.rdb.HGetAll(
			ctx,
			rmapContentKey(node.resources.nodeShutdown),
		).Result()
		if err != nil {
			return fmt.Errorf("Shutdown: failed to read peer shutdown state: %w", err)
		}
		nextExpiry := node.workerTTL
		active := 0
		activeNodes := make(map[string]struct{})
		heartbeats, err := node.rdb.HGetAll(
			ctx,
			rmapContentKey(node.resources.nodeKeepAlive),
		).Result()
		if err != nil {
			return fmt.Errorf("Shutdown: failed to read authoritative node heartbeats: %w", err)
		}
		for nodeID, timestamp := range heartbeats {
			if strings.HasPrefix(nodeID, "=") {
				continue
			}
			lastSeen, err := strconv.ParseInt(timestamp, 10, 64)
			if err != nil {
				return fmt.Errorf("Shutdown: invalid node keep-alive for %q: %w", nodeID, err)
			}
			remaining := node.workerTTL - now.Sub(time.Unix(0, lastSeen))
			if remaining <= 0 {
				cleaned, err := cleanupStalePoolNode(
					ctx,
					node.rdb,
					node.resources,
					nodeID,
					node.ID,
				)
				if err != nil {
					return fmt.Errorf("Shutdown: failed to reap stale node %q: %w", nodeID, err)
				}
				if cleaned {
					continue
				}
				remaining = shutdownErrorPoll
			}
			active++
			activeNodes[nodeID] = struct{}{}
			nextExpiry = min(nextExpiry, remaining)
		}
		for key, message := range shutdownState {
			if !strings.HasPrefix(key, "error:") {
				continue
			}
			nodeID := strings.TrimPrefix(key, "error:")
			if _, active := activeNodes[nodeID]; !active {
				continue
			}
			return fmt.Errorf("Shutdown: node %q failed to close: %s", nodeID, message)
		}
		if active == 0 {
			status, err := claimPoolCleanup(
				ctx,
				node.rdb,
				node.PoolName,
				node.poolStream.Generation(),
				node.ID,
				node.cleanupLease,
			)
			if err != nil {
				return fmt.Errorf("Shutdown: failed to claim pool cleanup: %w", err)
			}
			if status == poolCleanupClaimed || status == poolCleanupAlreadyComplete {
				return nil
			}
			active = 1
		}
		nextExpiry = min(nextExpiry, shutdownErrorPoll)
		timer := time.NewTimer(nextExpiry)
		select {
		case <-updates:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("Shutdown: waiting for pool nodes to close: %w", ctx.Err())
		}
	}
}

// cleanupNode destroys the node stream before closing maps. A destruction
// failure leaves map handles open and is returned so Close can be retried.
func (node *Node) cleanupNode(ctx context.Context) error {
	if err := node.nodeStream.Destroy(ctx); err != nil {
		return fmt.Errorf("failed to destroy node stream: %w", err)
	}
	for _, m := range node.maps() {
		if m != nil {
			m.Close()
		}
	}
	return nil
}

// maps returns the maps managed by the node.
func (node *Node) maps() []*rmap.Map {
	return []*rmap.Map{
		node.nodeKeepAliveMap,
		node.nodeShutdownMap,
		node.workerMap,
		node.workerKeepAliveMap,
		node.workerCleanupMap,
		node.jobMap,
		node.jobPendingMap,
		node.jobPayloadMap,
		node.tickerMap,
		node.schedulerJobMap,
	}
}

// Hash implements the Jump Consistent Hash algorithm.
// See https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf for details.
func (jh *jumpHash) Hash(key string, numBuckets int64) int64 {
	var b int64 = -1
	var j int64

	jh.mu.Lock()
	jh.h.Reset()
	_, err := io.WriteString(jh.h, key)
	sum := jh.h.Sum64()
	jh.mu.Unlock()
	if err != nil {
		panic(fmt.Errorf("jumpHash: write key: %w", err))
	}

	for j < numBuckets {
		b = j
		sum = sum*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((sum>>33)+1)))
	}
	return b
}

// pendingEventKey computes the key of a pending event from a worker ID and a
// stream event ID.
func pendingEventKey(workerID, eventID string) string {
	return fmt.Sprintf("%s:%s", workerID, eventID)
}

// nodeKeepAliveMapName returns the name of the replicated map used to store the
// node keep-alive timestamps.
func nodeKeepAliveMapName(pool string) string {
	return fmt.Sprintf("%s:node-keepalive", pool)
}

// nodeShutdownMapName returns the name of the replicated map used to store the
// worker status.
func nodeShutdownMapName(pool string) string {
	return fmt.Sprintf("%s:shutdown", pool)
}

// shutdownErrorKey identifies one peer's authoritative close failure.
func shutdownErrorKey(nodeID string) string {
	return fmt.Sprintf("error:%s", nodeID)
}

// poolCleanupGenerationsKey records completed pool-stream generations.
func poolCleanupGenerationsKey(pool string) string {
	return fmt.Sprintf("pulse:pool:%s:cleanup-generations", pool)
}

// workerMapName returns the name of the replicated map used to store the
// worker creation timestamps.
func workerMapName(pool string) string {
	return fmt.Sprintf("%s:workers", pool)
}

// workerKeepAliveMapName returns the name of the replicated map used to store the
// worker keep-alive timestamps.
func workerKeepAliveMapName(pool string) string {
	return fmt.Sprintf("%s:worker-keepalive", pool)
}

// workerCleanupMapName returns the name of the replicated map used to store the
// worker status.
func workerCleanupMapName(pool string) string {
	return fmt.Sprintf("%s:cleanup", pool)
}

// jobMapName returns the name of the replicated map used to store the
// jobs by worker ID.
func jobMapName(pool string) string {
	return fmt.Sprintf("%s:jobs", pool)
}

// jobPendingMapName returns the name of the replicated map used to store the
// pending jobs by job key.
func jobPendingMapName(poolName string) string {
	return poolName + ":pending-jobs"
}

// dispatchMapName returns the generation-owned durable dispatch record map.
func dispatchMapName(pool string) string {
	return fmt.Sprintf("%s:dispatches", pool)
}

// jobPayloadMapName returns the name of the replicated map used to store the
// job payloads by job key.
func jobPayloadMapName(pool string) string {
	return fmt.Sprintf("%s:job-payloads", pool)
}

// rmapContentKey returns the Redis hash key used by an rmap.
func rmapContentKey(name string) string {
	return fmt.Sprintf("map:%s:content", name)
}

// rmapUpdateChannel returns the Redis pubsub channel used by an rmap.
func rmapUpdateChannel(name string) string {
	return fmt.Sprintf("map:%s:updates", name)
}

// tickerMapName returns the name of the replicated map used to store ticker
// ticks.
func tickerMapName(pool string) string {
	return fmt.Sprintf("%s:tickers", pool)
}

// schedulerJobMapName returns the pre-generation scheduler ownership map name.
func schedulerJobMapName(pool string) string {
	return fmt.Sprintf("%s:scheduler-jobs", pool)
}

// poolStreamName returns the name of the stream used by pool events.
func poolStreamName(pool string) string {
	return fmt.Sprintf("%s:pool", pool)
}

// nodeStreamName returns the name of the stream used by node events.
func nodeStreamName(pool, nodeID string) string {
	return fmt.Sprintf("%s:node:%s", pool, nodeID)
}
