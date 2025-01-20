package pool

import (
	"context"
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

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming"
	soptions "goa.design/pulse/streaming/options"
)

type (
	// Node is a pool of workers.
	Node struct {
		ID                 string
		PoolName           string
		poolStream         *streaming.Stream // pool event stream for dispatching jobs
		poolSink           *streaming.Sink   // pool event sink
		nodeStream         *streaming.Stream // node event stream for receiving worker events
		nodeReader         *streaming.Reader // node event reader
		workerMap          *rmap.Map         // worker creation times by ID
		jobsMap            *rmap.Map         // jobs by worker ID
		jobPayloadsMap     *rmap.Map         // job payloads by job key
		nodeKeepAliveMap   *rmap.Map         // node keep-alive timestamps indexed by ID
		workerKeepAliveMap *rmap.Map         // worker keep-alive timestamps indexed by ID
		shutdownMap        *rmap.Map         // key is node ID that requested shutdown
		tickerMap          *rmap.Map         // ticker next tick time indexed by name
		workerTTL          time.Duration     // Worker considered dead if keep-alive not updated after this duration
		workerShutdownTTL  time.Duration     // Worker considered dead if not shutdown after this duration
		ackGracePeriod     time.Duration     // Wait for return status up to this duration
		clientOnly         bool
		logger             pulse.Logger
		h                  hasher
		stop               chan struct{}  // closed when node is stopped
		closed             chan struct{}  // closed when node is closed
		wg                 sync.WaitGroup // allows to wait until all goroutines exit
		rdb                *redis.Client

		localWorkers  sync.Map // workers created by this node
		workerStreams sync.Map // worker streams indexed by ID
		pendingJobs   sync.Map // channels used to send DispatchJob results, nil if event is requeued
		pendingEvents sync.Map // pending events indexed by sender and event IDs

		lock     sync.RWMutex
		closing  bool
		shutdown bool
	}

	// hasher is the interface implemented by types that can hash keys.
	hasher interface {
		Hash(key string, numBuckets int64) int64
	}

	// jumpHash implement Jump Consistent Hash.
	jumpHash struct {
		h hash.Hash64
	}
)

const (
	// evStartJob is the event used to send new job to workers.
	evStartJob string = "j"
	// evNotify is the event used to notify a worker running a specific job.
	evNotify string = "n"
	// evStopJob is the event used to stop a job.
	evStopJob string = "s"
	// evAck is the worker event used to ack a pool event.
	evAck string = "a"
	// evDispatchReturn is the event used to forward the worker start return
	// status to the node that dispatched the job.
	evDispatchReturn string = "d"
)

// pendingEventTTL is the TTL for pending events.
var pendingEventTTL = 2 * time.Minute

// AddNode adds a new node to the pool with the given name and returns it. The
// node can be used to dispatch jobs and add new workers. A node also routes
// dispatched jobs to the proper worker and acks the corresponding events once
// the worker acks the job.
//
// The options WithClientOnly can be used to create a node that can only be used
// to dispatch jobs. Such a node does not route or process jobs in the
// background.
func AddNode(ctx context.Context, poolName string, rdb *redis.Client, opts ...NodeOption) (*Node, error) {
	o := parseOptions(opts...)
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
		"worker_shutdown_ttl", o.workerShutdownTTL,
		"ack_grace_period", o.ackGracePeriod)

	wsm, err := rmap.Join(ctx, shutdownMapName(poolName), rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to join shutdown replicated map %q: %w", shutdownMapName(poolName), err)
	}
	if wsm.Len() > 0 {
		return nil, fmt.Errorf("AddNode: pool %q is shutting down", poolName)
	}

	nkm, err := rmap.Join(ctx, nodeKeepAliveMapName(poolName), rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to join node keep-alive map %q: %w", nodeKeepAliveMapName(poolName), err)
	}
	if _, err := nkm.Set(ctx, nodeID, strconv.FormatInt(time.Now().UnixNano(), 10)); err != nil {
		return nil, fmt.Errorf("AddNode: failed to set initial node keep-alive: %w", err)
	}

	poolStream, err := streaming.NewStream(poolStreamName(poolName), rdb,
		soptions.WithStreamMaxLen(o.maxQueuedJobs),
		soptions.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to create pool job stream %q: %w", poolStreamName(poolName), err)
	}

	var (
		wm  *rmap.Map
		jm  *rmap.Map
		jpm *rmap.Map
		km  *rmap.Map
		tm  *rmap.Map

		poolSink   *streaming.Sink
		nodeStream *streaming.Stream
		nodeReader *streaming.Reader
		closed     chan struct{}
	)

	if !o.clientOnly {
		wm, err = rmap.Join(ctx, workerMapName(poolName), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool workers replicated map %q: %w", workerMapName(poolName), err)
		}
		workerIDs := wm.Keys()
		logger.Info("joined", "workers", workerIDs)

		jm, err = rmap.Join(ctx, jobsMapName(poolName), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool jobs replicated map %q: %w", jobsMapName(poolName), err)
		}

		jpm, err = rmap.Join(ctx, jobPayloadsMapName(poolName), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool job payloads replicated map %q: %w", jobPayloadsMapName(poolName), err)
		}

		km, err = rmap.Join(ctx, workerKeepAliveMapName(poolName), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join worker keep-alive replicated map %q: %w", workerKeepAliveMapName(poolName), err)
		}

		tm, err = rmap.Join(ctx, tickerMapName(poolName), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to join pool ticker replicated map %q: %w", tickerMapName(poolName), err)
		}

		poolSink, err = poolStream.NewSink(ctx, "events",
			soptions.WithSinkBlockDuration(o.jobSinkBlockDuration),
			soptions.WithSinkAckGracePeriod(o.ackGracePeriod))
		if err != nil {
			return nil, fmt.Errorf("AddNode: failed to create events sink for stream %q: %w", poolStreamName(poolName), err)
		}
		closed = make(chan struct{})
	}

	nodeStream, err = streaming.NewStream(nodeStreamName(poolName, nodeID), rdb, soptions.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to create node event stream %q: %w", nodeStreamName(poolName, nodeID), err)
	}

	nodeReader, err = nodeStream.NewReader(ctx, soptions.WithReaderBlockDuration(o.jobSinkBlockDuration), soptions.WithReaderStartAtOldest())
	if err != nil {
		return nil, fmt.Errorf("AddNode: failed to create node event reader for stream %q: %w", nodeStreamName(poolName, nodeID), err)
	}

	p := &Node{
		ID:                 nodeID,
		PoolName:           poolName,
		nodeKeepAliveMap:   nkm,
		workerKeepAliveMap: km,
		workerMap:          wm,
		jobsMap:            jm,
		jobPayloadsMap:     jpm,
		shutdownMap:        wsm,
		tickerMap:          tm,
		workerStreams:      sync.Map{},
		pendingJobs:        sync.Map{},
		pendingEvents:      sync.Map{},
		poolStream:         poolStream,
		poolSink:           poolSink,
		nodeStream:         nodeStream,
		nodeReader:         nodeReader,
		clientOnly:         o.clientOnly,
		workerTTL:          o.workerTTL,
		workerShutdownTTL:  o.workerShutdownTTL,
		ackGracePeriod:     o.ackGracePeriod,
		h:                  jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
		stop:               make(chan struct{}),
		closed:             closed,
		rdb:                rdb,
		logger:             logger,
	}

	nch := nodeReader.Subscribe()

	if o.clientOnly {
		logger.Info("client-only")
		p.wg.Add(3)
		pulse.Go(ctx, func() { p.handleNodeEvents(ctx, nch) }) // to handle job acks
		pulse.Go(ctx, func() { p.processInactiveNodes(ctx) })
		pulse.Go(ctx, func() { p.updateNodeKeepAlive(ctx) })
		return p, nil
	}

	p.wg.Add(7)
	pulse.Go(ctx, func() { p.handlePoolEvents(ctx, poolSink.Subscribe()) })
	pulse.Go(ctx, func() { p.handleNodeEvents(ctx, nch) })
	pulse.Go(ctx, func() { p.watchWorkers(ctx) })
	pulse.Go(ctx, func() { p.watchShutdown(ctx) })
	pulse.Go(ctx, func() { p.processInactiveNodes(ctx) })
	pulse.Go(ctx, func() { p.processInactiveWorkers(ctx) })
	pulse.Go(ctx, func() { p.updateNodeKeepAlive(ctx) })

	return p, nil
}

// AddWorker adds a new worker to the pool and returns it. The worker starts
// processing jobs immediately. handler can optionally implement the
// NotificationHandler interface to handle notifications.
func (node *Node) AddWorker(ctx context.Context, handler JobHandler) (*Worker, error) {
	if node.IsClosed() {
		return nil, fmt.Errorf("AddWorker: pool %q is closed", node.PoolName)
	}
	if node.clientOnly {
		return nil, fmt.Errorf("AddWorker: pool %q is client-only", node.PoolName)
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
	w.stop(ctx)
	if err := w.requeueJobs(ctx); err != nil {
		node.logger.Error(fmt.Errorf("RemoveWorker: failed to requeue jobs for worker %q: %w", w.ID, err))
	}
	node.cleanupWorker(ctx, w.ID)
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
// - an error returned by the worker's start handler if the job fails to start
// - an error if the pool is closed or if there's a failure in adding the job
//
// The method blocks until one of the above conditions is met.
func (node *Node) DispatchJob(ctx context.Context, key string, payload []byte) error {
	if node.IsClosed() {
		return fmt.Errorf("DispatchJob: pool %q is closed", node.PoolName)
	}

	job := marshalJob(&Job{Key: key, Payload: payload, CreatedAt: time.Now(), NodeID: node.ID})
	eventID, err := node.poolStream.Add(ctx, evStartJob, job)
	if err != nil {
		return fmt.Errorf("DispatchJob: failed to add job to stream %q: %w", node.poolStream.Name, err)
	}

	cherr := make(chan error, 1)
	node.pendingJobs.Store(eventID, cherr)

	timer := time.NewTimer(2 * node.ackGracePeriod)
	defer timer.Stop()

	select {
	case err = <-cherr:
	case <-timer.C:
		err = fmt.Errorf("DispatchJob: job %q timed out, TTL: %v", key, 2*node.ackGracePeriod)
	case <-ctx.Done():
		err = ctx.Err()
	}

	node.pendingJobs.Delete(eventID)
	close(cherr)

	if err != nil {
		node.logger.Error(fmt.Errorf("DispatchJob: failed to dispatch job: %w", err), "key", key)
		return err
	}

	node.logger.Info("dispatched", "key", key)
	return nil
}

// StopJob stops the job with the given key.
func (node *Node) StopJob(ctx context.Context, key string) error {
	if node.IsClosed() {
		return fmt.Errorf("StopJob: pool %q is closed", node.PoolName)
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
	jobByNodes := node.jobsMap.Map()
	for _, jobs := range jobByNodes {
		jobKeys = append(jobKeys, strings.Split(jobs, ",")...)
	}
	return jobKeys
}

// JobPayload returns the payload of the job with the given key.
// It returns:
// - (payload, true) if the job exists and has a payload
// - (nil, true) if the job exists but has an empty payload
// - (nil, false) if the job does not exist
func (node *Node) JobPayload(key string) ([]byte, bool) {
	payload, ok := node.jobPayloadsMap.Get(key)
	if !ok {
		return nil, false
	}
	if payload == "" {
		return nil, true
	}
	return []byte(payload), true
}

// NotifyWorker notifies the worker that handles the job with the given key.
func (node *Node) NotifyWorker(ctx context.Context, key string, payload []byte) error {
	if node.IsClosed() {
		return fmt.Errorf("NotifyWorker: pool %q is closed", node.PoolName)
	}
	if _, err := node.poolStream.Add(ctx, evNotify, marshalNotification(key, payload)); err != nil {
		return fmt.Errorf("NotifyWorker: failed to add notification to stream %q: %w", node.poolStream.Name, err)
	}
	node.logger.Info("notification sent", "key", key)
	return nil
}

// Shutdown stops the pool workers gracefully across all nodes. It notifies all
// workers and waits until they are completed. Shutdown prevents the pool nodes
// from creating new workers and the pool workers from accepting new jobs. After
// Shutdown returns, the node object cannot be used anymore and should be
// discarded. One of Shutdown or Close should be called before the node is
// garbage collected unless it is client-only.
func (node *Node) Shutdown(ctx context.Context) error {
	if node.IsClosed() {
		return nil
	}
	if node.clientOnly {
		return fmt.Errorf("Shutdown: client-only node cannot shutdown worker pool")
	}

	// Signal all nodes to shutdown.
	if _, err := node.shutdownMap.SetAndWait(ctx, "shutdown", node.ID); err != nil {
		node.logger.Error(fmt.Errorf("Shutdown: failed to set shutdown status in shutdown map: %w", err))
	}
	<-node.closed // Wait for this node to be closed
	node.cleanupPool(ctx)

	node.logger.Info("shutdown")
	return nil
}

// Close stops the node workers and closes the Redis connection but does
// not stop workers running in other nodes. It requeues all the jobs run by
// workers of the node. One of Shutdown or Close should be called before the
// node is garbage collected unless it is client-only.
func (node *Node) Close(ctx context.Context) error {
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
	return node.closing
}

// close stops the node and its workers, optionally requeuing jobs. If shutdown
// is true, jobs are not requeued as the pool is being shutdown. Otherwise, jobs
// are requeued to be picked up by other nodes. The method stops all workers,
// waits for background goroutines to complete, cleans up resources and closes
// connections. It is idempotent and can be called multiple times safely.
func (node *Node) close(ctx context.Context, shutdown bool) error {
	node.lock.Lock()
	if node.closing {
		node.lock.Unlock()
		return nil
	}
	node.closing = true
	node.lock.Unlock()

	// If we're shutting down then stop all the jobs.
	if shutdown {
		node.stopAllJobs(ctx)
	}

	// Stop all workers before waiting for goroutines
	var wg sync.WaitGroup
	node.localWorkers.Range(func(key, value any) bool {
		wg.Add(1)
		pulse.Go(ctx, func() {
			defer wg.Done()
			value.(*Worker).stop(ctx)
			// Remove worker immediately to avoid job requeuing by other nodes
			node.cleanupWorker(ctx, value.(*Worker).ID)
		})
		return true
	})
	wg.Wait()

	// Stop all goroutines
	close(node.stop)
	node.wg.Wait()

	// Requeue jobs if not shutting down, after stopping goroutines to avoid receiving new jobs
	if !shutdown {
		if err := node.requeueAllJobs(ctx); err != nil {
			node.logger.Error(fmt.Errorf("close: failed to requeue jobs: %w", err))
		}
	}

	// Cleanup resources
	node.cleanupNode(ctx)

	// Signal that the node is closed
	close(node.closed)

	node.logger.Info("closed")
	return nil
}

// stopAllJobs stops all jobs running on the node.
func (node *Node) stopAllJobs(ctx context.Context) {
	var wg sync.WaitGroup
	var total atomic.Int32
	node.localWorkers.Range(func(key, value any) bool {
		wg.Add(1)
		worker := value.(*Worker)
		pulse.Go(ctx, func() {
			defer wg.Done()
			for _, job := range worker.Jobs() {
				if err := worker.stopJob(ctx, job.Key, false); err != nil {
					node.logger.Error(fmt.Errorf("Close: failed to stop job %q for worker %q: %w", job.Key, worker.ID, err))
				}
				total.Add(1)
			}
		})
		return true
	})
	wg.Wait()
	node.logger.Info("stopped all jobs", "total", total.Load())
}

// handlePoolEvents reads events from the pool job stream.
func (node *Node) handlePoolEvents(ctx context.Context, c <-chan *streaming.Event) {
	defer node.wg.Done()

	for {
		select {
		case ev := <-c:
			if err := node.routeWorkerEvent(ctx, ev); err != nil {
				node.logger.Error(fmt.Errorf("handlePoolEvents: failed to route event: %w", err))
			}
		case <-node.stop:
			node.poolSink.Close(ctx)
			return
		}
	}
}

// routeWorkerEvent routes a dispatched event to the proper worker.
func (node *Node) routeWorkerEvent(ctx context.Context, ev *streaming.Event) error {
	// Compute the worker ID that will handle the job.
	key := unmarshalJobKey(ev.Payload)
	activeWorkers := node.activeWorkers()
	if len(activeWorkers) == 0 {
		return fmt.Errorf("routeWorkerEvent: no active worker in pool %q", node.PoolName)
	}
	wid := activeWorkers[node.h.Hash(key, int64(len(activeWorkers)))]

	// Stream the event to the worker corresponding to the key hash.
	stream, err := node.workerStream(ctx, wid)
	if err != nil {
		return err
	}

	var eventID string
	eventID, err = stream.Add(ctx, ev.EventName, marshalEnvelope(node.ID, ev.Payload))
	if err != nil {
		return fmt.Errorf("routeWorkerEvent: failed to add event %s to worker stream %q: %w", ev.EventName, workerStreamName(wid), err)
	}
	node.logger.Debug("routed", "event", ev.EventName, "id", ev.ID, "worker", wid, "worker-event-id", eventID)

	// Record the event in the pending events map for future ack.
	node.pendingEvents.Store(pendingEventKey(wid, eventID), ev)

	return nil
}

// handleNodeEvents reads events from the node event stream and acks the pending
// events that correspond to jobs that are now running or done.
func (node *Node) handleNodeEvents(ctx context.Context, c <-chan *streaming.Event) {
	defer node.wg.Done()

	for {
		select {
		case ev := <-c:
			node.processNodeEvent(ctx, ev)
		case <-node.stop:
			node.nodeReader.Close()
			return
		}
	}
}

// processNodeEvent processes a node event.
func (node *Node) processNodeEvent(ctx context.Context, ev *streaming.Event) {
	switch ev.EventName {
	case evAck:
		// Event sent by worker to ack a dispatched job.
		node.logger.Debug("handleNodeEvents: received ack", "event", ev.EventName, "id", ev.ID)
		node.ackWorkerEvent(ctx, ev)
	case evDispatchReturn:
		// Event sent by pool node to node that originally dispatched the job.
		node.logger.Debug("handleNodeEvents: received dispatch return", "event", ev.EventName, "id", ev.ID)
		node.returnDispatchStatus(ctx, ev)
	}
}

// ackWorkerEvent acks the pending event that corresponds to the acked job.  If
// the event was a dispatched job then it sends a dispatch return event to the
// node that dispatched the job.
func (node *Node) ackWorkerEvent(ctx context.Context, ev *streaming.Event) {
	workerID, payload := unmarshalEnvelope(ev.Payload)
	ack := unmarshalAck(payload)
	key := pendingEventKey(workerID, ack.EventID)
	val, ok := node.pendingEvents.Load(key)
	if !ok {
		node.logger.Error(fmt.Errorf("ackWorkerEvent: received unknown event %s from worker %s", ack.EventID, workerID))
		return
	}
	pending := val.(*streaming.Event)

	// If a dispatched job then send a return event to the node that
	// dispatched the job.
	if pending.EventName == evStartJob {
		_, nodeID := unmarshalJobKeyAndNodeID(pending.Payload)
		stream, err := streaming.NewStream(nodeStreamName(node.PoolName, nodeID), node.rdb, soptions.WithStreamLogger(node.logger))
		if err != nil {
			node.logger.Error(fmt.Errorf("ackWorkerEvent: failed to create node event stream %q: %w", nodeStreamName(node.PoolName, nodeID), err))
			return
		}
		ack.EventID = pending.ID
		if _, err := stream.Add(ctx, evDispatchReturn, marshalAck(ack)); err != nil {
			node.logger.Error(fmt.Errorf("ackWorkerEvent: failed to dispatch return to stream %q: %w", nodeStreamName(node.PoolName, nodeID), err))
		}
	}

	// Ack the sink event so it does not get redelivered.
	if err := node.poolSink.Ack(ctx, pending); err != nil {
		node.logger.Error(fmt.Errorf("ackWorkerEvent: failed to ack event: %w", err), "event", pending.EventName, "id", pending.ID)
	}
	node.pendingEvents.Delete(key)

	// Garbage collect stale events.
	var staleKeys []string
	node.pendingEvents.Range(func(key, value any) bool {
		ev := value.(*streaming.Event)
		if time.Since(ev.CreatedAt()) > pendingEventTTL {
			staleKeys = append(staleKeys, key.(string))
			node.logger.Error(fmt.Errorf("ackWorkerEvent: stale event, removing from pending events"), "event", ev.EventName, "id", ev.ID, "since", time.Since(ev.CreatedAt()), "TTL", pendingEventTTL)
		}
		return true
	})
	for _, key := range staleKeys {
		node.pendingEvents.Delete(key)
	}
}

// returnDispatchStatus returns the start job result to the caller.
func (node *Node) returnDispatchStatus(_ context.Context, ev *streaming.Event) {
	ack := unmarshalAck(ev.Payload)
	val, ok := node.pendingJobs.Load(ack.EventID)
	if !ok {
		node.logger.Error(fmt.Errorf("returnDispatchStatus: received dispatch return for unknown event"), "id", ack.EventID)
		return
	}
	node.logger.Debug("dispatch return", "event", ev.EventName, "id", ev.ID, "ack-id", ack.EventID)
	if val == nil {
		// Event was requeued, just clean up
		node.pendingJobs.Delete(ack.EventID)
		return
	}
	var err error
	if ack.Error != "" {
		err = errors.New(ack.Error)
	}
	val.(chan error) <- err
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
	// First cleanup the local workers that are no longer active.
	node.localWorkers.Range(func(key, value any) bool {
		worker := value.(*Worker)
		if _, ok := node.workerMap.Get(worker.ID); !ok {
			// If it's not in the worker map, then it's not active and its jobs
			// have already been requeued.
			node.logger.Info("handleWorkerMapUpdate: removing inactive local worker", "worker", worker.ID)
			if err := node.deleteWorker(ctx, worker.ID); err != nil {
				node.logger.Error(fmt.Errorf("handleWorkerMapUpdate: failed to delete inactive worker %q: %w", worker.ID, err), "worker", worker.ID)
			}
			worker.stop(ctx)
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

// requeueJob requeues the given worker jobs.
func (node *Node) requeueJob(ctx context.Context, workerID string, job *Job) (chan error, error) {
	if _, removed, err := node.jobsMap.RemoveValues(ctx, workerID, job.Key); err != nil {
		return nil, fmt.Errorf("requeueJob: failed to remove job %q from jobs map during rebalance: %w", job.Key, err)
	} else if !removed {
		node.logger.Debug("requeueJob: job already removed from jobs map during rebalance", "key", job.Key, "worker", workerID)
		return nil, nil
	}
	node.logger.Debug("requeuing job", "key", job.Key, "worker", workerID)
	job.NodeID = node.ID

	eventID, err := node.poolStream.Add(ctx, evStartJob, marshalJob(job))
	if err != nil {
		if _, err := node.jobsMap.AppendValues(ctx, workerID, job.Key); err != nil {
			node.logger.Error(fmt.Errorf("requeueJob: failed to re-add job to jobs map: %w", err), "job", job.Key)
		}
		return nil, fmt.Errorf("requeueJob: failed to add job %q to stream %q: %w", job.Key, node.poolStream.Name, err)
	}
	cherr := make(chan error, 1)
	node.pendingJobs.Store(eventID, cherr)
	return cherr, nil
}

// watchShutdown monitors the pool shutdown map and initiates node shutdown when updated.
func (node *Node) watchShutdown(ctx context.Context) {
	defer node.wg.Done()
	for {
		select {
		case <-node.stop:
			return
		case <-node.shutdownMap.Subscribe():
			node.logger.Debug("watchShutdown: shutdown map updated")
			// Handle shutdown in a separate goroutine to allow this one to exit
			pulse.Go(ctx, func() { node.handleShutdown(ctx) })
		}
	}
}

// handleShutdown closes the node.
func (node *Node) handleShutdown(ctx context.Context) {
	if node.IsClosed() {
		return
	}
	sm := node.shutdownMap.Map()
	var requestingNode string
	for _, node := range sm {
		// There is only one value in the map
		requestingNode = node
	}
	node.logger.Debug("handleShutdown: shutting down", "requested-by", requestingNode)
	node.close(ctx, true)

	node.lock.Lock()
	node.shutdown = true
	node.lock.Unlock()
	node.logger.Info("shutdown", "requested-by", requestingNode)
}

// processInactiveNodes periodically checks for inactive nodes and destroys their streams.
func (node *Node) processInactiveNodes(ctx context.Context) {
	defer node.wg.Done()
	ticker := time.NewTicker(node.workerTTL)
	defer ticker.Stop()

	for {
		select {
		case <-node.stop:
			return
		case <-ticker.C:
			node.cleanupInactiveNodes(ctx)
		}
	}
}

func (node *Node) cleanupInactiveNodes(ctx context.Context) {
	nodeMap := node.nodeKeepAliveMap.Map()
	for nodeID, lastSeen := range nodeMap {
		if nodeID == node.ID || node.isActive(lastSeen, node.workerTTL) {
			continue
		}

		node.logger.Info("cleaning up inactive node", "node", nodeID)

		// Clean up node's stream
		stream := nodeStreamName(node.PoolName, nodeID)
		if s, err := streaming.NewStream(stream, node.rdb, soptions.WithStreamLogger(node.logger)); err == nil {
			if err := s.Destroy(ctx); err != nil {
				node.logger.Error(fmt.Errorf("cleanupInactiveNodes: failed to destroy stream: %w", err))
			}
		}

		// Remove from keep-alive map
		if _, err := node.nodeKeepAliveMap.Delete(ctx, nodeID); err != nil {
			node.logger.Error(fmt.Errorf("cleanupInactiveNodes: failed to delete node: %w", err))
		}
	}
}

// processInactiveWorkers periodically checks for inactive workers and requeues their jobs.
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

func (node *Node) cleanupInactiveWorkers(ctx context.Context) {
	alive := node.workerKeepAliveMap.Map()
	for id, ls := range alive {
		lsi, err := strconv.ParseInt(ls, 10, 64)
		if err != nil {
			node.logger.Error(fmt.Errorf("cleanupInactiveWorkers: failed to parse last seen timestamp: %w", err), "worker", id)
			continue
		}
		lastSeen := time.Unix(0, lsi)
		lsd := time.Since(lastSeen)
		if lsd <= node.workerTTL {
			continue
		}
		node.logger.Debug("cleanupInactiveWorkers: removing worker", "worker", id, "last-seen", lsd, "ttl", node.workerTTL)

		// Use optimistic locking to set the keep-alive timestamp to a value
		// in the future so that another node does not also requeue the jobs.
		next := lsi + node.workerTTL.Nanoseconds()
		last, err := node.workerKeepAliveMap.TestAndSet(ctx, id, ls, strconv.FormatInt(next, 10))
		if err != nil {
			node.logger.Error(fmt.Errorf("cleanupInactiveWorkers: failed to set keep-alive timestamp: %w", err), "worker", id)
			continue
		}
		if last != ls {
			node.logger.Debug("cleanupInactiveWorkers: keep-alive timestamp for worker already set by another node", "worker", id)
			continue
		}

		keys, ok := node.jobsMap.GetValues(id)
		if !ok {
			// Worker has no jobs, so delete it right away.
			if err := node.deleteWorker(ctx, id); err != nil {
				node.logger.Error(fmt.Errorf("cleanupInactiveWorkers: failed to delete worker %q: %w", id, err), "worker", id)
			}
			continue
		}
		requeued := make(map[string]chan error)
		for _, key := range keys {
			payload, ok := node.JobPayload(key)
			if !ok {
				node.logger.Error(fmt.Errorf("cleanupInactiveWorkers: failed to get job payload for %q: %w", key, err), "worker", id)
				continue
			}
			job := &Job{
				Key:       key,
				Payload:   []byte(payload),
				CreatedAt: time.Now(),
				NodeID:    node.ID,
			}
			cherr, err := node.requeueJob(ctx, id, job)
			if err != nil {
				node.logger.Error(fmt.Errorf("cleanupInactiveWorkers: failed to requeue inactive job: %w", err), "job", job.Key, "worker", id)
				continue
			}
			requeued[job.Key] = cherr
		}

		allRequeued := len(requeued) == len(keys)
		if !allRequeued {
			node.logger.Error(fmt.Errorf("cleanupInactiveWorkers: failed to requeue inactive jobs: %d/%d, will retry later", len(requeued), len(keys)), "worker", id)
		}
		if len(requeued) > 0 {
			pulse.Go(ctx, func() { node.processRequeuedJobs(ctx, id, requeued, allRequeued) })
		}
	}
}

// isActive checks if a timestamp is within TTL
func (node *Node) isActive(lastSeen string, ttl time.Duration) bool {
	lsi, err := strconv.ParseInt(lastSeen, 10, 64)
	if err != nil {
		node.logger.Error(fmt.Errorf("isActive: failed to parse last seen timestamp: %w", err))
		return false
	}
	return time.Since(time.Unix(0, lsi)) <= ttl
}

// Keep node alive
func (node *Node) updateNodeKeepAlive(ctx context.Context) {
	defer node.wg.Done()
	ticker := time.NewTicker(node.workerTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-node.stop:
			return
		case <-ticker.C:
			if _, err := node.nodeKeepAliveMap.Set(ctx, node.ID,
				strconv.FormatInt(time.Now().UnixNano(), 10)); err != nil {
				node.logger.Error(fmt.Errorf("updateNodeKeepAlive: failed to update timestamp: %w", err))
			}
		}
	}
}

// processRequeuedJobs processes the requeued jobs concurrently.
func (node *Node) processRequeuedJobs(ctx context.Context, id string, requeued map[string]chan error, deleteWorker bool) {
	var wg sync.WaitGroup
	var succeeded int64
	for key, cherr := range requeued {
		wg.Add(1)
		pulse.Go(ctx, func() {
			defer wg.Done()
			select {
			case err := <-cherr:
				if err != nil {
					node.logger.Error(fmt.Errorf("processRequeuedJobs: failed to requeue job: %w", err), "job", key, "worker", id)
					return
				}
				atomic.AddInt64(&succeeded, 1)
			case <-time.After(node.workerTTL):
				node.logger.Error(fmt.Errorf("processRequeuedJobs: timeout waiting for requeue result"), "job", key, "worker", id, "timout", node.workerTTL)
			}
		})
	}
	wg.Wait()

	if succeeded != int64(len(requeued)) {
		node.logger.Error(fmt.Errorf("processRequeuedJobs: failed to requeue all inactive jobs: %d/%d, will retry later", succeeded, len(requeued)), "worker", id)
		return
	}

	node.logger.Info("requeued worker jobs", "worker", id, "requeued", len(requeued))
	if deleteWorker {
		if err := node.deleteWorker(ctx, id); err != nil {
			node.logger.Error(fmt.Errorf("processRequeuedJobs: failed to delete worker %q: %w", id, err), "worker", id)
		}
	}
}

// activeWorkers returns the IDs of the active workers in the pool.
func (node *Node) activeWorkers() []string {
	workers := node.workerMap.Map()
	workerCreatedAtByID := make(map[string]int64)
	var sortedIDs []string
	for id, createdAt := range workers {
		if createdAt == "-" {
			continue // worker is in the process of being removed
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
		lsi, err := strconv.ParseInt(ls, 10, 64)
		if err != nil {
			node.logger.Error(fmt.Errorf("activeWorkers: failed to parse last seen timestamp for worker %q: %w", id, err))
			continue
		}
		if time.Since(time.Unix(0, lsi)) <= node.workerTTL {
			activeIDs = append(activeIDs, id)
		}
	}

	return activeIDs
}

// deleteWorker removes a remote worker from the pool deleting the worker stream.
func (node *Node) deleteWorker(ctx context.Context, id string) error {
	node.logger.Debug("deleteWorker: deleting worker", "worker", id)
	if _, err := node.workerKeepAliveMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("deleteWorker: failed to delete worker %q from keep-alive map: %w", id, err))
	}
	if _, err := node.workerMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("deleteWorker: failed to delete worker %q from workers map: %w", id, err))
	}
	if _, err := node.jobsMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("deleteWorker: failed to delete worker %q from jobs map: %w", id, err))
	}
	stream, err := node.workerStream(ctx, id)
	if err != nil {
		return fmt.Errorf("deleteWorker: failed to retrieve worker stream for %q: %w", id, err)
	}
	if err := stream.Destroy(ctx); err != nil {
		node.logger.Error(fmt.Errorf("deleteWorker: failed to delete worker stream: %w", err))
	}
	return nil
}

// workerStream retrieves the stream for a worker. It caches the result in the
// workerStreams map. Caller is responsible for locking.
func (node *Node) workerStream(_ context.Context, id string) (*streaming.Stream, error) {
	val, ok := node.workerStreams.Load(id)
	if !ok {
		s, err := streaming.NewStream(workerStreamName(id), node.rdb, soptions.WithStreamLogger(node.logger))
		if err != nil {
			return nil, fmt.Errorf("workerStream: failed to retrieve stream for worker %q: %w", id, err)
		}
		node.workerStreams.Store(id, s)
		return s, nil
	}
	return val.(*streaming.Stream), nil
}

// cleanup removes the worker from all pool maps.
func (node *Node) cleanupWorker(ctx context.Context, id string) {
	if _, err := node.workerMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("failed to remove worker %s from worker map: %w", id, err))
	}
	if _, err := node.workerKeepAliveMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("failed to remove worker %s from keep alive map: %w", id, err))
	}
	if _, err := node.jobsMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("failed to remove worker %s from jobs map: %w", id, err))
	}
	node.workerStreams.Delete(id)
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
		pulse.Go(ctx, func() {
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

// cleanupPool removes the pool resources from Redis.
func (node *Node) cleanupPool(ctx context.Context) {
	for _, m := range node.maps() {
		if m != nil {
			if err := m.Reset(ctx); err != nil {
				node.logger.Error(fmt.Errorf("cleanupPool: failed to reset map: %w", err))
			}
		}
	}
	if err := node.poolStream.Destroy(ctx); err != nil {
		node.logger.Error(fmt.Errorf("cleanupPool: failed to destroy pool stream: %w", err))
	}
}

// cleanupNode closes the node resources.
func (node *Node) cleanupNode(ctx context.Context) {
	for _, m := range node.maps() {
		if m != nil {
			m.Close()
		}
	}
	if err := node.nodeStream.Destroy(ctx); err != nil {
		node.logger.Error(fmt.Errorf("cleanupNode: failed to destroy node stream: %w", err))
	}
}

// maps returns the maps managed by the node.
func (node *Node) maps() []*rmap.Map {
	return []*rmap.Map{
		node.jobPayloadsMap,
		node.jobsMap,
		node.nodeKeepAliveMap,
		node.workerKeepAliveMap,
		node.shutdownMap,
		node.tickerMap,
		node.workerMap,
	}
}

// Hash implements the Jump Consistent Hash algorithm.
// See https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf for details.
func (jh jumpHash) Hash(key string, numBuckets int64) int64 {
	var b int64 = -1
	var j int64

	jh.h.Reset()
	io.WriteString(jh.h, key) // nolint: errcheck
	sum := jh.h.Sum64()

	for j < numBuckets {
		b = j
		sum = sum*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((sum>>33)+1)))
	}
	return b
}

// nodeKeepAliveMapName returns the name of the replicated map used to store the
// node keep-alive timestamps.
func nodeKeepAliveMapName(pool string) string {
	return fmt.Sprintf("%s:node-keepalive", pool)
}

// workerMapName returns the name of the replicated map used to store the
// worker creation timestamps.
func workerMapName(pool string) string {
	return fmt.Sprintf("%s:workers", pool)
}

// jobsMapName returns the name of the replicated map used to store the
// jobs by worker ID.
func jobsMapName(pool string) string {
	return fmt.Sprintf("%s:jobs", pool)
}

// jobPayloadsMapName returns the name of the replicated map used to store the
// job payloads by job key.
func jobPayloadsMapName(pool string) string {
	return fmt.Sprintf("%s:job-payloads", pool)
}

// workerKeepAliveMapName returns the name of the replicated map used to store the
// worker keep-alive timestamps.
func workerKeepAliveMapName(pool string) string {
	return fmt.Sprintf("%s:worker-keepalive", pool)
}

// tickerMapName returns the name of the replicated map used to store ticker
// ticks.
func tickerMapName(pool string) string {
	return fmt.Sprintf("%s:tickers", pool)
}

// shutdownMapName returns the name of the replicated map used to store the
// worker status.
func shutdownMapName(pool string) string {
	return fmt.Sprintf("%s:shutdown", pool)
}

// poolStreamName returns the name of the stream used by pool events.
func poolStreamName(pool string) string {
	return fmt.Sprintf("%s:pool", pool)
}

// nodeStreamName returns the name of the stream used by node events.
func nodeStreamName(pool, nodeID string) string {
	return fmt.Sprintf("%s:node:%s", pool, nodeID)
}

// pendingEventKey computes the key of a pending event from a worker ID and a
// stream event ID.
func pendingEventKey(workerID, eventID string) string {
	return fmt.Sprintf("%s:%s", workerID, eventID)
}
