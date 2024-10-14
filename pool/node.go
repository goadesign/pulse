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
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"

	"goa.design/clue/log"
	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming"
	soptions "goa.design/pulse/streaming/options"
)

type (
	// Node is a pool of workers.
	Node struct {
		Name              string
		NodeID            string
		poolStream        *streaming.Stream // pool event stream for dispatching jobs
		poolSink          *streaming.Sink   // pool event sink
		nodeStream        *streaming.Stream // node event stream for receiving worker events
		nodeReader        *streaming.Reader // node event reader
		workerMap         *rmap.Map         // worker creation times by ID
		jobsMap           *rmap.Map         // jobs by worker ID
		jobPayloadsMap    *rmap.Map         // job payloads by job key
		keepAliveMap      *rmap.Map         // worker keep-alive timestamps indexed by ID
		shutdownMap       *rmap.Map         // key is node ID that requested shutdown
		tickerMap         *rmap.Map         // ticker next tick time indexed by name
		workerTTL         time.Duration     // Worker considered dead if keep-alive not updated after this duration
		workerShutdownTTL time.Duration     // Worker considered dead if not shutdown after this duration
		pendingJobTTL     time.Duration     // Job lease expires if not acked after this duration
		logger            pulse.Logger
		h                 hasher
		stop              chan struct{}  // closed when node is stopped
		wg                sync.WaitGroup // allows to wait until all goroutines exit
		rdb               *redis.Client

		lock          sync.Mutex
		localWorkers  []*Worker                    // workers created by this node
		workerStreams map[string]*streaming.Stream // worker streams indexed by ID
		pendingJobs   map[string]chan error        // channels used to send DispatchJob results, nil if event is requeued
		pendingEvents map[string]*streaming.Event  // pending events indexed by sender and event IDs
		clientOnly    bool
		closing       bool
		shutdown      bool
		closed        bool
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

// AddNode adds a new node to the pool with the given name and returns it. The
// node can be used to dispatch jobs and add new workers. A node also routes
// dispatched jobs to the proper worker and acks the corresponding events once
// the worker acks the job.
//
// The options WithClientOnly can be used to create a node that can only be used
// to dispatch jobs. Such a node does not route or process jobs in the
// background.
func AddNode(ctx context.Context, name string, rdb *redis.Client, opts ...NodeOption) (*Node, error) {
	o := parseOptions(opts...)
	logger := o.logger
	if logger == nil {
		logger = pulse.NoopLogger()
	}
	wsm, err := rmap.Join(ctx, shutdownMapName(name), rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join shutdown replicated map %q: %w", shutdownMapName(name), err)
	}
	if wsm.Len() > 0 {
		return nil, fmt.Errorf("pool %q is shutting down", name)
	}
	poolStream, err := streaming.NewStream(poolStreamName(name), rdb,
		soptions.WithStreamMaxLen(o.maxQueuedJobs),
		soptions.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create pool job stream %q: %w", poolStreamName(name), err)
	}
	nodeID := ulid.Make().String()
	var (
		wm         *rmap.Map
		jm         *rmap.Map
		jpm        *rmap.Map
		km         *rmap.Map
		tm         *rmap.Map
		poolSink   *streaming.Sink
		nodeStream *streaming.Stream
		nodeReader *streaming.Reader
	)
	if !o.clientOnly {
		wm, err = rmap.Join(ctx, workerMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool workers replicated map %q: %w", workerMapName(name), err)
		}
		jm, err = rmap.Join(ctx, jobsMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool jobs replicated map %q: %w", jobsMapName(name), err)
		}
		jpm, err = rmap.Join(ctx, jobPayloadsMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool job payloads replicated map %q: %w", jobPayloadsMapName(name), err)
		}
		km, err = rmap.Join(ctx, keepAliveMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool keep-alive replicated map %q: %w", keepAliveMapName(name), err)
		}
		tm, err = rmap.Join(ctx, tickerMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool ticker replicated map %q: %w", tickerMapName(name), err)
		}
		poolSink, err = poolStream.NewSink(ctx, "events",
			soptions.WithSinkBlockDuration(o.jobSinkBlockDuration),
			soptions.WithSinkAckGracePeriod(o.ackGracePeriod))
		if err != nil {
			return nil, fmt.Errorf("failed to create events sink for stream %q: %w", poolStreamName(name), err)
		}
	}
	nodeStream, err = streaming.NewStream(nodeStreamName(name, nodeID), rdb, soptions.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create node event stream %q: %w", nodeStreamName(name, nodeID), err)
	}
	nodeReader, err = nodeStream.NewReader(ctx, soptions.WithReaderBlockDuration(o.jobSinkBlockDuration), soptions.WithReaderStartAtOldest())
	if err != nil {
		return nil, fmt.Errorf("failed to create node event reader for stream %q: %w", nodeStreamName(name, nodeID), err)
	}

	p := &Node{
		Name:              name,
		NodeID:            nodeID,
		keepAliveMap:      km,
		workerMap:         wm,
		jobsMap:           jm,
		jobPayloadsMap:    jpm,
		shutdownMap:       wsm,
		tickerMap:         tm,
		workerStreams:     make(map[string]*streaming.Stream),
		pendingJobs:       make(map[string]chan error),
		pendingEvents:     make(map[string]*streaming.Event),
		poolStream:        poolStream,
		poolSink:          poolSink,
		nodeStream:        nodeStream,
		nodeReader:        nodeReader,
		clientOnly:        o.clientOnly,
		workerTTL:         o.workerTTL,
		workerShutdownTTL: o.workerShutdownTTL,
		pendingJobTTL:     o.pendingJobTTL,
		logger:            logger.WithPrefix("pool", name, "node", nodeID),
		h:                 jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
		stop:              make(chan struct{}),
		rdb:               rdb,
	}

	nch := nodeReader.Subscribe()

	if o.clientOnly {
		logger.Info("client-only")
		p.wg.Add(1)
		go p.handleNodeEvents(nch) // to handle job acks
		return p, nil
	}

	p.wg.Add(4)
	pch := poolSink.Subscribe()
	go p.handlePoolEvents(pch) // handleXXX handles streaming events
	go p.handleNodeEvents(nch)
	go p.manageWorkers() // manageXXX handles map updates
	go p.manageShutdown(ctx)
	return p, nil
}

// AddWorker adds a new worker to the pool and returns it. The worker starts
// processing jobs immediately. handler can optionally implement the
// NotificationHandler interface to handle notifications.
func (node *Node) AddWorker(ctx context.Context, handler JobHandler) (*Worker, error) {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing {
		return nil, fmt.Errorf("pool %q is closed", node.Name)
	}
	if node.clientOnly {
		return nil, fmt.Errorf("pool %q is client-only", node.Name)
	}
	w, err := newWorker(ctx, node, handler)
	if err != nil {
		return nil, err
	}
	node.localWorkers = append(node.localWorkers, w)
	node.workerStreams[w.ID] = w.stream
	node.logger.Info("added worker", "worker", w.ID)
	return w, nil
}

// RemoveWorker stops the worker, removes it from the pool and requeues all its
// jobs.
func (node *Node) RemoveWorker(ctx context.Context, w *Worker) error {
	node.lock.Lock()
	defer node.lock.Unlock()
	w.stopAndWait(ctx)
	w.requeueJobs(ctx)
	delete(node.workerStreams, w.ID)
	for i, w2 := range node.localWorkers {
		if w2 == w {
			node.localWorkers = append(node.localWorkers[:i], node.localWorkers[i+1:]...)
			break
		}
	}
	node.logger.Info("removed worker", "worker", w.ID)
	return nil
}

// Workers returns the list of workers running in the local node.
func (node *Node) Workers() []*Worker {
	node.lock.Lock()
	defer node.lock.Unlock()
	workers := make([]*Worker, len(node.localWorkers))
	copy(workers, node.localWorkers)
	return workers
}

// DispatchJob dispatches a job to the proper worker in the pool.
// It returns the error returned by the worker's start handler if any.
// If the context is done before the job is dispatched, the context error is returned.
func (node *Node) DispatchJob(ctx context.Context, key string, payload []byte) error {
	// Send job to pool stream.
	node.lock.Lock()
	if node.closing {
		node.lock.Unlock()
		return fmt.Errorf("pool %q is closed", node.Name)
	}
	job := marshalJob(&Job{Key: key, Payload: payload, CreatedAt: time.Now(), NodeID: node.NodeID})
	eventID, err := node.poolStream.Add(ctx, evStartJob, job)
	if err != nil {
		node.lock.Unlock()
		return fmt.Errorf("failed to add job to stream %q: %w", node.poolStream.Name, err)
	}
	cherr := make(chan error, 1)
	node.pendingJobs[eventID] = cherr
	node.lock.Unlock()

	// Wait for return status.
	select {
	case err = <-cherr:
	case <-ctx.Done():
		err = ctx.Err()
	}

	close(cherr)
	if err == nil {
		node.logger.Info("dispatched", "key", key)
	}
	return err
}

// StopJob stops the job with the given key.
func (node *Node) StopJob(ctx context.Context, key string) error {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing {
		return fmt.Errorf("pool %q is closed", node.Name)
	}
	if _, err := node.poolStream.Add(ctx, evStopJob, marshalJobKey(key)); err != nil {
		return fmt.Errorf("failed to add stop job to stream %q: %w", node.poolStream.Name, err)
	}
	node.logger.Info("stop requested", "key", key)
	return nil
}

// NotifyWorker notifies the worker that handles the job with the given key.
func (node *Node) NotifyWorker(ctx context.Context, key string, payload []byte) error {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing {
		return fmt.Errorf("pool %q is closed", node.Name)
	}
	if _, err := node.poolStream.Add(ctx, evNotify, marshalNotification(key, payload)); err != nil {
		return fmt.Errorf("failed to add notification to stream %q: %w", node.poolStream.Name, err)
	}
	node.logger.Info("notification sent", "key", key)
	return nil
}

// Shutdown stops the pool workers gracefully across all nodes. It notifies all
// workers and waits until they are completed. Shutdown prevents the pool nodes
// from creating new workers and the pool workers from accepting new jobs.
func (node *Node) Shutdown(ctx context.Context) error {
	node.lock.Lock()
	if node.closing {
		node.lock.Unlock()
		return nil
	}
	if node.clientOnly {
		node.lock.Unlock()
		return fmt.Errorf("pool %q is client-only", node.Name)
	}
	node.lock.Unlock()
	node.logger.Info("shutting down")

	// Signal all nodes to shutdown.
	if _, err := node.shutdownMap.SetAndWait(ctx, "shutdown", node.NodeID); err != nil {
		node.logger.Error(fmt.Errorf("failed to set shutdown status in shutdown map: %w", err))
	}

	<-node.stop // Wait for this node to be closed

	// Destroy the pool stream.
	if err := node.poolStream.Destroy(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to destroy pool stream: %w", err))
	}

	// Now clean up the shutdown replicated map.
	wsm, err := rmap.Join(ctx, shutdownMapName(node.Name), node.rdb, rmap.WithLogger(node.logger))
	if err != nil {
		node.logger.Error(fmt.Errorf("failed to join shutdown map for cleanup: %w", err))
	}
	if err := wsm.Reset(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to reset shutdown map: %w", err))
	}

	node.logger.Info("shutdown complete")
	return nil
}

// Close stops the pool node workers and closes the Redis connection but does
// not stop workers running in other nodes. It requeues all the jobs run by
// workers of the node.  One of Shutdown or Close should be called before the
// node is garbage collected unless it is client-only.
func (node *Node) Close(ctx context.Context) error {
	return node.close(ctx, true)
}

func (node *Node) close(ctx context.Context, requeue bool) error {
	node.lock.Lock()
	if node.closing {
		node.lock.Unlock()
		return nil
	}
	node.logger.Info("closing")
	node.closing = true

	// Need to stop workers before requeueing jobs to prevent
	// requeued jobs from being handled by this node.
	var wg sync.WaitGroup
	for _, w := range node.localWorkers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			w.stopAndWait(ctx)
		}(w)
	}
	wg.Wait()

	if requeue {
		for _, w := range node.localWorkers {
			w.requeueJobs(ctx)
		}
	}

	node.localWorkers = nil
	if !node.clientOnly {
		node.poolSink.Close()
		node.tickerMap.Close()
		node.keepAliveMap.Close()
	}
	node.nodeReader.Close()
	if err := node.nodeStream.Destroy(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to destroy node event stream: %w", err))
	}
	node.closed = true
	close(node.stop)
	node.lock.Unlock()
	node.wg.Wait()
	node.logger.Info("closed")
	return nil
}

// IsShutdown returns true if the pool is shutdown.
func (node *Node) IsShutdown() bool {
	node.lock.Lock()
	defer node.lock.Unlock()
	return node.shutdown
}

// IsClosed returns true if the node is closed.
func (node *Node) IsClosed() bool {
	node.lock.Lock()
	defer node.lock.Unlock()
	return node.closed
}

// handlePoolEvents reads events from the pool job stream.
func (node *Node) handlePoolEvents(c <-chan *streaming.Event) {
	defer node.wg.Done()
	defer node.logger.Debug("handlePoolEvents: exiting")
	ctx := context.Background()
	for ev := range c {
		if node.IsClosed() {
			node.logger.Info("ignoring event, node is closed", "event", ev.EventName, "id", ev.ID)
			continue
		}
		node.logger.Debug("routing", "event", ev.EventName, "id", ev.ID)
		if err := node.routeWorkerEvent(ctx, ev); err != nil {
			node.logger.Error(fmt.Errorf("failed to route event: %w, will retry after %v", err, node.pendingJobTTL), "event", ev.EventName, "id", ev.ID)
		}
	}
}

// routeWorkerEvent routes a dispatched event to the proper worker.
func (node *Node) routeWorkerEvent(ctx context.Context, ev *streaming.Event) error {
	node.lock.Lock()
	defer node.lock.Unlock()

	// Compute the worker ID that will handle the job.
	key := unmarshalJobKey(ev.Payload)
	activeWorkers := node.activeWorkers(ctx)
	if len(activeWorkers) == 0 {
		return fmt.Errorf("no active worker in pool %q", node.Name)
	}
	wid := activeWorkers[node.h.Hash(key, int64(len(activeWorkers)))]

	// Stream the event to the worker corresponding to the key hash.
	stream, err := node.workerStream(ctx, wid)
	if err != nil {
		return err
	}

	var eventID string
	eventID, err = stream.Add(ctx, ev.EventName, marshalEnvelope(node.NodeID, ev.Payload))
	if err != nil {
		return fmt.Errorf("failed to add event %s to worker stream %q: %w", ev.EventName, workerStreamName(wid), err)
	}
	node.logger.Debug("routed", "event", ev.EventName, "id", ev.ID, "worker", wid, "worker-event-id", eventID)

	// Record the event in the pending events map for future ack.
	node.pendingEvents[wid+":"+eventID] = ev

	return nil
}

// handleNodeEvents reads events from the node event stream and acks the pending
// events that correspond to jobs that are now running or done.
func (node *Node) handleNodeEvents(c <-chan *streaming.Event) {
	defer node.wg.Done()
	ctx := context.Background()
	for {
		select {
		case ev, ok := <-c:
			if !ok {
				node.logger.Debug("handleNodeEvents: exiting")
				return
			}
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
		case <-node.stop:
			node.logger.Debug("handleNodeEvents: received stop signal, exiting")
			node.nodeReader.Close()
			if err := node.nodeStream.Destroy(ctx); err != nil {
				node.logger.Error(fmt.Errorf("failed to destroy node event stream: %w", err))
			}
			return
		}
	}
}

// ackWorkerEvent acks the pending event that corresponds to the acked job.  If
// the event was a dispatched job then it sends a dispatch return event to the
// node that dispatched the job.
func (node *Node) ackWorkerEvent(ctx context.Context, ev *streaming.Event) {
	node.lock.Lock()
	defer node.lock.Unlock()

	workerID, payload := unmarshalEnvelope(ev.Payload)
	ack := unmarshalAck(payload)
	key := workerID + ":" + ack.EventID
	pending, ok := node.pendingEvents[key]
	if !ok {
		node.logger.Error(fmt.Errorf("received event %s from worker %s that was not dispatched", ack.EventID, workerID))
		return
	}

	// If a dispatched job then send a return event to the node that
	// dispatched the job.
	if pending.EventName == evStartJob {
		_, nodeID := unmarshalJobKeyAndNodeID(pending.Payload)
		stream, err := streaming.NewStream(nodeStreamName(node.Name, nodeID), node.rdb, soptions.WithStreamLogger(node.logger))
		if err != nil {
			node.logger.Error(fmt.Errorf("failed to create node event stream %q: %w", nodeStreamName(node.Name, nodeID), err))
			return
		}
		ack.EventID = pending.ID
		if _, err := stream.Add(ctx, evDispatchReturn, marshalAck(ack)); err != nil {
			node.logger.Error(fmt.Errorf("failed to dispatch return to stream %q: %w", nodeStreamName(node.Name, nodeID), err))
		}
	}

	// Ack the sink event so it does not get redelivered.
	if err := node.poolSink.Ack(ctx, pending); err != nil {
		node.logger.Error(fmt.Errorf("failed to ack event: %w", err), "event", pending.EventName, "id", pending.ID)
	}
	delete(node.pendingEvents, key)

	// Garbage collect stale events.
	var staleKeys []string
	for key, ev := range node.pendingEvents {
		if time.Since(ev.CreatedAt()) > 2*node.pendingJobTTL {
			staleKeys = append(staleKeys, key)
		}
	}
	for _, key := range staleKeys {
		node.logger.Error(fmt.Errorf("stale event, removing from pending events"), "event", node.pendingEvents[key].EventName, "id", key)
		delete(node.pendingEvents, key)
	}
}

// returnDispatchStatus returns the start job result to the caller.
func (node *Node) returnDispatchStatus(_ context.Context, ev *streaming.Event) {
	node.lock.Lock()
	defer node.lock.Unlock()

	ack := unmarshalAck(ev.Payload)
	cherr, ok := node.pendingJobs[ack.EventID]
	if !ok {
		node.logger.Error(fmt.Errorf("received dispatch return for unknown event"), "id", ack.EventID)
		return
	}
	node.logger.Debug("dispatch return", "event", ev.EventName, "id", ev.ID, "ack-id", ack.EventID)
	delete(node.pendingJobs, ack.EventID)
	if cherr == nil {
		// Event was requeued.
		return
	}
	var err error
	if ack.Error != "" {
		err = errors.New(ack.Error)
	}
	cherr <- err
}

// manageWorkers monitors the workers replicated map and triggers job rebalancing
// when workers are added or removed from the pool.
func (node *Node) manageWorkers() {
	defer node.wg.Done()
	defer node.workerMap.Close()

	ctx := context.Background()
	workerMapUpdates := node.workerMap.Subscribe()

	for {
		select {
		case <-workerMapUpdates:
			node.logger.Debug("manageWorkers: worker map updated")
			node.handleWorkerMapUpdate(ctx)
		case <-node.stop:
			node.logger.Debug("manageWorkers: received stop signal, exiting")
			return
		}
	}
}

// handleWorkerMapUpdate is called when the worker map is updated.
func (node *Node) handleWorkerMapUpdate(ctx context.Context) {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing {
		return
	}
	activeIDs := node.activeWorkers(ctx)
	if len(activeIDs) == 0 {
		return
	}
	for _, worker := range node.localWorkers {
		node.rebalanceWorker(ctx, worker, activeIDs)
	}
}

// rebalanceWorker rebalances the jobs handled by the worker across the active
// workers in the pool.
func (node *Node) rebalanceWorker(ctx context.Context, worker *Worker, activeIDs []string) {
	worker.lock.Lock()
	defer worker.lock.Unlock()
	if worker.stopped {
		return
	}
	numIDs := int64(len(activeIDs))
	for _, job := range worker.jobs {
		wid := activeIDs[node.h.Hash(job.Key, numIDs)]
		if wid == worker.ID {
			continue
		}
		if err := worker.handler.Stop(job.Key); err != nil {
			log.Errorf(ctx, err, "failed to stop job %q during rebalance", job.Key)
			continue
		}
		delete(worker.jobs, job.Key)
		if _, err := node.poolStream.Add(ctx, evStartJob, marshalJob(job)); err != nil {
			node.logger.Error(fmt.Errorf("failed to add job %q to stream %q: %w", job.Key, node.poolStream.Name, err))
		}
	}
}

// manageShutdown monitors the pool shutdown map and initiates node shutdown when updated.
func (node *Node) manageShutdown(ctx context.Context) {
	defer node.wg.Done()
	defer node.shutdownMap.Close()

	shutdownUpdates := node.shutdownMap.Subscribe()

	for {
		select {
		case <-shutdownUpdates:
			node.logger.Debug("manageShutdown: shutdown map updated, initiating shutdown")
			node.handleShutdownMapUpdate(ctx)
		case <-node.stop:
			node.logger.Debug("manageShutdown: received stop signal, exiting")
			return
		}
	}
}

// handleShutdownMapUpdate is called when the shutdown map is updated and closes
// the node.
func (node *Node) handleShutdownMapUpdate(ctx context.Context) {
	node.lock.Lock()
	if node.closing {
		node.lock.Unlock()
		return
	}
	node.shutdown = true
	sm := node.shutdownMap.Map()
	node.lock.Unlock()
	var requestingNode string
	for _, node := range sm {
		// There is only one value in the map
		requestingNode = node
	}
	node.logger.Info("shutdown", "requested-by", requestingNode)
	node.close(ctx, false)

	node.logger.Info("shutdown")
}

// activeWorkers returns the IDs of the active workers in the pool.
// It is the caller's responsibility to lock the node.
func (node *Node) activeWorkers(ctx context.Context) []string {
	// First, retrieve workers and sort IDs by creation time.
	workers := node.workerMap.Map()
	workerCreatedAtByID := make(map[string]int64, len(workers))
	for id, createdAt := range workers {
		cat, _ := strconv.ParseInt(createdAt, 10, 64)
		workerCreatedAtByID[id] = cat
	}
	sortedIDs := make([]string, 0, len(workerCreatedAtByID))
	for id := range workerCreatedAtByID {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Slice(sortedIDs, func(i, j int) bool {
		return workerCreatedAtByID[sortedIDs[i]] < workerCreatedAtByID[sortedIDs[j]]
	})

	// Then filter out workers that have not been seen for more than workerTTL.
	alive := node.keepAliveMap.Map()
	var activeIDs []string
	now := time.Now()
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
			node.logger.Error(fmt.Errorf("failed to parse last seen timestamp for worker %q: %w", id, err))
			continue
		}
		lastSeen := now.Sub(time.Unix(0, lsi))
		if lastSeen <= node.workerTTL {
			activeIDs = append(activeIDs, id)
			continue
		}
		node.logger.Info("deleting", "worker", id, "last seen", lastSeen, "TTL", node.workerTTL)

		// requeue the worker's keys first
		keys, _ := node.jobsMap.GetValues(id)
		var requeued []string
		for _, key := range keys {
			payload, ok := node.jobPayloadsMap.Get(key)
			if !ok {
				node.logger.Error(fmt.Errorf("payload for job %q not found", key))
				continue
			}
			job := &Job{
				Key:       key,
				Payload:   []byte(payload),
				CreatedAt: time.Now(),
				NodeID:    node.NodeID,
			}
			eventID, err := node.poolStream.Add(ctx, evStartJob, marshalJob(job))
			if err != nil {
				node.logger.Error(fmt.Errorf("failed to requeue inactive job %q: %w", job.Key, err))
				continue
			}
			node.pendingJobs[eventID] = nil
			requeued = append(requeued, job.Key)
			node.logger.Debug("requeued inactive", "job", job.Key, "event-id", eventID)
		}
		if _, err := node.jobsMap.RemoveValues(ctx, id, requeued...); err != nil {
			node.logger.Error(fmt.Errorf("failed to remove requeued job keys for worker %q: %w", id, err))
		}
		for _, key := range requeued {
			if _, err := node.jobPayloadsMap.Delete(ctx, key); err != nil {
				node.logger.Error(fmt.Errorf("failed to remove requeued job payload for job %q: %w", key, err))
			}
		}
		if len(requeued) != len(keys) {
			node.logger.Error(fmt.Errorf("failed to requeue all inactive jobs for worker %q: %d/%d, will retry later", id, len(requeued), len(keys)))
			continue
		}

		node.logger.Info("requeued worker jobs", "worker", id, "requeued", len(requeued))

		// then delete the worker
		if err := node.deleteWorker(ctx, id); err != nil {
			node.logger.Error(fmt.Errorf("failed to delete worker %q: %w", id, err))
		}
	}
	return activeIDs
}

// deleteWorker removes a worker from the pool deleting the worker stream.
func (node *Node) deleteWorker(ctx context.Context, id string) error {
	if _, err := node.keepAliveMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("failed to delete worker %q from keep-alive map: %w", id, err))
	}
	if _, err := node.workerMap.Delete(ctx, id); err != nil {
		node.logger.Error(fmt.Errorf("failed to delete worker %q from workers map: %w", id, err))
	}
	stream, err := node.workerStream(ctx, id)
	if err != nil {
		return err
	}
	if err := stream.Destroy(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to delete worker stream: %w", err))
	}
	return nil
}

// workerStream retrieves the stream for a worker. It caches the result in the
// workerStreams map. Caller is responsible for locking.
func (node *Node) workerStream(_ context.Context, id string) (*streaming.Stream, error) {
	stream, ok := node.workerStreams[id]
	if !ok {
		s, err := streaming.NewStream(workerStreamName(id), node.rdb, soptions.WithStreamLogger(node.logger))
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve stream for worker %q: %w", id, err)
		}
		node.workerStreams[id] = s
		stream = s
	}
	return stream, nil
}

// Jump Consistent Hashing, see https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
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

// keepAliveMapName returns the name of the replicated map used to store the
// worker keep-alive timestamps.
func keepAliveMapName(pool string) string {
	return fmt.Sprintf("%s:keepalive", pool)
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

// eventStreamName returns the name of the stream used by pool events.
func poolStreamName(pool string) string {
	return fmt.Sprintf("%s:pool", pool)
}

// nodeStreamName returns the name of the stream used by node events.
func nodeStreamName(pool, nodeID string) string {
	return fmt.Sprintf("%s:node:%s", pool, nodeID)
}
