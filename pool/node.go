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
	"goa.design/ponos/ponos"
	"goa.design/ponos/rmap"
	"goa.design/ponos/streaming"
	soptions "goa.design/ponos/streaming/options"
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
		keepAliveMap      *rmap.Map         // worker keep-alive timestamps indexed by ID
		shutdownMap       *rmap.Map         // key is node ID that requested shutdown
		workerTTL         time.Duration     // Worker considered dead if keep-alive not updated after this duration
		workerShutdownTTL time.Duration     // Worker considered dead if not shutdown after this duration
		pendingJobTTL     time.Duration     // Job lease expires if not acked after this duration
		logger            ponos.Logger
		h                 jumpHash
		stop              chan struct{}  // closed when node is stopped
		wg                sync.WaitGroup // allows to wait until all goroutines exit
		rdb               *redis.Client

		lock          sync.Mutex
		localWorkers  []*Worker                    // workers created by this node
		workerStreams map[string]*streaming.Stream // worker streams indexed by ID
		pendingJobs   map[string]chan error        // channels used to send DispatchJob results
		pendingEvents map[string]*streaming.Event  // pending events indexed by sender and event IDs
		clientOnly    bool
		closing       bool
		shuttingDown  bool
		closed        bool
		shutdown      bool
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
	// evShutdown is the event used to shutdown the pool.
	evShutdown string = "x"
	// evAck is the worker event used to ack a pool event.
	evAck string = "a"
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
		logger = ponos.NoopLogger()
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
		km         *rmap.Map
		poolSink   *streaming.Sink
		nodeStream *streaming.Stream
		nodeReader *streaming.Reader
	)
	if !o.clientOnly {
		wm, err = rmap.Join(ctx, workerMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool workers replicated map %q: %w", workerMapName(name), err)
		}
		km, err = rmap.Join(ctx, keepAliveMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool keep-alive replicated map %q: %w", keepAliveMapName(name), err)
		}
		poolSink, err = poolStream.NewSink(ctx, "events",
			soptions.WithSinkBlockDuration(o.jobSinkBlockDuration))
		if err != nil {
			return nil, fmt.Errorf("failed to create events sink for stream %q: %w", poolStreamName(name), err)
		}
		nodeStream, err = streaming.NewStream(nodeStreamName(name, nodeID), rdb, soptions.WithStreamLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to create node event stream %q: %w", nodeStreamName(name, nodeID), err)
		}
		nodeReader, err = nodeStream.NewReader(ctx, soptions.WithReaderBlockDuration(o.jobSinkBlockDuration), soptions.WithReaderStartAtOldest())
		if err != nil {
			return nil, fmt.Errorf("failed to create node event reader for stream %q: %w", nodeStreamName(name, nodeID), err)
		}
	}

	p := &Node{
		Name:              name,
		NodeID:            nodeID,
		keepAliveMap:      km,
		workerMap:         wm,
		shutdownMap:       wsm,
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

	if o.clientOnly {
		logger.Info("client-only")
		p.wg.Add(1)
		go p.manageShutdown()
		return p, nil
	}

	p.wg.Add(4)
	pch := poolSink.Subscribe()
	nch := nodeReader.Subscribe()
	go p.handlePoolEvents(pch) // handleXXX handles streaming events
	go p.handleNodeEvents(nch)
	go p.manageWorkers() // manageXXX handles map updates
	go p.manageShutdown()
	return p, nil
}

// AddWorker adds a new worker to the pool and returns it. The worker starts
// processing jobs immediately. handler can optionally implement the
// NotificationHandler interface to handle notifications.
func (node *Node) AddWorker(ctx context.Context, handler JobHandler) (*Worker, error) {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing || node.shuttingDown {
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
	return nil
}

// Workers returns the list of workers in the pool.
func (node *Node) Workers() []*Worker {
	node.lock.Lock()
	defer node.lock.Unlock()
	workers := make([]*Worker, len(node.localWorkers))
	copy(workers, node.localWorkers)
	return workers
}

// DispatchJob dispatches a job to the proper worker in the pool.
// It returns the error returned by the worker's start handler if any.
func (node *Node) DispatchJob(ctx context.Context, key string, payload []byte) error {
	cherr, err := node.lockAndDispatch(ctx, key, payload)
	if err != nil {
		return err
	}
	defer func() {
		node.lock.Lock()
		delete(node.pendingJobs, key)
		close(cherr)
		node.lock.Unlock()
	}()
	return <-cherr
}

// lockAndDispatch helps with locking.
func (node *Node) lockAndDispatch(ctx context.Context, key string, payload []byte) (chan error, error) {
	node.lock.Lock()
	defer node.lock.Unlock()
	if _, ok := node.pendingJobs[key]; ok {
		return nil, fmt.Errorf("job with key %q is already pending", key)
	}
	if node.closing || node.shuttingDown {
		return nil, fmt.Errorf("pool %q is closed", node.Name)
	}
	job := marshalJob(&Job{Key: key, Payload: payload, CreatedAt: time.Now()})
	if _, err := node.poolStream.Add(ctx, evStartJob, job); err != nil {
		return nil, fmt.Errorf("failed to add job to stream %q: %w", node.poolStream.Name, err)
	}
	cherr := make(chan error, 1)
	node.pendingJobs[key] = cherr
	return cherr, nil
}

// StopJob stops the job with the given key.
func (node *Node) StopJob(ctx context.Context, key string) error {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing || node.shuttingDown {
		return fmt.Errorf("pool %q is closed", node.Name)
	}
	if _, err := node.poolStream.Add(ctx, evStopJob, marshalJobKey(key)); err != nil {
		return fmt.Errorf("failed to add stop job to stream %q: %w", node.poolStream.Name, err)
	}
	return nil
}

// NotifyWorker notifies the worker that handles the job with the given key.
func (node *Node) NotifyWorker(ctx context.Context, key string, payload []byte) error {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing || node.shuttingDown {
		return fmt.Errorf("pool %q is closed", node.Name)
	}
	if _, err := node.poolStream.Add(ctx, evNotify, marshalNotification(key, payload)); err != nil {
		return fmt.Errorf("failed to add notification to stream %q: %w", node.poolStream.Name, err)
	}
	return nil
}

// Shutdown stops the pool workers gracefully across all nodes. It notifies all
// workers and waits until they are completed. Shutdown prevents the pool nodes
// from creating new workers and the pool workers from accepting new jobs.
func (node *Node) Shutdown(ctx context.Context) error {
	node.lock.Lock()
	if node.shuttingDown {
		node.lock.Unlock()
		return nil
	}
	if node.closing {
		node.lock.Unlock()
		return fmt.Errorf("pool %q is closed", node.Name)
	}
	if node.clientOnly {
		node.lock.Unlock()
		return fmt.Errorf("pool %q is client-only", node.Name)
	}
	node.logger.Info("shutting down")
	if _, err := node.poolStream.Add(ctx, evShutdown, []byte(node.NodeID)); err != nil {
		node.lock.Unlock()
		return fmt.Errorf("failed to add shutdown event to stream %q: %w", node.poolStream.Name, err)
	}
	// Copy to avoid races
	wgs := make([]*sync.WaitGroup, 0, len(node.localWorkers))
	for _, w := range node.localWorkers {
		wgs = append(wgs, &w.wg)
	}
	node.lock.Unlock()
	for _, wg := range wgs {
		wg.Wait()
	}
	close(node.stop)
	node.wg.Wait()
	node.lock.Lock()
	defer node.lock.Unlock()
	node.cleanup() // cleanup first then close maps
	node.keepAliveMap.Close()
	node.workerMap.Close()
	node.shutdownMap.Close()
	node.nodeReader.Close()
	node.nodeStream.Destroy(ctx)
	node.shutdown = true
	node.logger.Info("shutdown")
	return nil
}

// Close stops the pool node workers and closes the Redis connection but does
// not stop workers running in other nodes. It requeues all the jobs run by
// workers of the node  One of Shutdown or Close should be called before the
// node is garbage collected unless it is client-only.
func (node *Node) Close(ctx context.Context) error {
	node.lock.Lock()
	if node.shuttingDown {
		node.lock.Unlock()
		return fmt.Errorf("pool %q is shutdown", node.Name)
	}
	if node.closing {
		node.lock.Unlock()
		return nil
	}
	node.logger.Info("closing")
	node.closing = true
	for _, w := range node.localWorkers {
		go w.stopAndWait(ctx)
	}
	for _, w := range node.localWorkers {
		w.requeueJobs(ctx)
	}
	node.localWorkers = nil
	if !node.clientOnly {
		node.poolSink.Close()
		node.keepAliveMap.Close()
		node.workerMap.Close()
		node.shutdownMap.Close()
		node.nodeReader.Close()
		node.nodeStream.Destroy(ctx)
	}
	node.closed = true
	close(node.stop)
	node.lock.Unlock()
	node.wg.Wait()
	node.logger.Info("closed")
	return nil
}

// IsShutdown returns true if the node is shutdown.
func (node *Node) IsShutdown() bool {
	node.lock.Lock()
	defer node.lock.Unlock()
	return node.shuttingDown
}

// IsClosed returns true if the node is closed.
func (node *Node) IsClosed() bool {
	node.lock.Lock()
	defer node.lock.Unlock()
	return node.closed
}

// handlePoolEvents reads events from the pool job stream. If the event is a
// dispatched job then it routes it to the appropriate worker. If the event is a
// shutdown request then it writes to the shutdown map to notify all nodes.
func (node *Node) handlePoolEvents(c <-chan *streaming.Event) {
	defer node.wg.Done()
	ctx := context.Background()
	for ev := range c {
		switch ev.EventName {
		case evStartJob, evNotify, evStopJob:
			if node.IsShutdown() {
				node.logger.Info("ignoring event, pool is shutdown", "event", ev.EventName, "event-id", ev.ID)
				continue
			}
			node.logger.Debug("routing", "event", ev.EventName, "event-id", ev.ID)
			if err := node.routeWorkerEvent(ctx, ev); err != nil {
				node.logger.Error(fmt.Errorf("failed to route event: %w, will retry after %v", err, node.pendingJobTTL), "event", ev.EventName, "event-id", ev.ID)
			}
		case evShutdown:
			node.poolSink.Close() // Closes p.eventSink.C
			if _, err := node.shutdownMap.Set(ctx, "shutdown", node.NodeID); err != nil {
				node.logger.Error(fmt.Errorf("failed to set shutdown status in shutdown map: %w", err))
			}
		}
	}
}

// routeWorkerEvent routes a dispatched event to the proper worker.
func (node *Node) routeWorkerEvent(ctx context.Context, ev *streaming.Event) error {
	node.lock.Lock()
	defer node.lock.Unlock()

	// Compute the worker ID that will handle the job.
	key := unmarshalJobKey(ev.Payload)
	wid, err := node.jobWorker(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to route job %q to worker: %w", key, err)
	}

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
	node.logger.Debug("routed", "event", ev.EventName, "event-id", ev.ID, "worker", wid, "worker-event-id", eventID)

	// Record the event in the pending event replicated map.
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
				return
			}
			node.lock.Lock()
			workerID, payload := unmarshalEnvelope(ev.Payload)
			ack := unmarshalAck(payload)
			key := workerID + ":" + ack.EventID
			pending, ok := node.pendingEvents[key]
			if !ok {
				node.logger.Error(fmt.Errorf("received event %s from worker %s that was not dispatched", ack.EventID, workerID))
				node.lock.Unlock()
				continue
			}
			p, ok := node.pendingJobs[unmarshalJobKey(pending.Payload)]
			if !ok {
				node.logger.Error(fmt.Errorf("received event %s from worker %s that was not dispatched", ack.EventID, workerID))
			} else {
				var err error
				if ack.Error != "" {
					err = errors.New(ack.Error)
				}
				p <- err
			}
			if err := node.poolSink.Ack(ctx, pending); err != nil {
				node.logger.Error(fmt.Errorf("failed to ack event: %w", err), "event", pending.EventName, "event-id", pending.ID)
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
				node.logger.Error(fmt.Errorf("stale event, removing from pending events"), "event", node.pendingEvents[key].EventName, "key", key)
				delete(node.pendingEvents, key)
			}
			node.lock.Unlock()
		case <-node.stop:
			node.nodeReader.Close()
			node.nodeStream.Destroy(ctx)
			return
		}
	}
}

// manageWorkers receives notifications from the workers replicated map and
// rebalances jobs across workers when a new worker is added or removed.
// TBD: what to do if requeue fails?
func (node *Node) manageWorkers() {
	defer node.wg.Done()
	ctx := context.Background()
	c := node.workerMap.Subscribe()
	for {
		select {
		case <-c:
			node.handleWorkerMapUpdate(ctx)
		case <-node.stop:
			return
		}
	}
}

// handleWorkerMapUpdate is called when the worker map is updated.
func (node *Node) handleWorkerMapUpdate(ctx context.Context) {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing || node.shuttingDown {
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

// manageShutdown listens to the pool shutdown map and stops all the workers
// owned by the node when it is updated.
func (node *Node) manageShutdown() {
	defer node.wg.Done()
	c := node.shutdownMap.Subscribe()
	for {
		select {
		case <-c:
			node.handleShutdownMapUpdate()
		case <-node.stop:
			return
		}
	}
}

// handleShutdownMapUpdate is called when the shutdown map is updated.
func (node *Node) handleShutdownMapUpdate() {
	node.lock.Lock()
	defer node.lock.Unlock()
	if node.closing || node.shuttingDown {
		return
	}
	node.shuttingDown = true
	if node.clientOnly {
		node.shutdown = true
		node.shutdownMap.Close()
		return
	}
	sm := node.shutdownMap.Map()
	var requestingNode string
	for _, node := range sm {
		// There is only one value in the map
		requestingNode = node
	}
	node.logger.Info("shutdown", "requested-by", requestingNode)
	for _, w := range node.localWorkers {
		// Add to stream instead of calling stop directly to ensure that the
		// worker is stopped only after all pending events have been processed.
		w.stream.Add(context.Background(), evShutdown, []byte(requestingNode))
	}
}

// jobWorker returns the ID of the worker that handles the job with the given key.
// It is the caller's responsibility to lock the node.
func (node *Node) jobWorker(ctx context.Context, key string) (string, error) {
	activeWorkers := node.activeWorkers(ctx)
	if len(activeWorkers) == 0 {
		return "", fmt.Errorf("no active worker in pool %q", node.Name)
	}
	return activeWorkers[node.h.Hash(key, int64(len(activeWorkers)))], nil
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
	for _, id := range sortedIDs {
		a, ok := alive[id]
		if !ok {
			// This could happen if a worker is removed from the
			// pool and the last seen map deletion replicates before
			// the workers map deletion.
			continue
		}
		nanos, _ := strconv.ParseInt(a, 10, 64)
		t := time.Unix(0, nanos)
		horizon := t.Add(node.workerTTL)
		if horizon.After(time.Now()) {
			activeIDs = append(activeIDs, id)
		} else {
			node.logger.Info("deleting", "worker", id, "last seen", t, "TTL", node.workerTTL)
			if err := node.deleteWorker(ctx, id); err != nil {
				node.logger.Error(fmt.Errorf("failed to delete worker %q: %w", id, err))
			}
		}
	}
	return activeIDs
}

// Delete all the Redis keys used by the pool.
func (node *Node) cleanup() error {
	ctx := context.Background()
	if err := node.shutdownMap.Reset(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to delete shutdown map: %w", err))
	}
	if err := node.keepAliveMap.Reset(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to reset keep-alive map: %w", err))
	}
	if err := node.workerMap.Reset(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to reset workers map: %w", err))
	}
	if err := node.poolStream.Destroy(ctx); err != nil {
		node.logger.Error(fmt.Errorf("failed to destroy job stream: %w", err))
	}
	return nil
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
func (node *Node) workerStream(ctx context.Context, id string) (*streaming.Stream, error) {
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
	io.WriteString(jh.h, key)
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

// keepAliveMapName returns the name of the replicated map used to store the
// worker keep-alive timestamps.
func keepAliveMapName(pool string) string {
	return fmt.Sprintf("%s:keepalive", pool)
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
