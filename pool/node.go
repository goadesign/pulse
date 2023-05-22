package pool

import (
	"context"
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

	"goa.design/ponos/ponos"
	"goa.design/ponos/rmap"
	"goa.design/ponos/streaming"
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
		done              chan struct{}  // closed when node is stopped
		wg                sync.WaitGroup // allows to wait until all goroutines exit
		rdb               *redis.Client

		lock          sync.Mutex
		localWorkers  []*Worker                    // workers created by this node
		workerStreams map[string]*streaming.Stream // worker streams indexed by ID
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
// node can be used to dispatch jobs and to add new workers. A node also routes
// dispatched jobs to the proper worker and acks the corresponding events once
// the worker acks the job.
//
// The options WithClientOnly can be used to create a node that can only be used
// to dispatch jobs. Such a node does not route or process jobs in the
// background.
func AddNode(ctx context.Context, name string, rdb *redis.Client, opts ...PoolOption) (*Node, error) {
	options := defaultPoolOptions()
	for _, opt := range opts {
		opt(options)
	}
	logger := options.logger
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
	poolStream, err := streaming.NewStream(ctx, poolStreamName(name), rdb,
		streaming.WithStreamMaxLen(options.maxQueuedJobs),
		streaming.WithStreamLogger(logger))
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
	if !options.clientOnly {
		wm, err = rmap.Join(ctx, workerMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool workers replicated map %q: %w", workerMapName(name), err)
		}
		km, err = rmap.Join(ctx, keepAliveMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool keep-alive replicated map %q: %w", keepAliveMapName(name), err)
		}
		poolSink, err = poolStream.NewSink(ctx, "events",
			streaming.WithSinkBlockDuration(options.jobSinkBlockDuration))
		if err != nil {
			return nil, fmt.Errorf("failed to create events sink for stream %q: %w", poolStreamName(name), err)
		}
		nodeStream, err = streaming.NewStream(ctx, nodeStreamName(name, nodeID), rdb, streaming.WithStreamLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to create node event stream %q: %w", nodeStreamName(name, nodeID), err)
		}
		nodeReader, err = nodeStream.NewReader(ctx, streaming.WithReaderBlockDuration(options.jobSinkBlockDuration), streaming.WithReaderStartAtOldest())
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
		pendingEvents:     make(map[string]*streaming.Event),
		poolStream:        poolStream,
		poolSink:          poolSink,
		nodeStream:        nodeStream,
		nodeReader:        nodeReader,
		clientOnly:        options.clientOnly,
		workerTTL:         options.workerTTL,
		workerShutdownTTL: options.workerShutdownTTL,
		pendingJobTTL:     options.pendingJobTTL,
		logger:            logger.WithPrefix("pool", name, "node", nodeID),
		h:                 jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
		done:              make(chan struct{}),
		rdb:               rdb,
	}

	if options.clientOnly {
		logger.Info("client-only")
		p.wg.Add(1)
		go p.manageShutdown()
		return p, nil
	}

	p.wg.Add(4)
	go p.handlePoolEvents() // handleXXX handles streaming events
	go p.handleNodeEvents()
	go p.manageWorkers() // manageXXX handles map updates
	go p.manageShutdown()
	return p, nil
}

// AddWorker adds a new worker to the pool and returns it. The worker starts
// processing jobs immediately. handler can optionally implement the
// NotificationHandler interface to handle notifications.
func (p *Node) AddWorker(ctx context.Context, handler JobHandler, opts ...WorkerOption) (*Worker, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closing || p.shuttingDown {
		return nil, fmt.Errorf("pool %q is closed", p.Name)
	}
	if p.clientOnly {
		return nil, fmt.Errorf("pool %q is client-only", p.Name)
	}
	w, err := newWorker(ctx, p, handler, opts...)
	if err != nil {
		return nil, err
	}
	p.localWorkers = append(p.localWorkers, w)
	p.workerStreams[w.ID] = w.stream
	return w, nil
}

// RemoveWorker stops the worker, removes it from the pool and requeues all its
// jobs.
func (p *Node) RemoveWorker(ctx context.Context, w *Worker) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	w.stopAndWait(ctx)
	w.requeueJobs(ctx)
	delete(p.workerStreams, w.ID)
	for i, w2 := range p.localWorkers {
		if w2 == w {
			p.localWorkers = append(p.localWorkers[:i], p.localWorkers[i+1:]...)
			break
		}
	}
	return nil
}

// Workers returns the list of workers in the pool.
func (p *Node) Workers() []*Worker {
	p.lock.Lock()
	defer p.lock.Unlock()
	workers := make([]*Worker, len(p.localWorkers))
	copy(workers, p.localWorkers)
	return workers
}

// DispatchJob dispatches a job to the proper worker in the pool.
func (p *Node) DispatchJob(ctx context.Context, key string, payload []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closing || p.shuttingDown {
		return fmt.Errorf("pool %q is closed", p.Name)
	}
	job := marshalJob(&Job{Key: key, Payload: payload, CreatedAt: time.Now()})
	if _, err := p.poolStream.Add(ctx, evStartJob, job); err != nil {
		return fmt.Errorf("failed to add job to stream %q: %w", p.poolStream.Name, err)
	}
	return nil
}

// StopJob stops the job with the given key.
func (p *Node) StopJob(ctx context.Context, key string) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closing || p.shuttingDown {
		return fmt.Errorf("pool %q is closed", p.Name)
	}
	if _, err := p.poolStream.Add(ctx, evStopJob, marshalJobKey(key)); err != nil {
		return fmt.Errorf("failed to add stop job to stream %q: %w", p.poolStream.Name, err)
	}
	return nil
}

// NotifyWorker notifies the worker that handles the job with the given key.
func (p *Node) NotifyWorker(ctx context.Context, key string, payload []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closing || p.shuttingDown {
		return fmt.Errorf("pool %q is closed", p.Name)
	}
	if _, err := p.poolStream.Add(ctx, evNotify, marshalNotification(key, payload)); err != nil {
		return fmt.Errorf("failed to add notification to stream %q: %w", p.poolStream.Name, err)
	}
	return nil
}

// Shutdown stops the pool workers gracefully across all nodes. It notifies all
// workers and waits until they are completed. Shutdown prevents the pool nodes
// from creating new workers and the pool workers from accepting new jobs.
func (p *Node) Shutdown(ctx context.Context) error {
	p.lock.Lock()
	if p.shuttingDown {
		p.lock.Unlock()
		return nil
	}
	if p.closing {
		p.lock.Unlock()
		return fmt.Errorf("pool %q is closed", p.Name)
	}
	if p.clientOnly {
		p.lock.Unlock()
		return fmt.Errorf("pool %q is client-only", p.Name)
	}
	p.logger.Info("shutting down")
	if _, err := p.poolStream.Add(ctx, evShutdown, []byte(p.NodeID)); err != nil {
		p.lock.Unlock()
		return fmt.Errorf("failed to add shutdown event to stream %q: %w", p.poolStream.Name, err)
	}
	// Copy to avoid races
	wgs := make([]*sync.WaitGroup, 0, len(p.localWorkers))
	for _, w := range p.localWorkers {
		wgs = append(wgs, &w.wg)
	}
	p.lock.Unlock()
	for _, wg := range wgs {
		wg.Wait()
	}
	close(p.done)
	p.wg.Wait()
	p.lock.Lock()
	p.cleanup()
	p.closeMaps()
	p.shutdown = true
	p.lock.Unlock()
	p.logger.Info("shutdown")
	return nil
}

// Close stops the pool node workers and closes the Redis connection but does
// not stop workers running in other nodes. It requeues all the jobs run by
// workers of the node  One of Shutdown or Close should be called before the
// node is garbage collected unless it is client-only.
func (p *Node) Close(ctx context.Context) error {
	p.lock.Lock()
	if p.shuttingDown {
		p.lock.Unlock()
		return fmt.Errorf("pool %q is shutdown", p.Name)
	}
	if p.closing {
		p.lock.Unlock()
		return nil
	}
	p.logger.Info("closing")
	p.closing = true
	for _, w := range p.localWorkers {
		go w.stopAndWait(ctx)
	}
	for _, w := range p.localWorkers {
		w.requeueJobs(ctx)
	}
	p.localWorkers = nil
	if !p.clientOnly {
		p.poolSink.Close()
	}
	p.closeMaps()
	p.closed = true
	close(p.done)
	p.lock.Unlock()
	p.wg.Wait()
	p.logger.Info("closed")
	return nil
}

// IsShutdown returns true if the node is shutdown.
func (p *Node) IsShutdown() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.shuttingDown
}

// IsClosed returns true if the node is closed.
func (p *Node) IsClosed() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.closed
}

// handlePoolEvents reads events from the pool job stream. If the event is a
// dispatched job then it routes it to the appropriate worker. If the event is a
// shutdown request then it writes to the shutdown map to notify all nodes.
func (p *Node) handlePoolEvents() {
	defer p.wg.Done()
	ctx := context.Background()
	for ev := range p.poolSink.C {
		switch ev.EventName {
		case evStartJob, evNotify, evStopJob:
			if p.IsShutdown() {
				p.logger.Info("ignoring event, pool is shutdown", "event", ev.EventName, "event-id", ev.ID)
				continue
			}
			p.logger.Debug("routing", "event", ev.EventName, "event-id", ev.ID)
			if err := p.routePoolEvent(ctx, ev); err != nil {
				p.logger.Error(fmt.Errorf("failed to route event: %w", err), "event", ev.EventName, "event-id", ev.ID)
			}
		case evShutdown:
			p.poolSink.Close() // Closes p.eventSink.C
			if _, err := p.shutdownMap.Set(ctx, "shutdown", p.NodeID); err != nil {
				p.logger.Error(fmt.Errorf("failed to set shutdown status in shutdown map: %w", err))
			}
		}
	}
}

// routePoolEvent routes a dispatched event to the proper worker.
func (p *Node) routePoolEvent(ctx context.Context, ev *streaming.Event) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Compute the worker ID that will handle the job.
	key := unmarshalJobKey(ev.Payload)

	// Retry a few times in case a worker is currently stopping, has closed
	// its stream but the replicated map hasn't been updated yet.
	retries := 0
	var err error
	for retries < 10 {
		wid, err := p.jobWorker(ctx, key)
		if err != nil {
			// Don't retry, jobWorker already retried.
			return fmt.Errorf("failed to route job %q to worker: %w", key, err)
		}

		// Stream the event to the worker corresponding to the key hash.
		stream, err := p.workerStream(ctx, wid)
		if err != nil {
			return err
		}
		var eventID string
		eventID, err = stream.Add(ctx, ev.EventName, marshalEnvelope(p.NodeID, ev.Payload))
		if err != nil {
			p.logger.Error(fmt.Errorf("failed to add event %s to worker stream %q, attempt %d: %w", ev.EventName, workerStreamName(wid), retries+1, err))
			retries++
			continue
		}
		p.logger.Debug("routed", "event", ev.EventName, "event-id", ev.ID, "worker", wid, "worker-event-id", eventID)

		// Record the event in the pending event replicated map.
		p.pendingEvents[wid+":"+eventID] = ev

		return nil
	}
	return fmt.Errorf("failed to route job %q to worker after %d retries: %w", key, retries, err)
}

// handleNodeEvents reads events from the node event stream and acks the pending
// events that correspond to jobs that are now running or done.
func (p *Node) handleNodeEvents() {
	defer p.wg.Done()
	ctx := context.Background()
	for {
		select {
		case ev := <-p.nodeReader.C:
			p.lock.Lock()
			workerID, payload := unmarshalEnvelope(ev.Payload)
			eventID := string(payload)
			key := workerID + ":" + eventID
			pending, ok := p.pendingEvents[key]
			if !ok {
				p.logger.Error(fmt.Errorf("received event %s from worker %s that was not dispatched", eventID, workerID))
				p.lock.Unlock()
				continue
			}
			if err := p.poolSink.Ack(ctx, pending); err != nil {
				p.logger.Error(fmt.Errorf("failed to ack event: %w", err), "event", pending.EventName, "event-id", pending.ID)
			}
			delete(p.pendingEvents, key)

			// Garbage collect stale events.
			var staleKeys []string
			for key, ev := range p.pendingEvents {
				if time.Since(ev.CreatedAt()) > 2*p.pendingJobTTL {
					staleKeys = append(staleKeys, key)
				}
			}
			for _, key := range staleKeys {
				p.logger.Error(fmt.Errorf("stale event, removing from pending events"), "event", p.pendingEvents[key].EventName, "key", key)
				delete(p.pendingEvents, key)
			}
			p.lock.Unlock()
		case <-p.done:
			p.nodeReader.Close()
			p.nodeStream.Destroy(ctx)
			return
		}
	}
}

// manageWorkers receives notifications from the workers replicated map and
// rebalances jobs across workers when a new worker is added or removed.
// TBD: what to do if requeue fails?
func (p *Node) manageWorkers() {
	defer p.wg.Done()
	ctx := context.Background()
	for range p.workerMap.Subscribe() {
		p.lock.Lock()
		if p.closing || p.shuttingDown {
			p.lock.Unlock()
			return
		}
		activeIDs := p.activeWorkers(ctx)
		if len(activeIDs) == 0 {
			p.lock.Unlock()
			continue
		}
		numIDs := int64(len(activeIDs))
		for _, worker := range p.localWorkers {
			for _, job := range worker.jobs {
				wid := activeIDs[p.h.Hash(job.Key, numIDs)]
				if wid == worker.ID {
					continue
				}
				if err := worker.stopJob(ctx, job.Key); err != nil {
					p.logger.Error(fmt.Errorf("failed to stop job %q for rebalance: %w", job.Key, err))
					continue
				}
				if _, err := p.poolStream.Add(ctx, evStartJob, marshalJob(job)); err != nil {
					p.logger.Error(fmt.Errorf("failed to add job %q to stream %q: %w", job.Key, p.poolStream.Name, err))
				}
			}
		}
		p.lock.Unlock()
	}
}

// manageShutdown listens to the pool shutdown map and stops all the workers
// owned by the node when it is updated.
func (p *Node) manageShutdown() {
	defer p.wg.Done()
	<-p.shutdownMap.Subscribe()
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closing {
		return
	}
	if p.clientOnly {
		p.shuttingDown = true
		p.shutdown = true
		p.shutdownMap.Close()
		return
	}
	p.shuttingDown = true
	sm := p.shutdownMap.Map()
	var requestingNode string
	for _, node := range sm {
		// There is only one value in the map
		requestingNode = node
	}
	p.logger.Info("shutdown", "requested-by", requestingNode)
	for _, w := range p.localWorkers {
		// Add to stream instead of calling stop directly to ensure that the
		// worker is stopped only after all pending events have been processed.
		w.stream.Add(context.Background(), evShutdown, []byte(requestingNode))
	}
}

// jobWorker returns the ID of the worker that handles the job with the given key.
// It is the caller's responsibility to lock the node.
func (p *Node) jobWorker(ctx context.Context, key string) (string, error) {
	activeWorkers := p.activeWorkers(ctx)
	if len(activeWorkers) == 0 {
		return "", fmt.Errorf("no active worker in pool %q", p.Name)
	}
	return activeWorkers[p.h.Hash(key, int64(len(activeWorkers)))], nil
}

// activeWorkers returns the IDs of the active workers in the pool.
// It is the caller's responsibility to lock the node.
func (p *Node) activeWorkers(ctx context.Context) []string {
	const (
		// maxRouteRetries is the maximum number of times a node retries to route a
		// job to a worker. The retries are necessary to handle the case where a
		// worker is added to the pool and the workers and keep-alive maps have not
		// been replicated yet.
		maxRouteRetries = 10
		// retryRouteDelay is the delay between route retries.
		retryRouteDelay = 100 * time.Millisecond
	)
	var activeIDs []string
	retries := 0
	wasMissing := false
	for len(activeIDs) == 0 && retries < maxRouteRetries {
		// First, retrieve workers and sort IDs by creation time.
		workers := p.workerMap.Map()
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
		alive := p.keepAliveMap.Map()
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
			horizon := t.Add(p.workerTTL)
			if horizon.After(time.Now()) {
				activeIDs = append(activeIDs, id)
			} else {
				p.logger.Info("deleting", "worker", id, "last seen", t, "TTL", p.workerTTL)
				if err := p.deleteWorker(ctx, id); err != nil {
					p.logger.Error(fmt.Errorf("failed to delete worker %q: %w", id, err))
				}
			}
		}
		if len(activeIDs) == 0 {
			// We want to wait a bit and retry in case the workers
			// and/or keep-alive maps has not been replicated yet.
			p.logger.Info("no active worker in pool, waiting...")
			wasMissing = true
			retries++
			time.Sleep(retryRouteDelay)
		} else if wasMissing {
			p.logger.Info("active workers found")
		}
	}
	return activeIDs
}

// Delete all the Redis keys used by the pool.
func (p *Node) cleanup() error {
	ctx := context.Background()
	if err := p.shutdownMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete shutdown map: %w", err))
	}
	if err := p.keepAliveMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset keep-alive map: %w", err))
	}
	if err := p.workerMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset workers map: %w", err))
	}
	if err := p.poolStream.Destroy(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to destroy job stream: %w", err))
	}
	return nil
}

// closeMaps stops all replicated maps.
func (p *Node) closeMaps() {
	p.shutdownMap.Close()
	if !p.clientOnly {
		p.keepAliveMap.Close()
		p.workerMap.Close()
	}
}

// deleteWorker removes a worker from the pool deleting the worker stream.
func (p *Node) deleteWorker(ctx context.Context, id string) error {
	if _, err := p.keepAliveMap.Delete(ctx, id); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker %q from keep-alive map: %w", id, err))
	}
	if _, err := p.workerMap.Delete(ctx, id); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker %q from workers map: %w", id, err))
	}
	stream, err := p.workerStream(ctx, id)
	if err != nil {
		return err
	}
	if err := stream.Destroy(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker stream: %w", err))
	}
	return nil
}

// workerStream retrieves the stream for a worker. It caches the result in the
// workerStreams map. Caller is responsible for locking.
func (p *Node) workerStream(ctx context.Context, id string) (*streaming.Stream, error) {
	stream, ok := p.workerStreams[id]
	if !ok {
		s, err := streaming.NewStream(ctx, workerStreamName(id), p.rdb, streaming.WithStreamLogger(p.logger))
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve stream for worker %q: %w", id, err)
		}
		p.workerStreams[id] = s
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
