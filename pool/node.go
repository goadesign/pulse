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
		Name                string
		NodeID              string
		shutdownMap         *rmap.Map         // key is node ID that requested shutdown
		keepAliveMap        *rmap.Map         // worker keep-alive timestamps indexed by ID
		workersMap          *rmap.Map         // worker creation times by ID
		pendingJobsMap      *rmap.Map         // creation timestamp and status by pending job ID
		jobStream           *streaming.Stream // pool job stream
		jobSink             *streaming.Sink   // pool job sink
		workerTTL           time.Duration     // duration after which a worker is considered dead
		leaseTTL            time.Duration     // duration after which a message lease expires
		maxQueuedJobs       int               // maximum number of jobs that can be queued
		maxShutdownDuration time.Duration     // maximum time to wait for workers to shutdown
		logger              ponos.Logger
		h                   jumpHash
		done                chan struct{}  // closed when node is stopped
		wg                  sync.WaitGroup // allows to wait until all goroutines exit
		rdb                 *redis.Client

		lock          sync.Mutex
		workers       []*Worker                    // workers created by this node
		workerStreams map[string]*streaming.Stream // worker streams indexed by ID
		pendingJobs   map[string]*streaming.Event  // pending jobs indexed by unique ID
		clientOnly    bool
		stopping      bool
		shuttingDown  bool
		stopped       bool
		shutdown      bool
	}

	// pendingJob is the value stored in the pending jobs replicated map.
	pendingJob struct {
		Key       string // job key
		CreatedAt int64  // job creation time in nanoseconds
		Done      bool   // true if the job has been processed
	}

	// jumpHash implement Jump Consistent Hash.
	jumpHash struct {
		h hash.Hash64
	}
)

const (
	// evJob is the event used to send new job to workers.
	evJob string = "j"
	// evShutdown is the event used to stop a worker.
	evShutdown string = "s"
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
	jobStream, err := streaming.NewStream(ctx, jobStreamName(name), rdb, streaming.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create pool job stream %q: %w", jobStreamName(name), err)
	}
	var (
		wm      *rmap.Map
		km      *rmap.Map
		jm      *rmap.Map
		jobSink *streaming.Sink
	)
	if !options.clientOnly {
		wm, err = rmap.Join(ctx, workersMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool workers replicated map %q: %w", workersMapName(name), err)
		}
		km, err = rmap.Join(ctx, keepAliveMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool keep-alive replicated map %q: %w", keepAliveMapName(name), err)
		}
		jm, err = rmap.Join(ctx, jobsMapName(name), rdb, rmap.WithLogger(logger))
		if err != nil {
			return nil, fmt.Errorf("failed to join pool jobs replicated map %q: %w", jobsMapName(name), err)
		}
		jobSink, err = jobStream.NewSink(ctx, "jobs")
		if err != nil {
			return nil, fmt.Errorf("failed to create jobs sink for stream %q: %w", jobStreamName(name), err)
		}
	}
	nodeID := ulid.Make().String()

	p := &Node{
		Name:                name,
		NodeID:              nodeID,
		keepAliveMap:        km,
		workersMap:          wm,
		pendingJobsMap:      jm,
		shutdownMap:         wsm,
		workerStreams:       make(map[string]*streaming.Stream),
		jobStream:           jobStream,
		jobSink:             jobSink,
		pendingJobs:         make(map[string]*streaming.Event),
		clientOnly:          options.clientOnly,
		workerTTL:           options.workerTTL,
		leaseTTL:            options.leaseTTL,
		maxQueuedJobs:       options.maxQueuedJobs,
		maxShutdownDuration: options.maxShutdownDuration,
		logger:              logger.WithPrefix("pool", name, "node", nodeID),
		h:                   jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
		done:                make(chan struct{}),
		rdb:                 rdb,
	}

	if options.clientOnly {
		logger.Info("client-only")
		p.wg.Add(1)
		go p.handleShutdown()
		return p, nil
	}

	p.wg.Add(3)
	go p.handleEvents()
	go p.managePendingJobs()
	go p.handleShutdown()
	return p, nil
}

// AddWorker adds a new worker to the pool and returns it. The worker starts
// processing jobs immediately.
func (p *Node) AddWorker(ctx context.Context, opts ...WorkerOption) (*Worker, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopping || p.shuttingDown {
		return nil, fmt.Errorf("pool %q is stopped", p.Name)
	}
	if p.clientOnly {
		return nil, fmt.Errorf("pool %q is client-only", p.Name)
	}
	w, err := newWorker(ctx, p, opts...)
	if err != nil {
		return nil, err
	}
	p.workers = append(p.workers, w)
	p.workerStreams[w.ID] = w.stream
	return w, nil
}

// DispatchJob dispatches a job to the proper worker in the pool.
func (p *Node) DispatchJob(ctx context.Context, key string, payload []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopping || p.shuttingDown {
		return fmt.Errorf("pool %q is stopped", p.Name)
	}
	job := &Job{
		Key:       key,
		Payload:   payload,
		CreatedAt: time.Now(),
	}
	if _, err := p.jobStream.Add(ctx, evJob, marshalJob(job)); err != nil {
		return fmt.Errorf("failed to add job to stream %q: %w", p.jobStream.Name, err)
	}
	return nil
}

// Shutdown causes all nodes to stop accepting new jobs. Workers handle pending
// jobs until they are processed or expire. Shutdown returns once all workers in
// all nodes have stopped.
func (p *Node) Shutdown(ctx context.Context) error {
	p.lock.Lock()
	if p.stopping {
		p.lock.Unlock()
		return fmt.Errorf("pool %q is stopped", p.Name)
	}
	if p.shuttingDown {
		p.lock.Unlock()
		return nil
	}
	if p.clientOnly {
		return fmt.Errorf("pool %q is client-only", p.Name)
	}
	p.logger.Info("requesting shutdown")
	if _, err := p.jobStream.Add(ctx, evShutdown, []byte(p.NodeID)); err != nil {
		p.lock.Unlock()
		return fmt.Errorf("failed to add shutdown event to stream %q: %w", p.jobStream.Name, err)
	}
	p.lock.Unlock()
	// Now wait for all workers to stop
	for {
		done := false
		select {
		case <-p.workersMap.C:
			if p.workersMap.Len() > 0 {
				continue
			}
			close(p.done)
			done = true
		case <-time.After(p.maxShutdownDuration):
			p.logger.Error(fmt.Errorf("failed to shutdown pool %q: timeout after %s", p.Name, p.maxShutdownDuration))
		}
		if done {
			break
		}
	}
	p.wg.Wait()
	p.lock.Lock()
	p.cleanup()
	p.stopMaps()
	p.shutdown = true
	p.lock.Unlock()
	p.logger.Info("shutdown")
	return nil
}

// Stop stops the pool node but does not stop workers running in other nodes.
// One of Shutdown or Stop must be called before the node is garbage collected
// unless it is client-only.
func (p *Node) Stop(ctx context.Context) error {
	p.lock.Lock()
	if p.shuttingDown {
		p.lock.Unlock()
		return fmt.Errorf("pool %q is shutdown", p.Name)
	}
	if p.stopping {
		p.lock.Unlock()
		return nil
	}
	p.stopping = true
	p.logger.Info("stopping")
	if !p.clientOnly {
		p.jobSink.Stop()
	}
	close(p.done)
	p.lock.Unlock()
	p.wg.Wait()
	p.lock.Lock()
	p.stopMaps()
	p.stopped = true
	p.lock.Unlock()
	p.logger.Info("stopped")
	return nil
}

// IsShutdown returns true if the node is shutdown.
func (p *Node) IsShutdown() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.shuttingDown
}

// IsStopped returns true if the node is stopped.
func (p *Node) IsStopped() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.stopped
}

// handleEvents reads events from the pool job stream. If the event is a
// dispatched job then it routes it to the appropriate worker. If the event is a
// shutdown request then it writes to the shutdown map to notify all nodes.
func (p *Node) handleEvents() {
	defer p.wg.Done()
	ctx := context.Background()
	for ev := range p.jobSink.C {
		switch ev.EventName {
		case evJob:
			if p.shutdownMap.Len() > 0 {
				p.logger.Info("ignoring job, pool is shutting down")
				continue
			}
			p.logger.Debug("routing job", "event", ev.ID)
			if err := p.routeJob(ctx, ev); err != nil {
				p.logger.Error(fmt.Errorf("failed to route job: %w", err))
			}
		case evShutdown:
			p.jobSink.Stop()
			p.logger.Info("broadcasting shutdown to all nodes")
			if _, err := p.shutdownMap.Set(ctx, "shutdown", p.NodeID); err != nil {
				p.logger.Error(fmt.Errorf("failed to set shutdown status in shutdown map: %w", err))
			}
		}
	}
}

// handleShutdown listens to the pool shutdown map and stops all the workers
// owned by the node when it is updated.
func (p *Node) handleShutdown() {
	defer p.wg.Done()
	<-p.shutdownMap.C
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopping {
		return
	}
	if p.clientOnly {
		p.shuttingDown = true
		p.shutdown = true
		if err := p.shutdownMap.Stop(); err != nil {
			p.logger.Error(fmt.Errorf("failed to stop shutdown map: %w", err))
		}
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
	for _, w := range p.workers {
		w.stream.Add(context.Background(), evShutdown, []byte(requestingNode), streaming.WithOnlyIfStreamExists())
	}
}

// routeJob routes a dispatched job to the proper worker. It records the job status as routed.
func (p *Node) routeJob(ctx context.Context, ev *streaming.Event) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	var activeIDs []string
	retries := 0
	for len(activeIDs) == 0 && retries < 10 {
		// First, retrieve workers and sort IDs by creation time.
		workers := p.workersMap.Map()
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
				p.logger.Info("deleting stale worker", "id", id, "last seen", t, "TTL", p.workerTTL)
				if err := p.deleteWorker(ctx, id); err != nil {
					p.logger.Error(fmt.Errorf("failed to delete worker %q: %w", id, err))
				}
			}
		}
		if len(activeIDs) == 0 {
			// We want to wait a bit and retry in case the workers
			// and/or keep-alive maps has not been replicated yet.
			p.logger.Info("no active worker in pool, waiting...")
			retries++
			time.Sleep(100 * time.Millisecond)
		}
	}
	if len(activeIDs) == 0 {
		return fmt.Errorf("no active worker in pool %q", p.Name)
	}
	// Record the job status in the jobs replicated map.
	key := unmarshalJobKey(ev.Payload)
	jid := ulid.Make().String()
	pjob := pendingJob{Key: key, CreatedAt: time.Now().UnixNano()}
	if _, err := p.pendingJobsMap.Set(ctx, jid, marshalPendingJob(&pjob)); err != nil {
		return fmt.Errorf("failed to set job %q in jobs map: %w", key, err)
	}

	// Finally, stream the job to the worker corresponding to the key hash.
	wid := activeIDs[p.h.Hash(key, int64(len(activeIDs)))]
	stream, err := p.workerStream(ctx, wid)
	if err != nil {
		return err
	}
	// Use OnlyIfStreamExists to avoid creating a stream for a worker that
	// just stopped. Job will get requeued.
	if _, err := stream.Add(ctx, evJob, ev.Payload, streaming.WithOnlyIfStreamExists()); err != nil {
		return fmt.Errorf("failed to add job to worker stream %q: %w", workerJobsStreamName(wid), err)
	}
	p.pendingJobs[jid] = ev

	return nil
}

// managePendingJobs received notifications from the pending jobs replicated map
// and acks jobs the pool created that are done, it then removes the job from
// the map.  Note: if a job is assigned to a worker that never processes it then
// the job stream will redeliver the job to the pool. The pool will then route
// the job to another worker. This is why we don't need to explicitely delete
// jobs from the pending jobs map after some time.
func (p *Node) managePendingJobs() {
	defer p.wg.Done()
	ctx := context.Background()
	for {
		select {
		case <-p.pendingJobsMap.C:
			p.handlePendingJobs(ctx)
		case <-p.done:
			hasPendingJobs := p.handlePendingJobs(ctx) > 0
			for hasPendingJobs {
				select {
				case <-p.pendingJobsMap.C:
					hasPendingJobs = p.handlePendingJobs(ctx) > 0
				case <-time.After(p.maxShutdownDuration):
					p.logger.Error(fmt.Errorf("failed to drain pending jobs: timeout after %s", p.maxShutdownDuration))
					hasPendingJobs = false
				}
			}
			return
		}
	}
}

// handlePengingJobs handles pending jobs notifications. It checks whether a job
// that the node routed is done and if so acks it. handlePendingJobs returns the
// number of jobs still pending for the node.
func (p *Node) handlePendingJobs(ctx context.Context) int {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.shuttingDown {
		return 0 // job sink might be gone
	}
	pending := p.pendingJobsMap.Map()
	for id, job := range p.pendingJobs {
		pj, ok := pending[id]
		if !ok {
			continue
		}
		pjob := unmarshalPendingJob(pj)
		if !pjob.Done {
			continue
		}
		delete(p.pendingJobs, id)
		if err := p.jobSink.Ack(ctx, job); err != nil {
			p.logger.Error(fmt.Errorf("failed to ack job %q: %w", id, err))
		}
		p.logger.Debug("job acked", "id", id)
	}
	return len(p.pendingJobs)
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
	if err := p.workersMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset workers map: %w", err))
	}
	if err := p.pendingJobsMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset pending jobs map: %w", err))
	}
	if err := p.jobStream.Destroy(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to destroy job stream: %w", err))
	}
	return nil
}

// stopMaps stops all replicated maps.
func (p *Node) stopMaps() {
	if err := p.shutdownMap.Stop(); err != nil {
		p.logger.Error(fmt.Errorf("failed to stop shutdown map: %w", err))
	}
	if !p.clientOnly {
		if err := p.keepAliveMap.Stop(); err != nil {
			p.logger.Error(fmt.Errorf("failed to stop keep-alive map: %w", err))
		}
		if err := p.workersMap.Stop(); err != nil {
			p.logger.Error(fmt.Errorf("failed to stop workers map: %w", err))
		}
		if err := p.pendingJobsMap.Stop(); err != nil {
			p.logger.Error(fmt.Errorf("failed to stop pending jobs map: %w", err))
		}
	}
}

// deleteWorker removes a worker from the pool deleting the worker stream.
func (p *Node) deleteWorker(ctx context.Context, id string) error {
	if _, err := p.keepAliveMap.Delete(ctx, id); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker %q from keep-alive map: %w", id, err))
	}
	if _, err := p.workersMap.Delete(ctx, id); err != nil {
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
		s, err := streaming.NewStream(ctx, workerJobsStreamName(id), p.rdb, streaming.WithStreamLogger(p.logger))
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

// keepAliveMapName returns the name of the replicated map used to store the
// worker keep-alive timestamps.
func keepAliveMapName(pool string) string {
	return fmt.Sprintf("%s:keepalive", pool)
}

// workersMapName returns the name of the replicated map used to store the
// worker creation timestamps.
func workersMapName(pool string) string {
	return fmt.Sprintf("%s:workers", pool)
}

// shutdownMapName returns the name of the replicated map used to store the
// worker status.
func shutdownMapName(pool string) string {
	return fmt.Sprintf("%s:shutdown", pool)
}

// jobsMapName returns the name of the replicated map used to store the job
// creation timestamps.
func jobsMapName(pool string) string {
	return fmt.Sprintf("%s:jobs", pool)
}

// jobStreamName returns the name of the stream used to dispatch jobs.
func jobStreamName(pool string) string {
	return fmt.Sprintf("%s:jobs", pool)
}
