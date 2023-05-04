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

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"

	"goa.design/ponos/ponos"
	"goa.design/ponos/rmap"
	"goa.design/ponos/streaming"
)

type (
	// WorkerPool is a pool of workers.
	WorkerPool struct {
		Name              string
		keepAliveMap      *rmap.Map                    // worker keep-alive timestamps indexed by ID
		workersMap        *rmap.Map                    // worker creation times by ID
		pendingJobsMap    *rmap.Map                    // creation timestamp and status by pending job ID
		workerShutdownMap *rmap.Map                    // map used to stop workers indexes worker status by ID
		workerStreams     map[string]*streaming.Stream // worker streams indexed by ID
		jobStream         *streaming.Stream            // stream of jobs so we can requeue them if a worker dies
		jobSink           *streaming.Sink              // reader for the job stream
		pendingJobs       map[string]*streaming.Event  // pending jobs indexed by unique ID
		workerTTL         time.Duration                // duration after which a worker is considered dead
		leaseTTL          time.Duration                // duration after which a message lease expires
		maxQueuedJobs     int                          // maximum number of jobs that can be queued
		logger            ponos.Logger
		h                 jumpHash
		done              chan struct{} // closed when pool is stopped
		rdb               *redis.Client

		lock    sync.Mutex
		stopped bool // prevents new workers and new jobs from being created when true
	}

	// poolWorker is the value stored in the workers replicated map.
	poolWorker struct {
		ID          uuid.UUID // worker ID
		CreatedAt   int64     // worker creation time in nanoseconds
		RefreshedAt int64     // worker last keep-alive time in nanoseconds
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
	// evStop is the event used to shutdown workers.
	evStop string = "s"
)

// shutdownStatusKey is the key used to store the shutdown status in the pool
// status replicated map.
const shutdownStatusKey = "sd"

// Pool returns a client to the pool with the given name.
func Pool(ctx context.Context, name string, rdb *redis.Client, opts ...PoolOption) (*WorkerPool, error) {
	options := defaultPoolOptions()
	for _, opt := range opts {
		opt(options)
	}
	logger := options.logger
	if logger == nil {
		logger = ponos.NoopLogger()
	}
	km, err := rmap.Join(ctx, keepAliveMapName(name), rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join pool keep-alive replicated map %q: %w", keepAliveMapName(name), err)
	}
	wm, err := rmap.Join(ctx, workersMapName(name), rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join pool workers replicated map %q: %w", workersMapName(name), err)
	}
	wsm, err := rmap.Join(ctx, workerShutdownMapName(name), rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join pool worker status replicated map %q: %w", workerShutdownMapName(name), err)
	}
	jm, err := rmap.Join(ctx, jobsMapName(name), rdb, rmap.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to join pool jobs replicated map %q: %w", jobsMapName(name), err)
	}
	stream, err := streaming.NewStream(ctx, streamName(name), rdb, streaming.WithStreamLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create pool job stream %q: %w", streamName(name), err)
	}
	sink, err := stream.NewSink(ctx, "jobs", streaming.WithSinkBlockDuration(time.Minute), streaming.WithSinkMaxPolled(1))
	if err != nil {
		return nil, fmt.Errorf("failed to create pool job sink %q: %w", streamName(name), err)
	}

	p := &WorkerPool{
		Name:              name,
		keepAliveMap:      km,
		workersMap:        wm,
		pendingJobsMap:    jm,
		workerShutdownMap: wsm,
		workerStreams:     make(map[string]*streaming.Stream),
		jobStream:         stream,
		jobSink:           sink,
		pendingJobs:       make(map[string]*streaming.Event),
		workerTTL:         options.workerTTL,
		leaseTTL:          options.leaseTTL,
		maxQueuedJobs:     options.maxQueuedJobs,
		logger:            logger,
		h:                 jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
		done:              make(chan struct{}),
		rdb:               rdb,
	}

	go p.routeJobs()
	go p.managePendingJobs()

	return p, nil
}

// NewWorker creates a new worker and adds it to the pool.  The worker starts
// processing jobs immediately.
func (p *WorkerPool) NewWorker(ctx context.Context, opts ...WorkerOption) (*Worker, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopped {
		return nil, fmt.Errorf("pool %q is stopped", p.Name)
	}
	if len(p.workerShutdownMap.Keys()) > 0 {
		return nil, fmt.Errorf("pool %q is shutting down", p.Name)
	}
	w, err := newWorker(ctx, p, opts...)
	if err != nil {
		return nil, err
	}
	p.workerStreams[w.ID] = w.jobsStream
	return w, nil
}

// DispatchJob dispatches a job to the proper worker in the pool.
func (p *WorkerPool) DispatchJob(ctx context.Context, key string, payload []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopped {
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

// Stop causes all workers to stop once all pending jobs either complete or
// expire.  It is safe to call Stop multiple times.  Any job created by remote
// clients after Stop is called is ignored. Stop blocks until all workers have
// stopped or wait duration has elapsed.
func (p *WorkerPool) Stop(ctx context.Context, wait time.Duration) error {
	p.lock.Lock()
	if p.stopped {
		p.lock.Unlock()
		return nil
	}
	p.jobSink.Stop()
	p.workerShutdownMap.Set(ctx, shutdownStatusKey, "true") // Prevent new workers from joining.
	workers := p.workersMap.Map()
	for id := range workers {
		stream, err := p.workerStream(ctx, id)
		if err != nil {
			p.logger.Error(fmt.Errorf("stop: %w", err))
			continue
		}
		if _, err := stream.Add(ctx, evStop, nil); err != nil {
			p.logger.Error(fmt.Errorf("failed to add shutdown event to worker %q: %w", id, err))
		}
	}
	close(p.done)
	p.stopped = true
	p.lock.Unlock()

	defer p.cleanup(ctx)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timed out waiting for workers to stop")
		case <-p.workerShutdownMap.C:
			if len(p.workersMap.Keys())+1 == len(p.workerShutdownMap.Keys()) {
				return nil
			}
		case <-p.workersMap.C:
			if len(p.workersMap.Keys())+1 == len(p.workerShutdownMap.Keys()) {
				return nil
			}
		}
	}
}

// routeJobs reads events from the pool job stream and routes them to the
// appropriate worker.
func (p *WorkerPool) routeJobs() {
	ctx := context.Background()
	for ev := range p.jobSink.C {
		p.logger.Debug("routing job", "event", ev.ID)
		if err := p.routeJob(ctx, ev); err != nil {
			p.logger.Error(fmt.Errorf("failed to route job: %w", err))
		}
	}
}

// routeJob routes a dispatched job to the proper worker. It records the job status as routed.
func (p *WorkerPool) routeJob(ctx context.Context, ev *streaming.Event) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopped {
		return fmt.Errorf("pool %q is stopped", p.Name)
	}

	// First, retrieve workers and sort IDs by creation time.
	workers := p.workersMap.Map()
	if len(workers) == 0 {
		return fmt.Errorf("no worker in pool %q", p.Name)
	}
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
		horizon := time.Unix(0, nanos).Add(p.workerTTL)
		if horizon.After(time.Now()) {
			activeIDs = append(activeIDs, id)
		} else {
			if err := p.deleteWorker(ctx, id); err != nil {
				p.logger.Error(fmt.Errorf("failed to delete worker %q: %w", id, err))
			}
		}
	}
	if len(activeIDs) == 0 {
		return fmt.Errorf("no active worker in pool %q", p.Name)
	}
	// Record the job status in the jobs replicated map.
	key := unmarshalJobKey(ev.Payload)
	jid := uuid.NewString()
	pjob := pendingJob{Key: key, CreatedAt: time.Now().UnixNano()}
	if _, err := p.pendingJobsMap.Set(ctx, jid, marshalPendingJob(&pjob)); err != nil {
		return fmt.Errorf("failed to set job %q in jobs map: %w", key, err)
	}

	// Finally, stream the job to the worker group corresponding to the key hash.
	wid := activeIDs[p.h.Hash(key, int64(len(activeIDs)))]
	stream, err := p.workerStream(ctx, wid)
	if err != nil {
		return err
	}
	if _, err := stream.Add(ctx, evJob, ev.Payload); err != nil {
		return fmt.Errorf("failed to add job to worker stream %q: %w", workerJobsStreamName(wid), err)
	}
	p.pendingJobs[jid] = ev

	return nil
}

// managePendingJobs checks the entries in the pending jobs replicated map and
// acks jobs the pool created that are done, it then removes the job from the
// map.  Note: if a job is assigned to a worker that never processes it then the
// job stream will redeliver the job to the pool. The pool will then route the
// job to another worker. This is why we don't need to explicitely delete jobs
// from the pending jobs map after some time.
func (p *WorkerPool) managePendingJobs() {
	ticker := time.NewTicker(p.leaseTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-p.pendingJobsMap.C:
			p.ackDoneJobs()
		case <-ticker.C:
			p.rerouteStaleJobs()
		case <-p.done:
			return
		}
	}
}

// ackDoneJobs acks jobs that are done and removes them from the pending jobs map.
func (p *WorkerPool) ackDoneJobs() {
	p.lock.Lock()
	defer p.lock.Unlock()
	pending := p.pendingJobsMap.Map()
	for id, job := range p.pendingJobs {
		if pj, ok := pending[id]; ok {
			pjob := unmarshalPendingJob(pj)
			if pjob.Done {
				if err := p.jobSink.Ack(context.Background(), job); err != nil {
					p.logger.Error(fmt.Errorf("failed to ack job %q: %w", id, err))
				}
				delete(p.pendingJobs, id)
				if _, err := p.pendingJobsMap.Delete(context.Background(), id); err != nil {
					p.logger.Error(fmt.Errorf("failed to delete pending job %q from pending jobs map: %w", id, err))
				}
			}
		}
	}
}

// rerouteStaleJobs reroutes jobs that have been pending for more than leaseTTL using the Redis XAUTOCLAIM command.
func (p *WorkerPool) rerouteStaleJobs() {
	args := redis.XAutoClaimArgs{
		Stream:   p.jobStream.Name,
		Group:    p.jobSink.Name,
		Consumer: p.Name,
		MinIdle:  p.leaseTTL,
		Start:    "0-0",
	}
	if err := p.rdb.XAutoClaim(context.Background(), &args).Err(); err != nil {
		p.logger.Error(fmt.Errorf("failed to reroute stale jobs: %w", err))
	}
}

// deleteWorker removes a worker from the pool deleting the worker stream.
func (p *WorkerPool) deleteWorker(ctx context.Context, id string) error {
	fmt.Println("deleting worker", id)
	defer fmt.Println("deleted worker", id)
	if _, err := p.keepAliveMap.Delete(ctx, id); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker %q from keep-alive map: %w", id, err))
	}
	fmt.Println("deleting worker", id)
	if _, err := p.workersMap.Delete(ctx, id); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker %q from workers map: %w", id, err))
	}
	fmt.Println("deleting stream", id)
	stream, err := p.workerStream(ctx, id)
	if err != nil {
		return err
	}
	fmt.Println("destroying stream", id)
	if err := stream.Destroy(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker stream: %w", err))
	}
	return nil
}

// workerStream retrieves the stream for a worker. It caches the result in the
// workerStreams map. Caller is responsible for locking.
func (p *WorkerPool) workerStream(ctx context.Context, id string) (*streaming.Stream, error) {
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

// cleanup deletes the pool replicated maps and the worker streams.
func (p *WorkerPool) cleanup(ctx context.Context) {
	if err := p.keepAliveMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset keep-alive map: %w", err))
	}
	if err := p.workersMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset workers map: %w", err))
	}
	if err := p.pendingJobsMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset pending jobs map: %w", err))
	}
	if err := p.workerShutdownMap.Reset(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to reset worker shutdown map: %w", err))
	}
	if err := p.jobStream.Destroy(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to destroy job stream: %w", err))
	}
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
	return fmt.Sprintf("ponos:pool:keepalive:%s", pool)
}

// workersMapName returns the name of the replicated map used to store the
// worker creation timestamps.
func workersMapName(pool string) string {
	return fmt.Sprintf("ponos:pool:workers:%s", pool)
}

// workerShutdownMapName returns the name of the replicated map used to store the
// worker status.
func workerShutdownMapName(pool string) string {
	return fmt.Sprintf("ponos:pool:workers:shutdown:%s", pool)
}

// jobsMapName returns the name of the replicated map used to store the job
// creation timestamps.
func jobsMapName(pool string) string {
	return fmt.Sprintf("ponos:pool:jobs:%s", pool)
}

// streamName returns the name of the stream used to dispatch jobs.
func streamName(pool string) string {
	return fmt.Sprintf("ponos:pool:stream:%s", pool)
}
