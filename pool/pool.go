package pool

import (
	"context"
	"encoding/json"
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
		Name          string
		statusMap     *rmap.Map                    // status replicated map used to track pool status across all clients
		keepAliveMap  *rmap.Map                    // worker keep-alive timestamps indexed by worker ID
		workersMap    *rmap.Map                    // worker replicated map indexes worker group and creation time by worker ID
		workerStreams map[string]*streaming.Stream // worker streams indexed by worker ID
		jobStream     *streaming.Stream            // stream of jobs so we can requeue them if a worker dies
		jobSink       *streaming.Sink              // reader for the job stream
		workerTTL     time.Duration                // duration after which a worker is considered dead
		leaseTTL      time.Duration                // duration after which a message lease expires
		maxQueuedJobs int                          // maximum number of jobs that can be queued
		logger        ponos.Logger
		h             jumpHash
		done          chan struct{} // closed when pool is stopped
		rdb           *redis.Client

		lock    sync.Mutex
		stopped bool // prevents new workers and new jobs from being created when true
	}

	// poolWorker is the value stored in the workers replicated map.
	poolWorker struct {
		ID          uuid.UUID // worker ID
		CreatedAt   int64     // worker creation time in nanoseconds
		RefreshedAt int64     // worker last keep-alive time in nanoseconds
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
	sm, err := rmap.Join(ctx, statusMapName(name), rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to join pool status replicated map %q: %w", statusMapName(name), err)
	}
	// fail early if pool is stopped
	if _, ok := sm.Get(shutdownStatusKey); ok {
		return nil, fmt.Errorf("pool %q is stopped", name)
	}
	km, err := rmap.Join(ctx, keepAliveMapName(name), rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to join pool keep-alive replicated map %q: %w", keepAliveMapName(name), err)
	}
	wm, err := rmap.Join(ctx, workerHashName(name), rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to join pool workers replicated map %q: %w", workerHashName(name), err)
	}
	stream, err := streaming.NewStream(ctx, streamName(name), rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool job stream %q: %w", streamName(name), err)
	}
	sink, err := stream.NewSink(ctx, "jobs")
	if err != nil {
		return nil, fmt.Errorf("failed to create pool job sink %q: %w", streamName(name), err)
	}

	options := defaultPoolOptions()
	for _, opt := range opts {
		opt(options)
	}
	p := &WorkerPool{
		Name:          name,
		statusMap:     sm,
		keepAliveMap:  km,
		workersMap:    wm,
		workerStreams: make(map[string]*streaming.Stream),
		jobStream:     stream,
		jobSink:       sink,
		workerTTL:     options.workerTTL,
		leaseTTL:      options.leaseTTL,
		maxQueuedJobs: options.maxQueuedJobs,
		logger:        options.logger.WithPrefix("pool", name),
		h:             jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
		done:          make(chan struct{}),
		rdb:           rdb,
	}

	go p.routeJobs()
	go p.handleShutdown()

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
	w, err := newWorker(ctx, p, opts...)
	if err != nil {
		return nil, err
	}
	p.workerStreams[w.ID] = w.stream
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
	m, err := marshalJob(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}
	if err := p.jobStream.Add(ctx, evJob, m); err != nil {
		return fmt.Errorf("failed to add job to stream %q: %w", p.jobStream.Name, err)
	}

	return nil
}

// Stop cancels all pending jobs and causes all workers to stop.  Stop
// also prevents new workers from being created. It is safe to call Stop
// multiple times.  Any job created by remote clients after Stop is called
// is ignored. Once stopped a pool cannot be restarted.
func (p *WorkerPool) Stop(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.stopped {
		return nil
	}
	if _, err := p.statusMap.Set(ctx, statusMapName(p.Name), "stop"); err != nil {
		return fmt.Errorf("failed to set pool stop status: %w", err)
	}
	workers := p.workersMap.Map()
	for id := range workers {
		stream, err := p.workerStream(ctx, id)
		if err != nil {
			p.logger.Error(fmt.Errorf("stop: %w", err))
			continue
		}
		if err := stream.Add(ctx, evStop, nil); err != nil {
			p.logger.Error(fmt.Errorf("failed to add shutdown event to worker %q: %w", id, err))
		}
	}
	close(p.done)
	p.stopped = true
	return nil
}

// routeJobs reads events from the pool job stream and routes them to the
// appropriate worker.
func (p *WorkerPool) routeJobs() {
	ctx := context.Background()
	for msg := range p.jobStream.C {
		key, err := unmarshalJobKey(msg.Payload)
		if err != nil {
			p.logger.Error(fmt.Errorf("failed to unmarshal job: %w", err))
			continue
		}
		if err := p.routeJob(ctx, key, msg.Payload); err != nil {
			p.logger.Error(fmt.Errorf("failed to add job: %w", err))
		}
	}
}

// routeJob routes a dispatched job to the proper worker.
func (p *WorkerPool) routeJob(ctx context.Context, key string, payload []byte) error {
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

	// Finally, stream the job to the worker group corresponding to the key hash.
	wid := activeIDs[p.h.Hash(key, int64(len(activeIDs)))]
	stream, err := p.workerStream(ctx, wid)
	if err != nil {
		return err
	}
	if err := stream.Add(ctx, evJob, payload); err != nil {
		return fmt.Errorf("failed to add job to worker stream %q: %w", workerStreamName(wid), err)
	}

	return nil
}

// Handle remove pool shutdown request.
func (p *WorkerPool) handleShutdown() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			if _, ok := p.statusMap.Get(shutdownStatusKey); ok {
				if err := p.Stop(context.Background()); err != nil {
					p.logger.Error(fmt.Errorf("failed to shutdown pool: %w", err))
				}
				return
			}
		}
	}
}

// deleteWorker removes a worker from the pool deleting the worker stream.
func (p *WorkerPool) deleteWorker(ctx context.Context, id string) error {
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
	// re-assign any queued jobs to other workers.
	sink, err := stream.NewSink(ctx, "sink")
	if err != nil {
		p.logger.Error(fmt.Errorf("failed to re-assign jobs for worker %q: %w", id, err))
	}
	for msg := range sink.C {
		if msg.EventName == evJob {
			job := &Job{}
			if err := json.Unmarshal(msg.Payload, job); err != nil {
				p.logger.Error(fmt.Errorf("failed to unmarshal job: %w", err))
				continue
			}
			if err := p.routeJob(ctx, job.Key, job.Payload); err != nil {
				p.logger.Error(fmt.Errorf("failed to re-assign job: %w", err))
			}
		}
	}
	if err := stream.Destroy(ctx); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker stream: %w", err))
	}
	return nil
}

// workerStream retrieves the stream for a worker. It caches the result in the
// workerStreams map.
func (p *WorkerPool) workerStream(ctx context.Context, id string) (*streaming.Stream, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	stream, ok := p.workerStreams[id]
	if !ok {
		s, err := streaming.NewStream(ctx, workerStreamName(id), p.rdb)
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

// statusMapName returns the name of the key used to store the pool status.
func statusMapName(pool string) string {
	return fmt.Sprintf("ponos:pool:status:%s", pool)
}

// keepAliveMapName returns the name of the key used to store the worker keep-alive
// timestamps.
func keepAliveMapName(pool string) string {
	return fmt.Sprintf("ponos:pool:keepalive:%s", pool)
}

// workerHashName returns the name of the hash used to store the worker creation
// timestamps.
func workerHashName(pool string) string {
	return fmt.Sprintf("ponos:pool:workers:%s", pool)
}

// streamName returns the name of the stream used to store jobs.
func streamName(pool string) string {
	return fmt.Sprintf("ponos:pool:stream:%s", pool)
}
