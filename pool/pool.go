package pool

import (
	"context"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"

	"goa.design/ponos/ponos"
	"goa.design/ponos/replicated"
	"goa.design/ponos/streams"
)

type (
	// WorkerPool is a pool of workers.
	WorkerPool struct {
		Name          string
		status        *replicated.Map // status replicated map used to track pool status across all clients
		workers       *replicated.Map // worker replicated map indexes worker group and creation time by worker ID
		lastSeen      *replicated.Map // worker keep-alive timestamps indexed by worker ID
		workerTTL     time.Duration   // duration after which a worker is considered dead
		leaseTTL      time.Duration   // duration after which a message lease expires
		maxQueuedJobs int             // maximum number of jobs that can be queued
		logger        ponos.Logger
		h             jumpHash
		rdb           *redis.Client

		lock        sync.Mutex
		knownGroups map[string]struct{} // worker groups last read from the workers replicated map
		shutdown    bool                // prevents new workers and new jobs from being created when true
	}

	// jumpHash implement Jump Consistent Hash.
	jumpHash struct {
		h hash.Hash64
	}
)

const (
	// Name of event used to send new job to workers.
	eventNewJob string = "new_job"
	// eventShtudown is the event used to shutdown workers.
	eventShutdown string = "shutdown"
)

// shutdownStatusKey is the key used to store the shutdown status in the pool
// status replicated map.
const shutdownStatusKey = "shutdown"

// Pool returns a client to the pool with the given name.
func Pool(ctx context.Context, name string, rdb *redis.Client, opts ...PoolOption) (*WorkerPool, error) {
	status, err := replicated.Join(ctx, poolStatusName(name), rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to join pool status replicated map %q: %w", poolStatusName(name), err)
	}
	// fail early if pool is shutdown
	if _, ok := status.Get(shutdownStatusKey); ok {
		return nil, fmt.Errorf("pool %q is shutdown", name)
	}
	workers, err := replicated.Join(ctx, workerHashName(name), rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to join pool workers replicated map %q: %w", workerHashName(name), err)
	}
	lastSeen, err := replicated.Join(ctx, workerLastSeenName(name), rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to join pool workers last seen replicated map %q: %w", workerLastSeenName(name), err)
	}
	options := defaultPoolOptions()
	for _, opt := range opts {
		opt(options)
	}
	p := &WorkerPool{
		Name:          name,
		status:        status,
		workers:       workers,
		knownGroups:   make(map[string]struct{}),
		lastSeen:      lastSeen,
		workerTTL:     options.workerTTL,
		leaseTTL:      options.leaseTTL,
		maxQueuedJobs: options.maxQueuedJobs,
		logger:        options.logger,
		h:             jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
		rdb:           rdb,
	}

	go p.ensureRunning(ctx)

	return p, nil
}

// NewWorker creates a new worker and adds it to pool.  The worker starts
// processing jobs immediately.
// Cancel ctx to stop the worker and remove it from the pool.
func (p *WorkerPool) NewWorker(ctx context.Context, opts ...WorkerOption) (*Worker, error) {
	p.lock.Lock()
	if p.shutdown {
		p.lock.Unlock()
		return nil, fmt.Errorf("pool %q is shutting down", p.Name)
	}
	p.lock.Unlock()
	options := defaultWorkerOptions()
	for _, opt := range opts {
		opt(options)
	}
	if options.group == "" {
		options.group = uuid.New().String()
	}
	c := make(chan *Job, options.jobChannelSize)
	w := &Worker{
		ID:        uuid.NewString(),
		Pool:      p.Name,
		Group:     options.group,
		C:         c,
		CreatedAt: time.Now(),
		c:         c,
		lastSeen:  p.lastSeen,
		workerTTL: p.workerTTL,
		logger:    p.logger,
	}
	if err := p.lastSeen.Set(ctx, w.ID, strconv.FormatInt(time.Now().UnixNano(), 10)); err != nil {
		return nil, fmt.Errorf("failed to add worker %q to pool %q: %w", w.ID, p.Name, err)
	}
	if err := p.workers.Set(ctx, w.ID, marshalWorker(w)); err != nil {
		return nil, fmt.Errorf("failed to add worker %q to pool %q: %w", w.ID, p.Name, err)
	}
	sink, err := streams.Stream(ctx, w.ID, p.rdb).NewSink(ctx, w.Group)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink for worker %q: %w", w.ID, err)
	}
	go w.run(ctx, sink)
	return w, nil
}

// NewJob adds a job to the pool.
func (p *WorkerPool) NewJob(ctx context.Context, key string, payload []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.shutdown {
		return fmt.Errorf("pool %q is shutting down", p.Name)
	}

	// First, retrieve workers and sort IDs by creation time.
	workers := p.workers.Map()
	if len(workers) == 0 {
		return fmt.Errorf("no worker in pool %q", p.Name)
	}
	workerIDsByGroup := make(map[string][]string)
	workerGroupByID := make(map[string]string)
	workerCreatedAtByID := make(map[string]time.Time)
	for id, v := range workers {
		group, createdAt := unmarshalWorker(v)
		workerIDsByGroup[group] = append(workerIDsByGroup[group], id)
		workerGroupByID[id] = group
		workerCreatedAtByID[id] = createdAt
	}
	sortedIDs := make([]string, 0, len(workerCreatedAtByID))
	for id := range workerCreatedAtByID {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Slice(sortedIDs, func(i, j int) bool {
		return workerCreatedAtByID[sortedIDs[i]].Before(workerCreatedAtByID[sortedIDs[j]])
	})

	// Then filter out workers that have not been seen for more than workerTTL.
	lastSeen := p.lastSeen.Map()
	var activeIDs []string
	for _, id := range sortedIDs {
		ls, ok := lastSeen[id]
		if !ok {
			// This could happen if a worker is removed from the
			// pool and the last seen map deletion replicates before
			// the workers map deletion.
			continue
		}
		nanos, _ := strconv.ParseInt(ls, 10, 64)
		horizon := time.Unix(0, nanos).Add(p.workerTTL)
		if horizon.After(time.Now()) {
			activeIDs = append(activeIDs, id)
		} else {
			if err := p.deleteWorker(ctx, id, workerIDsByGroup, workerGroupByID); err != nil {
				p.logger.Error(fmt.Errorf("failed to delete worker %q: %w", id, err))
			}
		}
	}

	// Finally, stream the job to the worker group corresponding to the key hash.
	if len(activeIDs) == 0 {
		return fmt.Errorf("no active worker in pool %q", p.Name)
	}
	wid := activeIDs[p.h.Hash(key, int64(len(activeIDs)))]
	group, _ := unmarshalWorker(workers[wid])
	job := &Job{
		Key:       key,
		Payload:   payload,
		CreatedAt: time.Now(),
	}
	if err := streams.Stream(ctx, group, p.rdb).Add(ctx, eventNewJob, marshalJob(job)); err != nil {
		return fmt.Errorf("failed to add job to worker group %q: %w", group, err)
	}

	return nil
}

// Shutdown cancels all pending jobs and causes all workers to stop.  Shutdown
// also prevents new workers from being created. It is safe to call Shutdown
// multiple times.  Any job created by remote clients after Shutdown is called
// is ignored.
func (p *WorkerPool) Shutdown(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.shutdown {
		return nil
	}

	if err := p.status.Set(ctx, poolStatusName(p.Name), "shutting down"); err != nil {
		return fmt.Errorf("failed to set pool shutdown status: %w", err)
	}
	workers := p.workers.Map()
	groups := make(map[string]struct{})
	for _, v := range workers {
		group, _ := unmarshalWorker(v)
		groups[group] = struct{}{}
	}
	for group := range groups {
		if err := streams.Stream(ctx, group, p.rdb).Add(ctx, eventShutdown, nil); err != nil {
			return fmt.Errorf("failed to add shutdown event to worker group %q: %w", group, err)
		}
	}
	p.shutdown = true
	return nil
}

// Make sure pool is not shutdown.
// There is a race condition between a client shutting down a pool and a
// client joining the pool and creating a new worker group. The client
// shutting down the pool will delete all the groups it knows about and
// then set the shutdown status. The client joining the pool will
// eventually read the shutdown status via ensureRunning and properly
// cleanup the worker groups it created.
func (p *WorkerPool) ensureRunning(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, ok := p.status.Get(shutdownStatusKey); ok {
				if err := p.Shutdown(ctx); err != nil {
					p.logger.Error(fmt.Errorf("failed to shutdown pool %q: %w", p.Name, err))
				}
				return
			}
		}
	}
}

// deleteWorker removes a worker from the pool deleting the worker group stream
// if it was the last worker in the group.
func (p *WorkerPool) deleteWorker(ctx context.Context, id string, workerIDsByGroup map[string][]string, workerGroupByID map[string]string) error {
	if err := p.lastSeen.Delete(ctx, id); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker %q from last seen map: %w", id, err))
	}
	if err := p.workers.Delete(ctx, id); err != nil {
		p.logger.Error(fmt.Errorf("failed to delete worker %q from workers map: %w", id, err))
	}
	group := workerGroupByID[id]
	if len(workerIDsByGroup[group]) == 1 {
		if err := streams.Stream(ctx, group, p.rdb).Destroy(ctx); err != nil {
			p.logger.Error(fmt.Errorf("failed to delete worker group %q: %w", group, err))
		}
	}
	return nil
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

// poolStatusName returns the name of the key used to store the pool status.
func poolStatusName(pool string) string {
	return fmt.Sprintf("ponos:%s:status", pool)
}

// workerHashName returns the name of the hash used to store the worker creation
// timestamps.
func workerHashName(pool string) string {
	return fmt.Sprintf("ponos:%s:workers", pool)
}

// workerLastSeenName returns the name of the hash used to store the worker last
// seen timestamps.
func workerLastSeenName(pool string) string {
	return fmt.Sprintf("ponos:%s:last_seen", pool)
}

// marshalWorker marshals the worker group and creation timestamp into a string.
func marshalWorker(w *Worker) string {
	return fmt.Sprintf("%s=%s", w.Group, w.CreatedAt.Format(time.RFC3339Nano))
}

// unmarshal unmarshals a worker group and creation timestamp from a string.
func unmarshalWorker(s string) (group string, createdAt time.Time) {
	parts := strings.Split(s, "=")
	createdAt, _ = time.Parse(time.RFC3339Nano, parts[1])
	return parts[0], createdAt
}

// marshalJob marshals a job into a string.
func marshalJob(j *Job) []byte {
	ts := j.CreatedAt.Format(time.RFC3339Nano)
	return []byte(fmt.Sprintf("%s=%s=%s", j.Key, ts, j.Payload))
}

// unmarshalJob unmarshals a job from a string created by marshalJob.
func unmarshalJob(data []byte) (*Job, error) {
	parts := strings.SplitN(string(data), "=", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid job data: %s", data)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid job creation timestamp: %s", parts[1])
	}
	return &Job{
		Key:       parts[0],
		CreatedAt: createdAt,
		Payload:   []byte(parts[2]),
	}, nil
}
