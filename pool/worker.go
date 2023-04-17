package pool

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"goa.design/ponos/ponos"
	"goa.design/ponos/rmap"
	"goa.design/ponos/streaming"
)

type (
	// Worker is a worker that handles jobs with a given payload type.
	Worker struct {
		// Unique worker ID
		ID string
		// Worker pool
		Pool *WorkerPool
		// C is the channel the worker receives jobs on.
		C <-chan *Job
		// CreatedAt is the time the worker was created.
		CreatedAt time.Time

		stream       *streaming.Stream
		reader       *streaming.Reader
		c            chan *Job
		done         chan struct{}
		workersMap   *rmap.Map
		keepAliveMap *rmap.Map
		workerTTL    time.Duration
		jobTTL       time.Duration
		logger       ponos.Logger

		lock    sync.Mutex
		stopped bool
	}

	// Job is a job that can be added to a worker.
	Job struct {
		// Key is used to identify the worker that handles the job.
		Key string
		// Payload is the job payload.
		Payload []byte
		// CreatedAt is the time the job was created.
		CreatedAt time.Time
	}
)

// newWorker creates a new worker.
func newWorker(ctx context.Context, p *WorkerPool, opts ...WorkerOption) (*Worker, error) {
	options := defaultWorkerOptions()
	for _, opt := range opts {
		opt(options)
	}
	c := make(chan *Job, options.jobChannelSize)
	wid := uuid.NewString()
	createdAt := time.Now()
	if _, err := p.workersMap.Set(ctx, wid, strconv.FormatInt(createdAt.UnixNano(), 10)); err != nil {
		return nil, fmt.Errorf("failed to add worker %q to pool %q: %w", wid, p.Name, err)
	}
	stream, err := streaming.NewStream(ctx, workerStreamName(wid), p.rdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream for worker %q: %w", wid, err)
	}
	reader, err := stream.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for worker %q: %w", wid, err)
	}
	w := &Worker{
		ID:           wid,
		Pool:         p,
		C:            c,
		CreatedAt:    time.Now(),
		stream:       stream,
		reader:       reader,
		c:            c,
		done:         make(chan struct{}),
		workersMap:   p.workersMap,
		keepAliveMap: p.keepAliveMap,
		workerTTL:    p.workerTTL,
		logger:       p.logger.WithPrefix("worker", wid),
	}

	go w.handleJobs()
	go w.keepAlive()

	return w, nil
}

// Stop stops the worker and removes it from the pool. It is safe to call Stop
// multiple times.
func (w *Worker) Stop(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.stopped {
		return nil
	}
	if _, err := w.workersMap.Delete(ctx, w.ID); err != nil {
		return fmt.Errorf("failed to remove worker %q from pool %q: %w", w.ID, w.Pool.Name, err)
	}
	if _, err := w.keepAliveMap.Delete(ctx, w.ID); err != nil {
		return fmt.Errorf("failed to remove worker %q from keep alive map: %w", w.ID, err)
	}
	// Requeue any jobs that were not processed.
	w.reader.Stop()
	for msg := range w.reader.C {
		if msg.EventName == evJob {
			job, err := unmarshalJob(msg.Payload)
			if err != nil {
				w.logger.Error(fmt.Errorf("failed to unmarshal job: %w", err))
				continue
			}
			if err := w.Pool.routeJob(ctx, job.Key, job.Payload); err != nil {
				w.logger.Error(fmt.Errorf("failed to requeue job: %w", err))
			}
		}
	}
	if err := w.stream.Destroy(ctx); err != nil {
		return fmt.Errorf("failed to destroy stream for worker %q: %w", w.ID, err)
	}
	close(w.done)
	w.logger.Info("stopped")
	return nil
}

// handleJobs is the worker loop.
func (w *Worker) handleJobs() {
	ctx := context.Background()
	for msg := range w.reader.C {
		switch msg.EventName {
		case evJob:
			job, err := unmarshalJob(msg.Payload)
			if err != nil {
				w.logger.Error(fmt.Errorf("failed to unmarshal job: %w", err))
				continue
			}
			if job.CreatedAt.Add(w.jobTTL).After(time.Now()) {
				w.logger.Error(fmt.Errorf("job %s expired (created %s)", job.Key, job.CreatedAt.Round(time.Second)))
				continue
			}
			w.c <- job
		case evStop:
			if err := w.Stop(ctx); err != nil {
				w.logger.Error(fmt.Errorf("failed to stop: %w", err))
			}
		default:
			w.logger.Error(fmt.Errorf("unexpected event %s", msg.EventName))
		}
	}
}

// keepAlive keeps the worker registration up-to-date until ctx is cancelled.
func (w *Worker) keepAlive() {
	ticker := time.NewTicker(w.workerTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			aliveAt := strconv.FormatInt(time.Now().UnixNano(), 10)
			if _, err := w.keepAliveMap.Set(context.Background(), w.ID, aliveAt); err != nil {
				w.logger.Error(fmt.Errorf("failed to update worker keep-alive: %w", err))
			}
		case <-w.done:
			return
		}
	}
}

// workerStreamName returns the name of the stream used to communicate with the
// worker with the given ID.
func workerStreamName(id string) string {
	return "worker:" + id
}
