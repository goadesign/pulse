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

		jobsStream        *streaming.Stream
		reader            *streaming.Reader
		c                 chan *Job
		done              chan struct{}
		workersMap        *rmap.Map
		keepAliveMap      *rmap.Map
		workerShutdownMap *rmap.Map
		workerTTL         time.Duration
		jobTTL            time.Duration
		logger            ponos.Logger

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
	jobsStream, err := streaming.NewStream(ctx, workerJobsStreamName(wid), p.rdb, streaming.WithStreamLogger(p.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create jobs stream for worker %q: %w", wid, err)
	}
	reader, err := jobsStream.NewReader(ctx, streaming.WithReaderBlockDuration(p.workerTTL), streaming.WithReaderMaxPolled(1))
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for worker %q: %w", wid, err)
	}
	w := &Worker{
		ID:                wid,
		Pool:              p,
		C:                 c,
		CreatedAt:         time.Now(),
		jobsStream:        jobsStream,
		reader:            reader,
		c:                 c,
		done:              make(chan struct{}),
		workersMap:        p.workersMap,
		keepAliveMap:      p.keepAliveMap,
		workerShutdownMap: p.workerShutdownMap,
		workerTTL:         p.workerTTL,
		logger:            p.logger.WithPrefix("worker", wid),
	}

	go w.handleEvents()
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
	w.reader.Stop()
	if err := w.jobsStream.Destroy(ctx); err != nil {
		return fmt.Errorf("failed to destroy stream for worker %q: %w", w.ID, err)
	}
	close(w.done)
	w.logger.Info("stopped")
	return nil
}

// handleEvents is the worker loop.
func (w *Worker) handleEvents() {
	ctx := context.Background()
	for msg := range w.reader.C {
		switch msg.EventName {
		case evJob:
			job := unmarshalJob(msg.Payload)
			if job.CreatedAt.Add(w.jobTTL).After(time.Now()) {
				w.logger.Error(fmt.Errorf("job %s expired (created %s)", job.Key, job.CreatedAt.Round(time.Second)))
				continue
			}
			w.c <- job
			pendingJobs := w.Pool.pendingJobsMap.Map()
			var pendingJobID string
			var pendingJob *pendingJob
			for id, pj := range pendingJobs {
				upj := unmarshalPendingJob(pj)
				if upj.Key == job.Key {
					pendingJobID = id
					pendingJob = upj
					break
				}
			}
			if pendingJobID == "" {
				w.logger.Error(fmt.Errorf("job %s not found in pending jobs map", job.Key))
				continue
			}
			pendingJob.Done = true
			if _, err := w.Pool.pendingJobsMap.Set(ctx, pendingJobID, string(marshalPendingJob(pendingJob))); err != nil {
				w.logger.Error(fmt.Errorf("failed to remove pending job %s: %w", job.Key, err))
			}
		case evStop:
			if err := w.Stop(ctx); err != nil {
				w.logger.Error(fmt.Errorf("failed to stop: %w", err))
			}
			w.workerShutdownMap.Set(ctx, w.ID, "true")
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

// workerJobsStreamName returns the name of the stream used to communicate with the
// worker with the given ID.
func workerJobsStreamName(id string) string {
	return "worker:" + id
}
