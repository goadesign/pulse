package pool

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
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
		Pool *Node
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
		wg                sync.WaitGroup

		lock    sync.Mutex
		stopped bool
	}
)

// newWorker creates a new worker.
func newWorker(ctx context.Context, p *Node, opts ...WorkerOption) (*Worker, error) {
	options := defaultWorkerOptions()
	for _, opt := range opts {
		opt(options)
	}
	c := make(chan *Job, options.jobChannelSize)
	wid := ulid.Make().String()
	createdAt := time.Now()
	if _, err := p.workersMap.Set(ctx, wid, strconv.FormatInt(createdAt.UnixNano(), 10)); err != nil {
		return nil, fmt.Errorf("failed to add worker %q to pool %q: %w", wid, p.Name, err)
	}
	jobsStream, err := streaming.NewStream(ctx, workerJobsStreamName(wid), p.rdb, streaming.WithStreamLogger(p.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create jobs stream for worker %q: %w", wid, err)
	}
	reader, err := jobsStream.NewReader(ctx, streaming.WithReaderBlockDuration(p.workerTTL))
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
		workerShutdownMap: p.shutdownMap,
		workerTTL:         p.workerTTL,
		logger:            p.logger.WithPrefix("worker", wid),
	}

	w.wg.Add(3)
	go w.handleEvents()
	go w.keepAlive()
	go w.handlePoolShutdown()

	return w, nil
}

// Stop stops the worker and removes it from the pool. It is safe to call Stop
// multiple times.
func (w *Worker) Stop(ctx context.Context) error {
	w.lock.Lock()
	if w.stopped {
		w.lock.Unlock()
		return nil
	}
	w.stopped = true
	var err error
	if _, er := w.workersMap.Delete(ctx, w.ID); er != nil {
		err = fmt.Errorf("failed to remove worker %q from pool %q: %w", w.ID, w.Pool.Name, er)
	}
	if _, er := w.keepAliveMap.Delete(ctx, w.ID); er != nil {
		err = fmt.Errorf("failed to remove worker %q from keep alive map: %w", w.ID, er)
	}
	w.reader.Stop()
	if er := w.jobsStream.Destroy(ctx); er != nil {
		err = fmt.Errorf("failed to destroy stream for worker %q: %w", w.ID, er)
	}
	close(w.done)
	if err != nil {
		w.logger.Error(err)
	}
	w.lock.Unlock()
	w.wg.Wait()
	w.logger.Info("stopped")
	return err
}

// handleEvents is the worker loop.
func (w *Worker) handleEvents() {
	defer w.wg.Done()
	for {
		select {
		case msg, ok := <-w.reader.C:
			if !ok {
				return
			}
			switch msg.EventName {
			case evJob:
				if !ok {
					return
				}
				w.lock.Lock()
				if w.stopped {
					w.lock.Unlock()
					return
				}
				w.lock.Unlock()
				job := unmarshalJob(msg.Payload)
				if job.CreatedAt.Add(w.jobTTL).After(time.Now()) {
					w.logger.Error(fmt.Errorf("job %s expired (created %s, TTL %s)",
						job.Key, job.CreatedAt.Round(time.Second), w.jobTTL))
					continue
				}
				w.logger.Info("received job", "key", job.Key)
				w.c <- job
			case evStop:
				w.logger.Info("received stop", "node", string(msg.Payload))
				if err := w.Stop(context.Background()); err != nil {
					w.logger.Error(fmt.Errorf("failed to stop worker: %w", err))
				}
			}
		case <-w.done:
			return
		}
	}
}

// keepAlive keeps the worker registration up-to-date until ctx is cancelled.
func (w *Worker) keepAlive() {
	defer w.wg.Done()
	ctx := context.Background()
	update := func() {
		w.lock.Lock()
		defer w.lock.Unlock()
		if w.stopped {
			// Let's not recreate the map if we just deleted it
			return
		}
		now := strconv.FormatInt(time.Now().UnixNano(), 10)
		if _, err := w.keepAliveMap.Set(ctx, w.ID, now); err != nil {
			w.logger.Error(fmt.Errorf("failed to update worker keep-alive: %w", err))
		}
	}
	update()
	ticker := time.NewTicker(w.workerTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			update()
		case <-w.done:
			return
		}
	}
}

// handlePoolShutdown handles the shutdown signal by stopping the worker.
func (w *Worker) handlePoolShutdown() {
	defer w.wg.Done()
	ctx := context.Background()
	for {
		select {
		case <-w.workerShutdownMap.C:
			w.Stop(ctx)
			return
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
