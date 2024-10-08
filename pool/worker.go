package pool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming"
	soptions "goa.design/pulse/streaming/options"
)

type (
	// Worker is a worker that handles jobs with a given payload type.
	Worker struct {
		// Unique worker ID
		ID string
		// Worker pool node where worker is running.
		Node *Node
		// Time worker was created.
		CreatedAt time.Time

		handler           JobHandler
		stream            *streaming.Stream
		reader            *streaming.Reader
		done              chan struct{}
		workersMap        *rmap.Map
		keepAliveMap      *rmap.Map
		shutdownMap       *rmap.Map
		workerTTL         time.Duration
		workerShutdownTTL time.Duration
		pendingJobTTL     time.Duration
		logger            pulse.Logger
		wg                sync.WaitGroup

		lock        sync.Mutex
		jobs        map[string]*Job // jobs being handled by the worker indexed by job key
		nodeStreams map[string]*streaming.Stream
		stopped     bool
	}

	// Job is a job that can be added to a worker.
	Job struct {
		// Key is used to identify the worker that handles the job.
		Key string
		// Payload is the job payload.
		Payload []byte
		// CreatedAt is the time the job was created.
		CreatedAt time.Time
		// Worker is the worker that handles the job.
		Worker *Worker
		// NodeID is the ID of the node that created the job.
		NodeID string
	}

	// JobHandler starts and stops jobs.
	JobHandler interface {
		// Start starts a job.
		Start(job *Job) error
		// Stop stops a job with a given key.
		Stop(key string) error
	}

	// NotificationHandler handle job notifications.
	NotificationHandler interface {
		// HandleNotification handles a notification.
		HandleNotification(key string, payload []byte) error
	}

	// ack is a worker event acknowledgement.
	ack struct {
		// EventID is the ID of the event being acknowledged.
		EventID string
		// Error is the error that occurred while handling the event if any.
		Error string
	}
)

// newWorker creates a new worker.
func newWorker(ctx context.Context, p *Node, h JobHandler) (*Worker, error) {
	wid := ulid.Make().String()
	createdAt := time.Now()
	if _, err := p.workerMap.SetAndWait(ctx, wid, strconv.FormatInt(createdAt.UnixNano(), 10)); err != nil {
		return nil, fmt.Errorf("failed to add worker %q to pool %q: %w", wid, p.Name, err)
	}
	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := p.keepAliveMap.SetAndWait(ctx, wid, now); err != nil {
		return nil, fmt.Errorf("failed to update worker keep-alive: %w", err)
	}
	stream, err := streaming.NewStream(workerStreamName(wid), p.rdb, soptions.WithStreamLogger(p.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create jobs stream for worker %q: %w", wid, err)
	}
	reader, err := stream.NewReader(ctx, soptions.WithReaderBlockDuration(p.workerTTL), soptions.WithReaderStartAtOldest())
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for worker %q: %w", wid, err)
	}
	w := &Worker{
		ID:                wid,
		Node:              p,
		handler:           h,
		CreatedAt:         time.Now(),
		stream:            stream,
		reader:            reader,
		done:              make(chan struct{}),
		workersMap:        p.workerMap,
		keepAliveMap:      p.keepAliveMap,
		shutdownMap:       p.shutdownMap,
		workerTTL:         p.workerTTL,
		workerShutdownTTL: p.workerShutdownTTL,
		logger:            p.logger.WithPrefix("worker", wid),
		jobs:              make(map[string]*Job),
		nodeStreams:       make(map[string]*streaming.Stream),
	}

	w.wg.Add(2)
	go w.handleEvents(reader.Subscribe())
	go w.keepAlive()

	return w, nil
}

// Jobs returns the jobs handled by the worker.
func (w *Worker) Jobs() []*Job {
	w.lock.Lock()
	defer w.lock.Unlock()
	keys := make([]string, 0, len(w.jobs))
	for key := range w.jobs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	jobs := make([]*Job, 0, len(w.jobs))
	for _, key := range keys {
		jobs = append(jobs, w.jobs[key])
	}
	return jobs
}

// handleEvents is the worker loop.
func (w *Worker) handleEvents(c <-chan *streaming.Event) {
	defer w.wg.Done()
	ctx := context.Background()
	for {
		select {
		case ev, ok := <-c:
			if !ok {
				return
			}
			if ev.EventName == evShutdown {
				w.logger.Info("stop", "from", string(ev.Payload))
				w.stop(ctx)
				return
			}
			nodeID, payload := unmarshalEnvelope(ev.Payload)
			var err error
			switch ev.EventName {
			case evStartJob:
				err = w.startJob(ctx, unmarshalJob(payload))
			case evStopJob:
				w.lock.Lock()
				err = w.stopJob(ctx, unmarshalJobKey(payload))
				w.lock.Unlock()
			case evNotify:
				key, payload := unmarshalNotification(payload)
				err = w.notify(ctx, key, payload)
			}
			if err != nil {
				if errors.Is(err, errRequeue) {
					w.logger.Info("requeue", ev.EventName, "after", w.pendingJobTTL, "error", err)
					continue
				}
				w.ackPoolEvent(ctx, nodeID, ev.ID, err)
				w.logger.Error(fmt.Errorf("%s handler failed: %w", ev.EventName, err), "event-id", ev.ID)
				continue
			}
			w.ackPoolEvent(ctx, nodeID, ev.ID, nil)
			w.logger.Info("handled", "event-id", ev.ID, "event-name", ev.EventName)
		case <-w.done:
			return
		}
	}
}

// stop stops the reader, the worker goroutines and removes the worker from the
// workers and keep-alive maps.
// TBD: what to do if requeue fails?
func (w *Worker) stop(ctx context.Context) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.stopped {
		return
	}
	w.stopped = true
	var err error
	if _, er := w.workersMap.Delete(ctx, w.ID); er != nil {
		err = fmt.Errorf("failed to remove worker %q from pool %q: %w", w.ID, w.Node.Name, er)
	}
	if _, er := w.keepAliveMap.Delete(ctx, w.ID); er != nil {
		err = fmt.Errorf("failed to remove worker %q from keep alive map: %w", w.ID, er)
	}
	w.reader.Close()
	if er := w.stream.Destroy(ctx); er != nil {
		err = fmt.Errorf("failed to destroy stream for worker %q: %w", w.ID, er)
	}
	close(w.done)
	if err != nil {
		w.logger.Error(err)
	}
}

// stopAndWait stops the worker and waits for its goroutines to exit up to
// w.workerShutdownTTL time.
func (w *Worker) stopAndWait(ctx context.Context) {
	w.stop(ctx)
	c := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(c)
	}()
	select {
	case <-c:
	case <-time.After(w.workerShutdownTTL):
		w.logger.Error(fmt.Errorf("stop timeout"), "after", w.workerShutdownTTL)
	}
}

// startJob starts a job.
func (w *Worker) startJob(_ context.Context, job *Job) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.stopped {
		return fmt.Errorf("worker %q stopped", w.ID)
	}
	job.Worker = w
	if err := w.handler.Start(job); err != nil {
		return err
	}
	w.logger.Info("started job", "key", job.Key)
	job.Worker = w
	w.jobs[job.Key] = job
	return nil
}

// stopJob stops a job.
// worker.lock must be held when calling this method.
func (w *Worker) stopJob(_ context.Context, key string) error {
	if _, ok := w.jobs[key]; !ok {
		return fmt.Errorf("job %s not found", key)
	}
	if err := w.handler.Stop(key); err != nil {
		return fmt.Errorf("failed to stop job %q: %w", key, err)
	}
	w.logger.Info("stopped job", "key", key)
	delete(w.jobs, key)
	return nil
}

// notify notifies the worker with the given payload.
func (w *Worker) notify(_ context.Context, key string, payload []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.stopped {
		w.logger.Debug("worker stopped, ignoring notification")
		return nil
	}
	nh, ok := w.handler.(NotificationHandler)
	if !ok {
		w.logger.Debug("worker does not implement NotificationHandler, ignoring notification")
		return nil
	}
	w.logger.Debug("handled notification", "payload", string(payload))
	return nh.HandleNotification(key, payload)
}

// ackPoolEvent acknowledges the pool event that originated from the node with
// the given ID.
func (w *Worker) ackPoolEvent(ctx context.Context, nodeID, eventID string, ackerr error) {
	stream, ok := w.nodeStreams[nodeID]
	if !ok {
		var err error
		stream, err = streaming.NewStream(nodeStreamName(w.Node.Name, nodeID), w.Node.rdb, soptions.WithStreamLogger(w.logger))
		if err != nil {
			w.logger.Error(fmt.Errorf("failed to create stream for node %q: %w", nodeID, err))
			return
		}
		w.nodeStreams[nodeID] = stream
	}
	var msg string
	if ackerr != nil {
		msg = ackerr.Error()
	}
	ack := &ack{EventID: eventID, Error: msg}
	if _, err := stream.Add(ctx, evAck, marshalEnvelope(w.ID, marshalAck(ack))); err != nil {
		w.logger.Error(fmt.Errorf("failed to ack event %q from node %q: %w", eventID, nodeID, err))
	}
}

// keepAlive keeps the worker registration up-to-date until ctx is cancelled.
func (w *Worker) keepAlive() {
	defer w.wg.Done()
	ctx := context.Background()
	ticker := time.NewTicker(w.workerTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.lock.Lock()
			if w.stopped {
				w.lock.Unlock()
				// Let's not recreate the map if we just deleted it
				return
			}
			now := strconv.FormatInt(time.Now().UnixNano(), 10)
			if _, err := w.keepAliveMap.Set(ctx, w.ID, now); err != nil {
				w.logger.Error(fmt.Errorf("failed to update worker keep-alive: %w", err))
			}
			w.lock.Unlock()
		case <-w.done:
			return
		}
	}
}

// requeueJobs requeues the jobs handled by the worker.
// This should be done after the worker is stopped.
func (w *Worker) requeueJobs(ctx context.Context) {
	w.lock.Lock()
	defer w.lock.Unlock()
	for _, job := range w.jobs {
		if err := w.stopJob(ctx, job.Key); err != nil {
			w.logger.Error(fmt.Errorf("failed to stop job %q: %w", job.Key, err))
		}
		if _, err := w.Node.poolStream.Add(ctx, evStartJob, marshalJob(job)); err != nil {
			w.logger.Error(fmt.Errorf("failed to requeue job %q: %w", job.Key, err))
		}
	}
	w.jobs = nil
}

// workerEventsStreamName returns the name of the stream used to communicate with the
// worker with the given ID.
func workerStreamName(id string) string {
	return "worker:" + id
}
