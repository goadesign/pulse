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
		// Time worker was created.
		CreatedAt time.Time

		node              *Node
		handler           JobHandler
		stream            *streaming.Stream
		reader            *streaming.Reader
		done              chan struct{}
		jobsMap           *rmap.Map
		jobPayloadsMap    *rmap.Map
		keepAliveMap      *rmap.Map
		shutdownMap       *rmap.Map
		workerTTL         time.Duration
		workerShutdownTTL time.Duration
		pendingJobTTL     time.Duration
		logger            pulse.Logger
		wg                sync.WaitGroup

		jobs        sync.Map // jobs being handled by the worker indexed by job key
		nodeStreams sync.Map

		lock    sync.RWMutex
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
func newWorker(ctx context.Context, node *Node, h JobHandler) (*Worker, error) {
	wid := ulid.Make().String()
	createdAt := time.Now()
	if _, err := node.workerMap.SetAndWait(ctx, wid, strconv.FormatInt(createdAt.UnixNano(), 10)); err != nil {
		return nil, fmt.Errorf("failed to add worker %q to pool %q: %w", wid, node.PoolName, err)
	}
	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := node.workerKeepAliveMap.SetAndWait(ctx, wid, now); err != nil {
		return nil, fmt.Errorf("failed to update worker keep-alive: %w", err)
	}
	stream, err := streaming.NewStream(workerStreamName(wid), node.rdb, soptions.WithStreamLogger(node.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create jobs stream for worker %q: %w", wid, err)
	}
	reader, err := stream.NewReader(ctx, soptions.WithReaderBlockDuration(node.workerTTL/2), soptions.WithReaderStartAtOldest())
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for worker %q: %w", wid, err)
	}
	w := &Worker{
		ID:                wid,
		node:              node,
		handler:           h,
		CreatedAt:         time.Now(),
		stream:            stream,
		reader:            reader,
		done:              make(chan struct{}),
		jobsMap:           node.jobsMap,
		jobPayloadsMap:    node.jobPayloadsMap,
		keepAliveMap:      node.workerKeepAliveMap,
		shutdownMap:       node.shutdownMap,
		workerTTL:         node.workerTTL,
		workerShutdownTTL: node.workerShutdownTTL,
		logger:            node.logger.WithPrefix("worker", wid),
		jobs:              sync.Map{},
		nodeStreams:       sync.Map{},
	}

	w.logger.Info("created",
		"worker_ttl", w.workerTTL,
		"worker_shutdown_ttl", w.workerShutdownTTL)

	w.wg.Add(2)
	pulse.Go(ctx, func() { w.handleEvents(ctx, reader.Subscribe()) })
	pulse.Go(ctx, func() { w.keepAlive(ctx) })

	return w, nil
}

// Jobs returns the jobs handled by the worker.
func (w *Worker) Jobs() []*Job {
	var keys []string
	w.jobs.Range(func(key, _ any) bool {
		keys = append(keys, key.(string))
		return true
	})
	sort.Strings(keys)
	jobs := make([]*Job, 0, len(keys))
	for _, key := range keys {
		j, ok := w.jobs.Load(key)
		if !ok {
			continue
		}
		job := j.(*Job)
		jobs = append(jobs, &Job{
			Key:       key,
			Payload:   job.Payload,
			CreatedAt: job.CreatedAt,
			Worker:    &Worker{ID: w.ID, node: w.node, CreatedAt: w.CreatedAt},
			NodeID:    job.NodeID,
		})
	}
	return jobs
}

// IsStopped returns true if the worker is stopped.
func (w *Worker) IsStopped() bool {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.stopped
}

// handleEvents is the worker loop.
func (w *Worker) handleEvents(ctx context.Context, c <-chan *streaming.Event) {
	defer w.wg.Done()

	for {
		select {
		case ev, ok := <-c:
			if !ok {
				return
			}
			nodeID, payload := unmarshalEnvelope(ev.Payload)
			var err error
			switch ev.EventName {
			case evStartJob:
				w.logger.Debug("handleEvents: received start job", "event", ev.EventName, "id", ev.ID)
				err = w.startJob(ctx, unmarshalJob(payload))
			case evStopJob:
				w.logger.Debug("handleEvents: received stop job", "event", ev.EventName, "id", ev.ID)
				err = w.stopJob(ctx, unmarshalJobKey(payload), false)
			case evNotify:
				w.logger.Debug("handleEvents: received notify", "event", ev.EventName, "id", ev.ID)
				key, payload := unmarshalNotification(payload)
				err = w.notify(ctx, key, payload)
			}
			if err != nil {
				if errors.Is(err, ErrRequeue) {
					w.logger.Info("requeue", "event", ev.EventName, "id", ev.ID, "after", w.pendingJobTTL)
					continue
				}
				w.ackPoolEvent(ctx, nodeID, ev.ID, err)
				w.logger.Error(fmt.Errorf("handler failed: %w", err), "event", ev.EventName, "id", ev.ID)
				continue
			}
			w.ackPoolEvent(ctx, nodeID, ev.ID, nil)
		case <-w.done:
			return
		}
	}
}

// stop stops the reader, destroys the stream and closes the worker.
func (w *Worker) stop(ctx context.Context) {
	w.lock.Lock()
	if w.stopped {
		w.lock.Unlock()
		return
	}
	w.stopped = true
	w.lock.Unlock()
	w.reader.Close()
	if err := w.stream.Destroy(ctx); err != nil {
		w.logger.Error(fmt.Errorf("failed to destroy stream for worker: %w", err))
	}
	close(w.done)
	w.wg.Wait()
}

// startJob starts a job.
func (w *Worker) startJob(ctx context.Context, job *Job) error {
	if w.IsStopped() {
		return fmt.Errorf("worker %q stopped", w.ID)
	}
	if _, err := w.jobsMap.AppendUniqueValues(ctx, w.ID, job.Key); err != nil {
		w.logger.Error(fmt.Errorf("failed to add job %q to jobs map: %w, requeueing", job.Key, err))
		return ErrRequeue
	}
	if _, err := w.jobPayloadsMap.Set(ctx, job.Key, string(job.Payload)); err != nil {
		w.logger.Error(fmt.Errorf("failed to add job payload %q to job payloads map: %w, requeueing", job.Key, err))
		return ErrRequeue
	}
	job.Worker = w
	if err := w.handler.Start(job); err != nil {
		w.logger.Debug("handler failed to start job", "job", job.Key, "error", err)
		if _, _, err := w.jobsMap.RemoveValues(ctx, w.ID, job.Key); err != nil {
			w.logger.Error(fmt.Errorf("start failure handling: failed to remove job %q from jobs map: %w", job.Key, err))
		}
		if _, err := w.jobPayloadsMap.Delete(ctx, job.Key); err != nil {
			w.logger.Error(fmt.Errorf("start failure handling: failed to remove job payload %q from job payloads map: %w", job.Key, err))
		}
		return err
	}
	w.logger.Info("started job", "job", job.Key)
	w.jobs.Store(job.Key, job)
	return nil
}

// stopJob stops a job.
func (w *Worker) stopJob(ctx context.Context, key string, forRequeue bool) error {
	if _, ok := w.jobs.Load(key); !ok {
		return fmt.Errorf("job %s not found in local worker", key)
	}
	if err := w.handler.Stop(key); err != nil {
		return fmt.Errorf("failed to stop job %q: %w", key, err)
	}
	w.logger.Debug("stopped job", "job", key)
	w.jobs.Delete(key)
	if _, _, err := w.jobsMap.RemoveValues(ctx, w.ID, key); err != nil {
		w.logger.Error(fmt.Errorf("stop job: failed to remove job %q from jobs map: %w", key, err))
	}
	if !forRequeue {
		if _, err := w.jobPayloadsMap.Delete(ctx, key); err != nil {
			w.logger.Error(fmt.Errorf("stop job: failed to remove job payload %q from job payloads map: %w", key, err))
		}
	}
	w.logger.Info("stopped job", "job", key, "for_requeue", forRequeue)
	return nil
}

// notify notifies the worker with the given payload.
func (w *Worker) notify(_ context.Context, key string, payload []byte) error {
	if w.IsStopped() {
		w.logger.Debug("worker stopped, ignoring notification")
		return nil
	}
	nh, ok := w.handler.(NotificationHandler)
	if !ok {
		w.logger.Error(fmt.Errorf("worker does not implement NotificationHandler, ignoring notification"), "worker", w.ID)
		return nil
	}
	w.logger.Debug("handled notification", "payload", string(payload))
	return nh.HandleNotification(key, payload)
}

// ackPoolEvent acknowledges the pool event that originated from the node with
// the given ID.
func (w *Worker) ackPoolEvent(ctx context.Context, nodeID, eventID string, ackerr error) {
	stream, ok := w.nodeStreams.Load(nodeID)
	if !ok {
		var err error
		stream, err = streaming.NewStream(nodeStreamName(w.node.PoolName, nodeID), w.node.rdb, soptions.WithStreamLogger(w.logger))
		if err != nil {
			w.logger.Error(fmt.Errorf("failed to create stream for node %q: %w", nodeID, err))
			return
		}
		w.nodeStreams.Store(nodeID, stream)
	}
	var msg string
	if ackerr != nil {
		msg = ackerr.Error()
	}
	ack := &ack{EventID: eventID, Error: msg}
	if _, err := stream.(*streaming.Stream).Add(ctx, evAck, marshalEnvelope(w.ID, marshalAck(ack))); err != nil {
		w.logger.Error(fmt.Errorf("failed to ack event %q from node %q: %w", eventID, nodeID, err))
	}
}

// keepAlive keeps the worker registration up-to-date until ctx is cancelled.
func (w *Worker) keepAlive(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.workerTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if w.IsStopped() {
				return // Let's not recreate the map if we just deleted it
			}
			now := strconv.FormatInt(time.Now().UnixNano(), 10)
			if _, err := w.keepAliveMap.Set(ctx, w.ID, now); err != nil {
				w.logger.Error(fmt.Errorf("failed to update worker keep-alive: %w", err))
			}
		case <-w.done:
			return
		}
	}
}

// rebalance rebalances the jobs handled by the worker.
func (w *Worker) rebalance(ctx context.Context, activeWorkers []string) {
	w.logger.Debug("rebalance")
	rebalanced := make(map[string]*Job)
	w.jobs.Range(func(key, value any) bool {
		job := value.(*Job)
		wid := activeWorkers[w.node.h.Hash(job.Key, int64(len(activeWorkers)))]
		if wid != w.ID {
			rebalanced[job.Key] = job
		}
		return true
	})
	total := len(rebalanced)
	if total == 0 {
		w.logger.Debug("rebalance: no jobs to rebalance")
		return
	}
	cherrs := make(map[string]chan error, total)
	for key, job := range rebalanced {
		if err := w.handler.Stop(key); err != nil {
			w.logger.Error(fmt.Errorf("rebalance: failed to stop job: %w", err), "job", key)
			continue
		}
		w.logger.Debug("stopped job", "job", key)
		w.jobs.Delete(key)
		cherr, err := w.node.requeueJob(ctx, w.ID, job)
		if err != nil {
			w.logger.Error(fmt.Errorf("rebalance: failed to requeue job: %w", err), "job", key)
			if err := w.handler.Start(job); err != nil {
				w.logger.Error(fmt.Errorf("rebalance: failed to restart job: %w", err), "job", key)
			}
			continue
		}
		delete(rebalanced, key)
		cherrs[key] = cherr
	}
	pulse.Go(ctx, func() { w.node.processRequeuedJobs(ctx, w.ID, cherrs, false) })
}

// requeueJobs requeues the jobs handled by the worker.
// This should be done after the worker is stopped.
func (w *Worker) requeueJobs(ctx context.Context) error {
	jobsToRequeue := make(map[string]*Job)
	jobCount := 0
	w.jobs.Range(func(key, value any) bool {
		job := value.(*Job)
		jobsToRequeue[key.(string)] = job
		jobCount++
		return true
	})
	if jobCount == 0 {
		w.logger.Debug("requeueJobs: no jobs to requeue")
		return nil
	}
	createdAt := strconv.FormatInt(w.CreatedAt.UnixNano(), 10)
	w.logger.Debug("requeueJobs: requeuing", "jobs", jobCount)

	// First mark the worker as inactive so that requeued jobs are not assigned to this worker
	// Use optimistic locking to avoid race conditions.
	prev, err := w.node.workerMap.TestAndSet(ctx, w.ID, createdAt, "-")
	if err != nil {
		return fmt.Errorf("requeueJobs: failed to mark worker as inactive: %w", err)
	}
	if prev == "-" {
		w.logger.Debug("requeueJobs: jobs already requeued, skipping requeue")
		return nil
	}

	retryUntil := time.Now().Add(w.workerTTL)
	for retryUntil.After(time.Now()) {
		remainingJobs := w.attemptRequeue(ctx, jobsToRequeue)
		jobsToRequeue = remainingJobs
		if len(remainingJobs) == 0 {
			break
		}
	}

	failedCount := len(jobsToRequeue)
	w.logger.Info("requeued", "jobs", jobCount, "failed", failedCount)
	if failedCount > 0 {
		return fmt.Errorf("requeueJobs: failed to requeue %d/%d jobs after retrying for %v", failedCount, jobCount, w.workerTTL)
	}

	return nil
}

// attemptRequeue attempts to requeue the jobs in the given map.
// It returns any job that failed to be requeued.
func (w *Worker) attemptRequeue(ctx context.Context, jobsToRequeue map[string]*Job) map[string]*Job {
	var wg sync.WaitGroup
	type result struct {
		key string
		err error
	}
	resultChan := make(chan result, len(jobsToRequeue))
	defer close(resultChan)

	wg.Add(len(jobsToRequeue))
	for key, job := range jobsToRequeue {
		pulse.Go(ctx, func() {
			defer wg.Done()
			err := w.requeueJob(ctx, job)
			if err != nil {
				w.logger.Error(fmt.Errorf("failed to requeue job: %w", err), "job", key)
			} else {
				w.logger.Debug("requeueJobs: requeued", "job", key)
			}
			resultChan <- result{key: key, err: err}
		})
	}
	wg.Wait()

	remainingJobs := make(map[string]*Job)
	for {
		select {
		case res := <-resultChan:
			if res.err != nil {
				w.logger.Error(fmt.Errorf("requeueJobs: failed to requeue job %q: %w", res.key, res.err))
				remainingJobs[res.key] = jobsToRequeue[res.key]
				continue
			}
			delete(remainingJobs, res.key)
			w.logger.Info("requeued", "job", res.key)
			if len(remainingJobs) == 0 {
				w.logger.Debug("requeueJobs: all jobs requeued")
				return remainingJobs
			}
		case <-time.After(w.workerShutdownTTL):
			w.logger.Error(fmt.Errorf("requeueJobs: timeout reached, some jobs may not have been processed"))
			return remainingJobs
		}
	}
}

// requeueJob requeues a job.
func (w *Worker) requeueJob(ctx context.Context, job *Job) error {
	eventID, err := w.node.poolStream.Add(ctx, evStartJob, marshalJob(job))
	if err != nil {
		return fmt.Errorf("requeueJob: failed to add job to pool stream: %w", err)
	}
	w.node.pendingJobs.Store(eventID, nil)
	if err := w.stopJob(ctx, job.Key, true); err != nil {
		return fmt.Errorf("failed to stop job: %w", err)
	}
	return nil
}

// workerStreamName returns the name of the stream used to communicate with the
// worker with the given ID.
func workerStreamName(id string) string {
	return "worker:" + id
}
