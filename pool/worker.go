package pool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
		jobsMap           *rmap.Map
		jobPayloadsMap    *rmap.Map
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
func newWorker(ctx context.Context, node *Node, h JobHandler) (*Worker, error) {
	wid := ulid.Make().String()
	createdAt := time.Now()
	if _, err := node.workerMap.SetAndWait(ctx, wid, strconv.FormatInt(createdAt.UnixNano(), 10)); err != nil {
		return nil, fmt.Errorf("failed to add worker %q to pool %q: %w", wid, node.Name, err)
	}
	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := node.keepAliveMap.SetAndWait(ctx, wid, now); err != nil {
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
		Node:              node,
		handler:           h,
		CreatedAt:         time.Now(),
		stream:            stream,
		reader:            reader,
		done:              make(chan struct{}),
		jobsMap:           node.jobsMap,
		jobPayloadsMap:    node.jobPayloadsMap,
		keepAliveMap:      node.keepAliveMap,
		shutdownMap:       node.shutdownMap,
		workerTTL:         node.workerTTL,
		workerShutdownTTL: node.workerShutdownTTL,
		logger:            node.logger.WithPrefix("worker", wid),
		jobs:              make(map[string]*Job),
		nodeStreams:       make(map[string]*streaming.Stream),
	}

	w.logger.Info("created",
		"worker_ttl", w.workerTTL,
		"worker_shutdown_ttl", w.workerShutdownTTL)

	w.wg.Add(2)
	go w.handleEvents(reader.Subscribe())
	go w.keepAlive(ctx)

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
			nodeID, payload := unmarshalEnvelope(ev.Payload)
			var err error
			switch ev.EventName {
			case evStartJob:
				w.logger.Debug("handleEvents: received start job", "event", ev.EventName, "id", ev.ID)
				err = w.startJob(ctx, unmarshalJob(payload))
			case evStopJob:
				w.logger.Debug("handleEvents: received stop job", "event", ev.EventName, "id", ev.ID)
				w.lock.Lock()
				err = w.stopJob(ctx, unmarshalJobKey(payload))
				w.lock.Unlock()
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
			w.logger.Debug("handleEvents: exiting")
			return
		}
	}
}

// stop stops the reader, the worker goroutines and removes the worker from the
// workers and keep-alive maps.
func (w *Worker) stop(ctx context.Context) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.stopped {
		return
	}
	w.stopped = true
	w.reader.Close()
	if err := w.stream.Destroy(ctx); err != nil {
		w.logger.Error(fmt.Errorf("failed to destroy stream for worker: %w", err))
	}
	close(w.done)
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
		w.logger.Debug("stopAndWait: worker stopped")
	case <-time.After(w.workerShutdownTTL):
		w.logger.Error(fmt.Errorf("stop timeout"), "after", w.workerShutdownTTL)
	}
}

// startJob starts a job.
func (w *Worker) startJob(ctx context.Context, job *Job) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.stopped {
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
		if _, _, err := w.jobsMap.RemoveValues(ctx, w.ID, job.Key); err != nil {
			w.logger.Error(fmt.Errorf("start failure handling: failed to remove job %q from jobs map: %w", job.Key, err))
		}
		if _, err := w.jobPayloadsMap.Delete(ctx, job.Key); err != nil {
			w.logger.Error(fmt.Errorf("start failure handling: failed to remove job payload %q from job payloads map: %w", job.Key, err))
		}
		return err
	}
	w.logger.Info("started job", "job", job.Key)
	w.jobs[job.Key] = job
	return nil
}

// stopJob stops a job.
// worker.lock must be held when calling this method.
func (w *Worker) stopJob(ctx context.Context, key string) error {
	if _, ok := w.jobs[key]; !ok {
		return fmt.Errorf("job %s not found", key)
	}
	if err := w.handler.Stop(key); err != nil {
		return fmt.Errorf("failed to stop job %q: %w", key, err)
	}
	if _, _, err := w.jobsMap.RemoveValues(ctx, w.ID, key); err != nil {
		w.logger.Error(fmt.Errorf("stop job: failed to remove job %q from jobs map: %w", key, err))
	}
	if _, err := w.jobPayloadsMap.Delete(ctx, key); err != nil {
		w.logger.Error(fmt.Errorf("stop job: failed to remove job payload %q from job payloads map: %w", key, err))
	}
	w.logger.Info("stopped job", "job", key)
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
func (w *Worker) keepAlive(ctx context.Context) {
	defer w.wg.Done()
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
			w.logger.Debug("keepAlive: exiting")
			return
		}
	}
}

// rebalance rebalances the jobs handled by the worker.
func (w *Worker) rebalance(ctx context.Context, activeWorkers []string) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(w.jobs) == 0 {
		return
	}
	w.logger.Debug("rebalance", "jobs", len(w.jobs))
	rebalanced := make(map[string]*Job)
	for _, job := range w.jobs {
		wid := activeWorkers[w.Node.h.Hash(job.Key, int64(len(activeWorkers)))]
		if wid != w.ID {
			rebalanced[job.Key] = job
		}
	}
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
		delete(w.jobs, key)
		cherr, err := w.Node.requeueJob(ctx, w.ID, job)
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
	go w.Node.processRequeuedJobs(ctx, w.ID, cherrs)
}

// requeueJobs requeues the jobs handled by the worker.
// This should be done after the worker is stopped.
func (w *Worker) requeueJobs(ctx context.Context) error {
	w.lock.Lock()
	jobCount := len(w.jobs)
	if jobCount == 0 {
		w.lock.Unlock()
		return nil
	}
	w.logger.Debug("requeueJobs: requeuing", "jobs", jobCount)
	jobsToRequeue := make(map[string]*Job, jobCount)
	for k, v := range w.jobs {
		jobsToRequeue[k] = v
	}
	createdAt := strconv.FormatInt(w.CreatedAt.UnixNano(), 10)
	w.lock.Unlock()

	// First mark the worker as inactive so that requeued jobs are not assigned to this worker
	// Use optimistic locking to avoid race conditions.
	prev, err := w.Node.workerMap.TestAndSet(ctx, w.ID, createdAt, "-")
	if err != nil {
		return fmt.Errorf("requeueJobs: failed to mark worker as inactive: %w", err)
	}
	if prev == "-" || prev == "" {
		w.logger.Debug("requeueJobs: worker already marked as inactive, skipping requeue")
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

	for key, job := range jobsToRequeue {
		wg.Add(1)
		go func(k string, j *Job) {
			defer wg.Done()
			err := w.requeueJob(ctx, j)
			resultChan <- result{key: k, err: err}
		}(key, job)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	remainingJobs := make(map[string]*Job)
	for {
		select {
		case res, ok := <-resultChan:
			if !ok {
				return remainingJobs
			}
			if res.err != nil {
				w.logger.Error(fmt.Errorf("requeueJobs: failed to requeue job %q: %w", res.key, res.err))
				remainingJobs[res.key] = jobsToRequeue[res.key]
				continue
			}
			delete(remainingJobs, res.key)
			w.logger.Info("requeued", "job", res.key)
		case <-time.After(w.workerTTL):
			w.logger.Error(fmt.Errorf("requeueJobs: timeout reached, some jobs may not have been processed"))
			return remainingJobs
		}
	}
}

// requeueJob requeues a job.
func (w *Worker) requeueJob(ctx context.Context, job *Job) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	err := w.stopJob(ctx, job.Key)
	if err != nil {
		return fmt.Errorf("failed to stop job: %w", err)
	}

	eventID, err := w.Node.poolStream.Add(ctx, evStartJob, marshalJob(job))
	if err != nil {
		return fmt.Errorf("requeueJob: failed to add job to pool stream: %w", err)
	}
	w.Node.pendingJobs[eventID] = nil
	return nil
}

// cleanup removes the worker from the workers, keep-alive and jobs maps.
func (w *Worker) cleanup(ctx context.Context) {
	if _, err := w.Node.workerMap.Delete(ctx, w.ID); err != nil {
		w.logger.Error(fmt.Errorf("failed to remove worker from worker map: %w", err))
	}
	if _, err := w.keepAliveMap.Delete(ctx, w.ID); err != nil {
		w.logger.Error(fmt.Errorf("failed to remove worker from keep alive map: %w", err))
	}
	keys, err := w.jobsMap.Delete(ctx, w.ID)
	if err != nil {
		w.logger.Error(fmt.Errorf("failed to remove worker from jobs map: %w", err))
	}
	if keys != "" {
		for _, key := range strings.Split(keys, ",") {
			if _, err := w.jobPayloadsMap.Delete(ctx, key); err != nil {
				w.logger.Error(fmt.Errorf("worker stop: failed to remove job payload %q from job payloads map: %w", key, err))
			}
		}
	}
}

// workerStreamName returns the name of the stream used to communicate with the
// worker with the given ID.
func workerStreamName(id string) string {
	return "worker:" + id
}
