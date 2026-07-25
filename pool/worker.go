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
	redis "github.com/redis/go-redis/v9"
	"goa.design/clue/log"

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
	"goa.design/pulse/streaming"
	"goa.design/pulse/streaming/options"
)

type (
	// Worker is a worker that handles jobs with a given payload type.
	Worker struct {
		// Unique worker ID
		ID string
		// Time worker was created.
		CreatedAt time.Time

		node           *Node
		handler        JobHandler
		stream         *streaming.Stream
		reader         *streaming.Reader
		done           chan struct{}
		jobsMap        *rmap.Map
		jobPayloadsMap *rmap.Map
		keepAliveMap   *rmap.Map
		shutdownMap    *rmap.Map
		workerTTL      time.Duration
		requeueTimeout time.Duration
		logger         pulse.Logger
		wg             sync.WaitGroup
		rebalanceLock  sync.Mutex

		jobs        sync.Map // jobs being handled by the worker indexed by job key
		nodeStreams sync.Map

		lock    sync.RWMutex
		stopped bool
		// streamDestroyed records completion of the retryable distributed stop
		// side effect after local worker goroutines have stopped.
		streamDestroyed bool
	}

	// Job is a job that can be added to a worker.
	Job struct {
		// Key is used to identify the worker that handles the job.
		Key string
		// Payload is the job payload.
		Payload []byte
		// CreatedAt is the time the job was created.
		CreatedAt time.Time
		// Requeued indicates that this start event moves or recovers an existing
		// durable job payload rather than admitting a new dispatched job.
		Requeued bool
		// Worker is the worker that handles the job.
		Worker *Worker
		// NodeID is the ID of the node that created the job.
		NodeID string
		// dispatchID correlates an admitted dispatch before its event is
		// published. It is intentionally not part of the public job contract.
		dispatchID string
	}

	// requeueResult reports one concurrent handoff attempt.
	requeueResult struct {
		key string
		err error
	}

	// JobHandler starts and stops jobs.
	JobHandler interface {
		// Start starts a job.
		Start(job *Job) error
		// Stop stops a job with a given key.
		Stop(key string) error
	}

	// NotificationHandler handles notifications for jobs owned by the worker.
	NotificationHandler interface {
		// HandleNotification handles a job-scoped notification.
		HandleNotification(key string, payload []byte) error
	}

	// MessageHandler handles keyed messages that are routed by the pool hash ring
	// without requiring a running job with the same key. Returning ErrRequeue
	// leaves the message pending for redelivery; any other error is terminal.
	MessageHandler interface {
		// HandleMessage handles a keyed message.
		HandleMessage(key string, payload []byte) error
	}

	// ack is a worker event acknowledgement.
	ack struct {
		// EventID is the ID of the event being acknowledged.
		EventID string
		// JobKey is the singleton admission key completed by this ack.
		JobKey string
		// Error is the error that occurred while handling the event if any.
		Error string
	}
)

var errJobNotOwned = errors.New("job not owned by worker")

// newWorker creates a new worker.
func newWorker(ctx context.Context, node *Node, h JobHandler) (*Worker, error) {
	if err := node.ensureGenerationActive(ctx); err != nil {
		return nil, err
	}
	wid := ulid.Make().String()
	createdAt, err := node.rdb.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to read Redis time for worker %q: %w", wid, err)
	}
	stream, err := streaming.NewStream(workerStreamName(wid), node.rdb, options.WithStreamLogger(node.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create jobs stream for worker %q: %w", wid, err)
	}
	if _, err := stream.Add(ctx, evInit, marshalEnvelope(node.ID, []byte(wid))); err != nil {
		destroyErr := stream.Destroy(context.WithoutCancel(ctx))
		return nil, errors.Join(
			fmt.Errorf("failed to add init event to worker stream %q: %w", workerStreamName(wid), err),
			destroyErr,
		)
	}
	reader, err := stream.NewReader(ctx, options.WithReaderBlockDuration(node.workerTTL/2), options.WithReaderStartAtOldest())
	if err != nil {
		destroyErr := stream.Destroy(context.WithoutCancel(ctx))
		return nil, errors.Join(
			fmt.Errorf("failed to create reader for worker %q: %w", wid, err),
			destroyErr,
		)
	}
	if err := node.setPoolMapAndWait(
		ctx,
		node.workerMap,
		node.resources.workers,
		wid,
		strconv.FormatInt(createdAt.UnixNano(), 10),
	); err != nil {
		reader.Close()
		destroyErr := stream.Destroy(context.WithoutCancel(ctx))
		return nil, errors.Join(
			fmt.Errorf("failed to add worker %q to pool %q: %w", wid, node.PoolName, err),
			destroyErr,
		)
	}
	now, err := node.updateWorkerHeartbeat(ctx, wid)
	if err == nil {
		err = waitPoolMapValue(ctx, node.workerKeepAliveMap, wid, now)
	}
	if err != nil {
		removeErr := node.deletePoolMap(context.WithoutCancel(ctx), node.resources.workers, wid)
		reader.Close()
		destroyErr := stream.Destroy(context.WithoutCancel(ctx))
		return nil, errors.Join(
			fmt.Errorf("failed to update worker keep-alive: %w", err),
			removeErr,
			destroyErr,
		)
	}
	w := &Worker{
		ID:             wid,
		node:           node,
		handler:        h,
		CreatedAt:      createdAt,
		stream:         stream,
		reader:         reader,
		done:           make(chan struct{}),
		jobsMap:        node.jobMap,
		jobPayloadsMap: node.jobPayloadMap,
		keepAliveMap:   node.workerKeepAliveMap,
		shutdownMap:    node.nodeShutdownMap,
		workerTTL:      node.workerTTL,
		requeueTimeout: node.requeueTimeout,
		logger:         node.logger.WithPrefix("worker", wid),
		jobs:           sync.Map{},
		nodeStreams:    sync.Map{},
	}

	w.logger.Info("created",
		"worker_ttl", w.workerTTL,
		"worker_requeue_timeout", w.requeueTimeout)

	w.wg.Add(2)

	// Create new context for the worker so that canceling the original one does
	// not cancel the worker.
	logCtx := context.Background()
	logCtx = log.WithContext(logCtx, ctx)
	events := reader.Subscribe()
	pulse.Go(w.logger, func() { w.handleEvents(logCtx, events) })
	pulse.Go(w.logger, func() { w.keepAlive(logCtx) })

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
			Requeued:  job.Requeued,
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
			if err := w.refreshHeartbeat(ctx); err != nil {
				w.logger.Error(fmt.Errorf("worker intake heartbeat failed: %w", err))
				if redis.HasErrorPrefix(err, "WORKERCLEANUPLOST") {
					return
				}
				continue
			}
			nodeID, payload, err := unmarshalEnvelope(ev.Payload)
			if err != nil {
				w.dropMalformedEvent(ctx, ev, fmt.Errorf("decode worker event envelope: %w", err))
				continue
			}
			var dispatched *Job
			switch ev.EventName {
			case evInit:
				w.logger.Debug("handleEvents: received init", "event", ev.EventName, "id", ev.ID)
				continue
			case evStartJob:
				w.logger.Debug("handleEvents: received start job", "event", ev.EventName, "id", ev.ID)
				dispatched, err = unmarshalJob(payload)
				if err == nil {
					err = w.startJob(ctx, dispatched)
				}
			case evMessage:
				w.logger.Debug("handleEvents: received message", "event", ev.EventName, "id", ev.ID)
				var key string
				key, payload, err = unmarshalKeyedPayload(payload)
				if err == nil {
					err = w.message(key, payload)
				}
			case evStopJob:
				w.logger.Debug("handleEvents: received stop job", "event", ev.EventName, "id", ev.ID)
				var key string
				key, err = unmarshalJobKey(payload)
				if err == nil {
					err = w.stopJob(ctx, key)
				}
			case evNotify:
				w.logger.Debug("handleEvents: received notify", "event", ev.EventName, "id", ev.ID)
				var key string
				key, payload, err = unmarshalKeyedPayload(payload)
				if err == nil {
					err = w.notify(ctx, key, payload)
				}
			default:
				err = fmt.Errorf("unknown worker event %q", ev.EventName)
			}
			if err != nil {
				if redis.HasErrorPrefix(err, "WORKERCLEANUPLOST") {
					return
				}
				if errors.Is(err, ErrRequeue) {
					w.logger.Info("requeue", "event", ev.EventName, "id", ev.ID)
					continue
				}
				if dispatched != nil && dispatched.dispatchID != "" {
					w.node.ownDispatchSettlement(w, nodeID, ev.ID, dispatched, err)
				} else {
					w.ackPoolEvent(ctx, nodeID, ev.ID, err)
				}
				w.logger.Error(fmt.Errorf("handler failed: %w", err), "event", ev.EventName, "id", ev.ID)
				continue
			}
			if dispatched != nil && dispatched.dispatchID != "" {
				w.node.ownDispatchSettlement(w, nodeID, ev.ID, dispatched, nil)
			} else {
				w.ackPoolEvent(ctx, nodeID, ev.ID, nil)
			}
		case <-w.done:
			w.logger.Debug("handleEvents: done")
			return
		}
	}
}

// dropMalformedEvent logs and removes an envelope that cannot identify its
// sender, so a permanent worker loop never reprocesses the poison entry.
func (w *Worker) dropMalformedEvent(ctx context.Context, event *streaming.Event, decodeErr error) {
	w.logger.Error(decodeErr, "event", event.EventName, "id", event.ID)
	if err := w.stream.Remove(ctx, event.ID); err != nil {
		w.logger.Error(fmt.Errorf("drop malformed worker event %s: %w", event.ID, err))
	}
}

// stop stops the reader, destroys the stream and closes the worker.
func (w *Worker) stop(ctx context.Context) error {
	w.stopLocal()
	w.lock.RLock()
	destroyed := w.streamDestroyed
	w.lock.RUnlock()
	if destroyed {
		return nil
	}
	if err := w.stream.Destroy(ctx); err != nil {
		return fmt.Errorf("failed to destroy stream for worker: %w", err)
	}
	w.lock.Lock()
	w.streamDestroyed = true
	w.lock.Unlock()
	return nil
}

// stopLocal stops and joins worker intake without mutating Redis. It is used
// when pool cleanup already destroyed the worker's generation-owned resources.
func (w *Worker) stopLocal() {
	firstAttempt := w.stopIntake()
	if firstAttempt {
		w.wg.Wait()
	}
}

// stopIntake closes worker-owned input without joining the calling goroutine.
func (w *Worker) stopIntake() bool {
	w.lock.Lock()
	firstAttempt := !w.stopped
	if firstAttempt {
		w.stopped = true
	}
	w.lock.Unlock()

	if firstAttempt {
		close(w.done)
		w.reader.Close()
	}
	return firstAttempt
}

// startJob starts a job.
func (w *Worker) startJob(ctx context.Context, job *Job) error {
	if w.IsStopped() {
		return fmt.Errorf("worker %q stopped", w.ID)
	}
	if err := w.refreshHeartbeat(ctx); err != nil {
		return err
	}
	if err := w.node.ensureGenerationActive(ctx); err != nil {
		return err
	}
	if job.dispatchID != "" {
		claimed, err := w.claimDispatchedStart(ctx, job)
		if err != nil {
			return errors.Join(ErrRequeue, err)
		}
		if !claimed {
			return ErrRequeue
		}
	} else {
		if err := w.node.appendPoolMapValue(ctx, w.node.resources.jobs, w.ID, job.Key); err != nil {
			w.logger.Error(fmt.Errorf("failed to add job %q to jobs map: %w, requeueing", job.Key, err))
			return ErrRequeue
		}
		if err := w.node.setPoolMap(ctx, w.node.resources.jobPayloads, job.Key, string(job.Payload)); err != nil {
			w.logger.Error(fmt.Errorf("failed to add job payload %q to job payloads map: %w, requeueing", job.Key, err))
			if cleanupErr := w.cleanupFailedStart(ctx, job.Key); cleanupErr != nil {
				return errors.Join(
					ErrRequeue,
					fmt.Errorf("persist job payload %q: %w", job.Key, err),
					cleanupErr,
				)
			}
			return ErrRequeue
		}
	}
	job.Worker = w
	if err := w.handler.Start(job); err != nil {
		w.logger.Debug("handler failed to start job", "job", job.Key, "error", err)
		if cleanupErr := w.cleanupFailedStart(ctx, job.Key); cleanupErr != nil {
			return errors.Join(ErrRequeue, err, cleanupErr)
		}
		return err
	}
	w.logger.Info("started job", "job", job.Key)
	w.jobs.Store(job.Key, job)
	return nil
}

// claimDispatchedStart creates durable ownership exactly once for the pending
// dispatch capability before the handler can run.
func (w *Worker) claimDispatchedStart(ctx context.Context, job *Job) (bool, error) {
	identity, err := dispatchIdentity(job.Key, job.Payload)
	if err != nil {
		return false, fmt.Errorf("claim dispatched start for job %q: %w", job.Key, err)
	}
	result, err := claimWorkerStartScript.Run(
		ctx,
		w.node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", w.node.poolStream.Name),
			rmapContentKey(w.node.resources.jobPending),
			dispatchRecordKey(w.node.resources.dispatches, job.dispatchID),
			rmapContentKey(w.node.resources.jobPayloads),
			rmapContentKey(w.node.resources.jobs),
			rmapUpdateChannel(w.node.resources.jobs),
			rmapUpdateChannel(w.node.resources.jobPayloads),
			rmapContentKey(w.node.resources.workers),
			rmapContentKey(w.node.resources.workerCleanup),
		},
		w.node.resources.generation,
		"active",
		job.Key,
		job.dispatchID,
		w.ID,
		job.Payload,
		identity,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("claim dispatched start for job %q: %w", job.Key, err)
	}
	return result == 1, nil
}

// cleanupFailedStart atomically removes durable ownership and payload under the
// exact pool-stream generation. Callers must retry the event while this recipe
// fails and may report terminal handler failure only after it succeeds.
func (w *Worker) cleanupFailedStart(ctx context.Context, key string) error {
	err := cleanupFailedStartScript.Run(
		ctx,
		w.node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", w.node.poolStream.Name),
			rmapContentKey(w.node.resources.jobs),
			rmapUpdateChannel(w.node.resources.jobs),
			rmapContentKey(w.node.resources.jobPayloads),
			rmapUpdateChannel(w.node.resources.jobPayloads),
		},
		w.node.resources.generation,
		w.ID,
		key,
		"active",
	).Err()
	if err != nil {
		return fmt.Errorf("clean failed start for job %q: %w", key, err)
	}
	return nil
}

// markDispatchSettled makes a successfully started exact-dispatch job eligible
// for ordinary running-job rebalancing only after its original dispatch event
// and durable terminal record have settled atomically.
func (w *Worker) markDispatchSettled(key, dispatchID string) {
	w.lock.Lock()
	defer w.lock.Unlock()
	value, ok := w.jobs.Load(key)
	if !ok {
		return
	}
	job := value.(*Job)
	if job.dispatchID != dispatchID {
		return
	}
	settled := *job
	settled.dispatchID = ""
	w.jobs.Store(key, &settled)
}

// stopJob stops a job.
func (w *Worker) stopJob(ctx context.Context, key string) error {
	if err := w.releaseJob(ctx, key); err != nil {
		if errors.Is(err, errJobNotOwned) {
			return ErrRequeue
		}
		return err
	}
	if err := w.node.deletePoolMap(ctx, w.node.resources.jobPayloads, key); err != nil {
		w.logger.Error(fmt.Errorf("stop job: failed to remove job payload %q from job payloads map: %w", key, err))
	}
	w.logger.Info("stopped job", "job", key)
	return nil
}

// releaseJob stops local execution and removes this worker's ownership while
// preserving the shared payload for another worker to claim.
func (w *Worker) releaseJob(ctx context.Context, key string) error {
	if _, ok := w.jobs.Load(key); !ok {
		return fmt.Errorf("%w: %s", errJobNotOwned, key)
	}
	if err := w.node.ensureGenerationActive(ctx); err != nil {
		return err
	}
	if err := w.handler.Stop(key); err != nil {
		return fmt.Errorf("failed to stop job %q: %w", key, err)
	}
	w.logger.Debug("stopped job", "job", key)
	w.jobs.Delete(key)
	if err := w.node.removePoolMapValue(ctx, w.node.resources.jobs, w.ID, key); err != nil {
		return fmt.Errorf("failed to release job %q from jobs map: %w", key, err)
	}
	return nil
}

// notify delivers a job-scoped notification after verifying local ownership.
func (w *Worker) notify(_ context.Context, key string, payload []byte) error {
	if w.IsStopped() {
		w.logger.Debug("worker stopped, ignoring notification")
		return nil
	}
	if _, ok := w.jobs.Load(key); !ok {
		return ErrRequeue
	}
	nh, ok := w.handler.(NotificationHandler)
	if !ok {
		w.logger.Error(fmt.Errorf("worker does not implement NotificationHandler, ignoring notification"), "worker", w.ID)
		return nil
	}
	w.logger.Debug("handled notification", "payload", string(payload))
	return nh.HandleNotification(key, payload)
}

// message handles a keyed message routed by the pool hash ring. Unlike
// notifications, messages are independent of job ownership.
func (w *Worker) message(key string, payload []byte) error {
	if w.IsStopped() {
		return fmt.Errorf("worker %q stopped", w.ID)
	}
	mh, ok := w.handler.(MessageHandler)
	if !ok {
		return fmt.Errorf("worker %q does not implement MessageHandler", w.ID)
	}
	w.logger.Debug("handled message", "payload", string(payload))
	return mh.HandleMessage(key, payload)
}

// ackPoolEvent publishes the worker outcome to the originating node. It retries
// while that node stream exists; a vanished exact generation leaves the
// original pool event to sink recovery, while dispatched starts are already
// durably terminal through settleDispatchedStart.
func (w *Worker) ackPoolEvent(ctx context.Context, nodeID, eventID string, ackerr error) {
	var msg string
	if ackerr != nil {
		msg = ackerr.Error()
	}
	ack := &ack{EventID: eventID, Error: msg}
	payload := marshalEnvelope(w.ID, marshalAck(ack))
	delay := 100 * time.Millisecond
	for {
		stream, err := w.node.getNodeStream(nodeID)
		if err == nil {
			_, err = stream.Add(ctx, evAck, payload, options.WithOnlyIfStreamExists())
		}
		if err == nil {
			return
		}
		if errors.Is(err, streaming.ErrStreamNotFound) ||
			errors.Is(err, streaming.ErrStreamDestroyed) {
			return
		}
		w.logger.Error(
			fmt.Errorf("failed to ack event %q from node %q: %w", eventID, nodeID, err),
			"retry_in",
			delay,
		)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			delay = min(delay*2, 5*time.Second)
		case <-ctx.Done():
			timer.Stop()
			return
		case <-w.done:
			timer.Stop()
			return
		}
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
			if err := w.refreshHeartbeat(ctx); err != nil {
				w.logger.Error(fmt.Errorf("failed to update worker keep-alive: %w", err))
				if redis.HasErrorPrefix(err, "WORKERCLEANUPLOST") {
					return
				}
			}
		case <-w.done:
			w.logger.Debug("keepAlive: done")
			return
		}
	}
}

// refreshHeartbeat renews the Redis-time worker liveness proof. A cleanup
// fence is terminal and closes intake before further handler or ownership work.
func (w *Worker) refreshHeartbeat(ctx context.Context) error {
	_, err := w.node.updateWorkerHeartbeat(ctx, w.ID)
	if redis.HasErrorPrefix(err, "WORKERCLEANUPLOST") {
		w.stopIntake()
	}
	return err
}

// rebalance rebalances the jobs handled by the worker.
func (w *Worker) rebalance(ctx context.Context, activeWorkers []string) {
	w.rebalanceLock.Lock()
	defer w.rebalanceLock.Unlock()

	w.logger.Debug("rebalance")
	rebalanced := make(map[string]*Job)
	w.jobs.Range(func(key, value any) bool {
		job := value.(*Job)
		if job.dispatchID != "" {
			return true
		}
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
	for key, job := range rebalanced {
		requeue := *job
		requeue.Requeued = true
		requeue.dispatchID = ""
		if err := w.releaseJob(ctx, key); err != nil {
			w.logger.Error(fmt.Errorf("rebalance: failed to release job: %w", err), "job", key)
			if _, ok := w.jobs.Load(key); !ok {
				if err := w.startJob(ctx, &requeue); err != nil {
					w.logger.Error(fmt.Errorf("rebalance: failed to restart job: %w", err), "job", key)
				}
			}
			continue
		}
		if _, err := w.node.poolStream.Add(ctx, evStartJob, marshalJob(&requeue)); err != nil {
			w.logger.Error(fmt.Errorf("rebalance: failed to requeue job: %w", err), "job", key)
			if err := w.startJob(ctx, &requeue); err != nil {
				w.logger.Error(fmt.Errorf("rebalance: failed to restart job: %w", err), "job", key)
				continue
			}
			continue
		}
		delete(rebalanced, key)
	}
}

// requeueJobs requeues the jobs handled by the worker.
// This should be done after the worker is stopped.
func (w *Worker) requeueJobs(ctx context.Context) error {
	jobsToRequeue := make(map[string]*Job)
	var unsettled []string
	jobCount := 0
	w.jobs.Range(func(key, value any) bool {
		job := value.(*Job)
		if job.dispatchID != "" {
			unsettled = append(unsettled, job.dispatchID)
			return true
		}
		jobsToRequeue[key.(string)] = job
		jobCount++
		return true
	})
	if len(unsettled) > 0 {
		sort.Strings(unsettled)
		return fmt.Errorf("requeueJobs: exact dispatch settlements still pending: %v", unsettled)
	}
	if jobCount == 0 {
		w.logger.Debug("requeueJobs: no jobs to requeue")
		return nil
	}
	w.logger.Debug("requeueJobs: requeuing", "jobs", jobCount)

	// Mark the worker inactive behind the cleanup fence so requeued jobs are
	// not assigned to this worker and exactly one party requeues: losing
	// deactivation means stale-worker cleanup owns (or already completed) the
	// requeue, so this worker must not publish duplicates.
	won, prev, err := w.node.deactivateWorker(ctx, w.ID)
	if err != nil {
		return fmt.Errorf("requeueJobs: failed to mark worker as inactive: %w", err)
	}
	if !won {
		w.logger.Debug("requeueJobs: requeue owned elsewhere, skipping", "registration", prev)
		return nil
	}
	if createdAt := strconv.FormatInt(w.CreatedAt.UnixNano(), 10); prev != createdAt {
		w.logger.Error(fmt.Errorf("requeueJobs: unexpected worker registration"), "worker", w.ID, "expected", createdAt, "got", prev)
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
	return w.attemptRequeueWith(ctx, jobsToRequeue, w.requeueJob)
}

// attemptRequeueWith runs every handoff concurrently under one timeout, joins
// all senders, and removes only jobs whose send completed successfully.
func (w *Worker) attemptRequeueWith(
	ctx context.Context,
	jobsToRequeue map[string]*Job,
	send func(context.Context, *Job) error,
) map[string]*Job {
	var wg sync.WaitGroup
	resultChan := make(chan requeueResult, len(jobsToRequeue))
	remainingJobs := make(map[string]*Job, len(jobsToRequeue))
	for key, job := range jobsToRequeue {
		remainingJobs[key] = job
	}

	attemptCtx, cancel := context.WithTimeout(ctx, w.requeueTimeout)
	defer cancel()
	wg.Add(len(jobsToRequeue))
	for key, job := range jobsToRequeue {
		pulse.Go(w.logger, func() {
			defer wg.Done()
			err := send(attemptCtx, job)
			if err != nil {
				w.logger.Error(fmt.Errorf("failed to requeue job: %w", err), "job", key)
			} else {
				w.logger.Debug("requeueJobs: requeued", "job", key)
			}
			resultChan <- requeueResult{key: key, err: err}
		})
	}

	timedOut := false
	for processed := 0; processed < len(jobsToRequeue); processed++ {
		select {
		case res := <-resultChan:
			if res.err != nil {
				w.logger.Error(fmt.Errorf("requeueJobs: failed to requeue job %q: %w", res.key, res.err))
				continue
			}
			delete(remainingJobs, res.key)
			w.logger.Info("requeued", "job", res.key)
		case <-attemptCtx.Done():
			timedOut = true
			cancel()
			processed = len(jobsToRequeue)
		}
	}
	wg.Wait()
	close(resultChan)
	for res := range resultChan {
		if res.err == nil {
			delete(remainingJobs, res.key)
			w.logger.Info("requeued", "job", res.key)
		}
	}
	if timedOut {
		w.logger.Error(fmt.Errorf("requeueJobs: timeout reached with %d jobs not handed off", len(remainingJobs)))
	}
	return remainingJobs
}

// requeueJob requeues a job.
func (w *Worker) requeueJob(ctx context.Context, job *Job) error {
	job.Requeued = true
	job.dispatchID = ""
	_, err := w.node.poolStream.Add(ctx, evStartJob, marshalJob(job))
	if err != nil {
		return fmt.Errorf("requeueJob: failed to add job to pool stream: %w", err)
	}

	// Stop locally, but do not touch the replicated job/payload maps: we want the
	// payload to remain available for distributed recovery until the job is
	// confirmed running elsewhere.
	if _, ok := w.jobs.Load(job.Key); ok {
		if err := w.handler.Stop(job.Key); err != nil {
			return fmt.Errorf("requeueJob: failed to stop job %q: %w", job.Key, err)
		}
		w.jobs.Delete(job.Key)
	}
	return nil
}

// workerStreamName returns the name of the stream used to communicate with the
// worker with the given ID.
func workerStreamName(id string) string {
	return "worker:" + id
}
