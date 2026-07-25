// Node-owned settlement separates durable handler outcomes from worker intake.
// Once a handler returns, worker removal may stop Redis intake but cannot cancel
// the exact dispatch obligation that atomically records the result and settles
// the original pool event.
package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/pulse"
)

type (
	// dispatchSettlements tracks node-owned terminal outcomes by worker so
	// graceful worker and node closure can join the exact obligations.
	dispatchSettlements struct {
		mu      sync.Mutex
		changed chan struct{}
		total   int
		workers map[string]int
		errs    map[string]error
	}
)

// newDispatchSettlements creates an empty obligation tracker.
func newDispatchSettlements() *dispatchSettlements {
	return &dispatchSettlements{
		changed: make(chan struct{}),
		workers: make(map[string]int),
		errs:    make(map[string]error),
	}
}

// ownDispatchSettlement transfers one known handler outcome from worker
// intake to a node-owned retry loop before intake can observe cancellation.
func (node *Node) ownDispatchSettlement(
	worker *Worker,
	routingNodeID, workerEventID string,
	job *Job,
	resultErr error,
) {
	finish := node.settlements.begin(worker.ID)
	pulse.Go(node.logger, func() {
		err := node.retryDispatchSettlement(job, resultErr)
		if err == nil {
			worker.markDispatchSettled(job.Key, job.dispatchID)
			if routingNodeID == node.ID {
				node.pendingEvents.Delete(pendingEventKey(worker.ID, workerEventID))
			} else {
				ackCtx, cancel := context.WithTimeout(
					context.Background(),
					min(node.workerTTL, time.Second),
				)
				worker.ackPoolEvent(ackCtx, routingNodeID, workerEventID, resultErr)
				cancel()
			}
		}
		finish(err)
	})
}

// retryDispatchSettlement retries transient Redis failures independently of
// worker cancellation. Generation loss is terminal and remains observable to
// RemoveWorker and Close.
func (node *Node) retryDispatchSettlement(job *Job, resultErr error) error {
	delay := 100 * time.Millisecond
	for {
		_, err := node.settleDispatch(context.Background(), job.Key, job.dispatchID, resultErr)
		if err == nil {
			return nil
		}
		node.logger.Error(err, "job", job.Key, "dispatch", job.dispatchID, "retry_in", delay)
		if errors.Is(err, ErrPoolGenerationLost) {
			return err
		}
		time.Sleep(delay)
		delay = min(delay*2, 5*time.Second)
	}
}

// releaseCrashedDispatchStart removes stale execution ownership only while the
// exact dispatch remains active. A false result means settlement won the race,
// so normal settled-job recovery may proceed.
func (node *Node) releaseCrashedDispatchStart(
	ctx context.Context,
	lease *workerCleanupLease,
	key, dispatchID string,
) (bool, error) {
	var workerID, owner, fence string
	if lease != nil {
		workerID = lease.workerID
		owner = lease.owner
		fence = lease.fence
	}
	result, err := releaseCrashedDispatchStartScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.jobPending),
			dispatchRecordKey(node.resources.dispatches, dispatchID),
			rmapContentKey(node.resources.jobs),
			rmapUpdateChannel(node.resources.jobs),
			rmapContentKey(node.resources.jobPayloads),
			rmapUpdateChannel(node.resources.jobPayloads),
			rmapContentKey(node.resources.workerCleanup),
		},
		node.resources.generation,
		workerID,
		key,
		dispatchID,
		"active",
		owner,
		fence,
	).Int64()
	if err != nil {
		return false, fmt.Errorf(
			"release crashed exact dispatch %q: %w",
			dispatchID,
			poolBoundaryError(err),
		)
	}
	return result == 1, nil
}

// activeDispatchID reads the authoritative job-key admission index. Stale
// worker recovery cannot rely on the eventually updated local rmap projection.
func (node *Node) activeDispatchID(ctx context.Context, key string) (string, error) {
	dispatchID, err := node.rdb.HGet(
		ctx,
		rmapContentKey(node.resources.jobPending),
		key,
	).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read active dispatch for job %q: %w", key, err)
	}
	return dispatchID, nil
}

// begin registers an obligation synchronously and returns its completion
// callback. Registration precedes the worker loop's next cancellation point.
func (s *dispatchSettlements) begin(workerID string) func(error) {
	s.mu.Lock()
	s.total++
	s.workers[workerID]++
	s.signalLocked()
	s.mu.Unlock()
	return func(err error) {
		s.finish(workerID, err)
	}
}

// waitWorker joins all outcomes observed by one worker before its jobs or
// distributed registration can be removed.
func (s *dispatchSettlements) waitWorker(ctx context.Context, workerID string) error {
	return s.wait(ctx, workerID)
}

// waitAll joins all node-owned outcomes before graceful node closure proceeds
// to detach the pool sink or destroy node resources.
func (s *dispatchSettlements) waitAll(ctx context.Context) error {
	return s.wait(ctx, "")
}

// finish records terminal settlement failure and wakes closure waiters.
func (s *dispatchSettlements) finish(workerID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total--
	s.workers[workerID]--
	if s.workers[workerID] == 0 {
		delete(s.workers, workerID)
	}
	if err != nil {
		s.errs[workerID] = errors.Join(s.errs[workerID], err)
	}
	s.signalLocked()
}

// wait blocks on tracker state changes without coupling the obligation to the
// caller's cancellation. A cancelled caller receives an explicit pending error
// and may retry closure with a fresh context.
func (s *dispatchSettlements) wait(ctx context.Context, workerID string) error {
	for {
		s.mu.Lock()
		count := s.total
		if workerID != "" {
			count = s.workers[workerID]
		}
		if count == 0 {
			err := s.errs[workerID]
			if workerID == "" {
				for _, workerErr := range s.errs {
					err = errors.Join(err, workerErr)
				}
			}
			s.mu.Unlock()
			return err
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("%d terminal dispatch settlements still pending: %w", count, ctx.Err())
		}
	}
}

// signalLocked broadcasts a tracker state change. The caller must hold mu.
func (s *dispatchSettlements) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}
