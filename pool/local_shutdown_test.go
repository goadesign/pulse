// These tests construct already accepted jobs with stopped intake and no Redis
// client. They check local handler release, joining, and retry through worker
// shutdown and node cleanup without running distributed recovery.
package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
)

func TestLocalShutdownReleasesAcceptedJobsWithoutDistributedState(t *testing.T) {
	var stopped []string
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(key string) error {
			stopped = append(stopped, key)
			return nil
		},
	}, "first", "second")
	job, ok := worker.jobs.Load("first")
	require.True(t, ok)
	job.(*Job).dispatchID = "pending-outcome"

	require.NoError(t, worker.stop(context.Background()))
	assert.ElementsMatch(t, []string{"first", "second"}, stopped)
	assert.Empty(t, worker.Jobs())
	assert.True(t, worker.IsStopped())

	require.NoError(t, worker.stop(context.Background()))
	assert.Len(t, stopped, 2, "successful Stops must not be repeated")
}

func TestLocalShutdownRetriesOnlyFailedStops(t *testing.T) {
	firstErr := errors.New("first handler still running")
	secondErr := errors.New("second handler still running")
	failures := map[string]error{"first": firstErr, "second": secondErr}
	calls := make(map[string]int)
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(key string) error {
			calls[key]++
			return failures[key]
		},
	}, "first", "second", "success")

	err := worker.stopLocal()
	assert.ErrorIs(t, err, firstErr)
	assert.ErrorIs(t, err, secondErr)
	require.Len(t, worker.Jobs(), 2)
	assert.Equal(t, map[string]int{"first": 1, "second": 1, "success": 1}, calls)

	clear(failures)
	require.NoError(t, worker.stopLocal())
	assert.Empty(t, worker.Jobs())
	assert.Equal(t, map[string]int{"first": 2, "second": 2, "success": 1}, calls)
}

func TestLocalShutdownJoinsIntakeAfterItWasAlreadyStopped(t *testing.T) {
	stopped := make(chan struct{}, 1)
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(string) error {
			stopped <- struct{}{}
			return nil
		},
	}, "job")
	worker.wg.Add(1)
	result := make(chan error, 1)
	go func() {
		result <- worker.stopLocal()
	}()

	select {
	case err := <-result:
		t.Errorf("shutdown returned before intake exited: %v", err)
	case <-stopped:
		t.Error("Stop ran before intake exited")
	case <-time.After(20 * time.Millisecond):
	}
	worker.wg.Done()
	require.NoError(t, awaitLocalShutdown(t, result))
	select {
	case <-stopped:
	default:
		t.Error("accepted handler was not stopped")
	}
}

func TestLocalShutdownConcurrentCallsWaitForTheSameStop(t *testing.T) {
	stopping := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(string) error {
			calls.Add(1)
			stopping <- struct{}{}
			<-release
			return nil
		},
	}, "job")
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		first <- worker.stopLocal()
	}()
	select {
	case <-stopping:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Stop did not begin")
	}
	go func() {
		second <- worker.stopLocal()
	}()
	select {
	case err := <-first:
		t.Errorf("first shutdown returned before Stop finished: %v", err)
	case err := <-second:
		t.Errorf("second shutdown returned before Stop finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)

	require.NoError(t, awaitLocalShutdown(t, first))
	require.NoError(t, awaitLocalShutdown(t, second))
	assert.Equal(t, int32(1), calls.Load())
}

func TestLocalShutdownCloseAfterCleanupWaitsForStop(t *testing.T) {
	stopping := make(chan struct{})
	release := make(chan struct{})
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(string) error {
			close(stopping)
			<-release
			return nil
		},
	}, "job")
	node := newLocalShutdownNode(worker)
	node.cleanupComplete = true
	result := make(chan error, 1)
	go func() {
		result <- node.Close(context.Background())
	}()

	select {
	case <-stopping:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Close did not call Stop")
	}
	select {
	case err := <-result:
		t.Errorf("Close returned before Stop finished: %v", err)
	default:
	}
	assert.False(t, node.IsClosed())
	close(release)

	require.NoError(t, awaitLocalShutdown(t, result))
	assert.True(t, node.IsClosed())
	assert.Empty(t, node.Workers())
	assert.Empty(t, worker.Jobs())
}

func TestLocalShutdownAfterDistributedCleanupRetriesFailedStops(t *testing.T) {
	for _, cleanupComplete := range []bool{false, true} {
		name := "lost-generation"
		if cleanupComplete {
			name = "completed-cleanup"
		}
		t.Run(name, func(t *testing.T) {
			stopErr := errors.New("handler still running")
			calls := make(map[string]int)
			worker := newLocalShutdownWorker(&mockJobHandler{
				stopFunc: func(key string) error {
					calls[key]++
					if key == "retry" {
						return stopErr
					}
					return nil
				},
			}, "success", "retry")
			node := newLocalShutdownNode(worker)
			node.cleanupComplete = cleanupComplete
			closeNode := func() error {
				if cleanupComplete {
					return node.Close(context.Background())
				}
				return node.closeAfterDistributedLoss(context.Background(), false)
			}

			assert.ErrorIs(t, closeNode(), stopErr)
			assert.False(t, node.IsClosed())
			assert.Len(t, node.Workers(), 1)
			require.Len(t, worker.Jobs(), 1)
			assert.Equal(t, "retry", worker.Jobs()[0].Key)
			select {
			case <-node.closed:
				t.Error("public closure channel closed while a Stop failed")
			default:
			}

			stopErr = nil
			require.NoError(t, closeNode())
			assert.True(t, node.IsClosed())
			assert.Equal(t, cleanupComplete, node.IsShutdown())
			assert.Empty(t, node.Workers())
			assert.Empty(t, worker.Jobs())
			assert.Equal(t, map[string]int{"success": 1, "retry": 2}, calls)
			require.NoError(t, closeNode())
			assert.Equal(t, map[string]int{"success": 1, "retry": 2}, calls)
		})
	}
}

func TestLocalShutdownOrdinaryCloseAndRemovalReturnStopFailure(t *testing.T) {
	for _, removal := range []bool{false, true} {
		name := "close"
		if removal {
			name = "remove-worker"
		}
		t.Run(name, func(t *testing.T) {
			stopErr := errors.New("handler still running")
			worker := newLocalShutdownWorker(&mockJobHandler{
				stopFunc: func(string) error {
					return stopErr
				},
			}, "job")
			node := newLocalShutdownNode(worker)

			var err error
			if removal {
				err = node.RemoveWorker(context.Background(), worker)
			} else {
				err = node.close(context.Background(), false)
			}
			assert.ErrorIs(t, err, stopErr)
			assert.False(t, node.IsClosed())
			assert.Len(t, node.Workers(), 1)
			assert.Len(t, worker.Jobs(), 1)
		})
	}
}

func TestLocalShutdownPreservesSettlementFailureAfterSuccessfulStops(t *testing.T) {
	calls := 0
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(string) error {
			calls++
			return nil
		},
	}, "job")
	node := newLocalShutdownNode(worker)
	node.cleanupComplete = true
	settlementErr := errors.New("dispatch outcome could not be saved")
	node.settlements.begin(worker.ID)(settlementErr)

	assert.ErrorIs(t, node.Close(context.Background()), settlementErr)
	assert.True(t, node.IsClosed())
	assert.Empty(t, node.Workers())
	assert.Empty(t, worker.Jobs())
	assert.ErrorIs(t, node.Close(context.Background()), settlementErr)
	assert.Equal(t, 1, calls)
}

// newLocalShutdownWorker represents accepted jobs after worker intake stopped.
// No streams or Redis clients are constructed, so any Stop can use only local
// state and a successful repeated worker stop needs no distributed operation.
func newLocalShutdownWorker(handler JobHandler, keys ...string) *Worker {
	worker := &Worker{
		ID:              "local-worker",
		handler:         handler,
		logger:          pulse.NoopLogger(),
		stopped:         true,
		streamDestroyed: true,
	}
	for _, key := range keys {
		worker.jobs.Store(key, &Job{Key: key, Payload: []byte("synthetic job")})
	}
	return worker
}

// newLocalShutdownNode owns the synthetic worker and local closure channels.
// Distributed resources are absent, so tests can exercise completed cleanup
// or a Stop failure that returns before distributed removal begins.
func newLocalShutdownNode(worker *Worker) *Node {
	node := &Node{
		logger:         pulse.NoopLogger(),
		stop:           make(chan struct{}),
		closed:         make(chan struct{}),
		scheduleCancel: func() {},
		settlements:    newDispatchSettlements(),
	}
	node.localWorkers.Store(worker.ID, worker)
	return node
}

// awaitLocalShutdown waits for a synthetic stop result and fails the test if
// the local call never finishes, so a joining regression cannot hang the suite.
func awaitLocalShutdown(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("local shutdown did not finish")
		return nil
	}
}
