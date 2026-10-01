// These tests use synthetic jobs, stopped intake, and in-process saved-data
// operations with no Redis client. They check acceptance, local handler release,
// joining, and retry in the production shutdown algorithms. They do not run
// distributed recovery or the pool-wide shutdown barrier.
package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/pulse"
	"goa.design/pulse/streaming"
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

func TestLocalShutdownRejectsStartsBeforeHandlerAcceptance(t *testing.T) {
	for _, dispatchID := range []string{"", "synthetic-exact-dispatch"} {
		name := "ordinary"
		if dispatchID != "" {
			name = "exact"
		}
		t.Run(name, func(t *testing.T) {
			for _, requeued := range []bool{false, true} {
				calls := 0
				worker := newLocalShutdownWorker(&mockJobHandler{
					startFunc: func(*Job) error {
						calls++
						return nil
					},
				})
				original := &Job{
					Key:        "not-accepted",
					Payload:    []byte("synthetic payload"),
					Requeued:   requeued,
					dispatchID: dispatchID,
				}
				job, err := unmarshalJob(marshalJob(original))
				require.NoError(t, err)

				// handleEvents checks ErrRequeue before either exact settlement
				// or ordinary acknowledgement, leaving this delivery pending.
				err = worker.startJob(context.Background(), job)
				assert.ErrorIs(t, err, ErrRequeue)
				assert.Zero(t, calls)
				assert.Empty(t, worker.Jobs())
				assert.Nil(t, job.Worker)
				acks, settlements := completeLocalShutdownStart(t, worker, job, err)
				assert.Zero(t, acks)
				assert.Zero(t, settlements)
			}
		})
	}
}

func TestLocalShutdownPreservesHandlerStartOutcomes(t *testing.T) {
	handlerErr := errors.New("synthetic handler rejected job")
	cleanupErr := errors.New("synthetic failed-start cleanup failed")
	for _, dispatchID := range []string{"", "synthetic-exact-dispatch"} {
		name := "ordinary"
		if dispatchID != "" {
			name = "exact"
		}
		t.Run(name, func(t *testing.T) {
			for _, test := range []struct {
				name       string
				startErr   error
				cleanupErr error
				requeue    bool
			}{
				{name: "accepted"},
				{name: "terminal-handler-error", startErr: handlerErr},
				{name: "unfinished-cleanup", startErr: handlerErr, cleanupErr: cleanupErr, requeue: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					startCalls := 0
					worker := newLocalShutdownWorker(&mockJobHandler{
						startFunc: func(job *Job) error {
							startCalls++
							assert.Equal(t, "saved-job", job.Key)
							assert.Equal(t, dispatchID, job.dispatchID)
							return test.startErr
						},
					})
					worker.stopped = false
					job := &Job{Key: "saved-job", dispatchID: dispatchID}
					cleanupCalls := 0
					cleanup := func(_ context.Context, key string) error {
						cleanupCalls++
						assert.Equal(t, job.Key, key)
						return test.cleanupErr
					}

					// The ownership phase is already complete in this fixture.
					// Exercise the same handler-acceptance phase used by startJob.
					worker.jobsLock.Lock()
					err := worker.startHandler(context.Background(), job, cleanup)
					worker.jobsLock.Unlock()
					assert.Equal(t, 1, startCalls)
					assert.Same(t, worker, job.Worker)
					assert.Equal(t, test.requeue, errors.Is(err, ErrRequeue))
					acks, settlements := completeLocalShutdownStart(t, worker, job, err)
					if test.requeue {
						assert.Zero(t, acks)
						assert.Zero(t, settlements)
					} else if dispatchID == "" {
						assert.Equal(t, 1, acks)
						assert.Zero(t, settlements)
					} else {
						assert.Zero(t, acks)
						assert.Equal(t, 1, settlements)
					}
					if test.startErr == nil {
						require.NoError(t, err)
						assert.Zero(t, cleanupCalls)
						require.Len(t, worker.Jobs(), 1)
					} else {
						assert.ErrorIs(t, err, handlerErr)
						assert.Equal(t, 1, cleanupCalls)
						assert.Empty(t, worker.Jobs())
						if test.cleanupErr == nil {
							assert.Same(t, handlerErr, err)
						} else {
							assert.ErrorIs(t, err, cleanupErr)
						}
					}
				})
			}
		})
	}
}

func TestLocalShutdownAttemptsFailedStopOnceBeforeRetry(t *testing.T) {
	stopErr := errors.New("synthetic handler still running")
	calls := 0
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(string) error {
			calls++
			return stopErr
		},
	}, "accepted-job")
	node := newLocalShutdownNode(worker)

	assert.ErrorIs(t, node.close(context.Background(), true), stopErr)
	assert.Equal(t, 1, calls, "Shutdown must have only one handler-release phase")
	assert.False(t, node.IsClosed())
	assert.False(t, node.IsShutdown())
	assert.Len(t, node.Workers(), 1)
	assert.Len(t, worker.Jobs(), 1)

	cleanupCalls := 0
	cleanup := func(_ context.Context, shutdown bool) error {
		cleanupCalls++
		assert.True(t, shutdown)
		assert.Empty(t, worker.Jobs())
		node.localWorkers.Delete(worker.ID)
		return nil
	}
	assert.ErrorIs(t, node.closeWithCleanup(context.Background(), true, cleanup), stopErr)
	assert.Zero(t, cleanupCalls, "failed local release prevents durable cleanup")
	stopErr = nil
	require.NoError(t, node.closeWithCleanup(context.Background(), true, cleanup))
	assert.Equal(t, 3, calls)
	assert.Equal(t, 1, cleanupCalls)
	assert.Empty(t, worker.Jobs())
	assert.True(t, node.IsClosed())
	assert.True(t, node.IsShutdown())
	assert.Empty(t, node.Workers())
	require.NoError(t, node.closeWithCleanup(context.Background(), true, cleanup))
	assert.Equal(t, 3, calls)
	assert.Equal(t, 1, cleanupCalls)
}

func TestLocalShutdownRetriesSavedCleanupWithoutRepeatedStops(t *testing.T) {
	for _, failure := range []string{"read", "delete", "partial-delete"} {
		t.Run(failure, func(t *testing.T) {
			stopCalls := 0
			worker := newLocalShutdownWorker(&mockJobHandler{
				stopFunc: func(string) error {
					stopCalls++
					return nil
				},
			}, "accepted-job")
			node := newLocalShutdownNode(worker)
			node.resources.jobPayloads = "synthetic-payloads"
			savedKeys := []string{"accepted-job", "saved-only-job"}
			payloads := map[string]bool{"accepted-job": true, "saved-only-job": true}
			cleanupErr := errors.New("synthetic saved-data operation failed")
			fail := true
			readCalls := 0
			deleteCalls := make(map[string]int)
			readKeys := func(_ context.Context, workerID string) ([]string, error) {
				readCalls++
				assert.Equal(t, worker.ID, workerID)
				assert.Empty(t, worker.Jobs(), "saved-data read follows successful Stop")
				if fail && failure == "read" {
					return nil, cleanupErr
				}
				return savedKeys, nil
			}
			deleteEntry := func(_ context.Context, mapName, key string) error {
				assert.Equal(t, node.resources.jobPayloads, mapName)
				deleteCalls[key]++
				if fail && (failure == "delete" || failure == "partial-delete" && key == "saved-only-job") {
					return cleanupErr
				}
				delete(payloads, key)
				return nil
			}

			cleanup := func(ctx context.Context, shutdown bool) error {
				assert.True(t, shutdown)
				if err := node.cleanupShutdownJobs(ctx, readKeys, deleteEntry); err != nil {
					return err
				}
				savedKeys = nil
				node.localWorkers.Delete(worker.ID)
				return nil
			}
			assert.ErrorIs(t, node.closeWithCleanup(context.Background(), true, cleanup), cleanupErr)
			assert.Equal(t, 1, stopCalls)
			assert.False(t, node.IsClosed())
			assert.Len(t, node.Workers(), 1)
			assert.Equal(t, []string{"accepted-job", "saved-only-job"}, savedKeys)
			if failure == "read" {
				assert.Empty(t, deleteCalls, "a read failure cannot hide jobs by deleting records")
			} else {
				assert.Equal(t, map[string]int{"accepted-job": 1, "saved-only-job": 1}, deleteCalls)
			}

			fail = false
			require.NoError(t, node.closeWithCleanup(context.Background(), true, cleanup))
			assert.Equal(t, 1, stopCalls, "cleanup retry must not repeat a successful Stop")
			assert.Equal(t, 2, readCalls, "each attempt reads the saved record")
			assert.Empty(t, payloads)
			assert.Empty(t, savedKeys)
			assert.Empty(t, node.Workers())
			assert.True(t, node.IsClosed())
			assert.True(t, node.IsShutdown())
			require.NoError(t, node.closeWithCleanup(context.Background(), true, cleanup))
			assert.Equal(t, 1, stopCalls)
			assert.Equal(t, 2, readCalls)
		})
	}
}

func TestLocalShutdownJoinsAcceptedStartBeforeSavedJobRead(t *testing.T) {
	stopCalls := 0
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(key string) error {
			stopCalls++
			assert.Equal(t, "late-accepted-job", key)
			return nil
		},
	})
	node := newLocalShutdownNode(worker)
	readCalls := 0
	readKeys := func(_ context.Context, workerID string) ([]string, error) {
		readCalls++
		assert.Equal(t, worker.ID, workerID)
		assert.Equal(t, 1, stopCalls)
		assert.Empty(t, worker.Jobs())
		return []string{"late-accepted-job"}, nil
	}
	deleteEntry := func(_ context.Context, _, key string) error {
		assert.Equal(t, "late-accepted-job", key)
		return nil
	}
	cleanup := func(ctx context.Context, shutdown bool) error {
		assert.True(t, shutdown)
		if err := node.cleanupShutdownJobs(ctx, readKeys, deleteEntry); err != nil {
			return err
		}
		node.localWorkers.Delete(worker.ID)
		return nil
	}
	worker.wg.Add(1)
	result := make(chan error, 1)
	go func() {
		result <- node.closeWithCleanup(context.Background(), true, cleanup)
	}()
	select {
	case err := <-result:
		t.Errorf("close returned before accepted start finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	worker.jobs.Store("late-accepted-job", &Job{Key: "late-accepted-job"})
	worker.wg.Done()
	require.NoError(t, awaitLocalShutdown(t, result))
	assert.Equal(t, 1, readCalls)
	assert.True(t, node.IsClosed())
	assert.Empty(t, node.Workers())
}

func TestLocalShutdownOrdinaryClosePreservesSavedRecovery(t *testing.T) {
	stopCalls := 0
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(string) error {
			stopCalls++
			return nil
		},
	}, "accepted-job")
	node := newLocalShutdownNode(worker)
	savedPayload := []byte("synthetic saved payload")
	savedKeys := []string{"accepted-job"}
	recoveryErr := errors.New("synthetic recovery still pending")
	cleanupCalls := 0
	cleanup := func(_ context.Context, shutdown bool) error {
		cleanupCalls++
		assert.False(t, shutdown)
		assert.Empty(t, worker.Jobs())
		assert.Equal(t, []byte("synthetic saved payload"), savedPayload)
		assert.Equal(t, []string{"accepted-job"}, savedKeys)
		if recoveryErr != nil {
			return recoveryErr
		}
		node.localWorkers.Delete(worker.ID)
		return nil
	}

	assert.ErrorIs(t, node.closeWithCleanup(context.Background(), false, cleanup), recoveryErr)
	assert.False(t, node.IsClosed())
	assert.True(t, node.closing)
	assert.Len(t, node.Workers(), 1)
	_, admissionErr := node.AddWorker(context.Background(), &mockJobHandler{})
	assert.Error(t, admissionErr, "local closing rejects new workers before storage access")
	recoveryErr = nil
	require.NoError(t, node.closeWithCleanup(context.Background(), false, cleanup))
	assert.Equal(t, 1, stopCalls)
	assert.Equal(t, 2, cleanupCalls)
	assert.True(t, node.IsClosed())
	assert.False(t, node.IsShutdown())
	assert.Equal(t, []string{"accepted-job"}, savedKeys, "delegated recovery retains the saved record")
}

func TestLocalShutdownJoinsTerminalOutcomeBeforeSavedCleanup(t *testing.T) {
	worker := newLocalShutdownWorker(&mockJobHandler{
		stopFunc: func(string) error {
			return nil
		},
	}, "accepted-job")
	node := newLocalShutdownNode(worker)
	finish := node.settlements.begin(worker.ID)
	cleanupCalls := 0
	cleanup := func(_ context.Context, shutdown bool) error {
		cleanupCalls++
		assert.True(t, shutdown)
		node.localWorkers.Delete(worker.ID)
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, node.closeWithCleanup(ctx, true, cleanup), context.Canceled)
	assert.False(t, node.IsClosed())
	assert.Empty(t, worker.Jobs())
	assert.Zero(t, cleanupCalls)
	finish(nil)

	require.NoError(t, node.closeWithCleanup(context.Background(), true, cleanup))
	assert.Equal(t, 1, cleanupCalls)
	assert.True(t, node.IsClosed())
}

func TestLocalShutdownSavedJobReadContract(t *testing.T) {
	readErr := errors.New("synthetic storage read failed")
	for _, test := range []struct {
		name    string
		value   string
		readErr error
		keys    []string
		invalid bool
	}{
		{name: "missing", readErr: redis.Nil},
		{name: "nonempty-array", value: `["accepted-job","saved-only-job"]`, keys: []string{"accepted-job", "saved-only-job"}},
		{name: "empty-array", value: `[]`, keys: []string{}},
		{name: "string-content", value: `["","  spaced  ","comma,key","quote\"key","key","key"]`, keys: []string{"", "  spaced  ", "comma,key", `quote"key`, "key", "key"}},
		{name: "json-whitespace", value: " \n [\"accepted-job\"] \t", keys: []string{"accepted-job"}},
		{name: "read-error", value: `["accepted-job"]`, readErr: readErr, invalid: true},
		{name: "top-null", value: `null`, invalid: true},
		{name: "top-object", value: `{}`, invalid: true},
		{name: "top-string", value: `"accepted-job"`, invalid: true},
		{name: "top-number", value: `1`, invalid: true},
		{name: "top-boolean", value: `true`, invalid: true},
		{name: "empty-value", value: ``, invalid: true},
		{name: "malformed-json", value: `[`, invalid: true},
		{name: "trailing-value", value: `[] []`, invalid: true},
		{name: "null-element", value: `[null]`, invalid: true},
		{name: "mixed-null-element", value: `["accepted-job",null]`, invalid: true},
		{name: "number-element", value: `[1]`, invalid: true},
		{name: "boolean-element", value: `[false]`, invalid: true},
		{name: "object-element", value: `[{}]`, invalid: true},
		{name: "array-element", value: `[[]]`, invalid: true},
		{name: "mixed-number-element", value: `["accepted-job",1]`, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, err := decodeWorkerJobKeys(test.value, test.readErr)
			if test.invalid {
				require.Error(t, err)
				assert.Nil(t, keys)
				if test.readErr != nil {
					assert.ErrorIs(t, err, test.readErr)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.keys, keys)
			}

			stopCalls := 0
			worker := newLocalShutdownWorker(&mockJobHandler{
				stopFunc: func(string) error {
					stopCalls++
					return nil
				},
			}, "accepted-job")
			node := newLocalShutdownNode(worker)
			node.resources.jobPayloads = "synthetic-payloads"
			records := map[string]string{worker.ID: test.value}
			if errors.Is(test.readErr, redis.Nil) {
				delete(records, worker.ID)
			}
			currentReadErr := test.readErr
			var deletedPayloads []string
			readKeys := func(_ context.Context, workerID string) ([]string, error) {
				value, exists := records[workerID]
				if currentReadErr != nil {
					return decodeWorkerJobKeys(value, currentReadErr)
				}
				if !exists {
					return decodeWorkerJobKeys("", redis.Nil)
				}
				return decodeWorkerJobKeys(value, nil)
			}
			deleteEntry := func(_ context.Context, mapName, key string) error {
				assert.Equal(t, node.resources.jobPayloads, mapName)
				deletedPayloads = append(deletedPayloads, key)
				return nil
			}
			cleanup := func(ctx context.Context, shutdown bool) error {
				assert.True(t, shutdown)
				if err := node.cleanupShutdownJobs(ctx, readKeys, deleteEntry); err != nil {
					return err
				}
				delete(records, worker.ID)
				node.localWorkers.Delete(worker.ID)
				return nil
			}

			err = node.closeWithCleanup(context.Background(), true, cleanup)
			if test.invalid {
				require.Error(t, err)
				if test.readErr != nil {
					assert.ErrorIs(t, err, test.readErr)
				}
				assert.False(t, node.IsClosed())
				assert.False(t, node.IsShutdown())
				assert.Len(t, node.Workers(), 1)
				assert.Equal(t, test.value, records[worker.ID])
				assert.Empty(t, deletedPayloads)
				assert.Empty(t, worker.Jobs())
				assert.Equal(t, 1, stopCalls)
				select {
				case <-node.closed:
					t.Error("invalid saved record must prevent publishing closure")
				default:
				}

				// A later read supplies valid stored data. Cleanup can then
				// finish without stopping the already released handler again.
				records[worker.ID] = `["accepted-job"]`
				currentReadErr = nil
				require.NoError(t, node.closeWithCleanup(context.Background(), true, cleanup))
				assert.Equal(t, []string{"accepted-job"}, deletedPayloads)
			} else {
				require.NoError(t, err)
				if len(test.keys) == 0 {
					assert.Empty(t, deletedPayloads)
				} else {
					assert.Equal(t, test.keys, deletedPayloads)
				}
			}
			assert.True(t, node.IsClosed())
			assert.True(t, node.IsShutdown())
			assert.Empty(t, records)
			assert.Empty(t, node.Workers())
			assert.Equal(t, 1, stopCalls)
			require.NoError(t, node.closeWithCleanup(context.Background(), true, cleanup))
			assert.Equal(t, 1, stopCalls)
		})
	}
}

// completeLocalShutdownStart sends a decoded start result through the worker
// loop's completion phase. In-process operations record which route receives
// the result and verify its worker, event, job, and error without storage access.
func completeLocalShutdownStart(t *testing.T, worker *Worker, job *Job, resultErr error) (int, int) {
	t.Helper()
	acks, settlements := 0, 0
	event := &streaming.Event{ID: "synthetic-event", EventName: evStartJob}
	acknowledge := func(_ context.Context, nodeID, eventID string, err error) {
		acks++
		assert.Equal(t, "synthetic-node", nodeID)
		assert.Equal(t, event.ID, eventID)
		assert.Equal(t, resultErr, err)
	}
	settle := func(owner *Worker, nodeID, eventID string, dispatched *Job, err error) {
		settlements++
		assert.Same(t, worker, owner)
		assert.Equal(t, "synthetic-node", nodeID)
		assert.Equal(t, event.ID, eventID)
		assert.Same(t, job, dispatched)
		assert.Equal(t, resultErr, err)
	}
	worker.completeEvent(context.Background(), "synthetic-node", event, job, resultErr, acknowledge, settle)
	return acks, settlements
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
