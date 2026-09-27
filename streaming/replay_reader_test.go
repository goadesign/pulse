// Synthetic command hooks exercise replay context ownership and joined shutdown
// without a server. Real Lua/trim/byte-admission proofs live in the companion
// integration tests; these hooks are never production dependencies.
package streaming

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type (
	replayStub struct {
		process func(context.Context, redis.Cmder) error
	}

	replayWireError string

	replayContextKey struct{}
)

func TestReplayReaderInputBeforeCommands(t *testing.T) {
	for i, id := range []string{"", "$", ">", "*", "1", "01-0", "1-00", "+1-0", "-1-0",
		"1-0-0", "1-18446744073709551616", "18446744073709551616-0", "1-0\n"} {
		t.Run(fmt.Sprintf("invalid_%d", i), func(t *testing.T) {
			var calls atomic.Int64
			s := replayStubStream(t, func(context.Context, redis.Cmder) error {
				calls.Add(1)
				return errors.New("unexpected command")
			})
			r, err := s.NewReplayReader(context.Background(), id, replayTestOptions())
			require.ErrorIs(t, err, ErrInvalidReplayPosition)
			require.Nil(t, r)
			require.Zero(t, calls.Load())
		})
	}
	for _, opts := range []ReplayReaderOptions{
		{MaxEvents: 0, MaxBytes: 1, BlockDuration: time.Millisecond},
		{MaxEvents: -1, MaxBytes: 1, BlockDuration: time.Millisecond},
		{MaxEvents: 1, MaxBytes: 0, BlockDuration: time.Millisecond},
		{MaxEvents: 1, MaxBytes: -1, BlockDuration: time.Millisecond},
		{MaxEvents: 1, MaxBytes: 1},
		{MaxEvents: 1, MaxBytes: 1, BlockDuration: time.Millisecond - 1},
	} {
		var calls atomic.Int64
		s := replayStubStream(t, func(context.Context, redis.Cmder) error {
			calls.Add(1)
			return errors.New("unexpected command")
		})
		_, err := s.NewReplayReader(context.Background(), "0-0", opts)
		require.Error(t, err)
		require.Zero(t, calls.Load())
	}
	for _, id := range []string{"0-0", "1-0", "0-1", "18446744073709551615-18446744073709551615"} {
		require.True(t, validReplayID(id))
	}
}

func TestReplayReaderOpeningCancellationJoinsCommand(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseCommand := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseCommand)
	s := replayStubStream(t, func(ctx context.Context, cmd redis.Cmder) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("private caller cancellation")
	result := make(chan error, 1)
	go func() {
		_, err := s.NewReplayReader(ctx, "0-0", replayTestOptions())
		result <- err
	}()
	awaitReplaySignal(t, started)
	cancel(cause)
	awaitReplaySignal(t, canceled)
	select {
	case err := <-result:
		t.Fatalf("opening returned before command joined: %v", err)
	default:
	}
	releaseCommand()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, cause)
	case <-time.After(max):
		t.Fatal("opening did not join")
	}
}

func TestReplayReaderWaitCancellationAndJoinedClose(t *testing.T) {
	for _, mode := range []string{"close", "caller", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			waiting, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseCommand := func() {
				releaseOnce.Do(func() {
					close(release)
				})
			}
			t.Cleanup(releaseCommand)
			s := replayStubStream(t, func(ctx context.Context, cmd redis.Cmder) error {
				if cmd.Name() == "xread" {
					close(waiting)
					<-ctx.Done()
					close(canceled)
					<-release
					return ctx.Err()
				}
				cmd.(*redis.Cmd).SetVal(replayStubResult(t.Name(), nil, nil))
				return nil
			})
			cause := errors.New("private caller cause")
			var ctx context.Context
			var cancel context.CancelFunc
			if mode == "deadline" {
				ctx, cancel = context.WithTimeoutCause(context.Background(), 500*time.Millisecond, cause)
			} else {
				var cancelCause context.CancelCauseFunc
				ctx, cancelCause = context.WithCancelCause(context.Background())
				cancel = func() {
					cancelCause(cause)
				}
			}
			defer cancel()
			r, err := s.NewReplayReader(ctx, "0-0", replayTestOptions())
			require.NoError(t, err)
			t.Cleanup(func() {
				releaseCommand()
				r.Close()
			})
			awaitReplaySignal(t, waiting)
			closed := make(chan struct{})
			if mode == "caller" {
				cancel()
			}
			if mode == "deadline" {
				awaitReplaySignal(t, canceled)
			}
			go func() {
				r.Close()
				close(closed)
			}()
			awaitReplaySignal(t, canceled)
			select {
			case <-closed:
				t.Fatal("Close returned while the command still owned cleanup")
			default:
			}
			select {
			case <-r.Subscribe():
				t.Fatal("subscription closed before command joined")
			default:
			}
			releaseCommand()
			awaitReplaySignal(t, closed)
			awaitReplayClosed(t, r)
			switch mode {
			case "close":
				require.NoError(t, r.Err())
			case "caller":
				require.ErrorIs(t, r.Err(), context.Canceled)
				require.ErrorIs(t, r.Err(), cause)
			case "deadline":
				require.ErrorIs(t, r.Err(), context.DeadlineExceeded)
				require.ErrorIs(t, r.Err(), cause)
			}
			r.Close()
		})
	}
}

func TestReplayReaderOneChannelBackpressureAndAnchorOwnership(t *testing.T) {
	var calls atomic.Int64
	s := replayStubStream(t, func(ctx context.Context, cmd redis.Cmder) error {
		calls.Add(1)
		cmd.(*redis.Cmd).SetVal(replayStubResult(t.Name(),
			[]any{replayStubEntry("1-0", "anchor", "original")},
			[]any{replayStubEntry("2-0", "first", "one"), replayStubEntry("3-0", "second", "two")},
		))
		return nil
	})
	r, err := s.NewReplayReader(context.Background(), "1-0", replayTestOptions())
	require.NoError(t, err)
	defer r.Close()
	require.Equal(t, r.Subscribe(), r.Subscribe())
	require.Zero(t, cap(r.Subscribe()))
	anchor, ok := r.Anchor()
	require.True(t, ok)
	require.Equal(t, "1-0", anchor.ID())
	payload := anchor.Payload()
	payload[0] = 'X'
	require.Equal(t, "original", string(anchor.Payload()))
	require.EqualValues(t, 1, calls.Load())
	r.Close()
	awaitReplayClosed(t, r)
	require.EqualValues(t, 1, calls.Load(), "blocked delivery must not perform another read")
	require.NoError(t, r.Err())
}

func TestReplayReaderEventTransferAndDependencyError(t *testing.T) {
	var calls atomic.Int64
	marker := errors.New("private dependency marker")
	failure := fmt.Errorf("wrapped: %w", errors.Join(errors.New("transport"), marker))
	s := replayStubStream(t, func(ctx context.Context, cmd redis.Cmder) error {
		if calls.Add(1) == 1 {
			cmd.(*redis.Cmd).SetVal(replayStubResult(t.Name(), nil,
				[]any{replayStubEntry("1-0", "event", "body")}))
			return nil
		}
		if cmd.Args()[8] != "1-0" {
			return fmt.Errorf("consumer mutation changed saved position: %v", cmd.Args()[8])
		}
		return failure
	})
	r, err := s.NewReplayReader(context.Background(), "0-0", replayTestOptions())
	require.NoError(t, err)
	defer r.Close()
	event := takeReplayEvent(t, r)
	event.ID = "consumer-owned"
	event.Payload[0] = 'X'
	awaitReplayClosed(t, r)
	require.ErrorIs(t, r.Err(), marker)
	require.EqualValues(t, 2, calls.Load(), "dependency failures are not retried by the reader")
	r.Close()
	require.ErrorIs(t, r.Err(), marker)
}

// Completion remains observable while a consumer handles its last event and
// has not yet attempted another receive from Subscribe.
func TestReplayReaderDoneReportsFailureWithoutConsumingClosure(t *testing.T) {
	var calls atomic.Int64
	marker := errors.New("replay source stopped")
	s := replayStubStream(t, func(ctx context.Context, cmd redis.Cmder) error {
		if calls.Add(1) == 1 {
			cmd.(*redis.Cmd).SetVal(replayStubResult(t.Name(), nil,
				[]any{replayStubEntry("1-0", "event", "body")}))
			return nil
		}
		return marker
	})
	r, err := s.NewReplayReader(context.Background(), "0-0", replayTestOptions())
	require.NoError(t, err)
	defer r.Close()
	done := r.Done()
	require.Equal(t, done, r.Done())
	select {
	case <-done:
		t.Fatal("reader completed before its pending event was consumed")
	default:
	}

	require.Equal(t, "1-0", takeReplayEvent(t, r).ID)
	awaitReplaySignal(t, done)
	require.ErrorIs(t, r.Err(), marker)
	require.EqualValues(t, 2, calls.Load())
	awaitReplayClosed(t, r)
	r.Close()
	require.ErrorIs(t, r.Err(), marker)
}

func TestReplayReaderCallerCancellationDuringBlockedDelivery(t *testing.T) {
	var calls atomic.Int64
	value := "caller value"
	s := replayStubStream(t, func(ctx context.Context, cmd redis.Cmder) error {
		if ctx.Value(replayContextKey{}) != value {
			return errors.New("caller context value lost")
		}
		calls.Add(1)
		cmd.(*redis.Cmd).SetVal(replayStubResult(t.Name(), nil,
			[]any{replayStubEntry("1-0", "first", "one"), replayStubEntry("2-0", "second", "two")}))
		return nil
	})
	parent := context.WithValue(context.Background(), replayContextKey{}, value)
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	r, err := s.NewReplayReader(ctx, "0-0", replayTestOptions())
	require.NoError(t, err)
	cause := errors.New("caller stopped blocked delivery")
	cancel(cause)
	r.Close()
	awaitReplayClosed(t, r)
	require.ErrorIs(t, r.Err(), context.Canceled)
	require.ErrorIs(t, r.Err(), cause)
	require.EqualValues(t, 1, calls.Load(), "no second batch behind a blocked subscriber")
}

func TestReplayReaderFullInt64Options(t *testing.T) {
	s := replayStubStream(t, func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xread" {
			<-ctx.Done()
			return ctx.Err()
		}
		require.Equal(t, []any{int64(2147483647), int64(4294967295),
			int64(2147483647), int64(4294967295)}, cmd.Args()[10:14])
		cmd.(*redis.Cmd).SetVal(replayStubResult(t.Name(), nil, nil))
		return nil
	})
	opts := replayTestOptions()
	opts.MaxEvents, opts.MaxBytes = math.MaxInt64, math.MaxInt64
	r, err := s.NewReplayReader(context.Background(), "0-0", opts)
	require.NoError(t, err)
	r.Close()
	require.NoError(t, r.Err())
}

func TestReplayReaderStrictResultDecoder(t *testing.T) {
	for _, fields := range [][]any{
		{"n", "event", "p", "", "n", "duplicate"},
		{"n", "event", "p", "", "extra", "unknown"},
		{"n", "", "p", ""},
		{"n", "event", "t", "missing payload"},
		{"n", "event", "p", 1},
		{"n", "event", "p"},
	} {
		events, err := decodeReplayEvents([]any{[]any{"1-0", fields}}, "stream", "1", 100)
		require.Error(t, err)
		require.Nil(t, events)
		require.NotErrorIs(t, err, ErrReplayPositionUnavailable)
	}
	entry := []any{[]any{"1-0", []any{"n", "e", "p", "", "t", ""}}}
	events, err := decodeReplayEvents(entry, "stream", "1", 4)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Empty(t, events[0].Payload)
	_, err = decodeReplayEvents(entry, "stream", "1", 3)
	require.ErrorContains(t, err, "byte admission")
}

func TestReplayReaderErrorCategories(t *testing.T) {
	for _, test := range []struct {
		wire     string
		bound    bool
		position string
		want     error
		cause    error
	}{
		{"REPLAYPOSITIONUNAVAILABLE", false, "1-0", ErrReplayPositionUnavailable, nil},
		{"REPLAYEVENTTOOLARGE", false, "0-0", ErrReplayEventTooLarge, nil},
		{"STREAMNOTFOUND", false, "0-0", ErrStreamNotFound, nil},
		{"STREAMNOTFOUND", false, "1-0", ErrReplayPositionUnavailable, ErrStreamNotFound},
		{"STREAMNOTFOUND", true, "0-0", ErrReplayPositionUnavailable, ErrStreamNotFound},
		{"STREAMDESTROYED", true, "1-0", ErrReplayPositionUnavailable, ErrStreamDestroyed},
		{"DEADLINEELAPSED", true, "1-0", ErrReplayPositionUnavailable, ErrDeadlineElapsed},
		{"STREAMCONFIGMISSING", true, "1-0", ErrStreamConfigMismatch, nil},
		{"STREAMCONFIGMISMATCH", true, "1-0", ErrStreamConfigMismatch, nil},
	} {
		t.Run(test.wire+test.position+fmt.Sprint(test.bound), func(t *testing.T) {
			err := replayReadError(replayWireError(test.wire), test.bound, test.position)
			require.ErrorIs(t, err, test.want)
			if test.cause != nil {
				require.ErrorIs(t, err, test.cause)
			}
			if test.want != ErrReplayPositionUnavailable {
				require.NotErrorIs(t, err, ErrReplayPositionUnavailable)
			}
		})
	}
}

// replayStubStream intercepts every command before dialing. It cannot contact a
// server, and closing it does not own any production or shared client.
func replayStubStream(t *testing.T, process func(context.Context, redis.Cmder) error) *Stream {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0", MaxRetries: -1})
	rdb.AddHook(&replayStub{process: process})
	t.Cleanup(func() {
		require.NoError(t, rdb.Close())
	})
	s, err := NewStream(t.Name(), rdb)
	require.NoError(t, err)
	return s
}

func replayTestOptions() ReplayReaderOptions {
	return ReplayReaderOptions{MaxEvents: 4, MaxBytes: 4096, BlockDuration: 25 * time.Millisecond}
}

func replayStubResult(name string, anchor, events []any) []any {
	return []any{"1", streamKey(name), "", "v=2|max=1000|mode=none|value=0|sliding=false",
		anchor, events}
}

func replayStubEntry(id, name, payload string) []any {
	return []any{id, []any{"n", name, "p", payload}}
}

func takeReplayEvent(t *testing.T, r *ReplayReader) *Event {
	t.Helper()
	select {
	case event, open := <-r.Subscribe():
		require.True(t, open, "reader ended: %v", r.Err())
		require.NotNil(t, event)
		return event
	case <-time.After(max):
		t.Fatal("timed out waiting for replay event")
		return nil
	}
}

// awaitReplayClosed observes channel closure without joining the reader first.
// Callers can check that Err is already available, then close the reader.
func awaitReplayClosed(t *testing.T, r *ReplayReader) {
	t.Helper()
	select {
	case event, open := <-r.Subscribe():
		require.False(t, open, "unexpected event: %v", event)
	case <-time.After(max):
		t.Fatal("replay subscription did not close")
	}
}

func awaitReplaySignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(max):
		t.Fatal("replay stage did not complete")
	}
}

func (h *replayStub) DialHook(next redis.DialHook) redis.DialHook {
	return func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("unexpected replay fixture dial")
	}
}

func (h *replayStub) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return h.process
}

func (h *replayStub) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(context.Context, []redis.Cmder) error {
		return errors.New("unexpected replay fixture pipeline")
	}
}

func (e replayWireError) Error() string {
	return string(e)
}

func (e replayWireError) RedisError() {
}
