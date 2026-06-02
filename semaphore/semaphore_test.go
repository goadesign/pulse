package semaphore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pulsetesting "goa.design/pulse/testing"
)

func TestAcquireBlocksUntilRelease(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	sem := testSemaphore(t, rdb, t.Name())

	first, err := sem.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, first.Release(ctx))
	}()

	acquired := make(chan acquireResult, 1)
	go func() {
		lease, err := sem.Acquire(ctx)
		acquired <- acquireResult{lease: lease, err: err}
	}()

	select {
	case res := <-acquired:
		require.NoError(t, res.err)
		require.NoError(t, res.lease.Release(ctx))
		t.Fatal("second acquire succeeded before release")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, first.Release(ctx))
	select {
	case res := <-acquired:
		require.NoError(t, res.err)
		require.NoError(t, res.lease.Release(ctx))
	case <-time.After(time.Second):
		t.Fatal("second acquire did not wake after release")
	}
}

func TestAcquireRespectsContextCancellation(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	sem := testSemaphore(t, rdb, t.Name())

	first, err := sem.Acquire(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, first.Release(ctx))
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = sem.Acquire(waitCtx)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestExpiredHolderFreesSlot(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	sem := testSemaphore(t, rdb, t.Name())

	require.NoError(t, rdb.ZAdd(ctx, sem.holders, redis.Z{Score: 1, Member: "stale"}).Err())
	lease, err := sem.Acquire(ctx)
	require.NoError(t, err)
	require.NoError(t, lease.Release(ctx))
}

func TestLeaseContextCancelsWhenHeartbeatLosesHolder(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	sem, err := New(rdb, t.Name(), 1, WithLeaseTTL(200*time.Millisecond), WithHeartbeatInterval(50*time.Millisecond))
	require.NoError(t, err)

	lease, err := sem.Acquire(ctx)
	require.NoError(t, err)
	require.NoError(t, rdb.ZRem(ctx, sem.holders, lease.Token()).Err())

	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lease context was not canceled after holder disappeared")
	}
	assert.ErrorIs(t, lease.Err(), ErrLeaseLost)
	assert.ErrorIs(t, lease.Release(ctx), ErrLeaseLost)
	assert.NoError(t, lease.Release(ctx))
}

func TestConcurrentAcquiresNeverExceedLimit(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	sem, err := New(rdb, t.Name(), 3, WithLeaseTTL(time.Second), WithHeartbeatInterval(200*time.Millisecond))
	require.NoError(t, err)

	var leases []*Lease
	for range 3 {
		lease, err := sem.Acquire(ctx)
		require.NoError(t, err)
		leases = append(leases, lease)
	}

	acquired := make(chan acquireResult, 1)
	go func() {
		lease, err := sem.Acquire(ctx)
		acquired <- acquireResult{lease: lease, err: err}
	}()

	select {
	case res := <-acquired:
		require.NoError(t, res.err)
		require.NoError(t, res.lease.Release(ctx))
		t.Fatal("fourth acquire succeeded while three leases were held")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, leases[0].Release(ctx))
	select {
	case res := <-acquired:
		require.NoError(t, res.err)
		require.NoError(t, res.lease.Release(ctx))
	case <-time.After(time.Second):
		t.Fatal("fourth acquire did not wake after one release")
	}
	for _, lease := range leases[1:] {
		require.NoError(t, lease.Release(ctx))
	}
}

func TestConcurrentAcquireStressNeverExceedsLimit(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	const limit = 4
	sem, err := New(rdb, t.Name(), limit, WithLeaseTTL(time.Second), WithHeartbeatInterval(200*time.Millisecond))
	require.NoError(t, err)

	var active atomic.Int64
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := range 4 {
				lease, err := sem.Acquire(ctx)
				if err != nil {
					errs <- err
					return
				}
				current := active.Add(1)
				if current > limit {
					errs <- fmt.Errorf("worker %d iteration %d observed %d active leases", worker, iteration, current)
				}
				time.Sleep(time.Millisecond)
				active.Add(-1)
				if err := lease.Release(ctx); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestAcquireOrderIsFIFO(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	sem := testSemaphore(t, rdb, t.Name())

	holder, err := sem.Acquire(ctx)
	require.NoError(t, err)

	acquired := make(chan acquireResult, 2)
	go acquireNamed(ctx, sem, "first", acquired)
	require.Eventually(t, func() bool {
		count, err := rdb.ZCard(ctx, sem.waiters).Result()
		return err == nil && count == 1
	}, time.Second, 10*time.Millisecond)

	go acquireNamed(ctx, sem, "second", acquired)
	require.Eventually(t, func() bool {
		count, err := rdb.ZCard(ctx, sem.waiters).Result()
		return err == nil && count == 2
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, holder.Release(ctx))
	first := <-acquired
	require.NoError(t, first.err)
	assert.Equal(t, "first", first.name)
	require.NoError(t, first.lease.Release(ctx))

	second := <-acquired
	require.NoError(t, second.err)
	assert.Equal(t, "second", second.name)
	require.NoError(t, second.lease.Release(ctx))
}

func TestReleaseCanRetryAfterContextCancellation(t *testing.T) {
	ctx := context.Background()
	rdb := pulsetesting.NewRedisClient(t)
	defer pulsetesting.CleanupRedis(t, rdb, true, t.Name())
	sem := testSemaphore(t, rdb, t.Name())

	lease, err := sem.Acquire(ctx)
	require.NoError(t, err)
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	require.Error(t, lease.Release(canceledCtx))
	require.NoError(t, lease.Release(ctx))
	require.NoError(t, lease.Release(ctx))
}

func testSemaphore(t *testing.T, rdb *redis.Client, name string) *Semaphore {
	t.Helper()
	sem, err := New(rdb, name, 1, WithLeaseTTL(time.Second), WithHeartbeatInterval(200*time.Millisecond))
	require.NoError(t, err)
	return sem
}

func acquireNamed(ctx context.Context, sem *Semaphore, name string, acquired chan<- acquireResult) {
	lease, err := sem.Acquire(ctx)
	acquired <- acquireResult{name: name, lease: lease, err: err}
}

type acquireResult struct {
	name  string
	lease *Lease
	err   error
}
