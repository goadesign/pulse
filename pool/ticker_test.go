package pool

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"
	"goa.design/pulse/pulse"

	ptesting "goa.design/pulse/testing"
)

func TestNewTicker(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, t.Name())
	ctx := log.Context(ptesting.NewTestContext(t), log.WithOutput(io.Discard))
	testName := strings.Replace(t.Name(), "/", "_", -1)
	node := newTestNode(t, ctx, rdb, testName)
	tickDuration := 10 * time.Millisecond

	// Create and test new ticker
	startTime := time.Now()
	ticker, err := node.NewTicker(ctx, "ticker1", tickDuration)
	require.NoError(t, err, "Failed to create new ticker")
	require.NotNil(t, ticker, "Ticker should not be nil")

	// Verify first tick
	firstTick := <-ticker.C
	assert.WithinDuration(t, startTime.Add(tickDuration), firstTick, time.Second, "First tick should occur after approximately one tick duration")

	// Verify next tick time and duration
	ticker.lock.Lock()
	nextTickTime, tickerDuration, err := deserialize(ticker.next)
	require.NoError(t, err)
	ticker.lock.Unlock()
	assert.WithinDuration(t, startTime.Add(tickDuration), nextTickTime, time.Second, "Next tick time should be approximately one tick duration from start")
	assert.Equal(t, tickDuration, tickerDuration, "Ticker duration should match the specified duration")

	// Test ticker stop
	ticker.Stop()
	select {
	case <-time.After(2 * tickDuration):
		// Timer expired without receiving tick, which is expected behavior
	case <-ticker.C:
		t.Error("Received tick after stopping ticker")
	}

	// Cleanup
	assert.NoError(t, node.Shutdown(ctx), "Failed to shutdown node")
}

func TestDeserializeTickerStateRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{
		"",
		"1",
		"not-a-time|1s",
		"1|not-a-duration",
		"1|0s",
		"1|500us",
		"1|1s|extra",
	} {
		t.Run(value, func(t *testing.T) {
			_, _, err := deserialize(value)
			require.Error(t, err)
		})
	}
}

func TestReplaceTickerTimer(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, true, t.Name())
	ctx := log.Context(ptesting.NewTestContext(t), log.WithOutput(io.Discard))
	testName := strings.Replace(t.Name(), "/", "_", -1)
	node := newTestNode(t, ctx, rdb, testName)

	// Define ticker durations
	shortDuration := 10 * time.Millisecond
	longDuration := 20 * time.Millisecond

	// Create first ticker
	now := time.Now()
	ticker1, err := node.NewTicker(ctx, testName, shortDuration)
	require.NoError(t, err)
	require.NotNil(t, ticker1)

	// Verify first ticker properties
	nextTick, tickDuration, err := deserialize(ticker1.next)
	require.NoError(t, err)
	assert.WithinDuration(t, now.Add(shortDuration), nextTick, time.Second, "First ticker: invalid next tick time")
	assert.Equal(t, shortDuration, tickDuration, "First ticker: invalid duration")

	// Create second ticker
	ticker2, err := node.NewTicker(ctx, testName, longDuration)
	require.NoError(t, err)
	require.NotNil(t, ticker2)

	// Verify second ticker properties
	ticker2.lock.Lock()
	nextTick, tickDuration, err = deserialize(ticker2.next)
	require.NoError(t, err)
	ticker2.lock.Unlock()
	assert.WithinDuration(t, now.Add(longDuration), nextTick, time.Second, "Second ticker: invalid next tick time")
	assert.Equal(t, longDuration, tickDuration, "Second ticker: invalid duration")

	// Stop both tickers
	ticker1.Stop()
	ticker2.Stop()

	// Verify that both tickers have stopped
	assert.True(t, verifyTickerStopped(t, ticker1, 2*shortDuration), "First ticker did not stop")
	assert.True(t, verifyTickerStopped(t, ticker2, 2*longDuration), "Second ticker did not stop")

	// Cleanup
	assert.NoError(t, node.Shutdown(ctx))
}

func TestHandleTickRetriesAfterMapWriteError(t *testing.T) {
	rdb := ptesting.NewRedisClient(t)
	defer ptesting.CleanupRedis(t, rdb, false, t.Name())
	ctx := log.Context(ptesting.NewTestContext(t), log.WithOutput(io.Discard))
	testName := strings.Replace(t.Name(), "/", "_", -1)

	hook := &ambiguousDispatchHook{
		err:        errors.New("ticker write failed"),
		scriptHash: testAndSetPoolMapScript.Hash(),
	}
	rdb.AddHook(hook)
	node := newTestNode(t, ctx, rdb, testName)
	require.NoError(t, testAndSetPoolMapScript.Load(ctx, rdb).Err())

	tickDuration := 10 * time.Millisecond
	next := serialize(time.Now().Add(tickDuration), tickDuration)
	require.NoError(t, node.setPoolMap(ctx, node.resources.tickers, testName, next))

	c := make(chan time.Time, 1)
	ticker := &Ticker{
		C:         c,
		c:         c,
		name:      testName,
		node:      node,
		tickerMap: node.tickerMap,
		next:      next,
		timer:     time.NewTimer(time.Hour),
		logger:    pulse.NoopLogger(),
	}
	t.Cleanup(func() {
		ticker.timer.Stop()
	})

	hook.fail.Store(true)
	start := time.Now()
	ticker.handleTick()

	select {
	case firedAt := <-ticker.timer.C:
		assert.WithinDuration(t, start.Add(tickDuration), firedAt, 50*time.Millisecond)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("ticker did not schedule a retry after a map write error")
	}

	select {
	case <-ticker.C:
		t.Fatal("ticker should not emit a tick when advancing the map fails")
	default:
	}
}

// verifyTickerStopped checks if a ticker has stopped by waiting for a duration longer than its tick interval
func verifyTickerStopped(t *testing.T, ticker *Ticker, waitDuration time.Duration) bool {
	t.Helper()
	timer := time.NewTimer(waitDuration)
	select {
	case <-timer.C:
		return true
	case <-ticker.C:
		return false
	}
}
