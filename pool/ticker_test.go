package pool

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"

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
	nextTickTime, tickerDuration := deserialize(ticker.next)
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
	nextTick, tickDuration := deserialize(ticker1.next)
	assert.WithinDuration(t, now.Add(shortDuration), nextTick, time.Second, "First ticker: invalid next tick time")
	assert.Equal(t, shortDuration, tickDuration, "First ticker: invalid duration")

	// Create second ticker
	ticker2, err := node.NewTicker(ctx, testName, longDuration)
	require.NoError(t, err)
	require.NotNil(t, ticker2)

	// Verify second ticker properties
	ticker2.lock.Lock()
	nextTick, tickDuration = deserialize(ticker2.next)
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
