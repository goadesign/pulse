package pool

import (
	"io"
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"
)

func TestNewTicker(t *testing.T) {
	var (
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		tctx     = testContext(t)
		ctx      = log.Context(tctx, log.WithOutput(io.Discard))
		testName = strings.Replace(t.Name(), "/", "_", -1)
		node     = newTestNode(t, ctx, rdb, testName)
		d        = 10 * time.Millisecond
	)
	now := time.Now()
	ticker, err := node.NewTicker(ctx, testName, d)
	assert.NoError(t, err)
	require.NotNil(t, ticker)
	ts := <-ticker.C
	assert.WithinDuration(t, now.Add(d), ts, time.Second, "invalid tick value")
	next, dur := deserialize(ticker.next)
	assert.WithinDuration(t, now.Add(d), next, time.Second, "invalid next tick value")
	assert.Equal(t, d, dur, "invalid duration")
	ticker.Stop()
	var ok bool
	timer := time.NewTimer(2 * d)
	select {
	case <-timer.C:
		ok = true
	case <-ticker.C:
	}
	assert.True(t, ok, "ticker did not stop")
	assert.NoError(t, node.Shutdown(ctx))
}

func TestReplaceTickerTimer(t *testing.T) {
	var (
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		tctx     = testContext(t)
		ctx      = log.Context(tctx, log.WithOutput(io.Discard))
		testName = strings.Replace(t.Name(), "/", "_", -1)
		node     = newTestNode(t, ctx, rdb, testName)
		d1, d2   = 10 * time.Millisecond, 20 * time.Millisecond
	)
	now := time.Now()
	ticker, err := node.NewTicker(ctx, testName, d1)
	assert.NoError(t, err)
	require.NotNil(t, ticker)
	next, dur := deserialize(ticker.next)
	assert.WithinDuration(t, now.Add(d1), next, time.Second, "invalid next tick value")
	assert.Equal(t, d1, dur, "invalid duration")
	ticker2, err := node.NewTicker(ctx, testName, d2)
	assert.NoError(t, err)
	require.NotNil(t, ticker2)
	next, dur = deserialize(ticker2.next)
	assert.WithinDuration(t, now.Add(d2), next, time.Second, "invalid next tick value")
	assert.Equal(t, d2, dur, "invalid duration")
	ticker.Stop()
	ticker2.Stop()
	var ok, ok2 bool
	timer := time.NewTimer(2 * d1)
	select {
	case <-timer.C:
		ok = true
	case <-ticker.C:
	}
	timer2 := time.NewTimer(2 * d2)
	select {
	case <-timer2.C:
		ok2 = true
	case <-ticker2.C:
	}
	assert.True(t, ok, "ticker did not stop")
	assert.True(t, ok2, "ticker2 did not stop")
	assert.NoError(t, node.Shutdown(ctx))
}

func TestReset(t *testing.T) {
	var (
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		tctx     = testContext(t)
		ctx      = log.Context(tctx, log.WithOutput(io.Discard))
		testName = strings.Replace(t.Name(), "/", "_", -1)
		node     = newTestNode(t, ctx, rdb, testName)
		d        = 10 * time.Millisecond
	)
	cases := []struct {
		name string
		stop bool
	}{
		{"stop", true},
		{"no-stop", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ticker, err := node.NewTicker(ctx, testName+c.name, d)
			assert.NoError(t, err)
			require.NotNil(t, ticker)
			<-ticker.C
			if c.stop {
				ticker.Stop()
			}
			ticker.Reset(d)
			<-ticker.C
			ticker.Stop()
			var ok bool
			timer := time.NewTimer(2 * d)
			select {
			case <-timer.C:
				ok = true
			case <-ticker.C:
			}
			assert.True(t, ok, "ticker did not stop")
		})
	}
	assert.NoError(t, node.Shutdown(ctx))
}
