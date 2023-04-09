package replicated

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"
	"goa.design/ponos/ponos"
)

var redisPwd = "redispassword"

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
}

func TestMapLocal(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: redisPwd,
	})
	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	ctx = log.Context(ctx, log.WithOutput(&buf))
	log.FlushAndDisableBuffering(ctx)

	// Join or create a replicated map
	m, err := Join(ctx, "test", rdb)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGPASS") {
			t.Fatal("Unexpected Redis password error (did you set REDIS_PASSWORD?)")
		} else if strings.Contains(err.Error(), "connection refused") {
			t.Fatal("Unexpected Redis connection error (is Redis running?)")
		}
	}
	assert.NoError(t, err)
	assert.NotNil(t, m)
	assert.NoError(t, m.Reset(ctx))

	wf := time.Second
	tck := time.Millisecond

	// Validate initial state
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	assert.Empty(t, m.Keys())

	// Write a value
	const key, val = "foo", "bar"
	assert.NoError(t, m.Set(ctx, key, val))

	// Check that the value is available eventually
	require.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.Equal(t, val, m.Map()[key])
	require.Len(t, m.Keys(), 1)
	assert.Equal(t, key, m.Keys()[0])
	v, ok := m.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val, v)

	// Delete the value
	assert.NoError(t, m.Delete(ctx, key))

	// Check that the value is no longer available
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	assert.Empty(t, m.Keys())
	_, ok = m.Get(key)
	assert.False(t, ok)

	// Write multiple values
	keys := []string{"foo", "bar", "baz", "int"}
	vals := []string{"foo", "bar", "baz", "42"}
	for i, k := range keys {
		assert.NoError(t, m.Set(ctx, k, vals[i]))
	}

	// Check that the values are eventually available
	require.Eventually(t, func() bool { return len(m.Map()) == 4 }, wf, tck)
	require.Len(t, m.Keys(), 4)
	for i, k := range keys {
		assert.Equal(t, vals[i], m.Map()[k])
		v, ok := m.Get(k)
		assert.True(t, ok)
		assert.Equal(t, vals[i], v)
	}

	// Increment an integer value
	assert.NoError(t, m.Increment(ctx, "int", 1))

	// Check that the value is eventually incremented
	require.Eventually(t, func() bool { return m.Map()["int"] == "43" }, wf, tck)
	vals[3] = "43"

	// Append to a string value
	assert.NoError(t, m.Append(ctx, "foo", "bar"))

	// Check that the value is eventually appended
	require.Eventually(t, func() bool { return m.Map()["foo"] == "foo,bar" }, wf, tck)

	// Append again
	assert.NoError(t, m.Append(ctx, "foo", "baz"))

	// Check that the value is eventually appended
	require.Eventually(t, func() bool { return m.Map()["foo"] == "foo,bar,baz" }, wf, tck)

	// Delete a value
	assert.NoError(t, m.Delete(ctx, keys[0]))

	// Check that the value is eventually no longer available
	assert.Eventually(t, func() bool { return len(m.Map()) == 3 }, wf, tck)
	assert.Len(t, m.Keys(), 3)
	v, ok = m.Get(keys[0])
	assert.False(t, ok)
	assert.Empty(t, v)
	for i, k := range keys[1:] {
		assert.Equal(t, vals[i+1], m.Map()[k])
		v, ok := m.Get(k)
		assert.True(t, ok)
		assert.Equal(t, vals[i+1], v)
	}

	// Reset the map
	assert.NoError(t, m.Reset(ctx))

	// Check that the values are eventually no longer available
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	assert.Empty(t, m.Keys())
	for _, k := range keys {
		v, ok := m.Get(k)
		assert.False(t, ok)
		assert.Empty(t, v)
	}

	// Write a value and close the map
	assert.NoError(t, m.Set(ctx, key, val))
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	cancel()
	assert.Eventually(t, func() bool { return m.done }, wf, tck)

	// Check that we can still read the map after it has been closed.
	assert.Len(t, m.Map(), 1)
	assert.Equal(t, val, m.Map()[key])
	require.Len(t, m.Keys(), 1)
	assert.Equal(t, key, m.Keys()[0])
	v, ok = m.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val, v)

	// Check that write methods fail
	assert.Error(t, m.Set(ctx, key, val))
	assert.Error(t, m.Delete(ctx, key))
	assert.Error(t, m.Reset(ctx))
}

func TestLogs(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	ctx = log.Context(ctx, log.WithOutput(&buf))
	log.FlushAndDisableBuffering(ctx)

	m, err := Join(ctx, "test", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)
	assert.NoError(t, m.Reset(ctx))

	const key, val = "foo", "bar"
	wf := time.Second
	tck := time.Millisecond
	assert.NoError(t, m.Set(ctx, key, val))
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.NoError(t, m.Delete(ctx, key))
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	assert.NoError(t, m.Set(ctx, key, val))
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.NoError(t, m.Reset(ctx))
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	cancel()
	assert.Eventually(t, func() bool { return m.done }, wf, tck)

	// Check that the logs contain the expected messages
	assert.Contains(t, buf.String(), `ponos: map test: joined`)
	assert.Contains(t, buf.String(), `ponos: map test: foo=bar`)
	assert.Contains(t, buf.String(), `ponos: map test: foo deleted`)
	assert.Contains(t, buf.String(), `ponos: map test: reset`)
	assert.Contains(t, buf.String(), `ponos: map test: closed`)
}

func TestJoinErrors(t *testing.T) {
	invalidRedisKey := "invalid*redis*key"
	m, err := Join(context.Background(), invalidRedisKey, nil)
	assert.Error(t, err)
	assert.Nil(t, m)
	doneContext := context.Background()
	doneContext, cancel := context.WithCancel(doneContext)
	cancel()
	m, err = Join(doneContext, "test", nil)
	assert.Error(t, err)
	assert.Nil(t, m)
}

func TestSetErrors(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m, err := Join(ctx, "test", rdb)
	assert.NoError(t, err)

	err = m.Set(ctx, "", "foo")
	assert.Error(t, err)
	err = m.Set(ctx, "foo=2", "bar")
	assert.Error(t, err)
}

func TestReconnect(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx, cancel := context.WithCancel(context.Background())
	var buf bytes.Buffer
	ctx = log.Context(ctx, log.WithOutput(&buf))
	log.FlushAndDisableBuffering(ctx)
	defer cancel()

	m, err := Join(ctx, "test", rdb, WithLogger(ponos.ClueLogger(ctx)))
	assert.NoError(t, err)

	// Write a value
	const key, val = "foo", "bar"
	assert.NoError(t, m.Set(ctx, key, val))

	// Check that the value is eventually available
	wf := time.Second
	tck := time.Millisecond
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.Equal(t, val, m.Map()[key])

	// Artificially close the connection
	assert.NoError(t, m.sub.Close())
	assert.Eventually(t, func() bool { return strings.Contains(buf.String(), "ponos: map test: disconnected") }, wf, tck)

	// Write a new value
	const key2, val2 = "foo2", "bar2"
	assert.NoError(t, m.Set(ctx, key2, val2))

	// Check that the new value is eventually available
	assert.Eventually(t, func() bool { return len(m.Map()) == 2 }, wf, tck)

	// Validate the logs
	assert.Contains(t, buf.String(), `ponos: map test: reconnect attempt 1`)
	assert.Contains(t, buf.String(), `ponos: map test: reconnected`)
}
