package rmap

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"
	"goa.design/pulse/pulse"
)

var (
	redisPwd = "redispassword"
	wf       = time.Second
	tck      = time.Millisecond
)

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
	var buf Buffer
	ctx := context.Background()
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

	// Validate initial state
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	assert.Empty(t, m.Keys())

	// Write a value
	const key, val = "foo", "bar"
	old, err := m.Set(ctx, key, val)
	assert.NoError(t, err)
	assert.Equal(t, "", old)

	// Check that the value is available eventually
	require.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.Equal(t, val, m.Map()[key])
	require.Len(t, m.Keys(), 1)
	assert.Equal(t, key, m.Keys()[0])
	v, ok := m.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val, v)

	// Delete the value
	old, err = m.Delete(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, val, old)

	// Check that the value is no longer available
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	assert.Empty(t, m.Keys())
	_, ok = m.Get(key)
	assert.False(t, ok)

	// Write multiple values
	keys := []string{"foo", "bar", "baz"}
	vals := []string{"foo", "bar", "baz"}
	for i, k := range keys {
		old, err = m.Set(ctx, k, vals[i])
		assert.NoError(t, err)
		assert.Equal(t, "", old)
	}

	// Check that the values are eventually available
	require.Eventually(t, func() bool { return len(m.Map()) == 3 }, wf, tck)
	require.Len(t, m.Keys(), 3)
	assert.Len(t, m.Keys(), 3)
	for i, k := range keys {
		assert.Equal(t, vals[i], m.Map()[k])
		v, ok := m.Get(k)
		assert.True(t, ok)
		assert.Equal(t, vals[i], v)
	}

	// Delete a value
	old, err = m.Delete(ctx, keys[0])
	assert.NoError(t, err)
	assert.Equal(t, vals[0], old)

	// Check that the value is eventually no longer available
	assert.Eventually(t, func() bool { return len(m.Map()) == 2 }, wf, tck)
	assert.Len(t, m.Keys(), 2)
	assert.Equal(t, m.Len(), 2)
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
	old, err = m.Set(ctx, key, val)
	assert.NoError(t, err)
	assert.Equal(t, "", old)
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	m.Close()
	assert.Eventually(t, func() bool { return m.closed }, wf, tck)

	// Check that we can still read the map after it has been closed.
	assert.Equal(t, m.Len(), 1)
	assert.Equal(t, val, m.Map()[key])
	require.Len(t, m.Keys(), 1)
	assert.Equal(t, key, m.Keys()[0])
	v, ok = m.Get(key)
	assert.True(t, ok)
	assert.Equal(t, val, v)

	// Check that write methods fail
	old, err = m.Set(ctx, key, val)
	assert.Error(t, err)
	assert.Equal(t, "", old)
	old, err = m.Delete(ctx, key)
	assert.Error(t, err)
	assert.Equal(t, "", old)
	assert.Error(t, m.Reset(ctx))

	// Cleanup
	m, err = Join(ctx, "test", rdb)
	require.NoError(t, err)
	cleanup(t, m)
}

func TestTestAndSet(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := context.Background()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)
	const key, testVal, val = "foo", "bar", "baz"

	old, err := m.Set(ctx, key, testVal)
	assert.NoError(t, err)
	assert.Equal(t, "", old)
	assert.Eventually(t, func() bool { return m.Map()[key] == testVal }, wf, tck)

	old, err = m.TestAndSet(ctx, key, testVal, val)
	assert.NoError(t, err)
	assert.Equal(t, testVal, old)
	assert.Eventually(t, func() bool { return m.Map()[key] == val }, wf, tck)

	old, err = m.TestAndSet(ctx, key, "", testVal)
	assert.NoError(t, err)
	assert.Equal(t, val, old)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, val, m.Map()[key])
}

func TestArrays(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := context.Background()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)

	const key, nonkey, val1, val2 = "foo", "none", "bar", "baz"
	res, err := m.AppendValues(ctx, key, val1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar"}, res)
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.Equal(t, val1, m.Map()[key])
	res, err = m.AppendValues(ctx, key, val2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar", "baz"}, res)
	assert.Eventually(t, func() bool { return len(strings.Split(m.Map()[key], ",")) == 2 }, wf, tck)
	assert.Equal(t, val1+","+val2, m.Map()[key])
	res, err = m.RemoveValues(ctx, key, val1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"baz"}, res)
	assert.Eventually(t, func() bool { return len(strings.Split(m.Map()[key], ",")) == 1 }, wf, tck)
	assert.Equal(t, val2, m.Map()[key])
	res, err = m.RemoveValues(ctx, key, val2)
	assert.NoError(t, err)
	assert.Nil(t, res)
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	res, err = m.RemoveValues(ctx, nonkey, val1)
	assert.NoError(t, err)
	assert.Nil(t, res)
	assert.Equal(t, m.rdb.Exists(ctx, key).Val(), int64(0))
}

func TestIncrement(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := context.Background()
	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)

	const key = "foo"
	res, err := m.Inc(ctx, key, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, res)
	assert.Eventually(t, func() bool { return m.Map()[key] == "1" }, wf, tck)
	res, err = m.Inc(ctx, key, 41)
	assert.NoError(t, err)
	assert.Equal(t, 42, res)
	assert.Eventually(t, func() bool { return m.Map()[key] == "42" }, wf, tck)
	res, err = m.Inc(ctx, key, -1)
	assert.NoError(t, err)
	assert.Equal(t, 41, res)
	assert.Eventually(t, func() bool { return m.Map()[key] == "41" }, wf, tck)
	res, err = m.Inc(ctx, key, -41)
	assert.NoError(t, err)
	assert.Equal(t, 0, res)
	assert.Eventually(t, func() bool { return m.Map()[key] == "0" }, wf, tck)
}

func TestLogs(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	var buf Buffer
	ctx := context.Background()
	ctx = log.Context(ctx, log.WithOutput(&buf), log.WithDebug())

	m, err := Join(ctx, "test", rdb, WithLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	assert.NoError(t, m.Reset(ctx))

	const key, val = "foo", "bar"
	old, err := m.Set(ctx, key, val)
	assert.NoError(t, err)
	assert.Equal(t, "", old)
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	old, err = m.Delete(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, val, old)
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	old, err = m.Set(ctx, key, val)
	assert.NoError(t, err)
	assert.Equal(t, "", old)
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.NoError(t, m.Reset(ctx))
	assert.Eventually(t, func() bool { return len(m.Map()) == 0 }, wf, tck)
	m.Close()
	assert.Eventually(t, func() bool { return m.closed }, wf, tck)

	// Check that the logs contain the expected messages
	assert.Contains(t, buf.String(), `joined`)
	assert.Contains(t, buf.String(), `foo=bar`)
	assert.Contains(t, buf.String(), `msg=deleted key=foo`)
	assert.Contains(t, buf.String(), `reset`)
	assert.Contains(t, buf.String(), `stopped`)
}

func TestJoinErrors(t *testing.T) {
	invalidRedisKey := "invalid*redis*key"
	m, err := Join(context.Background(), invalidRedisKey, nil)
	assert.Error(t, err)
	assert.Nil(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m, err = Join(ctx, "test", nil)
	assert.Error(t, err)
	assert.Nil(t, m)
}

func TestSetErrors(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)

	old, err := m.Set(ctx, "", "foo")
	assert.Error(t, err)
	assert.Equal(t, "", old)

	old, err = m.Set(ctx, "foo=2", "bar")
	assert.Error(t, err)
	assert.Equal(t, "", old)
}

func TestAppendValuesErrors(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := context.Background()

	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)

	res, err := m.AppendValues(ctx, "", "foo")
	assert.Error(t, err)
	assert.Equal(t, []string(nil), res)
	res, err = m.AppendValues(ctx, "foo=2", "bar")
	assert.Error(t, err)
	assert.Equal(t, []string(nil), res)
}

func TestRemoveValuesErrors(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := context.Background()

	m, err := Join(ctx, "test", rdb)
	require.NoError(t, err)
	defer cleanup(t, m)

	res, err := m.RemoveValues(ctx, "", "foo")
	assert.Error(t, err)
	assert.Equal(t, []string(nil), res)
	res, err = m.RemoveValues(ctx, "foo=2", "bar")
	assert.Error(t, err)
	assert.Equal(t, []string(nil), res)
}

func TestReconnect(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := context.Background()
	var buf Buffer
	ctx = log.Context(ctx, log.WithOutput(&buf))
	log.FlushAndDisableBuffering(ctx)

	m, err := Join(ctx, "test", rdb, WithLogger(pulse.ClueLogger(ctx)))
	require.NoError(t, err)
	defer cleanup(t, m)

	// Write a value
	const key, val = "foo", "bar"
	old, err := m.Set(ctx, key, val)
	assert.NoError(t, err)
	assert.Equal(t, "", old)

	// Check that the value is eventually available
	assert.Eventually(t, func() bool { return len(m.Map()) == 1 }, wf, tck)
	assert.Equal(t, val, m.Map()[key])

	// Artificially close the connection
	assert.NoError(t, m.sub.Close())
	assert.Eventually(t, func() bool { return strings.Contains(buf.String(), "disconnected") }, wf, tck)
	assert.Eventually(t, func() bool { return strings.Contains(buf.String(), "reconnected") }, wf, tck)

	// Write a new value
	const key2, val2 = "foo2", "bar2"
	old, err = m.Set(ctx, key2, val2)
	assert.NoError(t, err)
	assert.Equal(t, "", old)

	// Check that the new value is eventually available
	assert.Eventually(t, func() bool { return len(m.Map()) == 2 }, wf, tck)
}

func cleanup(t *testing.T, m *Map) {
	t.Helper()
	assert.NoError(t, m.Reset(context.Background()))
	m.Close()
	assert.Eventually(t, func() bool { return m.closed }, wf, tck)
}

// Buffer is a goroutine safe bytes.Buffer
type Buffer struct {
	buffer bytes.Buffer
	mutex  sync.Mutex
}

// Write appends the contents of p to the buffer, growing the buffer as needed. It returns
// the number of bytes written.
func (s *Buffer) Write(p []byte) (n int, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.buffer.Write(p)
}

// String returns the contents of the unread portion of the buffer
// as a string.  If the Buffer is a nil pointer, it returns "<nil>".
func (s *Buffer) String() string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.buffer.String()
}
