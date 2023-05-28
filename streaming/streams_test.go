package streaming

import (
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"goa.design/ponos/ponos"
)

var redisPwd = "redispassword"

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
}

func TestDestroy(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)

	s, err := NewStream("testDestroy", rdb)
	assert.NoError(t, err)
	assert.NoError(t, s.Destroy(ctx))
	exists, err := rdb.Exists(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	s2, err := NewStream("testDestroy2", rdb)
	assert.NoError(t, err)
	_, err = s2.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)
	exists, err = rdb.Exists(ctx, s2.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)
	assert.NoError(t, s2.Destroy(ctx))
}

func TestOptions(t *testing.T) {
	s, err := NewStream("testOptions", nil, WithStreamMaxLen(10), WithStreamLogger(nil))
	assert.NoError(t, err)
	assert.Equal(t, 10, s.MaxLen)
	assert.Equal(t, ponos.NoopLogger(), s.logger)
}

func TestAdd(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream("testAdd", rdb)
	assert.NoError(t, err)

	_, err = s.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)
	l, err := rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), l)
	v, err := rdb.XRange(ctx, s.key, "-", "+").Result()
	assert.NoError(t, err)
	assert.Equal(t, "foo", v[0].Values[nameKey])
	assert.Equal(t, "bar", v[0].Values[payloadKey])

	assert.NoError(t, s.Destroy(ctx))
}

func TestRemove(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream("testRemove", rdb)
	assert.NoError(t, err)

	_, err = s.Add(ctx, "foo", []byte("bar"))
	assert.NoError(t, err)
	_, err = s.Add(ctx, "foo2", []byte("bar2"))
	assert.NoError(t, err)

	l, err := rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), l)

	v, err := rdb.XRange(ctx, s.key, "-", "+").Result()
	assert.NoError(t, err)
	assert.NoError(t, s.Remove(ctx, v[0].ID))

	l, err = rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), l)

	assert.NoError(t, s.Destroy(ctx))
}

func TestTopic(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	ctx := testContext(t)
	s, err := NewStream("testTopic", rdb)
	assert.NoError(t, err)

	_, err = s.Add(ctx, "bar", []byte("baz"), WithTopic("foo"))
	assert.NoError(t, err)

	l, err := rdb.XLen(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), l)
	v, err := rdb.XRange(ctx, s.key, "-", "+").Result()
	assert.NoError(t, err)
	assert.Equal(t, "foo", v[0].Values[topicKey])
	assert.Equal(t, "bar", v[0].Values[nameKey])
	assert.Equal(t, "baz", v[0].Values[payloadKey])

	assert.NoError(t, s.Destroy(ctx))
}
