package streaming

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"goa.design/ponos/ponos"
)

var (
	redisPwd = "redispassword"
	ctx      = context.Background()
)

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
}

func TestDestroy(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})

	s, err := NewStream(ctx, "testDestroy", rdb)
	assert.NoError(t, err)
	assert.NoError(t, s.Destroy(ctx))
	exists, err := rdb.Exists(ctx, s.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)

	s2, err := NewStream(ctx, "testDestroy2", rdb)
	assert.NoError(t, err)
	s2.Add(ctx, "foo", []byte("bar"))
	exists, err = rdb.Exists(ctx, s2.key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exists)
	assert.NoError(t, s2.Destroy(ctx))
}

func TestOptions(t *testing.T) {
	s, err := NewStream(ctx, "testOptions", nil, WithMaxLen(10), WithLogger(nil))
	assert.NoError(t, err)
	assert.Equal(t, 10, s.MaxLen)
	assert.Equal(t, &ponos.NilLogger{}, s.logger)
}

func TestAdd(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	s, err := NewStream(ctx, "testAdd", rdb)
	assert.NoError(t, err)

	assert.NoError(t, s.Add(ctx, "foo", []byte("bar")))
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
	s, err := NewStream(ctx, "testRemove", rdb)
	assert.NoError(t, err)

	assert.NoError(t, s.Add(ctx, "foo", []byte("bar")))
	assert.NoError(t, s.Add(ctx, "foo2", []byte("bar2")))

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
	s, err := NewStream(ctx, "testTopic", rdb)
	assert.NoError(t, err)

	tp := s.NewTopic("foo")
	tp.Add(ctx, "bar", []byte("baz"))

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
