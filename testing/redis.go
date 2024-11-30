package testing

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redisPwd is the default test redis password, overridden by REDIS_PASSWORD env var
var redisPwd = "redispassword"

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
}

func NewRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
	require.NoError(t, rdb.Ping(context.Background()).Err())
	return rdb
}

// CleanupRedis cleans up the Redis database after a test.
// If checkClean is true, it will check for keys in the database that
// contain the test name and fail the test if any are found.
// It will then flush the database.
func CleanupRedis(t *testing.T, rdb *redis.Client, checkClean bool, testName string) {
	t.Helper()
	ctx := context.Background()
	if checkClean {
		keys, err := rdb.Keys(ctx, "*").Result()
		require.NoError(t, err)
		var filtered []string
		for _, k := range keys {
			if strings.HasSuffix(k, ":sinks:content") {
				// Sinks content is cleaned up asynchronously, so ignore it
				continue
			}
			if regexp.MustCompile(`^pulse:stream:[^:]+:node:.*`).MatchString(k) {
				// Node streams are cleaned up asynchronously, so ignore them
				continue
			}
			if strings.Contains(k, testName) {
				filtered = append(filtered, k)
			}
		}
		assert.Eventually(t, func() bool {
			return len(filtered) == 0
		}, time.Second, time.Millisecond*10, "found keys: %v", filtered)
	}
	assert.NoError(t, rdb.FlushDB(ctx).Err())
}
