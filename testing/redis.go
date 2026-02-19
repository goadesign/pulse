package testing

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// redisPwd is the default test redis password, overridden by REDIS_PASSWORD env var
	redisPwd = "redispassword"
	// redisAddr is the default test redis address, overridden by REDIS_ADDR env var
	redisAddr = "localhost:6379"
	// streamRegexp is a regular expression that matches valid stream keys
	streamRegexp = regexp.MustCompile(`^pulse:stream:[^:]+:node:.*`)
)

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisAddr = addr
	}
}

func NewRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPwd})
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
		var (
			filtered []string
			keysErr  error
		)
		assert.Eventually(t, func() bool {
			var keys []string
			keys, keysErr = rdb.Keys(ctx, "*").Result()
			if keysErr != nil {
				filtered = []string{fmt.Sprintf("keys error: %v", keysErr)}
				return false
			}
			filtered = filtered[:0]
			for _, k := range keys {
				if strings.HasSuffix(k, ":sinks:content") {
					// Sinks content is cleaned up asynchronously, so ignore it
					continue
				}
				if streamRegexp.MatchString(k) {
					// Node streams are cleaned up asynchronously, so ignore them
					continue
				}
				if strings.Contains(k, testName) {
					filtered = append(filtered, k)
				}
			}
			return len(filtered) == 0
		}, 5*time.Second, time.Millisecond*10, "found keys: %v", filtered)
		require.NoError(t, keysErr)
	}
	assert.NoError(t, rdb.FlushDB(ctx).Err())
}
