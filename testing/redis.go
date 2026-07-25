package testing

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
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
	// renewDatabaseLeaseScript extends only the lease still owned by this test.
	renewDatabaseLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return 1
`)
	// releaseDatabaseLeaseScript releases only the lease still owned by this
	// test process.
	releaseDatabaseLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call("DEL", KEYS[1])
`)
)

const (
	databaseLeaseTTL       = 2 * time.Minute
	databaseLeaseHeartbeat = 30 * time.Second
	databaseLeaseWait      = 30 * time.Second
)

type (
	// databaseLease owns one non-coordination Redis database for one test,
	// including cross-process heartbeat and release.
	databaseLease struct {
		coordinator  *redis.Client
		key          string
		token        string
		stop         chan struct{}
		done         chan struct{}
		lock         sync.Mutex
		heartbeatErr error
		t            *testing.T
	}
)

func init() {
	if p := os.Getenv("REDIS_PASSWORD"); p != "" {
		redisPwd = p
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisAddr = addr
	}
}

// NewRedisClient returns a client backed by a cross-process leased Redis
// database. Package names are irrelevant, and FlushDB is safe because the
// caller holds the database's exclusive lease until test cleanup completes.
func NewRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	lease, db := acquireDatabaseLease(t)
	t.Cleanup(func() {
		lease.release(t)
	})
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPwd, DB: db})
	require.NoError(t, rdb.Ping(context.Background()).Err())
	require.NoError(t, rdb.FlushDB(context.Background()).Err())
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
		clean := assert.Eventually(t, func() bool {
			filtered, err := remainingTestKeys(ctx, rdb, testName)
			return err == nil && len(filtered) == 0
		}, 5*time.Second, time.Millisecond*10)
		if !clean {
			filtered, err := remainingTestKeys(ctx, rdb, testName)
			require.NoError(t, err)
			t.Errorf("found keys: %v", filtered)
		}
	}
	assert.NoError(t, rdb.FlushDB(ctx).Err())
	assert.NoError(t, rdb.Close())
}

// remainingTestKeys returns keys owned by the named test that are not durable
// protocol state or resources with documented asynchronous cleanup.
func remainingTestKeys(ctx context.Context, rdb *redis.Client, testName string) ([]string, error) {
	keys, err := rdb.Keys(ctx, "*").Result()
	if err != nil {
		return nil, err
	}
	var filtered []string
	for _, key := range keys {
		if strings.HasPrefix(key, "pulse:pool:") && strings.HasSuffix(key, ":cleanup-generations") {
			continue
		}
		if strings.HasPrefix(key, "pulse:pool:") && strings.HasSuffix(key, ":resources") {
			continue
		}
		if isDestroyedStreamLifecycle(ctx, rdb, key) {
			continue
		}
		if strings.HasSuffix(key, ":sinks:content") {
			continue
		}
		if isDestroyTombstone(ctx, rdb, key) {
			continue
		}
		if streamRegexp.MatchString(key) {
			continue
		}
		if strings.Contains(key, testName) {
			filtered = append(filtered, key)
		}
	}
	return filtered, nil
}

func isDestroyedStreamLifecycle(ctx context.Context, rdb *redis.Client, key string) bool {
	if !strings.HasPrefix(key, "pulse:stream:") || !strings.HasSuffix(key, ":lifecycle") {
		return false
	}
	state, err := rdb.HGet(ctx, key, "state").Result()
	return err == nil && state == "destroyed"
}

func isDestroyTombstone(ctx context.Context, rdb *redis.Client, key string) bool {
	if !strings.HasPrefix(key, "map:") || !strings.HasSuffix(key, ":content") {
		return false
	}
	content, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return false
	}
	if len(content) != 2 {
		return false
	}
	if content["=kind"] != "destroy" {
		return false
	}
	_, ok := content["=rev"]
	return ok
}

// acquireDatabaseLease reserves one Redis database through DB 0, which is used
// only for coordination. Expired leases make crashed test processes harmless.
func acquireDatabaseLease(t *testing.T) (*databaseLease, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), databaseLeaseWait)
	defer cancel()
	coordinator := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPwd, DB: 0})
	if err := coordinator.Ping(ctx).Err(); err != nil {
		_ = coordinator.Close()
		require.NoError(t, err)
	}
	config, err := coordinator.ConfigGet(ctx, "databases").Result()
	if err != nil {
		_ = coordinator.Close()
		require.NoError(t, err)
	}
	databaseCount, err := strconv.Atoi(config["databases"])
	if err != nil || databaseCount < 2 {
		_ = coordinator.Close()
		require.NoError(t, fmt.Errorf("invalid Redis database capacity %q", config["databases"]))
	}
	token := ulid.Make().String()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		for db := 1; db < databaseCount; db++ {
			key := fmt.Sprintf("pulse:test:db-lease:%d", db)
			acquired, err := coordinator.SetNX(ctx, key, token, databaseLeaseTTL).Result()
			if err != nil {
				_ = coordinator.Close()
				require.NoError(t, err)
			}
			if !acquired {
				continue
			}
			lease := &databaseLease{
				coordinator: coordinator,
				key:         key,
				token:       token,
				stop:        make(chan struct{}),
				done:        make(chan struct{}),
				t:           t,
			}
			go lease.heartbeat()
			return lease, db
		}
		select {
		case <-ctx.Done():
			_ = coordinator.Close()
			require.NoError(
				t,
				ctx.Err(),
				"timed out waiting for one of %d isolated Redis test databases",
				databaseCount-1,
			)
		case <-ticker.C:
		}
	}
}

// heartbeat renews the lease while the test owns its database.
func (l *databaseLease) heartbeat() {
	defer close(l.done)
	ticker := time.NewTicker(databaseLeaseHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), databaseLeaseHeartbeat)
			renewed, err := renewDatabaseLeaseScript.Run(
				ctx,
				l.coordinator,
				[]string{l.key},
				l.token,
				databaseLeaseTTL.Milliseconds(),
			).Int64()
			cancel()
			if err != nil {
				heartbeatErr := fmt.Errorf("renew Redis database lease: %w", err)
				l.lock.Lock()
				l.heartbeatErr = heartbeatErr
				l.lock.Unlock()
				l.t.Errorf("%v", heartbeatErr)
				return
			}
			if renewed != 1 {
				heartbeatErr := fmt.Errorf("Redis database lease %q was lost", l.key)
				l.lock.Lock()
				l.heartbeatErr = heartbeatErr
				l.lock.Unlock()
				l.t.Errorf("%v", heartbeatErr)
				return
			}
		case <-l.stop:
			return
		}
	}
}

// release stops renewal and atomically frees the owned database lease.
func (l *databaseLease) release(t *testing.T) {
	t.Helper()
	close(l.stop)
	<-l.done
	l.lock.Lock()
	heartbeatErr := l.heartbeatErr
	l.lock.Unlock()
	assert.NoError(t, heartbeatErr)
	ctx, cancel := context.WithTimeout(context.Background(), databaseLeaseHeartbeat)
	defer cancel()
	err := releaseDatabaseLeaseScript.Run(ctx, l.coordinator, []string{l.key}, l.token).Err()
	assert.NoError(t, err)
	assert.NoError(t, l.coordinator.Close())
}
