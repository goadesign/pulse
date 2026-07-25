// Package pool keeps final pool cleanup as a Redis-leased obligation. The
// cleanup record is bounded to one generation, any process may reclaim an
// expired owner, and the shutdown admission fence remains present until every
// shared stream and map has been removed.
package pool

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const (
	poolCleanupFinishingState = "finishing"
	poolCleanupCompleteState  = "complete"

	poolCleanupBusy            = int64(0)
	poolCleanupClaimed         = int64(1)
	poolCleanupAlreadyComplete = int64(2)
)

var (
	// claimPoolCleanupScript atomically verifies the empty node barrier and
	// acquires or renews the cleanup lease using Redis's clock.
	claimPoolCleanupScript = redis.NewScript(`
for _, key in ipairs(redis.call("HKEYS", KEYS[1])) do
    if string.sub(key, 1, 1) ~= "=" then
        return 0
    end
end

local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
local state = redis.call("HGET", KEYS[2], "state")
local generation = redis.call("HGET", KEYS[2], "generation")
local owner = redis.call("HGET", KEYS[2], "owner")
local lease_until = tonumber(redis.call("HGET", KEYS[2], "lease_until") or "0")

if state == ARGV[4] and generation == ARGV[1] then
    return 2
end
if state == ARGV[3] and owner ~= ARGV[2] and lease_until > now then
    return 0
end

redis.call("HSET", KEYS[2],
    "state", ARGV[3],
    "generation", ARGV[1],
    "owner", ARGV[2],
    "lease_until", tostring(now + tonumber(ARGV[5])))
return 1
`)

	// completePoolCleanupScript verifies ownership, then compacts all historical
	// claim fields to one bounded completion marker.
	completePoolCleanupScript = redis.NewScript(`
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "owner") ~= ARGV[2]
or tonumber(redis.call("HGET", KEYS[1], "lease_until") or "0") <= now then
    return redis.error_reply("POOLCLEANUPLOST")
end
if redis.call("HGET", KEYS[2], "state") ~= ARGV[5]
or redis.call("HGET", KEYS[2], "generation") ~= ARGV[1] then
    return redis.error_reply("POOLRESOURCELOST")
end
redis.call("DEL", KEYS[1])
redis.call("HSET", KEYS[1], "state", ARGV[4], "generation", ARGV[1])
redis.call("HSET", KEYS[2], "state", ARGV[6])
return 1
`)

	// destroyCleanupStreamScript verifies the live cleanup lease and exact
	// stream incarnation in the same operation that invalidates and deletes it.
	destroyCleanupStreamScript = redis.NewScript(`
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "owner") ~= ARGV[2]
or tonumber(redis.call("HGET", KEYS[1], "lease_until") or "0") <= now then
    return redis.error_reply("POOLCLEANUPLOST")
end
redis.call("HSET", KEYS[1], "lease_until", tostring(now + tonumber(ARGV[4])))

if redis.call("HGET", KEYS[2], "generation") ~= ARGV[1] then
    return redis.error_reply("STREAMDESTROYED")
end
local state = redis.call("HGET", KEYS[2], "state")
if state == "destroyed" then
    return 0
end
if state ~= "active" then
    return redis.error_reply("STREAMDESTROYED")
end
local physical = redis.call("HGET", KEYS[2], "physical_key")
if not physical then
    return redis.error_reply("STREAMDESTROYED")
end
redis.call("HSET", KEYS[2], "state", "destroyed")
local rev = redis.call("HINCRBY", KEYS[3], "=rev", 1)
local resources = redis.call("SMEMBERS", KEYS[5])
if #resources > 0 then
    redis.call("DEL", unpack(resources))
end
redis.call("DEL", physical, physical .. ":sink-recovery:" .. ARGV[1], KEYS[3])
redis.call("DEL", KEYS[5])
redis.call("PUBLISH", KEYS[4], "destroy:" .. tostring(rev))
return 1
`)

	// destroyCleanupMapScript deletes one pool map only while this exact cleanup
	// owner still holds an unexpired lease.
	destroyCleanupMapScript = redis.NewScript(`
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "owner") ~= ARGV[2]
or tonumber(redis.call("HGET", KEYS[1], "lease_until") or "0") <= now then
    return redis.error_reply("POOLCLEANUPLOST")
end
redis.call("HSET", KEYS[1], "lease_until", tostring(now + tonumber(ARGV[4])))
local rev = redis.call("HINCRBY", KEYS[2], "=rev", 1)
redis.call("DEL", KEYS[2])
redis.call("PUBLISH", KEYS[3], "destroy:" .. tostring(rev))
return 1
`)

	// destroyCleanupDispatchesScript removes all unsettled per-dispatch records
	// while the exact cleanup owner holds its Redis-time lease. Settled records
	// are self-bounded by DispatchResultRetention and need no generation index.
	destroyCleanupDispatchesScript = redis.NewScript(`
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "owner") ~= ARGV[2]
or tonumber(redis.call("HGET", KEYS[1], "lease_until") or "0") <= now then
    return redis.error_reply("POOLCLEANUPLOST")
end
redis.call("HSET", KEYS[1], "lease_until", tostring(now + tonumber(ARGV[4])))
local records = redis.call("SMEMBERS", KEYS[2])
if #records > 0 then
    redis.call("DEL", unpack(records))
end
redis.call("DEL", KEYS[2])
return 1
`)
)

// resumeExpiredPoolCleanup lets AddNode complete an abandoned finishing claim
// before attempting admission under the same pool name.
func resumeExpiredPoolCleanup(
	ctx context.Context,
	pool, owner string,
	lease time.Duration,
	rdb *redis.Client,
) error {
	record, err := rdb.HGetAll(ctx, poolCleanupGenerationsKey(pool)).Result()
	if err != nil {
		return fmt.Errorf("failed to read pool cleanup claim: %w", err)
	}
	if record["state"] != poolCleanupFinishingState {
		return nil
	}
	generation := record["generation"]
	if generation == "" {
		return fmt.Errorf("pool %q has a finishing cleanup without a generation", pool)
	}
	resources, err := loadPoolResources(ctx, rdb, pool, generation)
	if err != nil {
		return fmt.Errorf("failed to load pool cleanup resources: %w", err)
	}
	if err := reapExpiredPoolNodes(ctx, rdb, pool, generation, owner, resources.workerTTL); err != nil {
		return err
	}
	status, err := claimPoolCleanup(ctx, rdb, pool, generation, owner, lease)
	if err != nil {
		return fmt.Errorf("failed to reclaim pool cleanup: %w", err)
	}
	if status == poolCleanupBusy {
		return fmt.Errorf("pool %q is shutting down", pool)
	}
	if status == poolCleanupAlreadyComplete {
		return nil
	}
	if err := cleanupPoolResources(ctx, rdb, pool, generation, owner, lease); err != nil {
		return fmt.Errorf("failed to resume pool cleanup: %w", err)
	}
	return nil
}

// claimPoolCleanup acquires or renews the persisted cleanup owner and lease.
func claimPoolCleanup(
	ctx context.Context,
	rdb *redis.Client,
	pool, generation, owner string,
	lease time.Duration,
) (int64, error) {
	resources, err := loadPoolResources(ctx, rdb, pool, generation)
	if err != nil {
		return 0, err
	}
	if lease != resources.cleanupLease {
		return 0, fmt.Errorf(
			"%w: pool %q cleanup lease is %v, requested %v",
			ErrPoolConfigMismatch,
			pool,
			resources.cleanupLease,
			lease,
		)
	}
	return claimPoolCleanupScript.Run(
		ctx,
		rdb,
		[]string{
			rmapContentKey(resources.nodeKeepAlive),
			poolCleanupGenerationsKey(pool),
		},
		generation,
		owner,
		poolCleanupFinishingState,
		poolCleanupCompleteState,
		strconv.FormatInt(lease.Milliseconds(), 10),
	).Int64()
}

// cleanupPoolResources renews ownership before each idempotent destructive
// step. The shutdown map is destroyed last, immediately before the bounded
// completion marker replaces the finishing claim.
func cleanupPoolResources(
	ctx context.Context,
	rdb *redis.Client,
	pool, generation, owner string,
	lease time.Duration,
) error {
	resources, err := loadPoolResources(ctx, rdb, pool, generation)
	if err != nil {
		return err
	}
	if lease != resources.cleanupLease {
		return fmt.Errorf(
			"%w: pool %q cleanup lease is %v, requested %v",
			ErrPoolConfigMismatch,
			pool,
			resources.cleanupLease,
			lease,
		)
	}
	if err := destroyCleanupStream(ctx, rdb, pool, generation, owner, lease); err != nil {
		return fmt.Errorf("destroy pool stream: %w", err)
	}
	if err := destroyPoolDispatches(ctx, rdb, pool, resources.dispatches, generation, owner, lease); err != nil {
		return err
	}

	for _, name := range resources.mapNames() {
		if name == resources.nodeShutdown {
			continue
		}
		if err := destroyPoolMap(ctx, rdb, pool, name, generation, owner, lease); err != nil {
			return err
		}
	}
	if err := destroyPoolMap(ctx, rdb, pool, resources.nodeShutdown, generation, owner, lease); err != nil {
		return err
	}
	if err := completePoolCleanupScript.Run(
		ctx,
		rdb,
		[]string{poolCleanupGenerationsKey(pool), poolResourcesKey(pool)},
		generation,
		owner,
		poolCleanupFinishingState,
		poolCleanupCompleteState,
		poolResourceStateActive,
		poolResourceStateDestroyed,
	).Err(); err != nil {
		return fmt.Errorf("failed to record cleanup completion: %w", err)
	}
	return nil
}

// destroyPoolDispatches deletes every active dispatch record and its bounded
// generation index under the current cleanup lease.
func destroyPoolDispatches(
	ctx context.Context,
	rdb *redis.Client,
	pool, resource, generation, owner string,
	lease time.Duration,
) error {
	err := destroyCleanupDispatchesScript.Run(
		ctx,
		rdb,
		[]string{
			poolCleanupGenerationsKey(pool),
			dispatchActiveKey(resource),
		},
		generation,
		owner,
		poolCleanupFinishingState,
		strconv.FormatInt(lease.Milliseconds(), 10),
	).Err()
	if err != nil {
		return fmt.Errorf("destroy pool dispatch records: %w", err)
	}
	return nil
}

// renewPoolCleanup proves that this process still owns cleanup and extends its
// Redis-time lease.
func renewPoolCleanup(
	ctx context.Context,
	rdb *redis.Client,
	pool, generation, owner string,
	lease time.Duration,
) error {
	status, err := claimPoolCleanup(ctx, rdb, pool, generation, owner, lease)
	if err != nil {
		return fmt.Errorf("renew cleanup lease: %w", err)
	}
	if status == poolCleanupBusy {
		return fmt.Errorf("cleanup lease for pool %q is owned by another process", pool)
	}
	if status == poolCleanupAlreadyComplete {
		return fmt.Errorf("cleanup for pool %q is already complete", pool)
	}
	return nil
}

// reapExpiredPoolNodes removes stale registrations before an abandoned cleanup
// claim is considered busy or failed.
func reapExpiredPoolNodes(
	ctx context.Context,
	rdb *redis.Client,
	pool, generation, owner string,
	ttl time.Duration,
) error {
	resources, err := loadPoolResources(ctx, rdb, pool, generation)
	if err != nil {
		return err
	}
	if ttl != resources.workerTTL {
		return fmt.Errorf(
			"%w: pool %q worker TTL is %v, requested %v",
			ErrPoolConfigMismatch,
			pool,
			resources.workerTTL,
			ttl,
		)
	}
	key := rmapContentKey(resources.nodeKeepAlive)
	nodes, err := rdb.HKeys(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("read pool node leases: %w", err)
	}
	for _, nodeID := range nodes {
		if strings.HasPrefix(nodeID, "=") {
			continue
		}
		if _, err := cleanupStalePoolNode(
			ctx,
			rdb,
			resources,
			nodeID,
			owner,
		); err != nil {
			return fmt.Errorf("reap stale node %q: %w", nodeID, err)
		}
	}
	return nil
}

// destroyPoolMap joins one known pool map solely to publish its canonical
// destroy operation, then releases the local replica.
func destroyPoolMap(
	ctx context.Context,
	rdb *redis.Client,
	pool, name, generation, owner string,
	lease time.Duration,
) error {
	if err := destroyCleanupMapScript.Run(
		ctx,
		rdb,
		[]string{
			poolCleanupGenerationsKey(pool),
			rmapContentKey(name),
			rmapUpdateChannel(name),
		},
		generation,
		owner,
		poolCleanupFinishingState,
		strconv.FormatInt(lease.Milliseconds(), 10),
	).Err(); err != nil {
		return fmt.Errorf("destroy pool map %q: %w", name, err)
	}
	return nil
}

// destroyCleanupStream invalidates the exact pool stream generation under the
// same Redis-time lease check that deletes its physical data and metadata.
func destroyCleanupStream(
	ctx context.Context,
	rdb *redis.Client,
	pool, generation, owner string,
	lease time.Duration,
) error {
	name := poolStreamName(pool)
	membership := rmapContentKey(fmt.Sprintf("stream:%s:generation:%s:sinks", name, generation))
	return destroyCleanupStreamScript.Run(
		ctx,
		rdb,
		[]string{
			poolCleanupGenerationsKey(pool),
			fmt.Sprintf("pulse:stream:%s:lifecycle", name),
			membership,
			rmapUpdateChannel(fmt.Sprintf("stream:%s:generation:%s:sinks", name, generation)),
			fmt.Sprintf("pulse:stream:%s:generation:%s:resources", name, generation),
		},
		generation,
		owner,
		poolCleanupFinishingState,
		strconv.FormatInt(lease.Milliseconds(), 10),
	).Err()
}
