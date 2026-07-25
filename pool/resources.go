// Package pool binds every shared Redis resource to one pool-stream
// incarnation. Existing deployments are adopted in place once; resources
// created after explicit cleanup are generation-qualified.
package pool

import (
	"context"
	"fmt"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type (
	// poolResources is the immutable Redis namespace selected for one pool
	// stream generation.
	poolResources struct {
		pool                    string
		generation              string
		nodeKeepAlive           string
		nodeShutdown            string
		workers                 string
		workerKeepAlive         string
		workerCleanup           string
		jobs                    string
		jobPending              string
		dispatches              string
		jobPayloads             string
		tickers                 string
		schedulerJobs           string
		maxQueuedJobs           int
		workerTTL               time.Duration
		cleanupLease            time.Duration
		dispatchResultRetention time.Duration
	}
)

const (
	poolResourceStateActive    = "active"
	poolResourceStateDestroyed = "destroyed"
)

var (
	// establishPoolResourcesScript adopts flat pre-generation resources when no
	// manifest exists. Once cleanup marks a manifest destroyed, the next stream
	// generation receives the supplied qualified names.
	establishPoolResourcesScript = redis.NewScript(`
if redis.call("HGET", KEYS[2], "state") ~= ARGV[4]
or redis.call("HGET", KEYS[2], "generation") ~= ARGV[1] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local state = redis.call("HGET", KEYS[1], "state")
local generation = redis.call("HGET", KEYS[1], "generation")
local function has_values(name)
    for _, key in ipairs(redis.call("HKEYS", "map:" .. name .. ":content")) do
        if string.sub(key, 1, 1) ~= "=" then
            return true
        end
    end
    return false
end
if not redis.call("HGET", KEYS[1], "format_version") then
    for i = 10, 20 do
        if has_values(ARGV[i]) then
            return redis.error_reply("POOLQUIESCENCEREQUIRED")
        end
    end
    local pool = string.sub(ARGV[10], 1, string.len(ARGV[10]) - string.len(":node-keepalive"))
    local map_prefix = "map:" .. pool .. ":"
    local cursor = "0"
    repeat
        local scan = redis.call("SCAN", cursor, "MATCH", "map:*:content", "COUNT", 100)
        cursor = scan[1]
        for _, key in ipairs(scan[2]) do
            if string.sub(key, 1, string.len(map_prefix)) == map_prefix then
                for _, field in ipairs(redis.call("HKEYS", key)) do
                    if string.sub(field, 1, 1) ~= "=" then
                        return redis.error_reply("POOLQUIESCENCEREQUIRED")
                    end
                end
            end
        end
    until cursor == "0"
    local physical = redis.call("HGET", KEYS[2], "physical_key")
    if physical and redis.call("EXISTS", physical) == 1 then
        if redis.call("XLEN", physical) > 0 or #redis.call("XINFO", "GROUPS", physical) > 0 then
            return redis.error_reply("POOLQUIESCENCEREQUIRED")
        end
    end
    local node_prefix = "pulse:stream:" .. pool .. ":node:"
    cursor = "0"
    repeat
        local scan = redis.call("SCAN", cursor, "MATCH", "pulse:stream:*", "COUNT", 100)
        cursor = scan[1]
        for _, key in ipairs(scan[2]) do
            if string.sub(key, 1, string.len(node_prefix)) == node_prefix then
                local suffix = string.sub(key, string.len(node_prefix) + 1)
                if not string.find(suffix, ":", 1, true) then
                    redis.call("DEL", key)
                end
            end
        end
    until cursor == "0"
end
if generation and not redis.call("HGET", KEYS[1], "format_version") then
    local keepalive = redis.call("HGET", KEYS[1], "node_keepalive")
    if keepalive then
        for _, key in ipairs(redis.call("HKEYS", "map:" .. keepalive .. ":content")) do
            if string.sub(key, 1, 1) ~= "=" then
                return redis.error_reply("POOLQUIESCENCEREQUIRED")
            end
        end
    end
end
if state == ARGV[2] and generation == ARGV[1] then
    local format = redis.call("HGET", KEYS[1], "format_version")
    if format and (format ~= ARGV[9]
    or redis.call("HGET", KEYS[1], "max_queued_jobs") ~= ARGV[5]
    or redis.call("HGET", KEYS[1], "worker_ttl_ms") ~= ARGV[6]
    or redis.call("HGET", KEYS[1], "cleanup_lease_ms") ~= ARGV[7]
    or redis.call("HGET", KEYS[1], "dispatch_result_retention_ms") ~= ARGV[8]) then
        return redis.error_reply("POOLCONFIGMISMATCH")
    end
    if format then
        return redis.call("HMGET", KEYS[1],
            "generation", "node_keepalive", "node_shutdown", "workers",
            "worker_keepalive", "worker_cleanup", "jobs", "job_pending",
                "dispatches", "job_payloads", "tickers", "scheduler_jobs",
                "max_queued_jobs", "worker_ttl_ms", "cleanup_lease_ms",
                "dispatch_result_retention_ms", "format_version")
    end
end
local use_qualified = state == ARGV[3]
local offset = use_qualified and 21 or 10
redis.call("HSET", KEYS[1],
    "state", ARGV[2],
    "generation", ARGV[1],
    "node_keepalive", ARGV[offset],
    "node_shutdown", ARGV[offset + 1],
    "workers", ARGV[offset + 2],
    "worker_keepalive", ARGV[offset + 3],
    "worker_cleanup", ARGV[offset + 4],
    "jobs", ARGV[offset + 5],
    "job_pending", ARGV[offset + 6],
        "dispatches", ARGV[offset + 7],
        "job_payloads", ARGV[offset + 8],
        "tickers", ARGV[offset + 9],
        "scheduler_jobs", ARGV[offset + 10],
    "max_queued_jobs", ARGV[5],
    "worker_ttl_ms", ARGV[6],
    "cleanup_lease_ms", ARGV[7],
    "dispatch_result_retention_ms", ARGV[8],
    "format_version", ARGV[9])
return redis.call("HMGET", KEYS[1],
    "generation", "node_keepalive", "node_shutdown", "workers",
    "worker_keepalive", "worker_cleanup", "jobs", "job_pending",
        "dispatches", "job_payloads", "tickers", "scheduler_jobs",
        "max_queued_jobs", "worker_ttl_ms", "cleanup_lease_ms",
        "dispatch_result_retention_ms", "format_version")
`)
)

// establishPoolResources records or loads the exact resource names selected
// for generation. The first observed generation adopts legacy flat names.
func establishPoolResources(
	ctx context.Context,
	rdb *redis.Client,
	pool, generation string,
	maxQueuedJobs int,
	workerTTL, cleanupLease, dispatchResultRetention time.Duration,
) (poolResources, error) {
	flat := flatPoolResources(pool, generation)
	qualified := qualifiedPoolResources(pool, generation)
	values, err := establishPoolResourcesScript.Run(
		ctx,
		rdb,
		[]string{
			poolResourcesKey(pool),
			fmt.Sprintf("pulse:stream:%s:lifecycle", poolStreamName(pool)),
		},
		append(
			[]any{
				generation,
				poolResourceStateActive,
				poolResourceStateDestroyed,
				"active",
				fmt.Sprintf("%d", maxQueuedJobs),
				strconv.FormatInt(workerTTL.Milliseconds(), 10),
				strconv.FormatInt(cleanupLease.Milliseconds(), 10),
				strconv.FormatInt(dispatchResultRetention.Milliseconds(), 10),
				"7",
			},
			append(flat.names(), qualified.names()...)...,
		)...,
	).Slice()
	if err != nil {
		return poolResources{}, fmt.Errorf("establish pool %q resources: %w", pool, poolBoundaryError(err))
	}
	resources, err := parsePoolResources(values)
	resources.pool = pool
	return resources, err
}

// loadPoolResources loads the exact resource manifest for generation.
func loadPoolResources(ctx context.Context, rdb *redis.Client, pool, generation string) (poolResources, error) {
	values, err := rdb.HMGet(
		ctx,
		poolResourcesKey(pool),
		"generation",
		"node_keepalive",
		"node_shutdown",
		"workers",
		"worker_keepalive",
		"worker_cleanup",
		"jobs",
		"job_pending",
		"dispatches",
		"job_payloads",
		"tickers",
		"scheduler_jobs",
		"max_queued_jobs",
		"worker_ttl_ms",
		"cleanup_lease_ms",
		"dispatch_result_retention_ms",
		"format_version",
	).Result()
	if err != nil {
		return poolResources{}, fmt.Errorf("load pool %q resources: %w", pool, err)
	}
	resources, err := parsePoolResources(values)
	if err != nil {
		return poolResources{}, fmt.Errorf("load pool %q resources: %w", pool, err)
	}
	if resources.generation != generation {
		return poolResources{}, fmt.Errorf(
			"pool %q resource generation mismatch: have %q, need %q",
			pool,
			resources.generation,
			generation,
		)
	}
	resources.pool = pool
	return resources, nil
}

// parsePoolResources validates the Redis-owned resource manifest.
func parsePoolResources(values []any) (poolResources, error) {
	if len(values) != 17 {
		return poolResources{}, fmt.Errorf("invalid resource manifest length %d", len(values))
	}
	decoded := make([]string, len(values))
	for i, value := range values {
		name, ok := value.(string)
		if !ok || name == "" {
			return poolResources{}, fmt.Errorf("invalid resource manifest field %d: %T", i, value)
		}
		decoded[i] = name
	}
	maxQueuedJobs, err := strconv.Atoi(decoded[12])
	if err != nil || maxQueuedJobs <= 0 {
		return poolResources{}, fmt.Errorf("invalid resource manifest capacity %q", decoded[12])
	}
	workerTTLMillis, err := strconv.ParseInt(decoded[13], 10, 64)
	if err != nil || workerTTLMillis <= 0 {
		return poolResources{}, fmt.Errorf("invalid resource manifest worker TTL %q", decoded[13])
	}
	cleanupLeaseMillis, err := strconv.ParseInt(decoded[14], 10, 64)
	if err != nil || cleanupLeaseMillis <= 0 {
		return poolResources{}, fmt.Errorf("invalid resource manifest cleanup lease %q", decoded[14])
	}
	dispatchRetentionMillis, err := strconv.ParseInt(decoded[15], 10, 64)
	if err != nil || dispatchRetentionMillis <= 0 {
		return poolResources{}, fmt.Errorf(
			"invalid resource manifest dispatch result retention %q",
			decoded[15],
		)
	}
	if decoded[16] != "7" {
		return poolResources{}, fmt.Errorf("unsupported resource manifest format %q", decoded[16])
	}
	return poolResources{
		generation:              decoded[0],
		nodeKeepAlive:           decoded[1],
		nodeShutdown:            decoded[2],
		workers:                 decoded[3],
		workerKeepAlive:         decoded[4],
		workerCleanup:           decoded[5],
		jobs:                    decoded[6],
		jobPending:              decoded[7],
		dispatches:              decoded[8],
		jobPayloads:             decoded[9],
		tickers:                 decoded[10],
		schedulerJobs:           decoded[11],
		maxQueuedJobs:           maxQueuedJobs,
		workerTTL:               time.Duration(workerTTLMillis) * time.Millisecond,
		cleanupLease:            time.Duration(cleanupLeaseMillis) * time.Millisecond,
		dispatchResultRetention: time.Duration(dispatchRetentionMillis) * time.Millisecond,
	}, nil
}

// flatPoolResources returns the pre-generation resource layout.
func flatPoolResources(pool, generation string) poolResources {
	return poolResources{
		pool:            pool,
		generation:      generation,
		nodeKeepAlive:   nodeKeepAliveMapName(pool),
		nodeShutdown:    nodeShutdownMapName(pool),
		workers:         workerMapName(pool),
		workerKeepAlive: workerKeepAliveMapName(pool),
		workerCleanup:   workerCleanupMapName(pool),
		jobs:            jobMapName(pool),
		jobPending:      jobPendingMapName(pool),
		dispatches:      dispatchMapName(pool),
		jobPayloads:     jobPayloadMapName(pool),
		tickers:         tickerMapName(pool),
		schedulerJobs:   schedulerJobMapName(pool),
	}
}

// qualifiedPoolResources returns the namespace used after explicit cleanup.
func qualifiedPoolResources(pool, generation string) poolResources {
	suffix := fmt.Sprintf(":generation:%s", generation)
	resources := flatPoolResources(pool, generation)
	resources.nodeKeepAlive += suffix
	resources.nodeShutdown += suffix
	resources.workers += suffix
	resources.workerKeepAlive += suffix
	resources.workerCleanup += suffix
	resources.jobs += suffix
	resources.jobPending += suffix
	resources.dispatches += suffix
	resources.jobPayloads += suffix
	resources.tickers += suffix
	resources.schedulerJobs += suffix
	return resources
}

// names returns the manifest names in the Lua contract order.
func (r poolResources) names() []any {
	return []any{
		r.nodeKeepAlive,
		r.nodeShutdown,
		r.workers,
		r.workerKeepAlive,
		r.workerCleanup,
		r.jobs,
		r.jobPending,
		r.dispatches,
		r.jobPayloads,
		r.tickers,
		r.schedulerJobs,
	}
}

// mapNames returns every generation-owned rmap name, including the dispatch
// map name whose rmap-format keys only legacy layouts populate: cleanup must
// delete those too or an adopted deployment retains them forever.
func (r poolResources) mapNames() []string {
	return []string{
		r.nodeKeepAlive,
		r.workers,
		r.dispatches,
		r.workerKeepAlive,
		r.workerCleanup,
		r.jobs,
		r.jobPending,
		r.jobPayloads,
		r.tickers,
		r.schedulerJobs,
		r.nodeShutdown,
	}
}

// poolResourcesKey stores the exact resource manifest for the active or most
// recently destroyed pool generation.
func poolResourcesKey(pool string) string {
	return fmt.Sprintf("pulse:pool:%s:resources", pool)
}
