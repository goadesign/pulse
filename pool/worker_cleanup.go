// Stale-worker recovery is owned by one Redis-time lease capability. Requeue
// publication verifies generation, owner token, fence, and lease in the same
// Lua operation and records a stable worker/job publication key, preventing
// overlap and ABA duplicates after takeover.
package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/streaming"
)

type (
	// workerCleanupLease is the exact capability returned by Redis.
	workerCleanupLease struct {
		workerID string
		owner    string
		fence    string
	}
)

var (
	// updateWorkerHeartbeatScript refreshes the authoritative Redis-time
	// heartbeat only while the worker remains registered and no cleanup fence
	// has been installed. A resumed stale worker therefore cannot resurrect
	// itself after cleanup begins.
	updateWorkerHeartbeatScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local registration = redis.call("HGET", KEYS[4], ARGV[3])
if not registration or registration == "-"
or redis.call("HGET", KEYS[3], ARGV[3]) then
    return redis.error_reply("WORKERCLEANUPLOST")
end
local clock = redis.call("TIME")
local timestamp = clock[1] .. string.format("%06d", clock[2]) .. "000"
redis.call("HSET", KEYS[2], ARGV[3], timestamp)
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "set")
local message = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(timestamp), timestamp,
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[5], "set:" .. message)
return timestamp
`)

	// acquireWorkerCleanupScript acquires, renews, or steals an expired
	// requeue lease. Foreign acquisition must atomically prove the
	// authoritative heartbeat expired; graceful self-acquisition (ARGV[8])
	// instead requires the worker's own live registration and atomically
	// marks it inactive, so the lease is the single fence deciding which
	// party — the worker or a cleanup owner — requeues the jobs.
	acquireWorkerCleanupScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local now_ns = tonumber(clock[1]) * 1000000000 + tonumber(clock[2]) * 1000
if ARGV[8] ~= "1" then
    local heartbeat = redis.call("HGET", KEYS[2], ARGV[3])
    if heartbeat then
        local heartbeat_ns = tonumber(heartbeat)
        if not heartbeat_ns then
            return redis.error_reply("WORKERHEARTBEATINVALID")
        end
        if heartbeat_ns + tonumber(ARGV[7]) >= now_ns then
            return {0, "live"}
        end
    end
end
local current = redis.call("HGET", KEYS[3], ARGV[3])
if current then
    local current_owner, current_fence, current_until =
        string.match(current, "^([^|]+)|([^|]+)|(%d+)$")
    if not current_owner then
        return redis.error_reply("WORKERCLEANUPINVALID")
    end
    if current_owner ~= ARGV[4] and tonumber(current_until) > now then
        return {0, current_fence}
    end
    if current_owner == ARGV[4] and tonumber(current_until) > now then
        redis.call("HSET", KEYS[3], ARGV[3],
            current_owner .. "|" .. current_fence .. "|" .. tostring(now + tonumber(ARGV[5])))
        return {1, current_fence}
    end
end
if ARGV[8] == "1" then
    local registration = redis.call("HGET", KEYS[5], ARGV[3])
    if not registration or registration == "-" then
        return {0, "inactive"}
    end
    local inactive = "-"
    redis.call("HSET", KEYS[5], ARGV[3], inactive)
    local registration_rev = tostring(redis.call("HINCRBY", KEYS[5], "=rev", 1))
    redis.call("HSET", KEYS[5], "=kind", "set")
    local registration_message = struct.pack(
        "ic0ic0ic0",
        string.len(ARGV[3]), ARGV[3],
        string.len(inactive), inactive,
        string.len(registration_rev), registration_rev
    )
    redis.call("PUBLISH", KEYS[6], "set:" .. registration_message)
end
local fence_field = ARGV[6]
local fence = tostring(redis.call("HINCRBY", KEYS[3], fence_field, 1))
local value = ARGV[4] .. "|" .. fence .. "|" .. tostring(now + tonumber(ARGV[5]))
redis.call("HSET", KEYS[3], ARGV[3], value)
local rev = tostring(redis.call("HINCRBY", KEYS[3], "=rev", 1))
redis.call("HSET", KEYS[3], "=kind", "set")
local message = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(value), value,
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[4], "set:" .. message)
return {1, fence}
`)

	// renewWorkerCleanupScript proves the exact owner still holds an unexpired
	// lease before a non-publication cleanup step begins.
	renewWorkerCleanupScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local current = redis.call("HGET", KEYS[2], ARGV[3])
local owner, fence, lease_until = string.match(current or "", "^([^|]+)|([^|]+)|(%d+)$")
if owner ~= ARGV[4] or fence ~= ARGV[5] or tonumber(lease_until or "0") <= now then
    return redis.error_reply("WORKERCLEANUPLOST")
end
redis.call("HSET", KEYS[2], ARGV[3],
    owner .. "|" .. fence .. "|" .. tostring(now + tonumber(ARGV[6])))
return 1
`)

	// publishWorkerRequeueScript atomically fences and deduplicates one stale
	// worker/job handoff before appending it to the pool stream.
	publishWorkerRequeueScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local current = redis.call("HGET", KEYS[2], ARGV[3])
local owner, fence, lease_until = string.match(current or "", "^([^|]+)|([^|]+)|(%d+)$")
if owner ~= ARGV[4] or fence ~= ARGV[5] or tonumber(lease_until or "0") <= now then
    return redis.error_reply("WORKERCLEANUPLOST")
end
redis.call("HSET", KEYS[2], ARGV[3],
    owner .. "|" .. fence .. "|" .. tostring(now + tonumber(ARGV[6])))
local existing = redis.call("HGET", KEYS[2], ARGV[9])
if existing then
    return {0, existing}
end
local encoded = redis.call("HGET", KEYS[4], ARGV[3])
if not encoded then
    return {2, ""}
end
local ok, jobs = pcall(cjson.decode, encoded)
if not ok or type(jobs) ~= "table" then
    return redis.error_reply("POOLMAPINVALID")
end
local owned = false
for _, job in ipairs(jobs) do
    if job == ARGV[7] then
        owned = true
        break
    end
end
if not owned then
    return {2, ""}
end
if not redis.call("HGET", KEYS[5], ARGV[7]) then
    return {3, ""}
end
local stream = redis.call("HGET", KEYS[1], "physical_key")
if not stream then
    return redis.error_reply("POOLGENERATIONLOST")
end
local event_id = redis.call("XADD", stream, "*", "n", ARGV[8], "p", ARGV[10])
redis.call("HSET", KEYS[2], ARGV[9], event_id)
return {1, event_id}
`)

	// removeStaleWorkerJobScript removes payload-less ownership only while the
	// exact cleanup lease is live and the authoritative payload remains absent.
	removeStaleWorkerJobScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local current = redis.call("HGET", KEYS[2], ARGV[3])
local owner, fence, lease_until = string.match(current or "", "^([^|]+)|([^|]+)|(%d+)$")
if owner ~= ARGV[4] or fence ~= ARGV[5] or tonumber(lease_until or "0") <= now then
    return redis.error_reply("WORKERCLEANUPLOST")
end
if redis.call("HGET", KEYS[5], ARGV[6]) then
    return 0
end
local encoded = redis.call("HGET", KEYS[3], ARGV[3])
if not encoded then
    return 1
end
local ok, jobs = pcall(cjson.decode, encoded)
if not ok or type(jobs) ~= "table" then
    return redis.error_reply("POOLMAPINVALID")
end
local remaining = {}
local changed = false
for _, job in ipairs(jobs) do
    if job == ARGV[6] then
        changed = true
    else
        table.insert(remaining, job)
    end
end
if not changed then
    return 1
end
local update = ""
if #remaining == 0 then
    redis.call("HDEL", KEYS[3], ARGV[3])
else
    update = cjson.encode(remaining)
    redis.call("HSET", KEYS[3], ARGV[3], update)
end
local rev = tostring(redis.call("HINCRBY", KEYS[3], "=rev", 1))
if update == "" then
    redis.call("HSET", KEYS[3], "=kind", "del")
    local message = struct.pack(
        "ic0ic0",
        string.len(ARGV[3]), ARGV[3],
        string.len(rev), rev
    )
    redis.call("PUBLISH", KEYS[4], "del:" .. message)
else
    redis.call("HSET", KEYS[3], "=kind", "set")
    local message = struct.pack(
        "ic0ic0ic0",
        string.len(ARGV[3]), ARGV[3],
        string.len(update), update,
        string.len(rev), rev
    )
    redis.call("PUBLISH", KEYS[4], "set:" .. message)
end
return 1
`)

	// deleteStaleWorkerScript destroys the exact worker stream incarnation and
	// removes worker discovery only while the cleanup capability is current.
	deleteStaleWorkerScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local current = redis.call("HGET", KEYS[2], ARGV[3])
local owner, fence, lease_until = string.match(current or "", "^([^|]+)|([^|]+)|(%d+)$")
if owner ~= ARGV[4] or fence ~= ARGV[5] or tonumber(lease_until or "0") <= now then
    return redis.error_reply("WORKERCLEANUPLOST")
end
redis.call("HSET", KEYS[2], ARGV[3],
    owner .. "|" .. fence .. "|" .. tostring(now + tonumber(ARGV[6])))

if redis.call("HGET", KEYS[3], "generation") == ARGV[7] then
    local state = redis.call("HGET", KEYS[3], "state")
    if state == "active" then
        local physical = redis.call("HGET", KEYS[3], "physical_key")
        if not physical then
            return redis.error_reply("STREAMDESTROYED")
        end
        redis.call("HSET", KEYS[3], "state", "destroyed")
        local resources = redis.call("SMEMBERS", KEYS[4])
        if #resources > 0 then
            redis.call("DEL", unpack(resources))
        end
        redis.call("DEL", physical, physical .. ":sink-recovery:" .. ARGV[7], KEYS[4])
    elseif state ~= "destroyed" then
        return redis.error_reply("STREAMDESTROYED")
    end
end

local function delete_field(content, channel, field)
    if redis.call("HDEL", content, field) == 0 then
        return
    end
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "del")
    local message = struct.pack(
        "ic0ic0",
        string.len(field), field,
        string.len(rev), rev
    )
    redis.call("PUBLISH", channel, "del:" .. message)
end
delete_field(KEYS[5], KEYS[6], ARGV[3])
delete_field(KEYS[7], KEYS[8], ARGV[3])
delete_field(KEYS[9], KEYS[10], ARGV[3])
return 1
`)

	// releaseWorkerCleanupScript releases only the exact capability. Completed
	// cleanup also removes its stable publication and fence metadata.
	releaseWorkerCleanupScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local current = redis.call("HGET", KEYS[2], ARGV[3])
local owner, fence = string.match(current or "", "^([^|]+)|([^|]+)|%d+$")
if owner ~= ARGV[4] or fence ~= ARGV[5] then
    return 0
end
redis.call("HDEL", KEYS[2], ARGV[3])
if ARGV[6] == "1" then
    local fields = redis.call("HKEYS", KEYS[2])
    for _, field in ipairs(fields) do
        if field == ARGV[7] or string.sub(field, 1, string.len(ARGV[8])) == ARGV[8] then
            redis.call("HDEL", KEYS[2], field)
        end
    end
end
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "del")
local message = struct.pack(
    "ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[3], "del:" .. message)
return 1
`)
)

// acquireWorkerCleanup returns an exact lease or nil while another owner is
// live. It proves the worker heartbeat expired before fencing it.
func (node *Node) acquireWorkerCleanup(
	ctx context.Context,
	workerID string,
) (*workerCleanupLease, error) {
	return node.runWorkerCleanupAcquire(ctx, workerID, false)
}

// acquireGracefulRequeue returns the requeue lease for this worker's own
// graceful shutdown, atomically marking its registration inactive, or nil
// when another owner already holds (or completed) the requeue.
func (node *Node) acquireGracefulRequeue(
	ctx context.Context,
	workerID string,
) (*workerCleanupLease, error) {
	return node.runWorkerCleanupAcquire(ctx, workerID, true)
}

// runWorkerCleanupAcquire runs the shared lease acquisition; graceful skips
// the heartbeat-expiry proof and deactivates the worker's registration.
func (node *Node) runWorkerCleanupAcquire(
	ctx context.Context,
	workerID string,
	graceful bool,
) (*workerCleanupLease, error) {
	owner := node.ID + "-" + ulid.Make().String()
	self := ""
	if graceful {
		self = "1"
	}
	raw, err := acquireWorkerCleanupScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.workerKeepAlive),
			rmapContentKey(node.resources.workerCleanup),
			rmapUpdateChannel(node.resources.workerCleanup),
			rmapContentKey(node.resources.workers),
			rmapUpdateChannel(node.resources.workers),
		},
		"active",
		node.resources.generation,
		workerID,
		owner,
		strconv.FormatInt(node.workerTTL.Milliseconds(), 10),
		workerCleanupFenceField(workerID),
		strconv.FormatInt(node.resources.workerTTL.Nanoseconds(), 10),
		self,
	).Slice()
	if err != nil {
		return nil, poolBoundaryError(err)
	}
	if len(raw) != 2 {
		return nil, fmt.Errorf("worker cleanup claim returned %d fields", len(raw))
	}
	status, ok := raw[0].(int64)
	if !ok || (status != 0 && status != 1) {
		return nil, fmt.Errorf("worker cleanup claim returned invalid status %T(%v)", raw[0], raw[0])
	}
	fence, ok := raw[1].(string)
	if !ok || fence == "" {
		return nil, fmt.Errorf("worker cleanup claim returned invalid fence %T", raw[1])
	}
	if status == 0 {
		return nil, nil
	}
	return &workerCleanupLease{workerID: workerID, owner: owner, fence: fence}, nil
}

// updateWorkerHeartbeat atomically proves workerID is still registered and not
// fenced, then records and returns Redis TIME from the authoritative map.
func (node *Node) updateWorkerHeartbeat(ctx context.Context, workerID string) (string, error) {
	timestamp, err := updateWorkerHeartbeatScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.workerKeepAlive),
			rmapContentKey(node.resources.workerCleanup),
			rmapContentKey(node.resources.workers),
			rmapUpdateChannel(node.resources.workerKeepAlive),
		},
		"active",
		node.resources.generation,
		workerID,
	).Text()
	return timestamp, poolBoundaryError(err)
}

// renewWorkerCleanup extends the exact stale-worker cleanup lease.
func (node *Node) renewWorkerCleanup(ctx context.Context, lease *workerCleanupLease) error {
	return poolBoundaryError(renewWorkerCleanupScript.Run(
		ctx,
		node.rdb,
		node.workerCleanupKeys()[:2],
		"active",
		node.resources.generation,
		lease.workerID,
		lease.owner,
		lease.fence,
		strconv.FormatInt(node.workerTTL.Milliseconds(), 10),
	).Err())
}

// publishWorkerRequeue publishes one stable worker/job handoff under the exact
// lease. Status 2 means ownership already moved; status 3 means stale metadata
// has no payload and must be removed by the lease owner.
func (node *Node) publishWorkerRequeue(
	ctx context.Context,
	lease *workerCleanupLease,
	job *Job,
) (int64, error) {
	raw, err := publishWorkerRequeueScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.workerCleanup),
			rmapUpdateChannel(node.resources.workerCleanup),
			rmapContentKey(node.resources.jobs),
			rmapContentKey(node.resources.jobPayloads),
		},
		"active",
		node.resources.generation,
		lease.workerID,
		lease.owner,
		lease.fence,
		strconv.FormatInt(node.workerTTL.Milliseconds(), 10),
		job.Key,
		evStartJob,
		workerRequeueField(lease.workerID, job.Key),
		marshalJob(job),
	).Slice()
	if err != nil {
		return 0, poolBoundaryError(err)
	}
	if len(raw) != 2 {
		return 0, fmt.Errorf("worker requeue returned %d fields", len(raw))
	}
	status, ok := raw[0].(int64)
	if !ok || status < 0 || status > 3 {
		return 0, fmt.Errorf("worker requeue returned invalid status %T(%v)", raw[0], raw[0])
	}
	return status, nil
}

// removeStaleWorkerJob removes one payload-less worker ownership under the
// exact cleanup lease. A false result means a payload appeared concurrently.
func (node *Node) removeStaleWorkerJob(
	ctx context.Context,
	lease *workerCleanupLease,
	jobKey string,
) (bool, error) {
	removed, err := removeStaleWorkerJobScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.workerCleanup),
			rmapContentKey(node.resources.jobs),
			rmapUpdateChannel(node.resources.jobs),
			rmapContentKey(node.resources.jobPayloads),
		},
		"active",
		node.resources.generation,
		lease.workerID,
		lease.owner,
		lease.fence,
		jobKey,
	).Int64()
	if err != nil {
		return false, poolBoundaryError(err)
	}
	return removed == 1, nil
}

// deleteStaleWorker destroys the worker stream and removes worker-owned map
// entries under the exact Redis-time cleanup fence.
func (node *Node) deleteStaleWorker(
	ctx context.Context,
	lease *workerCleanupLease,
) error {
	stream, err := node.getWorkerStream(lease.workerID)
	if err != nil {
		return err
	}
	if err := stream.Open(ctx); err != nil {
		if !errors.Is(err, streaming.ErrStreamNotFound) &&
			!errors.Is(err, streaming.ErrStreamDestroyed) {
			return err
		}
	}
	generation := stream.Generation()
	return poolBoundaryError(deleteStaleWorkerScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.workerCleanup),
			fmt.Sprintf("pulse:stream:%s:lifecycle", stream.Name),
			fmt.Sprintf("pulse:stream:%s:generation:%s:resources", stream.Name, generation),
			rmapContentKey(node.resources.workers),
			rmapUpdateChannel(node.resources.workers),
			rmapContentKey(node.resources.workerKeepAlive),
			rmapUpdateChannel(node.resources.workerKeepAlive),
			rmapContentKey(node.resources.jobs),
			rmapUpdateChannel(node.resources.jobs),
		},
		"active",
		node.resources.generation,
		lease.workerID,
		lease.owner,
		lease.fence,
		strconv.FormatInt(node.workerTTL.Milliseconds(), 10),
		generation,
	).Err())
}

// releaseWorkerCleanup releases the exact lease. Complete cleanup compacts all
// per-worker idempotency metadata.
func (node *Node) releaseWorkerCleanup(
	ctx context.Context,
	lease *workerCleanupLease,
	complete bool,
) error {
	completeValue := "0"
	if complete {
		completeValue = "1"
	}
	return poolBoundaryError(releaseWorkerCleanupScript.Run(
		ctx,
		node.rdb,
		node.workerCleanupKeys(),
		"active",
		node.resources.generation,
		lease.workerID,
		lease.owner,
		lease.fence,
		completeValue,
		workerCleanupFenceField(lease.workerID),
		workerRequeuePrefix(lease.workerID),
	).Err())
}

// workerCleanupLeaseActive strictly decodes one Redis-owned lease projection.
func workerCleanupLeaseActive(value string, now time.Time) (bool, error) {
	parts := strings.Split(value, "|")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return false, fmt.Errorf("invalid worker cleanup lease %q", value)
	}
	leaseUntil, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid worker cleanup lease %q: %w", value, err)
	}
	return leaseUntil > now.UnixMilli(), nil
}

// workerCleanupKeys returns lifecycle, content, and rmap update keys.
func (node *Node) workerCleanupKeys() []string {
	return []string{
		fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
		rmapContentKey(node.resources.workerCleanup),
		rmapUpdateChannel(node.resources.workerCleanup),
	}
}

// workerCleanupFenceField scopes the ABA counter for one worker.
func workerCleanupFenceField(workerID string) string {
	return "=cleanup-fence:" + hex.EncodeToString([]byte(workerID))
}

// workerRequeuePrefix scopes stable publication records for one stale worker.
func workerRequeuePrefix(workerID string) string {
	return "=requeue:" + hex.EncodeToString([]byte(workerID)) + ":"
}

// workerRequeueField identifies one stable stale-worker/job publication.
func workerRequeueField(workerID, jobKey string) string {
	return workerRequeuePrefix(workerID) + hex.EncodeToString([]byte(jobKey))
}
