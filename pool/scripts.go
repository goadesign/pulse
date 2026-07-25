// Package pool keeps its cross-map coordination scripts close to the pool
// admission code. These scripts preserve the rmap notification contract while
// making singleton job admission one atomic Redis operation.
package pool

import redis "github.com/redis/go-redis/v9"

const (
	dispatchClaimed int64 = iota + 1
	dispatchTerminal
	dispatchAlreadyPending
	dispatchAlreadyRunning
	dispatchCapacityReached
)

// nodeLivenessFenceLua rejects pool mutations from a stale node: the acting
// node must still hold its keep-alive registration and no stale-node cleanup
// fence may be installed. Composing scripts append the node keep-alive content
// key as the last KEYS entry and the node heartbeat field plus node-cleanup
// field as the last two ARGV entries.
const nodeLivenessFenceLua = `
local node_keepalive = KEYS[#KEYS]
if not redis.call("HGET", node_keepalive, ARGV[#ARGV - 1])
or redis.call("HGET", node_keepalive, ARGV[#ARGV]) then
    return redis.error_reply("NODECLEANUPLOST")
end
`

var (
	// luaDispatchJob atomically resolves an exact dispatch retry or admits one
	// new dispatch by writing its durable record, job-key index, and stream
	// event at one Redis linearization point.
	luaDispatchJob = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[4] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local identity = redis.call("HGET", KEYS[5], "identity")
if identity then
    if identity ~= ARGV[9] then
        return redis.error_reply("DISPATCHIDEMPOTENCYCONFLICT")
    end
    local state = redis.call("HGET", KEYS[5], "state")
    local event_id = redis.call("HGET", KEYS[5], "event")
    return {
        state == "terminal" and 2 or 1,
        event_id,
        redis.call("HGET", KEYS[5], "result") or "",
        redis.call("HGET", KEYS[5], "error") or ""
    }
end
local payload = redis.call("HGET", KEYS[2], ARGV[1])
if payload then
   return {4, "", "", ""}
end

local pending = redis.call("HGET", KEYS[3], ARGV[1])
if pending then
   return {3, "", "", ""}
end

local count = 0
for _, key in ipairs(redis.call("HKEYS", KEYS[3])) do
    if string.sub(key, 1, 1) ~= "=" then
        count = count + 1
    end
end
if count >= tonumber(ARGV[5]) then
    return {5, "", "", ""}
end

local stream = redis.call("HGET", KEYS[1], ARGV[8])
if not stream then
    return redis.error_reply("POOLGENERATIONLOST")
end
local event_id = redis.call("XADD", stream, "*", "n", ARGV[6], "p", ARGV[7])
redis.call("HSET", KEYS[5],
    "id", ARGV[2],
    "identity", ARGV[9],
    "key", ARGV[1],
    "event", event_id,
    "state", "pending",
    "result", "",
    "error", "")
redis.call("SADD", KEYS[6], KEYS[5])
redis.call("HSET", KEYS[3], ARGV[1], ARGV[2])
local rev = tostring(redis.call("HINCRBY", KEYS[3], "=rev", 1))
redis.call("HSET", KEYS[3], "=kind", "set")
local msg = struct.pack("ic0ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[2]), ARGV[2], string.len(rev), rev)
redis.call("PUBLISH", KEYS[4], "set:" .. msg)
return {1, event_id, "", ""}
`)

	// luaSettleDispatch atomically records the immutable terminal outcome,
	// clears admission, acknowledges the sink event, advances its durable
	// recovery cursor, and deletes the settled event.
	luaSettleDispatch = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[3]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[4] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local identity = redis.call("HGET", KEYS[4], "identity")
if not identity
or redis.call("HGET", KEYS[4], "id") ~= ARGV[2]
or redis.call("HGET", KEYS[4], "key") ~= ARGV[1] then
    return redis.error_reply("DISPATCHLOST")
end
local state = redis.call("HGET", KEYS[4], "state")
if state == "terminal" then
    return {
        redis.call("HGET", KEYS[4], "event"),
        redis.call("HGET", KEYS[4], "result") or "",
        redis.call("HGET", KEYS[4], "error") or ""
    }
end
if redis.call("HGET", KEYS[2], ARGV[1]) ~= ARGV[2] then
    return redis.error_reply("DISPATCHLOST")
end
local event_id = redis.call("HGET", KEYS[4], "event")
local stream = redis.call("HGET", KEYS[1], ARGV[5])
if not stream then
    return redis.error_reply("POOLGENERATIONLOST")
end

redis.call("HSET", KEYS[4],
    "state", "terminal",
    "result", ARGV[6],
    "error", ARGV[7])
redis.call("SREM", KEYS[5], KEYS[4])
redis.call("PEXPIRE", KEYS[4], ARGV[9])
redis.call("HDEL", KEYS[2], ARGV[1])
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "del")
local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(rev), rev)
redis.call("PUBLISH", KEYS[3], "del:" .. msg)
redis.call("XACK", stream, ARGV[8], event_id)
local pending = redis.call("XPENDING", stream, ARGV[8])
local cursor
if pending[1] == 0 then
    local groups = redis.call("XINFO", "GROUPS", stream)
    for _, group in ipairs(groups) do
        local name
        local delivered
        for i = 1, #group, 2 do
            if group[i] == "name" then
                name = group[i + 1]
            elseif group[i] == "last-delivered-id" then
                delivered = group[i + 1]
            end
        end
        if name == ARGV[8] then
            cursor = delivered
            break
        end
    end
else
    local previous = redis.call("XREVRANGE", stream, "(" .. pending[2], "-", "COUNT", 1)
    cursor = #previous == 0 and "0-0" or previous[1][1]
end
if cursor then
    local recovery = stream .. ":sink-recovery:" .. ARGV[4]
    redis.call("HSET", recovery, ARGV[8], cursor)
end
redis.call("XDEL", stream, event_id)
return {event_id, ARGV[6], ARGV[7]}
`)

	// readDispatchRecordScript authoritatively reads one exact dispatch after
	// verifying the pool generation and event identity. Local notifications are
	// only wake-up hints; callers use this result as the completion truth.
	readDispatchRecordScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local identity = redis.call("HGET", KEYS[2], "identity")
if not identity then
    return redis.error_reply("DISPATCHRECORDNOTFOUND")
end
if identity ~= ARGV[3] then
    return redis.error_reply("DISPATCHIDEMPOTENCYCONFLICT")
end
local state = redis.call("HGET", KEYS[2], "state")
return {
    state == "terminal" and 2 or 1,
    redis.call("HGET", KEYS[2], "event") or "",
    redis.call("HGET", KEYS[2], "result") or "",
    redis.call("HGET", KEYS[2], "error") or ""
}
`)

	// cleanupFailedStartScript atomically verifies this pool generation, removes
	// the worker's ownership, deletes the durable payload, and publishes both
	// rmap updates. A worker may publish terminal Start failure only after this
	// idempotent recipe succeeds.
	cleanupFailedStartScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[4]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[1] then
    return redis.error_reply("POOLGENERATIONLOST")
end

local worker = ARGV[2]
local job = ARGV[3]
local encoded = redis.call("HGET", KEYS[2], worker)
if encoded then
    local ok, values = pcall(cjson.decode, encoded)
    if not ok or type(values) ~= "table" then
        return redis.error_reply("INVALIDJOBOWNERSHIP")
    end
    local remaining = {}
    local removed = false
    for _, value in ipairs(values) do
        if value == job then
            removed = true
        else
            table.insert(remaining, value)
        end
    end
    if removed then
        local rev
        if #remaining == 0 then
            redis.call("HDEL", KEYS[2], worker)
            rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
            redis.call("HSET", KEYS[2], "=kind", "del")
            local msg = struct.pack("ic0ic0", string.len(worker), worker, string.len(rev), rev)
            redis.call("PUBLISH", KEYS[3], "del:" .. msg)
        else
            local value = cjson.encode(remaining)
            redis.call("HSET", KEYS[2], worker, value)
            rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
            redis.call("HSET", KEYS[2], "=kind", "set")
            local msg = struct.pack("ic0ic0ic0", string.len(worker), worker, string.len(value), value, string.len(rev), rev)
            redis.call("PUBLISH", KEYS[3], "set:" .. msg)
        end
    end
end

if redis.call("HDEL", KEYS[4], job) == 1 then
    local rev = tostring(redis.call("HINCRBY", KEYS[4], "=rev", 1))
    redis.call("HSET", KEYS[4], "=kind", "del")
    local msg = struct.pack("ic0ic0", string.len(job), job, string.len(rev), rev)
    redis.call("PUBLISH", KEYS[5], "del:" .. msg)
end
return 1
`)

	// releaseCrashedDispatchStartScript removes only the stale worker
	// ownership and payload for an exact dispatch that is still active. It
	// leaves the original dispatch event, ID, admission index, and record
	// untouched so sink recovery reclaims the same dispatch.
	releaseCrashedDispatchStartScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[5]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[1] then
    return redis.error_reply("POOLGENERATIONLOST")
end
if ARGV[2] ~= "" then
    local clock = redis.call("TIME")
    local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
    local cleanup = redis.call("HGET", KEYS[8], ARGV[2])
    local owner, fence, lease_until =
        string.match(cleanup or "", "^([^|]+)|([^|]+)|(%d+)$")
    if owner ~= ARGV[6] or fence ~= ARGV[7]
    or tonumber(lease_until or "0") <= now then
        return redis.error_reply("WORKERCLEANUPLOST")
    end
end
if redis.call("HGET", KEYS[2], ARGV[3]) ~= ARGV[4]
or redis.call("HGET", KEYS[3], "id") ~= ARGV[4]
or redis.call("HGET", KEYS[3], "key") ~= ARGV[3]
or redis.call("HGET", KEYS[3], "state") ~= "pending" then
    return 0
end

local worker = ARGV[2]
local job = ARGV[3]
if worker ~= "" then
    local encoded = redis.call("HGET", KEYS[4], worker)
    if encoded then
        local ok, values = pcall(cjson.decode, encoded)
        if not ok or type(values) ~= "table" then
            return redis.error_reply("INVALIDJOBOWNERSHIP")
        end
        local remaining = {}
        local removed = false
        for _, value in ipairs(values) do
            if value == job then
                removed = true
            else
                table.insert(remaining, value)
            end
        end
        if removed then
            local rev
            if #remaining == 0 then
                redis.call("HDEL", KEYS[4], worker)
                rev = tostring(redis.call("HINCRBY", KEYS[4], "=rev", 1))
                redis.call("HSET", KEYS[4], "=kind", "del")
                local msg = struct.pack("ic0ic0", string.len(worker), worker, string.len(rev), rev)
                redis.call("PUBLISH", KEYS[5], "del:" .. msg)
            else
                local value = cjson.encode(remaining)
                redis.call("HSET", KEYS[4], worker, value)
                rev = tostring(redis.call("HINCRBY", KEYS[4], "=rev", 1))
                redis.call("HSET", KEYS[4], "=kind", "set")
                local msg = struct.pack("ic0ic0ic0", string.len(worker), worker, string.len(value), value, string.len(rev), rev)
                redis.call("PUBLISH", KEYS[5], "set:" .. msg)
            end
        end
    end
end

if redis.call("HDEL", KEYS[6], job) == 1 then
    local rev = tostring(redis.call("HINCRBY", KEYS[6], "=rev", 1))
    redis.call("HSET", KEYS[6], "=kind", "del")
    local msg = struct.pack("ic0ic0", string.len(job), job, string.len(rev), rev)
    redis.call("PUBLISH", KEYS[7], "del:" .. msg)
end
return 1
`)

	// claimWorkerStartScript validates the exact dispatch guard and atomically
	// creates durable worker ownership plus payload before handler execution.
	// The claiming worker must still be registered with no cleanup fence
	// installed: heartbeat runs before the claim, so only this check makes a
	// worker paused past WorkerTTL unable to claim after authoritative takeover.
	// The claimed payload must match the dispatch record's canonical identity,
	// so a worker can never start a job whose bytes diverged from admission.
	claimWorkerStartScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[1] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local registration = redis.call("HGET", KEYS[8], ARGV[5])
if not registration or registration == "-"
or redis.call("HGET", KEYS[9], ARGV[5]) then
    return redis.error_reply("WORKERCLEANUPLOST")
end
local pending = redis.call("HGET", KEYS[2], ARGV[3])
if pending ~= ARGV[4] then
    return redis.error_reply("DISPATCHLOST")
end
if redis.call("HGET", KEYS[3], "id") ~= ARGV[4]
or redis.call("HGET", KEYS[3], "key") ~= ARGV[3]
or redis.call("HGET", KEYS[3], "state") ~= "pending" then
    return redis.error_reply("DISPATCHLOST")
end
if redis.call("HGET", KEYS[3], "identity") ~= ARGV[7] then
    return redis.error_reply("DISPATCHIDENTITYMISMATCH")
end
if redis.call("HEXISTS", KEYS[4], ARGV[3]) == 1 then
    return 0
end
local jobs = {}
local encoded = redis.call("HGET", KEYS[5], ARGV[5])
if encoded then
    local ok, decoded = pcall(cjson.decode, encoded)
    if not ok or type(decoded) ~= "table" then
        return redis.error_reply("INVALIDJOBOWNERSHIP")
    end
    jobs = decoded
end
table.insert(jobs, ARGV[3])
local jobs_value = cjson.encode(jobs)
redis.call("HSET", KEYS[5], ARGV[5], jobs_value)
local jobs_rev = tostring(redis.call("HINCRBY", KEYS[5], "=rev", 1))
redis.call("HSET", KEYS[5], "=kind", "set")
local jobs_msg = struct.pack("ic0ic0ic0", string.len(ARGV[5]), ARGV[5], string.len(jobs_value), jobs_value, string.len(jobs_rev), jobs_rev)
redis.call("PUBLISH", KEYS[6], "set:" .. jobs_msg)

redis.call("HSET", KEYS[4], ARGV[3], ARGV[6])
local payload_rev = tostring(redis.call("HINCRBY", KEYS[4], "=rev", 1))
redis.call("HSET", KEYS[4], "=kind", "set")
local payload_msg = struct.pack("ic0ic0ic0", string.len(ARGV[3]), ARGV[3], string.len(ARGV[6]), ARGV[6], string.len(payload_rev), payload_rev)
redis.call("PUBLISH", KEYS[7], "set:" .. payload_msg)
return 1
`)
)
