// Worker registration saves the worker's identity and first heartbeat together.
// Pool cleanup therefore observes either no worker or a worker with a current
// heartbeat. Repeated registration cannot revive a destroyed worker stream.
package pool

import (
	"context"
	"fmt"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/streaming"
)

var (
	registerWorkerScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
if redis.call("HGET", KEYS[7], ARGV[5]) then
    return redis.error_reply("NODECLEANUPLOST")
end
if redis.call("HGET", KEYS[8], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[8], "generation") ~= ARGV[6] then
    return redis.error_reply("STREAMDESTROYED")
end
local registration = redis.call("HGET", KEYS[2], ARGV[3])
if (registration and registration ~= ARGV[4])
or redis.call("HGET", KEYS[6], ARGV[3]) then
    return redis.error_reply("WORKERCLEANUPLOST")
end
-- Repeating this creation returns the saved heartbeat without replacing the
-- timestamp that the constructor is waiting to observe in its local map.
if registration then
    local heartbeat = redis.call("HGET", KEYS[4], ARGV[3])
    if not heartbeat or not tonumber(heartbeat) then
        return redis.error_reply("WORKERHEARTBEATINVALID")
    end
    return heartbeat
end
local clock = redis.call("TIME")
local timestamp = clock[1] .. string.format("%06d", clock[2]) .. "000"
local function save(content, channel, value)
    redis.call("HSET", content, ARGV[3], value)
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "set")
    local message = struct.pack(
        "ic0ic0ic0",
        string.len(ARGV[3]), ARGV[3],
        string.len(value), value,
        string.len(rev), rev
    )
    redis.call("PUBLISH", channel, "set:" .. message)
end
save(KEYS[4], KEYS[5], timestamp)
save(KEYS[2], KEYS[3], ARGV[4])
return timestamp
`)

	removeWorkerRegistrationScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
if redis.call("HGET", KEYS[6], "state") ~= "destroyed"
or redis.call("HGET", KEYS[6], "generation") ~= ARGV[5] then
    return redis.error_reply("STREAMDESTROYED")
end
local registration = redis.call("HGET", KEYS[2], ARGV[3])
if (registration and registration ~= ARGV[4])
or redis.call("HGET", KEYS[7], ARGV[3]) then
    return redis.error_reply("WORKERCLEANUPLOST")
end
local function remove(content, channel)
    if redis.call("HDEL", content, ARGV[3]) == 0 then
        return
    end
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "del")
    local message = struct.pack(
        "ic0ic0",
        string.len(ARGV[3]), ARGV[3],
        string.len(rev), rev
    )
    redis.call("PUBLISH", channel, "del:" .. message)
end
remove(KEYS[2], KEYS[3])
remove(KEYS[4], KEYS[5])
return 1
`)
)

// registerWorker saves a worker and its first Redis-time heartbeat while the
// pool, node, and original worker stream remain active. It waits for both local
// maps to observe the saved values before the constructor starts the worker.
// Repeated Redis commands reuse the saved heartbeat rather than replace the
// timestamp that an earlier successful response returned.
func (node *Node) registerWorker(ctx context.Context, workerID string, createdAt time.Time, stream *streaming.Stream) error {
	creation := strconv.FormatInt(createdAt.UnixNano(), 10)
	heartbeat, err := registerWorkerScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.workers),
			rmapUpdateChannel(node.resources.workers),
			rmapContentKey(node.resources.workerKeepAlive),
			rmapUpdateChannel(node.resources.workerKeepAlive),
			rmapContentKey(node.resources.workerCleanup),
			rmapContentKey(node.resources.nodeKeepAlive),
			fmt.Sprintf("pulse:stream:%s:lifecycle", stream.Name),
		},
		"active",
		node.resources.generation,
		workerID,
		creation,
		nodeCleanupField(node.ID),
		stream.Generation(),
	).Text()
	if err != nil {
		return poolBoundaryError(err)
	}
	if err := waitPoolMapValue(ctx, node.workerMap, workerID, creation); err != nil {
		return err
	}
	return waitPoolMapValue(ctx, node.workerKeepAliveMap, workerID, heartbeat)
}

// removeWorkerRegistration removes both saved entries for a failed constructor
// only after its original stream is destroyed. A changed pool or worker stream
// generation, or another cleanup owner's claim, is rejected without changing
// the records that owner needs to finish cleanup.
func (node *Node) removeWorkerRegistration(ctx context.Context, workerID string, createdAt time.Time, stream *streaming.Stream) error {
	return poolBoundaryError(removeWorkerRegistrationScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(node.resources.workers),
			rmapUpdateChannel(node.resources.workers),
			rmapContentKey(node.resources.workerKeepAlive),
			rmapUpdateChannel(node.resources.workerKeepAlive),
			fmt.Sprintf("pulse:stream:%s:lifecycle", stream.Name),
			rmapContentKey(node.resources.workerCleanup),
		},
		"active",
		node.resources.generation,
		workerID,
		strconv.FormatInt(createdAt.UnixNano(), 10),
		stream.Generation(),
	).Err())
}
