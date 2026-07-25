// Stale-node cleanup uses the node keep-alive map as one Redis-owned lease
// record. Heartbeat expiry, cleanup fencing, stream destruction, and discovery
// removal are decided by Redis TIME and exact owner tokens.
package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/streaming"
)

type (
	// nodeCleanupLease is the exact stale-node cleanup capability.
	nodeCleanupLease struct {
		nodeID string
		owner  string
		fence  string
	}
)

var (
	// acquireNodeCleanupScript atomically verifies heartbeat expiry and installs
	// one renewable cleanup fence using Redis TIME.
	acquireNodeCleanupScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local now_ns = tonumber(clock[1]) * 1000000000 + tonumber(clock[2]) * 1000
local heartbeat = redis.call("HGET", KEYS[2], ARGV[3])
if heartbeat then
    local heartbeat_ns = tonumber(heartbeat)
    if not heartbeat_ns then
        return redis.error_reply("NODEHEARTBEATINVALID")
    end
    if heartbeat_ns + tonumber(ARGV[6]) >= now_ns then
        return {0, "live"}
    end
end
local current = redis.call("HGET", KEYS[2], ARGV[7])
if current then
    if string.sub(current, 1, 5) == "dead|" then
        return {0, "dead"}
    end
    local current_owner, current_fence, current_until =
        string.match(current, "^([^|]+)|([^|]+)|(%d+)$")
    if not current_owner then
        return redis.error_reply("NODECLEANUPINVALID")
    end
    if current_owner ~= ARGV[4] and tonumber(current_until) > now then
        return {0, current_fence}
    end
    if current_owner == ARGV[4] and tonumber(current_until) > now then
        redis.call("HSET", KEYS[2], ARGV[7],
            current_owner .. "|" .. current_fence .. "|" .. tostring(now + tonumber(ARGV[5])))
        return {1, current_fence}
    end
end
local fence = tostring(redis.call("HINCRBY", KEYS[2], ARGV[8], 1))
redis.call("HSET", KEYS[2], ARGV[7],
    ARGV[4] .. "|" .. fence .. "|" .. tostring(now + tonumber(ARGV[5])))
return {1, fence}
`)

	// destroyStaleNodeScript destroys one exact node stream and removes its
	// heartbeat only while the supplied cleanup owner remains current.
	destroyStaleNodeScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local current = redis.call("HGET", KEYS[2], ARGV[7])
local owner, fence, lease_until = string.match(current or "", "^([^|]+)|([^|]+)|(%d+)$")
if owner ~= ARGV[4] or fence ~= ARGV[5] or tonumber(lease_until or "0") <= now then
    return redis.error_reply("NODECLEANUPLOST")
end
redis.call("HSET", KEYS[2], ARGV[7],
    owner .. "|" .. fence .. "|" .. tostring(now + tonumber(ARGV[6])))

if redis.call("HGET", KEYS[4], "generation") == ARGV[9] then
    local state = redis.call("HGET", KEYS[4], "state")
    if state == "active" then
        local physical = redis.call("HGET", KEYS[4], "physical_key")
        if not physical then
            return redis.error_reply("STREAMDESTROYED")
        end
        redis.call("HSET", KEYS[4], "state", "destroyed")
        local resources = redis.call("SMEMBERS", KEYS[5])
        if #resources > 0 then
            redis.call("DEL", unpack(resources))
        end
        redis.call("DEL", physical, physical .. ":sink-recovery:" .. ARGV[9], KEYS[5])
    elseif state ~= "destroyed" then
        return redis.error_reply("STREAMDESTROYED")
    end
end

redis.call("HDEL", KEYS[2], ARGV[3])
redis.call("HSET", KEYS[2], ARGV[7], "dead|" .. ARGV[5] .. "|0")
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "del")
local message = struct.pack(
    "ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[3], "del:" .. message)
redis.call("HDEL", KEYS[6], "error:" .. ARGV[3])
return 1
`)
)

// acquireNodeCleanup returns an exact cleanup lease only when Redis proves the
// node heartbeat expired by the generation's immutable WorkerTTL.
func acquireNodeCleanup(
	ctx context.Context,
	rdb *redis.Client,
	resources poolResources,
	nodeID, owner string,
) (*nodeCleanupLease, error) {
	raw, err := acquireNodeCleanupScript.Run(
		ctx,
		rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", poolStreamName(resources.pool)),
			rmapContentKey(resources.nodeKeepAlive),
		},
		"active",
		resources.generation,
		nodeID,
		owner,
		strconv.FormatInt(resources.cleanupLease.Milliseconds(), 10),
		strconv.FormatInt(resources.workerTTL.Nanoseconds(), 10),
		nodeCleanupField(nodeID),
		nodeCleanupFenceField(nodeID),
	).Slice()
	if err != nil {
		return nil, poolBoundaryError(err)
	}
	if len(raw) != 2 {
		return nil, fmt.Errorf("node cleanup claim returned %d fields", len(raw))
	}
	status, ok := raw[0].(int64)
	if !ok || (status != 0 && status != 1) {
		return nil, fmt.Errorf("node cleanup claim returned invalid status %T(%v)", raw[0], raw[0])
	}
	if status == 0 {
		return nil, nil
	}
	fence, ok := raw[1].(string)
	if !ok || fence == "" {
		return nil, fmt.Errorf("node cleanup claim returned invalid fence %T", raw[1])
	}
	return &nodeCleanupLease{nodeID: nodeID, owner: owner, fence: fence}, nil
}

// cleanupStalePoolNode atomically fences a stale node before destroying its
// exact stream incarnation and removing discovery.
func cleanupStalePoolNode(
	ctx context.Context,
	rdb *redis.Client,
	resources poolResources,
	nodeID, owner string,
) (bool, error) {
	lease, err := acquireNodeCleanup(ctx, rdb, resources, nodeID, owner)
	if err != nil || lease == nil {
		return false, err
	}
	stream, err := streaming.NewStream(
		nodeStreamName(resources.pool, nodeID),
		rdb,
	)
	if err != nil {
		return false, err
	}
	if err := stream.Open(ctx); err != nil && !errors.Is(err, streaming.ErrStreamNotFound) {
		return false, err
	}
	generation := stream.Generation()
	err = destroyStaleNodeScript.Run(
		ctx,
		rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", poolStreamName(resources.pool)),
			rmapContentKey(resources.nodeKeepAlive),
			rmapUpdateChannel(resources.nodeKeepAlive),
			fmt.Sprintf("pulse:stream:%s:lifecycle", stream.Name),
			fmt.Sprintf("pulse:stream:%s:generation:%s:resources", stream.Name, generation),
			rmapContentKey(resources.nodeShutdown),
		},
		"active",
		resources.generation,
		nodeID,
		lease.owner,
		lease.fence,
		strconv.FormatInt(resources.cleanupLease.Milliseconds(), 10),
		nodeCleanupField(nodeID),
		nodeCleanupFenceField(nodeID),
		generation,
	).Err()
	if err != nil {
		return false, poolBoundaryError(err)
	}
	return true, nil
}

// newNodeCleanupOwner returns a process-unique cleanup owner token.
func newNodeCleanupOwner(prefix string) string {
	return prefix + "-" + ulid.Make().String()
}

// nodeCleanupField stores the terminal cleanup fence beside the heartbeat.
func nodeCleanupField(nodeID string) string {
	return "=node-cleanup:" + hex.EncodeToString([]byte(nodeID))
}

// nodeCleanupFenceField stores the monotonic ABA counter for one node ID.
func nodeCleanupFenceField(nodeID string) string {
	return "=node-cleanup-fence:" + hex.EncodeToString([]byte(nodeID))
}
