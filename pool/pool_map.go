// Pool map mutations share the pool stream's Redis-owned generation fence.
// Worker and node operations use these recipes so cleanup and stale processes
// cannot interleave a lifecycle check with recreation of a deleted old map.
package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"goa.design/pulse/rmap"
)

// mutatePoolMapScript applies one rmap mutation only while the exact pool
// stream generation is active. It emits the canonical rmap wire update in the
// same Redis operation, so paused old-generation nodes cannot recreate deleted
// map keys after cleanup.
var mutatePoolMapScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1] or
   redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
if redis.call("HGET", KEYS[4], ARGV[6]) then
    return redis.error_reply("NODECLEANUPLOST")
end

local operation = ARGV[3]
local key = ARGV[4]
local value = ARGV[5]
local changed = false
local update = ""

if operation == "set" then
    redis.call("HSET", KEYS[2], key, value)
    changed = true
    update = value
elseif operation == "delete" then
    changed = redis.call("HDEL", KEYS[2], key) == 1
elseif operation == "append" then
    local values = {}
    local current = redis.call("HGET", KEYS[2], key)
    if current then
        local ok, decoded = pcall(cjson.decode, current)
        if not ok or type(decoded) ~= "table" then
            return redis.error_reply("POOLMAPINVALID")
        end
        values = decoded
    end
    for _, item in ipairs(values) do
        if item == value then
            return 0
        end
    end
    table.insert(values, value)
    update = cjson.encode(values)
    redis.call("HSET", KEYS[2], key, update)
    changed = true
elseif operation == "remove" then
    local current = redis.call("HGET", KEYS[2], key)
    if not current then
        return 0
    end
    local ok, values = pcall(cjson.decode, current)
    if not ok or type(values) ~= "table" then
        return redis.error_reply("POOLMAPINVALID")
    end
    local remaining = {}
    for _, item in ipairs(values) do
        if item ~= value then
            table.insert(remaining, item)
        else
            changed = true
        end
    end
    if not changed then
        return 0
    end
    if #remaining == 0 then
        redis.call("HDEL", KEYS[2], key)
    else
        update = cjson.encode(remaining)
        redis.call("HSET", KEYS[2], key, update)
    end
else
    return redis.error_reply("POOLMAPOPERATION")
end

if not changed then
    return 0
end
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
if operation == "delete" or (operation == "remove" and update == "") then
    redis.call("HSET", KEYS[2], "=kind", "del")
    local msg = struct.pack("ic0ic0", string.len(key), key, string.len(rev), rev)
    redis.call("PUBLISH", KEYS[3], "del:" .. msg)
else
    redis.call("HSET", KEYS[2], "=kind", "set")
    local msg = struct.pack(
        "ic0ic0ic0",
        string.len(key), key,
        string.len(update), update,
        string.len(rev), rev
    )
    redis.call("PUBLISH", KEYS[3], "set:" .. msg)
end
return 1
`)

// testAndSetPoolMapScript atomically advances a distributed ticker only while
// its exact pool generation remains active.
var testAndSetPoolMapScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1] or
   redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end
if redis.call("HGET", KEYS[4], ARGV[6]) then
    return redis.error_reply("NODECLEANUPLOST")
end
local current = redis.call("HGET", KEYS[2], ARGV[3])
if (ARGV[4] == "" and current) or
   (ARGV[4] ~= "" and current ~= ARGV[4]) then
    return current or ""
end
redis.call("HSET", KEYS[2], ARGV[3], ARGV[5])
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "set")
local msg = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(ARGV[5]), ARGV[5],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[3], "set:" .. msg)
return ARGV[4]
`)

// setPoolMap stores one value under the node's exact pool-generation fence.
func (node *Node) setPoolMap(ctx context.Context, name, key, value string) error {
	return node.mutatePoolMap(ctx, name, "set", key, value)
}

// setPoolMapAndWait stores one value and waits until the joined local replica
// has observed the Redis-published revision.
func (node *Node) setPoolMapAndWait(
	ctx context.Context,
	m *rmap.Map,
	name, key, value string,
) error {
	if err := node.setPoolMap(ctx, name, key, value); err != nil {
		return err
	}
	return waitPoolMapValue(ctx, m, key, value)
}

// waitPoolMapValue waits until the joined replica observes an already
// committed scripted map mutation.
func waitPoolMapValue(ctx context.Context, m *rmap.Map, key, value string) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if current, ok := m.Get(key); ok && current == value {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// deletePoolMap removes one value under the node's exact pool-generation fence.
func (node *Node) deletePoolMap(ctx context.Context, name, key string) error {
	return node.mutatePoolMap(ctx, name, "delete", key, "")
}

// testAndSetPoolMap replaces expected with value and returns the prior value
// under the exact generation fence.
func (node *Node) testAndSetPoolMap(
	ctx context.Context,
	name, key, expected, value string,
) (string, error) {
	previous, err := testAndSetPoolMapScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(name),
			rmapUpdateChannel(name),
			rmapContentKey(node.resources.nodeKeepAlive),
		},
		"active",
		node.resources.generation,
		key,
		expected,
		value,
		nodeCleanupField(node.ID),
	).Text()
	if err != nil {
		return "", poolBoundaryError(err)
	}
	return previous, nil
}

// appendPoolMapValue appends one unique array value under the exact generation
// fence used by worker ownership maps.
func (node *Node) appendPoolMapValue(ctx context.Context, name, key, value string) error {
	return node.mutatePoolMap(ctx, name, "append", key, value)
}

// removePoolMapValue removes one array value under the exact generation fence.
func (node *Node) removePoolMapValue(ctx context.Context, name, key, value string) error {
	return node.mutatePoolMap(ctx, name, "remove", key, value)
}

// mutatePoolMap verifies the active lifecycle record and publishes one
// generation-owned map mutation atomically.
func (node *Node) mutatePoolMap(ctx context.Context, name, operation, key, value string) error {
	err := mutatePoolMapScript.Run(
		ctx,
		node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", node.poolStream.Name),
			rmapContentKey(name),
			rmapUpdateChannel(name),
			rmapContentKey(node.resources.nodeKeepAlive),
		},
		"active",
		node.resources.generation,
		operation,
		key,
		value,
		nodeCleanupField(node.ID),
	).Err()
	if err == nil {
		return nil
	}
	boundaryErr := poolBoundaryError(err)
	if errors.Is(boundaryErr, ErrPoolGenerationLost) {
		if generationErr := node.ensureGenerationActive(ctx); generationErr != nil {
			return generationErr
		}
	}
	return boundaryErr
}
