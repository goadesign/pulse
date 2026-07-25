// Sink consumer attachment and detachment are single Redis operations fenced
// on the exact stream generation, so a destroyed generation's membership and
// keep-alive projections can never be recreated after Stream.Destroy deleted
// them. Per-consumer pending state decides whether Redis metadata can be
// deleted, while membership and keep-alive projections always move coherently.
package streaming

import (
	"context"
	"fmt"
	"strconv"

	redis "github.com/redis/go-redis/v9"
)

// registerSinkConsumerScript atomically verifies the exact stream generation,
// creates the Redis consumer, appends it to the sink membership map, and
// writes its initial keep-alive, so no consumer state exists on a destroyed
// generation and membership can never diverge from the consumer group.
//
// KEYS: [1]=lifecycle [2]=stream [3]=membership content [4]=membership channel
//
//	[5]=keepalive content [6]=keepalive channel
//
// ARGV: [1]=active state [2]=generation [3]=group [4]=consumer [5]=keep-alive
var registerSinkConsumerScript = redis.NewScript(`
local lifecycle_type = redis.call("TYPE", KEYS[1]).ok
if lifecycle_type ~= "hash" then
    return redis.error_reply("SINKLIFECYCLEINVALID " .. lifecycle_type)
end
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end
local membership_type = redis.call("TYPE", KEYS[3]).ok
if membership_type ~= "hash" and membership_type ~= "none" then
    return redis.error_reply("SINKMEMBERSHIPINVALID " .. membership_type)
end
local keepalive_type = redis.call("TYPE", KEYS[5]).ok
if keepalive_type ~= "hash" and keepalive_type ~= "none" then
    return redis.error_reply("SINKKEEPALIVEINVALID " .. keepalive_type)
end
local created = redis.pcall("XGROUP", "CREATECONSUMER", KEYS[2], ARGV[3], ARGV[4])
if type(created) == "table" and created.err then
    return redis.error_reply(created.err)
end

local function publish_set(content, channel, field, value)
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "set")
    local message = struct.pack(
        "ic0ic0ic0",
        string.len(field), field,
        string.len(value), value,
        string.len(rev), rev
    )
    redis.call("PUBLISH", channel, "set:" .. message)
end

local encoded = redis.call("HGET", KEYS[3], ARGV[3])
local consumers = {}
if encoded then
    local ok, decoded = pcall(cjson.decode, encoded)
    if not ok or type(decoded) ~= "table" then
        return redis.error_reply("SINKMEMBERSHIPINVALID")
    end
    consumers = decoded
end
local member = false
for _, consumer in ipairs(consumers) do
    if consumer == ARGV[4] then
        member = true
        break
    end
end
if not member then
    table.insert(consumers, ARGV[4])
    local value = cjson.encode(consumers)
    redis.call("HSET", KEYS[3], ARGV[3], value)
    publish_set(KEYS[3], KEYS[4], ARGV[3], value)
end

redis.call("HSET", KEYS[5], ARGV[4], ARGV[5])
publish_set(KEYS[5], KEYS[6], ARGV[4], ARGV[5])
return 1
`)

// fencedKeepAliveSetScript refreshes one consumer keep-alive only while the
// exact stream generation is active, so the periodic keep-alive loop cannot
// recreate the keep-alive map after Stream.Destroy deleted it.
//
// KEYS: [1]=lifecycle [2]=stream [3]=keepalive content [4]=keepalive channel
// ARGV: [1]=active state [2]=generation [3]=consumer [4]=keep-alive
var fencedKeepAliveSetScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end
local keepalive_type = redis.call("TYPE", KEYS[3]).ok
if keepalive_type ~= "hash" and keepalive_type ~= "none" then
    return redis.error_reply("SINKKEEPALIVEINVALID " .. keepalive_type)
end
redis.call("HSET", KEYS[3], ARGV[3], ARGV[4])
local rev = tostring(redis.call("HINCRBY", KEYS[3], "=rev", 1))
redis.call("HSET", KEYS[3], "=kind", "set")
local message = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(ARGV[4]), ARGV[4],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[4], "set:" .. message)
return 1
`)

var detachSinkConsumerScript = redis.NewScript(`
local lifecycle_type = redis.call("TYPE", KEYS[1]).ok
if lifecycle_type ~= "hash" then
    return redis.error_reply("SINKLIFECYCLEINVALID " .. lifecycle_type)
end
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end
local stream_type = redis.call("TYPE", KEYS[2]).ok
if stream_type ~= "stream" and stream_type ~= "none" then
    return redis.error_reply("SINKSTREAMINVALID " .. stream_type)
end
local membership_type = redis.call("TYPE", KEYS[3]).ok
if membership_type ~= "hash" and membership_type ~= "none" then
    return redis.error_reply("SINKMEMBERSHIPINVALID " .. membership_type)
end
local keepalive_type = redis.call("TYPE", KEYS[5]).ok
if keepalive_type ~= "hash" and keepalive_type ~= "none" then
    return redis.error_reply("SINKKEEPALIVEINVALID " .. keepalive_type)
end

local pending = {}
local pending_result = redis.pcall(
    "XPENDING", KEYS[2], ARGV[3], "-", "+", 1, ARGV[4]
)
if type(pending_result) == "table" and pending_result.err then
    if not string.find(pending_result.err, "NOGROUP", 1, true) then
        return redis.error_reply(pending_result.err)
    end
elseif type(pending_result) == "table" then
    pending = pending_result
end
if #pending == 0 then
    local groups = redis.pcall("XGROUP", "DELCONSUMER", KEYS[2], ARGV[3], ARGV[4])
    if type(groups) == "table" and groups.err
    and not string.find(groups.err, "NOGROUP", 1, true) then
        return redis.error_reply(groups.err)
    end
end

local function publish_delete(content, channel, field)
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "del")
    local message = struct.pack(
        "ic0ic0",
        string.len(field), field,
        string.len(rev), rev
    )
    redis.call("PUBLISH", channel, "del:" .. message)
end

local function publish_set(content, channel, field, value)
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "set")
    local message = struct.pack(
        "ic0ic0ic0",
        string.len(field), field,
        string.len(value), value,
        string.len(rev), rev
    )
    redis.call("PUBLISH", channel, "set:" .. message)
end

local encoded = redis.call("HGET", KEYS[3], ARGV[3])
if encoded then
    local ok, consumers = pcall(cjson.decode, encoded)
    if not ok or type(consumers) ~= "table" then
        return redis.error_reply("SINKMEMBERSHIPINVALID")
    end
    local remaining = {}
    for _, consumer in ipairs(consumers) do
        if consumer ~= ARGV[4] then
            table.insert(remaining, consumer)
        end
    end
    if #remaining == 0 then
        redis.call("HDEL", KEYS[3], ARGV[3])
        publish_delete(KEYS[3], KEYS[4], ARGV[3])
    elseif #remaining ~= #consumers then
        local value = cjson.encode(remaining)
        redis.call("HSET", KEYS[3], ARGV[3], value)
        publish_set(KEYS[3], KEYS[4], ARGV[3], value)
    end
end

if redis.call("HDEL", KEYS[5], ARGV[4]) == 1 then
    publish_delete(KEYS[5], KEYS[6], ARGV[4])
end
return #pending
`)

// detachSinkConsumer removes one consumer's distributed metadata. Redis
// deletes the consumer itself only when its own PEL is empty; pending entries
// remain claimable by stale-consumer recovery.
func detachSinkConsumer(
	ctx context.Context,
	state *sinkStream,
	sinkName, consumer string,
) (bool, error) {
	pending, err := detachSinkConsumerScript.Run(
		ctx,
		state.stream.rdb,
		[]string{
			state.stream.lifecycleKey,
			state.stream.key,
			consumersMapContentKey(state.stream),
			consumersMapChannelKey(state.stream),
			rmapContentKey(sinkKeepAliveMapName(state.stream, sinkName)),
			rmapChannelKey(sinkKeepAliveMapName(state.stream, sinkName)),
		},
		streamStateActive,
		state.stream.generation,
		sinkName,
		consumer,
	).Int64()
	if err != nil {
		return false, streamLifecycleBoundaryError(err)
	}
	if pending < 0 || pending > 1 {
		return false, fmt.Errorf("detach sink consumer returned invalid pending count %d", pending)
	}
	return pending == 0, nil
}

// registerSinkConsumer establishes one consumer's complete distributed
// metadata in a single generation-fenced operation. On error nothing was
// written, so callers need no Redis rollback for this consumer.
func registerSinkConsumer(
	ctx context.Context,
	state *sinkStream,
	sinkName, consumer string,
	keepAlive int64,
) error {
	err := registerSinkConsumerScript.Run(
		ctx,
		state.stream.rdb,
		[]string{
			state.stream.lifecycleKey,
			state.stream.key,
			consumersMapContentKey(state.stream),
			consumersMapChannelKey(state.stream),
			rmapContentKey(sinkKeepAliveMapName(state.stream, sinkName)),
			rmapChannelKey(sinkKeepAliveMapName(state.stream, sinkName)),
		},
		streamStateActive,
		state.stream.generation,
		sinkName,
		consumer,
		strconv.FormatInt(keepAlive, 10),
	).Err()
	if err != nil {
		return streamLifecycleBoundaryError(fmt.Errorf(
			"cannot register consumer %s for sink %s on stream %s: %w",
			consumer,
			sinkName,
			state.stream.Name,
			err,
		))
	}
	return nil
}

// setSinkKeepAlive refreshes one consumer keep-alive behind the generation
// fence. A destroyed generation returns ErrStreamDestroyed without recreating
// any keep-alive state.
func setSinkKeepAlive(
	ctx context.Context,
	state *sinkStream,
	sinkName, consumer string,
	keepAlive int64,
) error {
	err := fencedKeepAliveSetScript.Run(
		ctx,
		state.stream.rdb,
		[]string{
			state.stream.lifecycleKey,
			state.stream.key,
			rmapContentKey(sinkKeepAliveMapName(state.stream, sinkName)),
			rmapChannelKey(sinkKeepAliveMapName(state.stream, sinkName)),
		},
		streamStateActive,
		state.stream.generation,
		consumer,
		strconv.FormatInt(keepAlive, 10),
	).Err()
	if err != nil {
		return streamLifecycleBoundaryError(fmt.Errorf(
			"cannot refresh keep-alive for consumer %s of sink %s on stream %s: %w",
			consumer,
			sinkName,
			state.stream.Name,
			err,
		))
	}
	return nil
}
