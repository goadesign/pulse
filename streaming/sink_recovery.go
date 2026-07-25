// Package streaming persists each sink cursor in generation-qualified metadata
// beside its lifecycle-selected physical stream key. Recovery and
// acknowledgement verify the Redis lifecycle record in the same Lua operation,
// so stale handles cannot recreate groups or mutate a later incarnation.
package streaming

import (
	"context"
	"fmt"
	"strconv"

	redis "github.com/redis/go-redis/v9"
)

type (
	// recoveryAcker implements Event.Acker for one immutable stream generation.
	recoveryAcker struct {
		stream *Stream
	}
)

var (
	// ensureConsumerGroupScript creates or repairs a group, initializes its
	// durable cursor, and aligns recovery retention atomically after verifying
	// the exact stream generation.
	ensureConsumerGroupScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], ARGV[7]) ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end
local deadline = redis.call("HGET", KEYS[1], ARGV[8])
if deadline then
    local now = redis.call("TIME")
    local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
    if now_ms >= tonumber(deadline) then
        return redis.error_reply("DEADLINEELAPSED")
    end
end

local absent = redis.call("EXISTS", KEYS[2]) == 0
local start = redis.call("HGET", KEYS[3], ARGV[3])
if not start then
    start = ARGV[4]
end
local created = redis.pcall("XGROUP", "CREATE", KEYS[2], ARGV[3], start, "MKSTREAM")
if type(created) == "table" and created.err
and not string.find(created.err, "BUSYGROUP", 1, true) then
    return redis.error_reply(created.err)
end

if redis.call("HEXISTS", KEYS[3], ARGV[3]) == 0 then
    local pending = redis.call("XPENDING", KEYS[2], ARGV[3])
    local cursor
    if pending[1] == 0 then
        local groups = redis.call("XINFO", "GROUPS", KEYS[2])
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
            if name == ARGV[3] then
                cursor = delivered
                break
            end
        end
        if not cursor then
            return redis.error_reply("NOGROUP consumer group no longer exists")
        end
    else
        local previous = redis.call("XREVRANGE", KEYS[2], "(" .. pending[2], "-", "COUNT", 1)
        if #previous == 0 then
            cursor = "0-0"
        else
            cursor = previous[1][1]
        end
    end
    redis.call("HSET", KEYS[3], ARGV[3], cursor)
end

local ttl = tonumber(ARGV[5])
if deadline then
    redis.call("PEXPIREAT", KEYS[2], deadline)
    redis.call("PEXPIREAT", KEYS[3], deadline)
elseif ttl > 0 and (ARGV[6] == "1" or redis.call("PTTL", KEYS[2]) == -1) then
    redis.call("PEXPIRE", KEYS[2], ttl)
end
if absent then
    return 1
end
return 0
`)

	// recoveryCursorScript acknowledges one or more IDs and advances the
	// canonical cursor only across the prefix with no pending entry.
	recoveryCursorScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], ARGV[4]) ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end
local deadline = redis.call("HGET", KEYS[1], ARGV[5])
if deadline then
    local now = redis.call("TIME")
    local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
    if now_ms >= tonumber(deadline) then
        return redis.error_reply("DEADLINEELAPSED")
    end
end

local result = redis.call("XACK", KEYS[2], ARGV[3], unpack(ARGV, 6))
if result == 0 then
    return 0
end

local pending = redis.call("XPENDING", KEYS[2], ARGV[3])
local cursor
if pending[1] == 0 then
    local groups = redis.call("XINFO", "GROUPS", KEYS[2])
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
        if name == ARGV[3] then
            cursor = delivered
            break
        end
    end
    if not cursor then
        return redis.error_reply("NOGROUP consumer group no longer exists")
    end
else
    local previous = redis.call("XREVRANGE", KEYS[2], "(" .. pending[2], "-", "COUNT", 1)
    if #previous == 0 then
        cursor = "0-0"
    else
        cursor = previous[1][1]
    end
end
redis.call("HSET", KEYS[3], ARGV[3], cursor)
if deadline then
    redis.call("PEXPIREAT", KEYS[3], deadline)
end
return result
`)

	// destroyStreamScript invalidates and deletes exactly one generation. The
	// lifecycle record remains as the monotonic source used by the next
	// explicit NewStream call. The generation-qualified membership map is never
	// reused, so its destroy notification needs no persistent rmap tombstone.
	destroyStreamScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("STREAMDESTROYED")
end
local state = redis.call("HGET", KEYS[1], "state")
if state == ARGV[3] then
    return 0
end
if state ~= ARGV[1] then
    return redis.error_reply("STREAMDESTROYED")
end
if redis.call("HGET", KEYS[1], ARGV[4]) ~= KEYS[2] then
    return redis.error_reply("STREAMDESTROYED")
end

redis.call("HSET", KEYS[1], "state", ARGV[3])
local rev = redis.call("HINCRBY", KEYS[4], "=rev", 1)
local resources = redis.call("SMEMBERS", KEYS[6])
if #resources > 0 then
    redis.call("DEL", unpack(resources))
end
redis.call("DEL", KEYS[2], KEYS[3], KEYS[4])
redis.call("DEL", KEYS[6])
redis.call("PUBLISH", KEYS[5], "destroy:" .. tostring(rev))
return 1
`)
)

// ensureConsumerGroup creates or repairs a group for this exact generation.
// The boolean result reports whether its physical stream data was absent.
func ensureConsumerGroup(ctx context.Context, stream *Stream, group, configuredStart string) (bool, error) {
	if err := stream.ensureGeneration(ctx); err != nil {
		return false, err
	}
	absent, err := runEnsureConsumerGroup(ctx, stream, group, configuredStart)
	if err != nil {
		lifecycleErr := stream.lifecycleError(err)
		if !stream.reestablishLostGenesis(ctx, lifecycleErr) {
			return false, ensureConsumerGroupError(stream, group, lifecycleErr)
		}
		if absent, err = runEnsureConsumerGroup(ctx, stream, group, configuredStart); err != nil {
			return false, ensureConsumerGroupError(stream, group, stream.lifecycleError(err))
		}
	}
	return absent == 1, nil
}

// runEnsureConsumerGroup executes the fenced group recovery script once.
func runEnsureConsumerGroup(ctx context.Context, stream *Stream, group, configuredStart string) (int64, error) {
	return ensureConsumerGroupScript.Run(
		ctx,
		stream.rdb,
		[]string{stream.lifecycleKey, stream.key, recoveryCursorKey(stream)},
		streamStateActive,
		stream.generation,
		group,
		configuredStart,
		strconv.FormatInt(stream.ttl.Milliseconds(), 10),
		boolString(stream.ttlSliding),
		streamPhysicalKey,
		streamDeadlineKey,
	).Int64()
}

// ensureConsumerGroupError wraps group recovery failures with their identity.
func ensureConsumerGroupError(stream *Stream, group string, err error) error {
	return fmt.Errorf(
		"failed to ensure Redis consumer group %q for stream %q generation %s: %w",
		group,
		stream.Name,
		stream.generation,
		err,
	)
}

// XAck atomically acknowledges IDs and advances this generation's shared
// recovery cursor. The streamKey argument remains part of the public Acker
// contract; generated events bind the implementation to their own stream.
func (a *recoveryAcker) XAck(ctx context.Context, _ string, group string, ids ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	args := make([]any, 0, len(ids)+5)
	args = append(
		args,
		streamStateActive,
		a.stream.generation,
		group,
		streamPhysicalKey,
		streamDeadlineKey,
	)
	for _, id := range ids {
		args = append(args, id)
	}
	result, err := recoveryCursorScript.Run(
		ctx,
		a.stream.rdb,
		[]string{a.stream.lifecycleKey, a.stream.key, recoveryCursorKey(a.stream)},
		args...,
	).Int64()
	if err != nil {
		cmd.SetErr(fmt.Errorf(
			"failed to acknowledge events %q and advance consumer group %q on stream %q generation %s: %w",
			ids,
			group,
			a.stream.Name,
			a.stream.generation,
			a.stream.lifecycleError(err),
		))
		return cmd
	}
	cmd.SetVal(result)
	return cmd
}

// destroyStream atomically invalidates and deletes this exact generation.
func destroyStream(ctx context.Context, stream *Stream) error {
	if err := stream.loadExistingGeneration(ctx); err != nil {
		return err
	}
	err := destroyStreamScript.Run(
		ctx,
		stream.rdb,
		[]string{
			stream.lifecycleKey,
			stream.key,
			recoveryCursorKey(stream),
			consumersMapContentKey(stream),
			consumersMapChannelKey(stream),
			streamResourceRegistryKey(stream),
		},
		streamStateActive,
		stream.generation,
		streamStateDestroyed,
		streamPhysicalKey,
	).Err()
	if err != nil {
		return fmt.Errorf(
			"failed to destroy stream %q generation %s: %w",
			stream.Name,
			stream.generation,
			stream.lifecycleError(err),
		)
	}
	return nil
}

// recoveryCursorKey identifies all named-sink cursors for one generation while
// the physical event key remains stable.
func recoveryCursorKey(stream *Stream) string {
	return fmt.Sprintf("%s:sink-recovery:%s", stream.key, stream.generation)
}

// consumersMapContentKey identifies this generation's membership hash.
func consumersMapContentKey(stream *Stream) string {
	return fmt.Sprintf("map:%s:content", consumersMapName(stream))
}

// consumersMapChannelKey identifies this generation's membership updates.
func consumersMapChannelKey(stream *Stream) string {
	return rmapChannelKey(consumersMapName(stream))
}
