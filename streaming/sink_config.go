// Package streaming persists the behavior and coordination state shared by
// same-name sink replicas. Every attached stream incarnation owns an
// independent configuration, keepalive map, and fenced stale-recovery lease.
package streaming

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/streaming/options"
)

type (
	// sinkConfiguration is the delivery contract shared by every replica
	// attached to one stream incarnation.
	sinkConfiguration struct {
		filterKind  string
		filterValue string
		startID     string
		noAck       bool
		ackGrace    time.Duration
	}

	// sinkRecoveryLease is the Redis-issued fencing capability for one stale
	// recovery pass.
	sinkRecoveryLease struct {
		owner string
		fence int64
	}
)

const (
	sinkLeaseUnavailable = int64(0)
	sinkLeaseAcquired    = int64(1)
)

var (
	// ensureSinkConfigurationScript validates the exact stream incarnation and
	// atomically establishes one immutable configuration for all replicas.
	ensureSinkConfigurationScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= ARGV[3] then
    return redis.error_reply("STREAMDESTROYED")
end
local deadline = redis.call("HGET", KEYS[1], ARGV[9])
if deadline then
    local clock = redis.call("TIME")
    local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
    if now >= tonumber(deadline) then
        return redis.error_reply("DEADLINEELAPSED")
    end
end

local existing = redis.call("HMGET", KEYS[2],
    "filter_kind", "filter_value", "start_id", "no_ack", "ack_grace_ms")
if existing[1] then
    if existing[1] ~= ARGV[4]
    or existing[2] ~= ARGV[5]
    or existing[3] ~= ARGV[6]
    or existing[4] ~= ARGV[7]
    or existing[5] ~= ARGV[8] then
        return redis.error_reply("SINKCONFIGMISMATCH")
    end
else
    redis.call("HSET", KEYS[2],
        "filter_kind", ARGV[4],
        "filter_value", ARGV[5],
        "start_id", ARGV[6],
        "no_ack", ARGV[7],
        "ack_grace_ms", ARGV[8])
end
redis.call("SADD", KEYS[3], KEYS[2], KEYS[4], KEYS[5])
if deadline then
    redis.call("PEXPIREAT", KEYS[2], deadline)
    redis.call("PEXPIREAT", KEYS[3], deadline)
    redis.call("PEXPIREAT", KEYS[4], deadline)
    redis.call("PEXPIREAT", KEYS[5], deadline)
end
return 1
`)

	// acquireSinkRecoveryLeaseScript acquires or renews one owner-token lease
	// using Redis TIME and returns the monotonically increasing fencing token.
	acquireSinkRecoveryLeaseScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= ARGV[3] then
    return redis.error_reply("STREAMDESTROYED")
end
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
local deadline = redis.call("HGET", KEYS[1], ARGV[6])
if deadline and now >= tonumber(deadline) then
    return redis.error_reply("DEADLINEELAPSED")
end
local owner = redis.call("HGET", KEYS[2], "owner")
local lease_until = tonumber(redis.call("HGET", KEYS[2], "lease_until") or "0")
local fence = tonumber(redis.call("HGET", KEYS[2], "fence") or "0")
if lease_until > now and owner ~= ARGV[4] then
    return {0, fence}
end
if owner ~= ARGV[4] or lease_until <= now then
    fence = fence + 1
end
redis.call("HSET", KEYS[2],
    "owner", ARGV[4],
    "fence", tostring(fence),
    "lease_until", tostring(now + tonumber(ARGV[5])))
if deadline then
    redis.call("PEXPIREAT", KEYS[2], deadline)
end
return {1, fence}
`)

	// renewSinkRecoveryLeaseScript supports explicit lease maintenance tests
	// and administration. Recovery mutations do not rely on separate renewal.
	renewSinkRecoveryLeaseScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= ARGV[3] then
    return redis.error_reply("STREAMDESTROYED")
end
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
local deadline = redis.call("HGET", KEYS[1], ARGV[7])
if deadline and now >= tonumber(deadline) then
    return redis.error_reply("DEADLINEELAPSED")
end
if redis.call("HGET", KEYS[2], "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], "fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], "lease_until") or "0") <= now then
    return redis.error_reply("SINKLEASELOST")
end
redis.call("HSET", KEYS[2], "lease_until", tostring(now + tonumber(ARGV[6])))
if deadline then
    redis.call("PEXPIREAT", KEYS[2], deadline)
end
return 1
`)

	// fencedAutoClaimScript verifies the exact live fencing capability and
	// performs XAUTOCLAIM in the same Redis operation.
	fencedAutoClaimScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= ARGV[3] then
    return redis.error_reply("STREAMDESTROYED")
end
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
local deadline = redis.call("HGET", KEYS[1], ARGV[7])
if deadline and now >= tonumber(deadline) then
    return redis.error_reply("DEADLINEELAPSED")
end
if redis.call("HGET", KEYS[2], "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], "fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], "lease_until") or "0") <= now then
    return redis.error_reply("SINKLEASELOST")
end
redis.call("HSET", KEYS[2], "lease_until", tostring(now + tonumber(ARGV[6])))
if deadline then
    redis.call("PEXPIREAT", KEYS[2], deadline)
end
return redis.call(
    "XAUTOCLAIM",
    KEYS[3],
    ARGV[8],
    ARGV[9],
    ARGV[10],
    ARGV[11],
    "COUNT",
    ARGV[12]
)
`)

	// fencedStaleConsumerCleanupScript performs stale-consumer inspection and
	// every resulting group and replicated-map mutation under one exact lease
	// check. A predecessor whose lease expired cannot mutate the PEL or maps.
	fencedStaleConsumerCleanupScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2]
or redis.call("HGET", KEYS[1], "physical_key") ~= ARGV[3] then
    return redis.error_reply("STREAMDESTROYED")
end
local clock = redis.call("TIME")
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
local deadline = redis.call("HGET", KEYS[1], ARGV[7])
if deadline and now >= tonumber(deadline) then
    return redis.error_reply("DEADLINEELAPSED")
end
if redis.call("HGET", KEYS[2], "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], "fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], "lease_until") or "0") <= now then
    return redis.error_reply("SINKLEASELOST")
end
redis.call("HSET", KEYS[2], "lease_until", tostring(now + tonumber(ARGV[6])))
if deadline then
    redis.call("PEXPIREAT", KEYS[2], deadline)
end

local function publish_delete(content, channel, key)
    if redis.call("HDEL", content, key) == 0 then
        return
    end
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "del")
    local message = struct.pack("ic0ic0", string.len(key), key, string.len(rev), rev)
    redis.call("PUBLISH", channel, "del:" .. message)
end

local function remove_value(content, channel, key, removed_value)
    local encoded = redis.call("HGET", content, key)
    if not encoded then
        return
    end
    local values = {}
    local decoded_ok, decoded = pcall(cjson.decode, encoded)
    if decoded_ok and type(decoded) == "table" then
        values = decoded
    else
        for value in string.gmatch(encoded, "[^,]+") do
            table.insert(values, value)
        end
    end
    local remaining = {}
    local removed = false
    for _, value in ipairs(values) do
        if value == removed_value then
            removed = true
        else
            table.insert(remaining, value)
        end
    end
    if not removed then
        return
    end
    if #remaining == 0 then
        publish_delete(content, channel, key)
        return
    end
    local replacement = cjson.encode(remaining)
    redis.call("HSET", content, key, replacement)
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "set")
    local message = struct.pack(
        "ic0ic0ic0",
        string.len(key), key,
        string.len(replacement), replacement,
        string.len(rev), rev
    )
    redis.call("PUBLISH", channel, "set:" .. message)
end

local removed = {}
local malformed = {}
local consumers = redis.call("XINFO", "CONSUMERS", KEYS[3], ARGV[8])
for _, consumer in ipairs(consumers) do
    local name
    local pending
    for index = 1, #consumer, 2 do
        if consumer[index] == "name" then
            name = consumer[index + 1]
        elseif consumer[index] == "pending" then
            pending = tonumber(consumer[index + 1])
        end
    end
    if name then
        local heartbeat = redis.call("HGET", KEYS[4], name)
        local stale = heartbeat == false
        if heartbeat then
            local heartbeat_ns = tonumber(heartbeat)
            if not heartbeat_ns then
                table.insert(malformed, name)
            else
                stale = now - math.floor(heartbeat_ns / 1000000) > tonumber(ARGV[9])
            end
        end
        if stale then
            if pending == 0 then
                redis.call("XGROUP", "DELCONSUMER", KEYS[3], ARGV[8], name)
            end
            publish_delete(KEYS[4], KEYS[5], name)
            remove_value(KEYS[6], KEYS[7], ARGV[8], name)
            table.insert(removed, name)
        end
    end
end
return {removed, malformed}
`)
)

// ensureSinkConfiguration establishes or verifies one attached stream's shared
// sink behavior and registers its generation-owned metadata for Destroy.
func ensureSinkConfiguration(
	ctx context.Context,
	stream *Stream,
	name string,
	config sinkConfiguration,
) error {
	err := ensureSinkConfigurationScript.Run(
		ctx,
		stream.rdb,
		[]string{
			stream.lifecycleKey,
			sinkConfigurationKey(stream, name),
			streamResourceRegistryKey(stream),
			rmapContentKey(sinkKeepAliveMapName(stream, name)),
			staleLockName(stream, name),
		},
		streamStateActive,
		stream.generation,
		stream.key,
		config.filterKind,
		config.filterValue,
		config.startID,
		boolString(config.noAck),
		strconv.FormatInt(config.ackGrace.Milliseconds(), 10),
		streamDeadlineKey,
	).Err()
	if err == nil {
		return nil
	}
	lifecycleErr := stream.lifecycleError(err)
	if errors.Is(lifecycleErr, ErrStreamDestroyed) ||
		errors.Is(lifecycleErr, ErrDeadlineElapsed) {
		return lifecycleErr
	}
	if strings.Contains(err.Error(), "SINKCONFIGMISMATCH") {
		return fmt.Errorf(
			"sink %q configuration differs from existing replicas for stream %q generation %s",
			name,
			stream.Name,
			stream.generation,
		)
	}
	return fmt.Errorf("establish sink %q configuration: %w", name, err)
}

// sinkConfigurationForOptions returns the canonical persisted contract for a
// sink attachment.
func sinkConfigurationForOptions(o options.SinkOptions, startID string) sinkConfiguration {
	filterKind, filterValue := sinkFilterConfiguration(o)
	return sinkConfiguration{
		filterKind:  filterKind,
		filterValue: filterValue,
		startID:     startID,
		noAck:       o.NoAck,
		ackGrace:    o.AckGracePeriod,
	}
}

// sinkFilterConfiguration returns the canonical persisted filter contract.
func sinkFilterConfiguration(o options.SinkOptions) (string, string) {
	switch {
	case o.Topic != "":
		return "topic", o.Topic
	case o.TopicPattern != "":
		return "pattern", o.TopicPattern
	default:
		return "all", ""
	}
}

// sinkNamespace is the logical identity shared only by replicas of one sink
// on one stream incarnation.
func sinkNamespace(stream *Stream, sink string) string {
	return fmt.Sprintf("stream:%s:generation:%s:sink:%s", stream.Name, stream.generation, sink)
}

// sinkConfigurationKey stores immutable same-name replica behavior.
func sinkConfigurationKey(stream *Stream, sink string) string {
	return "pulse:" + sinkNamespace(stream, sink) + ":config"
}

// sinkKeepAliveMapName identifies the generation-scoped consumer heartbeat map.
func sinkKeepAliveMapName(stream *Stream, sink string) string {
	return sinkNamespace(stream, sink) + ":keepalive"
}

// staleLockName identifies the generation-scoped stale-recovery lease.
func staleLockName(stream *Stream, sink string) string {
	return "pulse:" + sinkNamespace(stream, sink) + ":stalelease"
}

// acquireSinkRecoveryLease obtains the current fencing token for one attached
// stream. A false result means another replica still owns recovery.
func acquireSinkRecoveryLease(
	ctx context.Context,
	stream *Stream,
	key, owner string,
	duration time.Duration,
) (sinkRecoveryLease, bool, error) {
	raw, err := acquireSinkRecoveryLeaseScript.Run(
		ctx,
		stream.rdb,
		[]string{stream.lifecycleKey, key},
		streamStateActive,
		stream.generation,
		stream.key,
		owner,
		strconv.FormatInt(duration.Milliseconds(), 10),
		streamDeadlineKey,
	).Slice()
	if err != nil {
		lifecycleErr := stream.lifecycleError(err)
		if errors.Is(lifecycleErr, ErrStreamDestroyed) ||
			errors.Is(lifecycleErr, ErrDeadlineElapsed) {
			return sinkRecoveryLease{}, false, lifecycleErr
		}
		return sinkRecoveryLease{}, false, fmt.Errorf("acquire sink recovery lease: %w", err)
	}
	if len(raw) != 2 {
		return sinkRecoveryLease{}, false, fmt.Errorf("invalid sink recovery lease result length %d", len(raw))
	}
	status, ok := raw[0].(int64)
	if !ok {
		return sinkRecoveryLease{}, false, fmt.Errorf("invalid sink recovery lease status %T", raw[0])
	}
	fence, ok := raw[1].(int64)
	if !ok {
		return sinkRecoveryLease{}, false, fmt.Errorf("invalid sink recovery lease fence %T", raw[1])
	}
	switch status {
	case sinkLeaseUnavailable:
		return sinkRecoveryLease{}, false, nil
	case sinkLeaseAcquired:
		return sinkRecoveryLease{owner: owner, fence: fence}, true, nil
	default:
		return sinkRecoveryLease{}, false, fmt.Errorf("invalid sink recovery lease status %d", status)
	}
}

// renewSinkRecoveryLease extends one exact current capability without
// performing a recovery mutation.
func renewSinkRecoveryLease(
	ctx context.Context,
	stream *Stream,
	key string,
	lease sinkRecoveryLease,
	duration time.Duration,
) error {
	err := renewSinkRecoveryLeaseScript.Run(
		ctx,
		stream.rdb,
		[]string{stream.lifecycleKey, key},
		streamStateActive,
		stream.generation,
		stream.key,
		lease.owner,
		strconv.FormatInt(lease.fence, 10),
		strconv.FormatInt(duration.Milliseconds(), 10),
		streamDeadlineKey,
	).Err()
	lifecycleErr := stream.lifecycleError(err)
	if errors.Is(lifecycleErr, ErrStreamDestroyed) ||
		errors.Is(lifecycleErr, ErrDeadlineElapsed) {
		return lifecycleErr
	}
	if err != nil {
		return fmt.Errorf("renew sink recovery lease: %w", err)
	}
	return nil
}

// fencedAutoClaim claims one idle batch only while lease is the exact
// unexpired Redis-owned fencing capability.
func fencedAutoClaim(
	ctx context.Context,
	stream *Stream,
	key string,
	lease sinkRecoveryLease,
	duration time.Duration,
	group, consumer string,
	minIdle time.Duration,
	start string,
	count int64,
) (string, []redis.XMessage, error) {
	raw, err := fencedAutoClaimScript.Run(
		ctx,
		stream.rdb,
		[]string{stream.lifecycleKey, key, stream.key},
		streamStateActive,
		stream.generation,
		stream.key,
		lease.owner,
		strconv.FormatInt(lease.fence, 10),
		strconv.FormatInt(duration.Milliseconds(), 10),
		streamDeadlineKey,
		group,
		consumer,
		strconv.FormatInt(minIdle.Milliseconds(), 10),
		start,
		strconv.FormatInt(count, 10),
	).Slice()
	lifecycleErr := stream.lifecycleError(err)
	if errors.Is(lifecycleErr, ErrStreamDestroyed) ||
		errors.Is(lifecycleErr, ErrDeadlineElapsed) {
		return start, nil, lifecycleErr
	}
	if err != nil {
		return start, nil, fmt.Errorf("fenced sink recovery claim: %w", err)
	}
	if len(raw) < 2 || len(raw) > 3 {
		return start, nil, fmt.Errorf("fenced sink recovery claim returned %d values", len(raw))
	}
	next, ok := raw[0].(string)
	if !ok {
		return start, nil, fmt.Errorf("fenced sink recovery claim returned invalid cursor %T", raw[0])
	}
	entries, ok := raw[1].([]any)
	if !ok {
		return start, nil, fmt.Errorf("fenced sink recovery claim returned invalid messages %T", raw[1])
	}
	messages, err := decodeSnapshotRange(entries)
	if err != nil {
		return start, nil, fmt.Errorf("decode fenced sink recovery claim: %w", err)
	}
	return next, messages, nil
}

// cleanupStaleConsumers removes stale group and replicated-map state only
// while lease remains the exact unexpired fencing capability.
func cleanupStaleConsumers(
	ctx context.Context,
	stream *Stream,
	key string,
	lease sinkRecoveryLease,
	duration time.Duration,
	group string,
	grace time.Duration,
) ([]string, []string, error) {
	raw, err := fencedStaleConsumerCleanupScript.Run(
		ctx,
		stream.rdb,
		[]string{
			stream.lifecycleKey,
			key,
			stream.key,
			rmapContentKey(sinkKeepAliveMapName(stream, group)),
			rmapChannelKey(sinkKeepAliveMapName(stream, group)),
			consumersMapContentKey(stream),
			consumersMapChannelKey(stream),
		},
		streamStateActive,
		stream.generation,
		stream.key,
		lease.owner,
		strconv.FormatInt(lease.fence, 10),
		strconv.FormatInt(duration.Milliseconds(), 10),
		streamDeadlineKey,
		group,
		strconv.FormatInt((2*grace).Milliseconds(), 10),
	).Slice()
	lifecycleErr := stream.lifecycleError(err)
	if errors.Is(lifecycleErr, ErrStreamDestroyed) ||
		errors.Is(lifecycleErr, ErrDeadlineElapsed) {
		return nil, nil, lifecycleErr
	}
	if err != nil {
		return nil, nil, fmt.Errorf("fenced stale-consumer cleanup: %w", err)
	}
	if len(raw) != 2 {
		return nil, nil, fmt.Errorf("fenced stale-consumer cleanup returned %d values", len(raw))
	}
	removed, err := stringSlice(raw[0])
	if err != nil {
		return nil, nil, fmt.Errorf("decode removed stale consumers: %w", err)
	}
	malformed, err := stringSlice(raw[1])
	if err != nil {
		return nil, nil, fmt.Errorf("decode malformed stale consumers: %w", err)
	}
	return removed, malformed, nil
}

// stringSlice validates one Redis Lua array of strings.
func stringSlice(value any) ([]string, error) {
	raw, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", value)
	}
	result := make([]string, len(raw))
	for i, item := range raw {
		result[i], ok = item.(string)
		if !ok {
			return nil, fmt.Errorf("item %d has type %T", i, item)
		}
	}
	return result, nil
}

// streamResourceRegistryKey owns generation-scoped metadata deleted by
// explicit Stream.Destroy.
func streamResourceRegistryKey(stream *Stream) string {
	return fmt.Sprintf(
		"pulse:stream:%s:generation:%s:resources",
		stream.Name,
		stream.generation,
	)
}

// validateSinkOptions rejects values that would cause Redis retry churn,
// invalid channel allocation, or immediate stale-consumer recovery.
func validateSinkOptions(o options.SinkOptions) error {
	switch {
	case o.Topic != "" && o.TopicPattern != "":
		return fmt.Errorf("sink topic and topic pattern are mutually exclusive")
	case o.HasConflictingStartOptions():
		return fmt.Errorf("sink cursor-start options are mutually exclusive")
	case o.BlockDuration < time.Millisecond:
		return fmt.Errorf("sink block duration must be at least 1ms")
	case o.MaxPolled <= 0:
		return fmt.Errorf("sink maximum polled events must be greater than zero")
	case o.BufferSize < 0:
		return fmt.Errorf("sink buffer size must be greater than or equal to zero")
	case o.AckGracePeriod < time.Millisecond:
		return fmt.Errorf("sink acknowledgement grace period must be at least 1ms")
	default:
		return nil
	}
}

// validateReaderOptions rejects ambiguous selection and cursor contracts plus
// values that would cause Redis retry churn or invalid channel allocation.
func validateReaderOptions(o options.ReaderOptions) error {
	switch {
	case o.Topic != "" && o.TopicPattern != "":
		return fmt.Errorf("reader topic and topic pattern are mutually exclusive")
	case o.HasConflictingStartOptions():
		return fmt.Errorf("reader cursor-start options are mutually exclusive")
	case o.BlockDuration < time.Millisecond:
		return fmt.Errorf("reader block duration must be at least 1ms")
	case o.MaxPolled <= 0:
		return fmt.Errorf("reader maximum polled events must be greater than zero")
	case o.BufferSize < 0:
		return fmt.Errorf("reader buffer size must be greater than or equal to zero")
	default:
		return nil
	}
}

// validateAddStreamOptions rejects ambiguous per-stream cursor contracts.
func validateAddStreamOptions(o options.AddStreamOptions) error {
	if o.HasConflictingStartOptions() {
		return fmt.Errorf("added stream cursor-start options are mutually exclusive")
	}
	return nil
}

// rmapContentKey returns the Redis hash used by the named replicated map.
func rmapContentKey(name string) string {
	return fmt.Sprintf("map:%s:content", name)
}

// rmapChannelKey is the Redis channel carrying rmap update notifications for
// the named replicated map.
func rmapChannelKey(name string) string {
	return fmt.Sprintf("map:%s:updates", name)
}
