// Exact publication provides generation-scoped idempotent writes and
// side-effect-free snapshots. Redis Lua owns both linearization points so
// retries, Destroy, and concurrent clients observe one canonical result.
package streaming

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/streaming/options"
)

type (
	// SnapshotEvent is an immutable event returned by Stream.Snapshot.
	SnapshotEvent struct {
		id         string
		streamName string
		generation string
		name       string
		topic      string
		payload    []byte
	}
)

const (
	maxIdempotencyKeyBytes = 256
	maxAddOnceBodyBytes    = 1 << 20
)

var (
	// addOnceScript verifies the stream generation and finite retention,
	// resolves the generation-scoped idempotency record, and publishes exactly
	// once. Redis TIME is authoritative for absolute-deadline admission and for
	// aligning retry metadata with fixed or sliding stream TTLs.
	addOnceScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
local generation = redis.call("HGET", KEYS[1], "generation")
local physical = redis.call("HGET", KEYS[1], ARGV[5])
local deadline = redis.call("HGET", KEYS[1], ARGV[6])
local ttl_owned = redis.call("HGET", KEYS[1], ARGV[7])
local retention = redis.call("HGET", KEYS[1], ARGV[15])
local recreate = false
local adopting_legacy = not generation

if ARGV[2] ~= "" then
    if state ~= ARGV[1] or generation ~= ARGV[2] or physical ~= ARGV[3] then
        return redis.error_reply("STREAMDESTROYED")
    end
else
    if not generation then
        generation = "1"
        physical = ARGV[4]
        recreate = true
    elseif state ~= ARGV[1] then
        generation = tostring(tonumber(generation) + 1)
        physical = ARGV[4] .. ":generation:" .. generation
        deadline = false
        ttl_owned = false
        retention = false
        recreate = true
    elseif not physical then
        physical = ARGV[4]
    end
end

if retention and ARGV[18] == "1" and retention ~= ARGV[16] then
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
local effective_retention = retention or ARGV[16]
local retention_mode = string.match(effective_retention, "|mode=([^|]+)|")
local retention_value = tonumber(string.match(effective_retention, "|value=(%d+)|"))
local retention_sliding = string.match(effective_retention, "|sliding=([^|]+)$")
local ttl = 0
local ttl_sliding = false
if ttl_owned == "1" then
    if retention_mode ~= "ttl" or not retention_value or retention_value <= 0 then
        return redis.error_reply("STREAMCONFIGMISMATCH")
    end
    ttl = retention_value
    ttl_sliding = retention_sliding == "true"
elseif deadline then
    if ARGV[8] ~= "" and deadline ~= ARGV[8] then
        return redis.error_reply("STREAMDEADLINECONFLICT")
    end
elseif retention_mode == "ttl" and retention_value and retention_value > 0 then
    ttl = retention_value
    ttl_sliding = retention_sliding == "true"
    ttl_owned = "1"
else
    if ARGV[8] == "" then
        return redis.error_reply("STREAMDEADLINEREQUIRED")
    end
    deadline = ARGV[8]
end
local now = redis.call("TIME")
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if deadline and now_ms >= tonumber(deadline) then
    return redis.error_reply("DEADLINEELAPSED")
end
local expiry_deadline = deadline
if ttl > 0 then
    local remaining = redis.call("PTTL", physical)
    if ttl_sliding or remaining < 0 then
        expiry_deadline = now_ms + ttl
    else
        expiry_deadline = now_ms + remaining
    end
end

local dedupe = ARGV[4] .. ":generation:" .. generation .. ":idempotency"
local recovery = physical .. ":sink-recovery:" .. generation
local resources_key = ARGV[4] .. ":generation:" .. generation .. ":resources"
local physical_type = redis.call("TYPE", physical)["ok"]
local dedupe_type = redis.call("TYPE", dedupe)["ok"]
local recovery_type = redis.call("TYPE", recovery)["ok"]
local resources_type = redis.call("TYPE", resources_key)["ok"]
if (physical_type ~= "none" and physical_type ~= "stream")
or (dedupe_type ~= "none" and dedupe_type ~= "hash")
or (recovery_type ~= "none" and recovery_type ~= "hash")
or (resources_type ~= "none" and resources_type ~= "set") then
    return redis.error_reply("STREAMRESOURCEINVALID")
end

local existing = redis.call("HGET", dedupe, ARGV[10])
if existing then
    local separator = string.find(existing, "\0", 1, true)
    if not separator then
        return redis.error_reply("IDEMPOTENCYINVALID")
    end
    local event_id = string.sub(existing, 1, separator - 1)
    local existing_identity = string.sub(existing, separator + 1)
    if existing_identity ~= ARGV[17] then
        return redis.error_reply("IDEMPOTENCYCONFLICT")
    end
    redis.call("HSET", recovery, "=deadline", expiry_deadline)
    redis.call("PEXPIREAT", physical, expiry_deadline)
    redis.call("PEXPIREAT", dedupe, expiry_deadline)
    redis.call("PEXPIREAT", recovery, expiry_deadline)
    local existing_resources = redis.call("SMEMBERS", resources_key)
    for _, resource in ipairs(existing_resources) do
        redis.call("PEXPIREAT", resource, expiry_deadline)
    end
    redis.call("PEXPIREAT", resources_key, expiry_deadline)
    return {generation, physical, deadline or "", effective_retention, 0, event_id}
end

if recreate then
    if adopting_legacy and ARGV[18] == "1" and tonumber(ARGV[9]) > 0 then
        redis.call("XTRIM", physical, "MAXLEN", "=", ARGV[9])
    end
    redis.call("HSET", KEYS[1],
        "generation", generation,
        "state", ARGV[1],
        ARGV[5], physical,
        ARGV[15], ARGV[16])
    if deadline then
        redis.call("HSET", KEYS[1], ARGV[6], deadline)
        redis.call("HDEL", KEYS[1], ARGV[7])
    else
        redis.call("HDEL", KEYS[1], ARGV[6])
        redis.call("HSET", KEYS[1], ARGV[7], "1")
    end
elseif redis.call("HGET", KEYS[1], ARGV[5]) == false then
    redis.call("HSET", KEYS[1], ARGV[5], physical)
end
if deadline and redis.call("HGET", KEYS[1], ARGV[6]) == false then
    redis.call("HSET", KEYS[1], ARGV[6], deadline)
elseif ttl > 0 then
    redis.call("HSET", KEYS[1], ARGV[7], "1")
end
if not retention then
    redis.call("HSET", KEYS[1], ARGV[15], ARGV[16])
end

local event_id
if ARGV[9] == "0" and ARGV[13] == "1" then
    event_id = redis.call("XADD", physical, "*", "n", ARGV[11], "p", ARGV[12], "t", ARGV[14])
elseif ARGV[9] == "0" then
    event_id = redis.call("XADD", physical, "*", "n", ARGV[11], "p", ARGV[12])
elseif ARGV[13] == "1" then
    event_id = redis.call("XADD", physical, "MAXLEN", "=", ARGV[9], "*",
        "n", ARGV[11], "p", ARGV[12], "t", ARGV[14])
else
    event_id = redis.call("XADD", physical, "MAXLEN", "=", ARGV[9], "*",
        "n", ARGV[11], "p", ARGV[12])
end
redis.call("HSET", dedupe, ARGV[10], event_id .. "\0" .. ARGV[17])
redis.call("HSET", recovery, "=deadline", expiry_deadline)
redis.call("SADD", resources_key, dedupe, recovery)

redis.call("PEXPIREAT", physical, expiry_deadline)
redis.call("PEXPIREAT", dedupe, expiry_deadline)
redis.call("PEXPIREAT", recovery, expiry_deadline)
local resources = redis.call("SMEMBERS", resources_key)
for _, resource in ipairs(resources) do
    redis.call("PEXPIREAT", resource, expiry_deadline)
end
redis.call("PEXPIREAT", resources_key, expiry_deadline)
return {generation, physical, deadline or "", effective_retention, 1, event_id}
`)

	// snapshotScript binds an unbound handle using the same zero-migration
	// lifecycle contract, verifies a bound handle, and returns one XRANGE
	// result at the operation's single Redis linearization point.
	snapshotScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
local generation = redis.call("HGET", KEYS[1], "generation")
if not generation then
    return {0}
end
local physical = redis.call("HGET", KEYS[1], ARGV[4])
local deadline = redis.call("HGET", KEYS[1], ARGV[5])
if state ~= ARGV[1] or (ARGV[2] ~= "" and generation ~= ARGV[2])
or (ARGV[3] ~= "" and physical ~= ARGV[3]) then
    return redis.error_reply("STREAMDESTROYED")
end
local retention = redis.call("HGET", KEYS[1], ARGV[6])
if not retention then
    return redis.error_reply("STREAMCONFIGMISSING")
end
if ARGV[8] == "1" and retention ~= ARGV[7] then
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
if string.find(retention, "|max=0|", 1, true) then
    return redis.error_reply("SNAPSHOTUNBOUNDED")
end
local max_len = tonumber(string.match(retention, "|max=(%d+)|"))
if not max_len or max_len <= 0 then
    return redis.error_reply("STREAMCONFIGMISSING")
end

if deadline then
    local now = redis.call("TIME")
    local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
    if now_ms >= tonumber(deadline) then
        return redis.error_reply("DEADLINEELAPSED")
    end
end
local events = redis.call("XRANGE", physical, "-", "+", "COUNT", max_len + 1)
if #events > max_len then
    return redis.error_reply("SNAPSHOTBOUNDVIOLATION")
end
return {1, generation, physical, deadline or "", retention, events}
`)
)

// AddOnce publishes one event for idempotencyKey in this stream generation.
// The first call stores the event ID and exact length-delimited event identity
// until the generation expires. Exact retries return that ID; content changes
// return ErrIdempotencyConflict. The active generation must have an absolute
// deadline or a finite TTL, and explicit retention options must match its
// immutable retention contract.
func (s *Stream) AddOnce(
	ctx context.Context,
	idempotencyKey string,
	name string,
	payload []byte,
	opts ...options.AddEvent,
) (string, error) {
	if len(idempotencyKey) == 0 {
		return "", fmt.Errorf("pulse streaming: idempotency key must not be empty")
	}
	if len(idempotencyKey) > maxIdempotencyKeyBytes {
		return "", fmt.Errorf(
			"pulse streaming: idempotency key exceeds %d bytes",
			maxIdempotencyKeyBytes,
		)
	}
	if name == "" {
		return "", fmt.Errorf("pulse streaming: event name must not be empty")
	}
	o := options.ParseAddEventOptions(opts...)
	identity, err := canonicalEventIdentity(name, o.Topic, payload)
	if err != nil {
		return "", err
	}
	if o.OnlyIfStreamExists {
		return "", fmt.Errorf(
			"pulse streaming: AddOnce does not support WithOnlyIfStreamExists",
		)
	}
	s.generationLock.Lock()
	defer s.generationLock.Unlock()
	bound := s.generation != ""
	result, err := addOnceScript.Run(
		ctx,
		s.rdb,
		[]string{s.lifecycleKey},
		streamStateActive,
		s.generation,
		s.key,
		streamKey(s.Name),
		streamPhysicalKey,
		streamDeadlineKey,
		streamTTLOwnedKey,
		s.requestedDeadline(),
		strconv.Itoa(s.maxLen),
		idempotencyKey,
		name,
		payload,
		boolString(o.Topic != ""),
		o.Topic,
		streamConfigKey,
		s.retention,
		identity,
		boolString(s.retentionExplicit),
	).Slice()
	if err != nil {
		if redis.HasErrorPrefix(err, "IDEMPOTENCYCONFLICT") {
			return "", fmt.Errorf(
				"%w: key %q",
				ErrIdempotencyConflict,
				idempotencyKey,
			)
		}
		return "", s.lifecycleError(err)
	}
	if len(result) != 6 {
		return "", fmt.Errorf("add once script returned %d values", len(result))
	}
	generation, physical, canonicalDeadline, retention, err := parseLifecycleIdentity(result[:4])
	if err != nil {
		return "", fmt.Errorf("pulse streaming: AddOnce identity: %w", err)
	}
	parsedDeadline, err := parseDeadline(canonicalDeadline)
	if err != nil {
		return "", fmt.Errorf("pulse streaming: AddOnce identity: %w", err)
	}
	eventID, err := parseAddOnceResult(result[4:])
	if err != nil {
		return "", err
	}
	// A bound handle is an immutable capability for one exact generation: the
	// script verified the identity, so only the unbound→bound transition may
	// write binding state. Post-bind writes would race with the unlocked reads
	// every verified operation performs after its own lock passage.
	if !bound {
		if err := s.applyRetentionConfig(retention); err != nil {
			return "", fmt.Errorf("pulse streaming: AddOnce identity: %w", err)
		}
		s.generation = generation
		s.key = physical
		s.deadline = parsedDeadline
	}
	return eventID, nil
}

// Snapshot returns every event currently retained by this exact bounded
// stream generation in Redis ID order. The immutable MaxLen contract bounds
// Redis script work and result memory. Redis reads at most MaxLen+1 entries and
// returns ErrSnapshotBoundExceeded if physical data violates that invariant;
// unbounded streams return ErrSnapshotUnbounded before XRANGE. Snapshot creates no reader, consumer
// group, cursor, acknowledgement state, or lifecycle. An absent lifecycle
// returns ErrStreamNotFound. A malformed Redis entry fails the whole snapshot.
func (s *Stream) Snapshot(ctx context.Context) ([]SnapshotEvent, error) {
	s.generationLock.Lock()
	defer s.generationLock.Unlock()
	bound := s.generation != ""
	raw, err := snapshotScript.Run(
		ctx,
		s.rdb,
		[]string{s.lifecycleKey},
		streamStateActive,
		s.generation,
		s.key,
		streamPhysicalKey,
		streamDeadlineKey,
		streamConfigKey,
		s.retention,
		boolString(s.retentionExplicit),
	).Slice()
	if err != nil {
		return nil, s.lifecycleError(err)
	}
	if len(raw) == 1 {
		return nil, ErrStreamNotFound
	}
	if len(raw) != 6 {
		return nil, fmt.Errorf("snapshot script returned %d values", len(raw))
	}
	generation, physical, deadline, retention, err := parseLifecycleIdentity(raw[1:5])
	if err != nil {
		return nil, fmt.Errorf("pulse streaming: snapshot identity: %w", err)
	}
	canonicalDeadline, err := parseDeadline(deadline)
	if err != nil {
		return nil, fmt.Errorf("pulse streaming: snapshot identity: %w", err)
	}
	rangeResult, ok := raw[5].([]any)
	if !ok {
		return nil, fmt.Errorf("pulse streaming: snapshot range has invalid type %T", raw[5])
	}
	// Only the unbound→bound transition may write binding state; see AddOnce.
	if !bound {
		if err := s.applyRetentionConfig(retention); err != nil {
			return nil, fmt.Errorf("pulse streaming: snapshot identity: %w", err)
		}
		s.generation = generation
		s.key = physical
		s.deadline = canonicalDeadline
	}
	messages, err := decodeSnapshotRange(rangeResult)
	if err != nil {
		return nil, err
	}
	events := make([]SnapshotEvent, len(messages))
	for i, message := range messages {
		name, topic, payload, err := decodeRedisEvent(message)
		if err != nil {
			return nil, fmt.Errorf("pulse streaming: snapshot: %w", err)
		}
		events[i] = SnapshotEvent{
			id:         message.ID,
			streamName: s.Name,
			generation: s.generation,
			name:       name,
			topic:      topic,
			payload:    append([]byte(nil), payload...),
		}
	}
	return events, nil
}

// ID returns the immutable Redis event ID.
func (e SnapshotEvent) ID() string {
	return e.id
}

// StreamName returns the logical stream name.
func (e SnapshotEvent) StreamName() string {
	return e.streamName
}

// StreamGeneration returns the immutable stream incarnation.
func (e SnapshotEvent) StreamGeneration() string {
	return e.generation
}

// EventName returns the producer-defined event name.
func (e SnapshotEvent) EventName() string {
	return e.name
}

// Topic returns the producer-defined topic, or empty when absent.
func (e SnapshotEvent) Topic() string {
	return e.topic
}

// Payload returns an independent copy of the event payload.
func (e SnapshotEvent) Payload() []byte {
	return append([]byte(nil), e.payload...)
}

// parseAddOnceResult validates the Lua result at the Redis boundary.
func parseAddOnceResult(result []any) (string, error) {
	if len(result) != 2 {
		return "", fmt.Errorf("add once script returned %d values", len(result))
	}
	status, ok := result[0].(int64)
	if !ok || (status != 0 && status != 1) {
		return "", fmt.Errorf("add once script returned invalid status %T(%v)", result[0], result[0])
	}
	eventID, ok := result[1].(string)
	if !ok || eventID == "" {
		return "", fmt.Errorf("add once script returned invalid event ID %T", result[1])
	}
	return eventID, nil
}

// canonicalEventIdentity length-prefixes exact event bytes so no field
// concatenation can alias another identity.
func canonicalEventIdentity(name, topic string, payload []byte) ([]byte, error) {
	total := len(name) + len(topic) + len(payload)
	if total > maxAddOnceBodyBytes {
		return nil, fmt.Errorf(
			"pulse streaming: AddOnce event identity exceeds %d bytes",
			maxAddOnceBodyBytes,
		)
	}
	var identity bytes.Buffer
	for _, field := range [][]byte{[]byte(name), []byte(topic), payload} {
		if err := binary.Write(&identity, binary.BigEndian, uint64(len(field))); err != nil {
			return nil, fmt.Errorf("pulse streaming: encode AddOnce event identity: %w", err)
		}
		if _, err := identity.Write(field); err != nil {
			return nil, fmt.Errorf("pulse streaming: encode AddOnce event identity: %w", err)
		}
	}
	return identity.Bytes(), nil
}

// decodeSnapshotRange validates Redis's nested XRANGE response without
// returning a partial message set.
func decodeSnapshotRange(raw []any) ([]redis.XMessage, error) {
	messages := make([]redis.XMessage, len(raw))
	for i, value := range raw {
		entry, ok := value.([]any)
		if !ok || len(entry) != 2 {
			return nil, fmt.Errorf(
				"pulse streaming: snapshot entry %d must contain ID and fields, got %T",
				i,
				value,
			)
		}
		id, ok := entry[0].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("pulse streaming: snapshot entry %d has invalid ID %T", i, entry[0])
		}
		fields, ok := entry[1].([]any)
		if !ok || len(fields)%2 != 0 {
			return nil, fmt.Errorf("pulse streaming: snapshot event %s has invalid fields %T", id, entry[1])
		}
		values := make(map[string]any, len(fields)/2)
		for field := 0; field < len(fields); field += 2 {
			key, ok := fields[field].(string)
			if !ok {
				return nil, fmt.Errorf(
					"pulse streaming: snapshot event %s has invalid field name %T",
					id,
					fields[field],
				)
			}
			fieldValue, ok := fields[field+1].(string)
			if !ok {
				return nil, fmt.Errorf(
					"pulse streaming: snapshot event %s field %q has invalid value %T",
					id,
					key,
					fields[field+1],
				)
			}
			values[key] = fieldValue
		}
		messages[i] = redis.XMessage{ID: id, Values: values}
	}
	return messages, nil
}

// idempotencyKeyMap stores generation-scoped publication IDs and content
// digests independently from MAXLEN-trimmed event data.
func idempotencyKeyMap(stream *Stream) string {
	return fmt.Sprintf(
		"%s%s:generation:%s:idempotency",
		streamKeyPrefix,
		stream.Name,
		stream.generation,
	)
}
