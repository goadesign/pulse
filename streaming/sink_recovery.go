// This file implements lossless consumer-group recovery and the stream
// lifecycle fence shared by sinks and Stream.Destroy.
//
// Every piece of durable sink metadata attached to a stream lives under the
// "pulse:streammeta:<stream>" prefix:
//
//   - <prefix>:lifecycle  hash with a "state" field ("active" or "destroyed").
//   - <prefix>:cursors    hash mapping sink name to the recovery cursor, the
//     highest event ID known to be fully acknowledged for that sink.
//   - <prefix>:sink:<sink>:lease  fenced lease used by sink_lease.go.
//
// The lifecycle hash is the destroy fence: every Lua script in this file and
// in sink_lease.go that mutates stream-scoped metadata reads the lifecycle
// state first, inside the same atomic script invocation as the write. Once
// Stream.Destroy marks a stream destroyed no concurrent sink loop can
// recreate its consumer group, membership map, cursor, or lease; only a
// deliberate NewSink/AddStream call (establish mode) reactivates the name.
//
// The recovery cursor is what makes NOGROUP recovery lossless: when a
// consumer group disappears (XGROUP DESTROY, Redis state loss) the group is
// recreated at the stored cursor rather than "$" so unacknowledged events are
// redelivered. The cursor is advanced by recomputing it from the exact PEL
// state each time events are acknowledged.
//
// Scripts publish replicated-map updates using the exact rmap wire protocol
// (see rmap/scripts.go) so live rmap clients observe fenced membership
// mutations exactly as if they had been made through the rmap API.
package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"

	redis "github.com/redis/go-redis/v9"
)

type (
	// recoveryAcker acknowledges sink events and atomically advances the
	// durable recovery cursor from the exact PEL state. It is the Acker
	// carried by every event delivered through a sink, so both Sink.Ack and
	// direct Event.Acker.XAck calls keep the recovery cursor current.
	recoveryAcker struct {
		rdb *redis.Client
	}
)

var (
	// ErrStreamDestroyed is returned when a sink operation targets a stream
	// that was destroyed with Stream.Destroy. The sink drops the stream from
	// its set instead of resurrecting its metadata.
	ErrStreamDestroyed = errors.New("stream is destroyed")

	// ErrSinkClosed is returned by AddStream and RemoveStream after Close.
	ErrSinkClosed = errors.New("sink is closed")
)

// streamDestroyedErrorPrefix is the Redis error prefix used by fenced scripts
// to reject writes against a destroyed stream.
const streamDestroyedErrorPrefix = "STREAMDESTROYED"

// lifecycleFenceLua guards a script against destroyed streams and activates
// absent lifecycles. Scripts using it must bind KEYS[1] to the lifecycle key
// and define an `establish` local ("1" reactivates a destroyed stream).
const lifecycleFenceLua = `
local state = redis.call("HGET", KEYS[1], "state")
if state == "destroyed" then
   if establish == "1" then
      redis.call("HSET", KEYS[1], "state", "active")
   else
      return redis.error_reply("STREAMDESTROYED stream was destroyed")
   end
elseif not state then
   redis.call("HSET", KEYS[1], "state", "active")
end
`

// recoveryCursorLua defines recovery_cursor(stream, group) which computes the
// highest event ID X such that every entry at or before X is acknowledged:
// the entry preceding the oldest pending entry when the PEL is not empty, the
// group last-delivered-id otherwise. Returns false when the group is gone.
const recoveryCursorLua = `
local function recovery_cursor(stream, group)
   local pending = redis.pcall("XPENDING", stream, group)
   if pending["err"] then
      return false
   end
   if pending[1] > 0 then
      local prev = redis.call("XREVRANGE", stream, "(" .. pending[2], "-", "COUNT", 1)
      if #prev == 0 then
         return "0-0"
      end
      return prev[1][1]
   end
   local groups = redis.call("XINFO", "GROUPS", stream)
   for _, info in ipairs(groups) do
      local name, last
      for i = 1, #info, 2 do
         if info[i] == "name" then name = info[i+1] end
         if info[i] == "last-delivered-id" then last = info[i+1] end
      end
      if name == group then
         return last
      end
   end
   return false
end
`

// membershipRemoveLua defines membership_remove(content, channel, field,
// value) which removes value from the JSON-array field of an rmap content
// hash and publishes the matching rmap protocol notification.
const membershipRemoveLua = `
local function membership_remove(content, channel, field, value)
   local v = redis.call("HGET", content, field)
   if not v then
      return
   end
   local values = {}
   local ok, decoded = pcall(cjson.decode, v)
   if ok and type(decoded) == "table" then
      values = decoded
   else
      for s in string.gmatch(v, "[^,]+") do
         table.insert(values, s)
      end
   end
   local remaining = {}
   local removed = false
   for _, item in ipairs(values) do
      if item == value then
         removed = true
      else
         table.insert(remaining, item)
      end
   end
   if not removed then
      return
   end
   if #remaining == 0 then
      redis.call("HDEL", content, field)
      local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
      redis.call("HSET", content, "=kind", "del")
      local msg = struct.pack("ic0ic0", string.len(field), field, string.len(rev), rev)
      redis.call("PUBLISH", channel, "del:" .. msg)
      return
   end
   local encoded = cjson.encode(remaining)
   redis.call("HSET", content, field, encoded)
   local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
   redis.call("HSET", content, "=kind", "set")
   local msg = struct.pack("ic0ic0ic0", string.len(field), field, string.len(encoded), encoded, string.len(rev), rev)
   redis.call("PUBLISH", channel, "set:" .. msg)
end
`

// ensureConsumerGroupScript creates or repairs the consumer group for a sink
// behind the lifecycle fence and restores the stream TTL even when the group
// already exists (BUSYGROUP). The group is created at the durable recovery
// cursor when one exists, at the caller start ID otherwise, and the cursor is
// recomputed and stored from live group state so it is always defined.
//
// KEYS: [1]=lifecycle [2]=stream [3]=cursors
// ARGV: [1]=group [2]=startID [3]=establish [4]=ttlMs [5]=ttlSliding
// Returns {created(0|1), cursor}.
var ensureConsumerGroupScript = redis.NewScript(`
local establish = ARGV[3]
` + lifecycleFenceLua + recoveryCursorLua + `
local start = redis.call("HGET", KEYS[3], ARGV[1])
if not start then
   start = ARGV[2]
end
local created = 1
local res = redis.pcall("XGROUP", "CREATE", KEYS[2], ARGV[1], start, "MKSTREAM")
if type(res) == "table" and res["err"] then
   if not string.find(res["err"], "BUSYGROUP", 1, true) then
      return res
   end
   created = 0
end
local cursor = recovery_cursor(KEYS[2], ARGV[1])
redis.call("HSET", KEYS[3], ARGV[1], cursor)
if tonumber(ARGV[4]) > 0 then
   if ARGV[5] == "1" then
      redis.call("PEXPIRE", KEYS[2], ARGV[4])
   else
      redis.call("PEXPIRE", KEYS[2], ARGV[4], "NX")
   end
end
return {created, cursor}
`)

// ackEventsScript acknowledges events and atomically advances the recovery
// cursor from the exact PEL state. Acking events of a destroyed stream or of
// a deleted group is a no-op returning 0, matching XACK semantics on missing
// keys.
//
// KEYS: [1]=lifecycle [2]=stream [3]=cursors
// ARGV: [1]=group [2..]=event IDs
var ackEventsScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "active" then
   return 0
end
` + recoveryCursorLua + `
local acked = redis.call("XACK", KEYS[2], ARGV[1], unpack(ARGV, 2))
local cursor = recovery_cursor(KEYS[2], ARGV[1])
if cursor then
   redis.call("HSET", KEYS[3], ARGV[1], cursor)
end
return acked
`)

// registerConsumerScript creates a Redis consumer in the sink group and
// appends it to the stream membership map behind the lifecycle fence, in one
// atomic operation so membership and consumer-group state cannot diverge.
//
// KEYS: [1]=lifecycle [2]=stream [3]=membership content [4]=membership channel
// ARGV: [1]=group [2]=consumer
var registerConsumerScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "active" then
   return redis.error_reply("STREAMDESTROYED stream was destroyed")
end
local res = redis.pcall("XGROUP", "CREATECONSUMER", KEYS[2], ARGV[1], ARGV[2])
if type(res) == "table" and res["err"] then
   return res
end
local field = ARGV[1]
local v = redis.call("HGET", KEYS[3], field)
local values = {}
if v then
   local ok, decoded = pcall(cjson.decode, v)
   if ok and type(decoded) == "table" then
      values = decoded
   else
      for s in string.gmatch(v, "[^,]+") do
         table.insert(values, s)
      end
   end
end
for _, item in ipairs(values) do
   if item == ARGV[2] then
      return 1
   end
end
table.insert(values, ARGV[2])
local encoded = cjson.encode(values)
redis.call("HSET", KEYS[3], field, encoded)
local rev = tostring(redis.call("HINCRBY", KEYS[3], "=rev", 1))
redis.call("HSET", KEYS[3], "=kind", "set")
local msg = struct.pack("ic0ic0ic0", string.len(field), field, string.len(encoded), encoded, string.len(rev), rev)
redis.call("PUBLISH", KEYS[4], "set:" .. msg)
return 1
`)

// detachConsumerScript rolls back a consumer registration: it removes the
// consumer from the membership map and deletes the Redis consumer when its
// PEL is empty. Used to compensate partial consumer rotation. A destroyed
// stream has no metadata left to detach so the script is then a no-op.
//
// KEYS: [1]=lifecycle [2]=stream [3]=membership content [4]=membership channel
// ARGV: [1]=group [2]=consumer
var detachConsumerScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "active" then
   return 0
end
` + membershipRemoveLua + `
local pending = redis.pcall("XPENDING", KEYS[2], ARGV[1], "-", "+", 1, ARGV[2])
if not pending["err"] and #pending == 0 then
   redis.call("XGROUP", "DELCONSUMER", KEYS[2], ARGV[1], ARGV[2])
end
membership_remove(KEYS[3], KEYS[4], ARGV[1], ARGV[2])
return 1
`)

// removeSinkStreamScript atomically removes a sink consumer from the stream
// membership map and, when it was the last member, destroys the consumer
// group, its recovery cursor, and its lease. Being a single script there is
// no partial failure between membership and group state to compensate.
//
// KEYS: [1]=lifecycle [2]=stream [3]=cursors [4]=membership content
// [5]=membership channel [6]=lease
// ARGV: [1]=group [2]=consumer
var removeSinkStreamScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "active" then
   return 1
end
` + membershipRemoveLua + `
membership_remove(KEYS[4], KEYS[5], ARGV[1], ARGV[2])
if not redis.call("HGET", KEYS[4], ARGV[1]) then
   redis.call("XGROUP", "DESTROY", KEYS[2], ARGV[1])
   redis.call("HDEL", KEYS[3], ARGV[1])
   redis.call("DEL", KEYS[6])
end
return 1
`)

// destroyStreamScript marks the stream destroyed and deletes every key owned
// by the stream in one atomic operation: events, recovery cursors, sink
// leases (enumerated from the membership map fields), and the membership map
// itself via the rmap destroy protocol. The lifecycle hash is kept as the
// destroyed tombstone that fences concurrent sink metadata writes.
//
// KEYS: [1]=lifecycle [2]=stream [3]=cursors [4]=membership content
// [5]=membership channel
// ARGV: [1]=lease key prefix (lease key is prefix .. sink .. ":lease")
var destroyStreamScript = redis.NewScript(`
redis.call("HSET", KEYS[1], "state", "destroyed")
redis.call("DEL", KEYS[2], KEYS[3])
if redis.call("EXISTS", KEYS[4]) == 1 then
   for _, field in ipairs(redis.call("HKEYS", KEYS[4])) do
      if field ~= "=rev" and field ~= "=kind" then
         redis.call("DEL", ARGV[1] .. field .. ":lease")
      end
   end
   local rev = redis.call("HINCRBY", KEYS[4], "=rev", 1)
   redis.call("DEL", KEYS[4])
   redis.call("HSET", KEYS[4], "=rev", rev, "=kind", "destroy")
   redis.call("PUBLISH", KEYS[5], "destroy:" .. tostring(rev))
end
return 1
`)

// ensureConsumerGroup creates or repairs the consumer group for sinkName on
// stream at the durable recovery cursor (startID for brand new groups) and
// restores the stream TTL. establish reactivates a destroyed stream and is
// reserved for deliberate NewSink/AddStream calls; recovery paths pass false
// so a destroyed stream fails with ErrStreamDestroyed instead of being
// resurrected. Returns whether the group was created and the stored cursor.
func ensureConsumerGroup(ctx context.Context, stream *Stream, sinkName, startID string, establish bool) (bool, string, error) {
	keys := []string{lifecycleKey(stream.key), stream.key, cursorsKey(stream.key)}
	args := []any{sinkName, startID, boolArg(establish), stream.ttl.Milliseconds(), boolArg(stream.ttlSliding)}
	res, err := ensureConsumerGroupScript.Run(ctx, stream.rdb, keys, args...).Slice()
	if err != nil {
		if isStreamDestroyedErr(err) {
			return false, "", fmt.Errorf("cannot create consumer group %s for stream %s: %w", sinkName, stream.Name, ErrStreamDestroyed)
		}
		return false, "", fmt.Errorf("failed to create consumer group %s for stream %s: %w", sinkName, stream.Name, err)
	}
	return res[0].(int64) == 1, res[1].(string), nil
}

// registerSinkConsumer atomically creates the Redis consumer for sinkName in
// the stream consumer group and records it in the stream membership map.
func registerSinkConsumer(ctx context.Context, stream *Stream, sinkName, consumer string) error {
	keys := []string{
		lifecycleKey(stream.key),
		stream.key,
		membershipContentKey(stream.Name),
		membershipChannelKey(stream.Name),
	}
	if err := registerConsumerScript.Run(ctx, stream.rdb, keys, sinkName, consumer).Err(); err != nil {
		if isStreamDestroyedErr(err) {
			return fmt.Errorf("cannot register consumer %s for stream %s: %w", consumer, stream.Name, ErrStreamDestroyed)
		}
		return fmt.Errorf("failed to register consumer %s for stream %s: %w", consumer, stream.Name, err)
	}
	return nil
}

// detachSinkConsumer compensates a partial consumer rotation by removing the
// consumer from the stream membership map and deleting the Redis consumer
// when it has no pending events.
func detachSinkConsumer(ctx context.Context, stream *Stream, sinkName, consumer string) error {
	keys := []string{
		lifecycleKey(stream.key),
		stream.key,
		membershipContentKey(stream.Name),
		membershipChannelKey(stream.Name),
	}
	if err := detachConsumerScript.Run(ctx, stream.rdb, keys, sinkName, consumer).Err(); err != nil {
		return fmt.Errorf("failed to detach consumer %s from stream %s: %w", consumer, stream.Name, err)
	}
	return nil
}

// removeSinkStream removes the sink consumer from the stream membership map
// and destroys the consumer group, recovery cursor, and lease when the
// consumer was the last member, all in one atomic operation.
func removeSinkStream(ctx context.Context, stream *Stream, sinkName, consumer string) error {
	keys := []string{
		lifecycleKey(stream.key),
		stream.key,
		cursorsKey(stream.key),
		membershipContentKey(stream.Name),
		membershipChannelKey(stream.Name),
		leaseKey(stream.key, sinkName),
	}
	if err := removeSinkStreamScript.Run(ctx, stream.rdb, keys, sinkName, consumer).Err(); err != nil {
		return fmt.Errorf("failed to remove sink %s from stream %s: %w", sinkName, stream.Name, err)
	}
	return nil
}

// destroyStream atomically marks the stream destroyed and deletes its events
// and sink metadata. It is idempotent and safe to call on streams that were
// never created.
func destroyStream(ctx context.Context, stream *Stream) error {
	keys := []string{
		lifecycleKey(stream.key),
		stream.key,
		cursorsKey(stream.key),
		membershipContentKey(stream.Name),
		membershipChannelKey(stream.Name),
	}
	return destroyStreamScript.Run(ctx, stream.rdb, keys, leaseKeyPrefix(stream.key)).Err()
}

// XAck acknowledges the events and advances the sink recovery cursor from the
// exact PEL state in one atomic operation. It satisfies the Acker interface
// so acknowledging a sink event through Event.Acker keeps recovery lossless.
func (a *recoveryAcker) XAck(ctx context.Context, streamKey, sinkName string, ids ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	keys := []string{lifecycleKey(streamKey), streamKey, cursorsKey(streamKey)}
	args := make([]any, 0, len(ids)+1)
	args = append(args, sinkName)
	for _, id := range ids {
		args = append(args, id)
	}
	acked, err := ackEventsScript.Run(ctx, a.rdb, keys, args...).Int64()
	if err != nil {
		cmd.SetErr(err)
		return cmd
	}
	cmd.SetVal(acked)
	return cmd
}

// streamMetaPrefix returns the metadata key prefix for the stream with the
// given event key. Metadata lives under a prefix distinct from the event key
// so stream names cannot collide with metadata key suffixes.
func streamMetaPrefix(streamKey string) string {
	return "pulse:streammeta:" + streamKey[len(streamKeyPrefix):]
}

// lifecycleKey returns the lifecycle fence key for the stream event key.
func lifecycleKey(streamKey string) string {
	return streamMetaPrefix(streamKey) + ":lifecycle"
}

// cursorsKey returns the recovery cursor hash key for the stream event key.
func cursorsKey(streamKey string) string {
	return streamMetaPrefix(streamKey) + ":cursors"
}

// leaseKeyPrefix returns the prefix of per-sink lease keys for the stream.
func leaseKeyPrefix(streamKey string) string {
	return streamMetaPrefix(streamKey) + ":sink:"
}

// leaseKey returns the fenced lease key for the sink on the stream.
func leaseKey(streamKey, sinkName string) string {
	return leaseKeyPrefix(streamKey) + sinkName + ":lease"
}

// membershipContentKey returns the rmap content key of the stream sink
// membership map (see consumersMapName).
func membershipContentKey(streamName string) string {
	return fmt.Sprintf("map:stream:%s:sinks:content", streamName)
}

// membershipChannelKey returns the rmap pubsub channel of the stream sink
// membership map.
func membershipChannelKey(streamName string) string {
	return fmt.Sprintf("map:stream:%s:sinks:updates", streamName)
}

// isStreamDestroyedErr reports whether err is the fenced-script rejection of
// a write against a destroyed stream.
func isStreamDestroyedErr(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), streamDestroyedErrorPrefix)
}

// boolArg encodes a boolean as the "0"/"1" convention used by the scripts.
func boolArg(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
