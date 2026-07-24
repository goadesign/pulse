// This file implements the fenced sink lease used for idle-message claiming
// and stale-consumer cleanup.
//
// Each sink (consumer group) holds at most one lease per stream, stored in a
// hash at "pulse:streammeta:<stream>:sink:<sink>:lease" with fields:
//
//   - owner:  the sink instance currently allowed to claim and clean up.
//   - fence:  a counter incremented on every ownership change.
//   - expiry: Redis-time (milliseconds) after which the lease is up for grabs.
//
// All timing uses the Redis TIME command evaluated inside the scripts so
// client clock skew cannot grant two instances the lease at once. Lease
// verification, renewal, and the guarded mutation (XAUTOCLAIM or consumer
// deletion) execute in a single script: once another instance takes over the
// lease the fence counter changes and every in-flight script from the stale
// owner fails before touching the PEL.
package streaming

import (
	"context"
	"fmt"
	"strings"

	redis "github.com/redis/go-redis/v9"
)

// leaseLostErrorPrefix is the Redis error prefix returned by fenced scripts
// when the caller no longer holds the lease.
const leaseLostErrorPrefix = "LEASELOST"

// leaseCheckLua verifies and renews the caller's lease. Scripts using it must
// bind KEYS[2] to the lease key, ARGV[1] to the owner, ARGV[2] to the fence
// value returned by acquisition, and ARGV[3] to the lease duration in
// milliseconds. It defines now_ms for subsequent statements.
const leaseCheckLua = `
local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local owner = redis.call("HGET", KEYS[2], "owner")
local fence = redis.call("HGET", KEYS[2], "fence")
local expiry = tonumber(redis.call("HGET", KEYS[2], "expiry") or "0")
if owner ~= ARGV[1] or fence ~= ARGV[2] or expiry < now_ms then
   return redis.error_reply("LEASELOST sink lease lost")
end
redis.call("HSET", KEYS[2], "expiry", now_ms + tonumber(ARGV[3]))
redis.call("PEXPIRE", KEYS[2], tonumber(ARGV[3]) * 2)
`

// acquireLeaseScript acquires or renews the sink lease using Redis time. A
// new owner (or a takeover of an expired lease) increments the fence counter
// so scripts still carrying the previous fence value can no longer mutate
// anything. Acquisition is fenced by the stream lifecycle: destroyed streams
// never get a new lease key.
//
// KEYS: [1]=lifecycle [2]=lease
// ARGV: [1]=owner [2]=leaseMs
// Returns {acquired(0|1), fence}.
var acquireLeaseScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "active" then
   return redis.error_reply("STREAMDESTROYED stream was destroyed")
end
local t = redis.call("TIME")
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local owner = redis.call("HGET", KEYS[2], "owner")
local expiry = tonumber(redis.call("HGET", KEYS[2], "expiry") or "0")
if owner and owner ~= ARGV[1] and expiry >= now_ms then
   return {0, 0}
end
local bump = 1
if owner == ARGV[1] and expiry >= now_ms then
   bump = 0
end
local fence = redis.call("HINCRBY", KEYS[2], "fence", bump)
redis.call("HSET", KEYS[2], "owner", ARGV[1], "expiry", now_ms + tonumber(ARGV[2]))
redis.call("PEXPIRE", KEYS[2], tonumber(ARGV[2]) * 2)
return {1, fence}
`)

// fencedAutoClaimScript renews the lease and claims idle messages in one
// atomic operation so a stale owner cannot move PEL entries after takeover.
//
// KEYS: [1]=lifecycle [2]=lease [3]=stream
// ARGV: [1]=owner [2]=fence [3]=leaseMs [4]=group [5]=consumer [6]=minIdleMs
// [7]=start [8]=count
// Returns the XAUTOCLAIM reply, or {"0-0", {}} when the group is gone.
var fencedAutoClaimScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "active" then
   return redis.error_reply("STREAMDESTROYED stream was destroyed")
end
` + leaseCheckLua + `
local res = redis.pcall("XAUTOCLAIM", KEYS[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], "COUNT", ARGV[8])
if type(res) == "table" and res["err"] then
   if string.find(res["err"], "NOGROUP", 1, true) then
      return {"0-0", {}}
   end
   return res
end
return res
`)

// fencedCleanupScript renews the lease and deletes stale consumers in one
// atomic operation. A consumer is stale when it has no pending events and its
// keep-alive is missing or older than the staleness threshold; its membership
// map entry and keep-alive record are removed with rmap protocol
// notifications. The caller's live consumer is never deleted.
//
// KEYS: [1]=lifecycle [2]=lease [3]=stream [4]=membership content
// [5]=membership channel [6]=keep-alive content [7]=keep-alive channel
// ARGV: [1]=owner [2]=fence [3]=leaseMs [4]=group [5]=staleNs [6]=live consumer
// Returns the deleted consumer names.
var fencedCleanupScript = redis.NewScript(`
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "active" then
   return redis.error_reply("STREAMDESTROYED stream was destroyed")
end
` + leaseCheckLua + membershipRemoveLua + `
local consumers = redis.pcall("XINFO", "CONSUMERS", KEYS[3], ARGV[4])
if type(consumers) == "table" and consumers["err"] then
   if string.find(consumers["err"], "NOGROUP", 1, true) then
      return {}
   end
   return consumers
end
local now_ns = (t[1] * 1000000 + t[2]) * 1000
local removed = {}
for _, info in ipairs(consumers) do
   local name, pending
   for i = 1, #info, 2 do
      if info[i] == "name" then name = info[i+1] end
      if info[i] == "pending" then pending = info[i+1] end
   end
   if name ~= ARGV[6] and pending == 0 then
      local ka = redis.call("HGET", KEYS[6], name)
      if not ka or now_ns - tonumber(ka) > tonumber(ARGV[5]) then
         redis.call("XGROUP", "DELCONSUMER", KEYS[3], ARGV[4], name)
         if ka then
            redis.call("HDEL", KEYS[6], name)
            local rev = tostring(redis.call("HINCRBY", KEYS[6], "=rev", 1))
            redis.call("HSET", KEYS[6], "=kind", "del")
            local msg = struct.pack("ic0ic0", string.len(name), name, string.len(rev), rev)
            redis.call("PUBLISH", KEYS[7], "del:" .. msg)
         end
         membership_remove(KEYS[4], KEYS[5], ARGV[4], name)
         table.insert(removed, name)
      end
   end
end
return removed
`)

// acquireSinkLease attempts to acquire or renew the sink lease for the stream
// using Redis time. It returns whether the lease is held and the fence value
// to pass to subsequent fenced operations.
func acquireSinkLease(ctx context.Context, stream *Stream, sinkName, owner string, leaseMs int64) (bool, int64, error) {
	keys := []string{lifecycleKey(stream.key), leaseKey(stream.key, sinkName)}
	res, err := acquireLeaseScript.Run(ctx, stream.rdb, keys, owner, leaseMs).Slice()
	if err != nil {
		if isStreamDestroyedErr(err) {
			return false, 0, fmt.Errorf("cannot acquire lease for stream %s: %w", stream.Name, ErrStreamDestroyed)
		}
		return false, 0, fmt.Errorf("failed to acquire lease for stream %s: %w", stream.Name, err)
	}
	return res[0].(int64) == 1, res[1].(int64), nil
}

// fencedAutoClaim renews the lease and claims up to count messages idle for
// at least minIdleMs, starting at start, assigning them to consumer. It
// returns the claimed messages and the next XAUTOCLAIM start cursor ("0-0"
// when the scan is complete).
func fencedAutoClaim(ctx context.Context, stream *Stream, sinkName, owner string, fence, leaseMs int64, consumer string, minIdleMs int64, start string, count int64) ([]redis.XMessage, string, error) {
	keys := []string{lifecycleKey(stream.key), leaseKey(stream.key, sinkName), stream.key}
	res, err := fencedAutoClaimScript.Run(ctx, stream.rdb, keys,
		owner, fence, leaseMs, sinkName, consumer, minIdleMs, start, count).Slice()
	if err != nil {
		return nil, "", err
	}
	next := res[0].(string)
	entries, ok := res[1].([]any)
	if !ok {
		return nil, "", fmt.Errorf("unexpected XAUTOCLAIM entries type %T", res[1])
	}
	msgs, err := decodeClaimedMessages(entries)
	if err != nil {
		return nil, "", err
	}
	return msgs, next, nil
}

// fencedCleanupStaleConsumers renews the lease and deletes consumers of the
// sink group that have no pending events and whose keep-alive is missing or
// older than staleNs nanoseconds of Redis time. liveConsumer is the caller's
// current consumer and is never deleted. It returns the deleted consumer
// names.
func fencedCleanupStaleConsumers(ctx context.Context, stream *Stream, sinkName, owner string, fence, leaseMs int64, staleNs int64, liveConsumer string) ([]string, error) {
	keys := []string{
		lifecycleKey(stream.key),
		leaseKey(stream.key, sinkName),
		stream.key,
		membershipContentKey(stream.Name),
		membershipChannelKey(stream.Name),
		keepAliveContentKey(sinkName),
		keepAliveChannelKey(sinkName),
	}
	res, err := fencedCleanupScript.Run(ctx, stream.rdb, keys,
		owner, fence, leaseMs, sinkName, staleNs, liveConsumer).StringSlice()
	if err != nil {
		return nil, err
	}
	return res, nil
}

// decodeClaimedMessages converts the raw Lua XAUTOCLAIM entries reply into
// XMessage values. Nil entries (tombstones of trimmed events reported by
// older Redis servers) are skipped.
func decodeClaimedMessages(entries []any) ([]redis.XMessage, error) {
	msgs := make([]redis.XMessage, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		pair, ok := entry.([]any)
		if !ok || len(pair) != 2 {
			return nil, fmt.Errorf("unexpected XAUTOCLAIM entry %v", entry)
		}
		id, ok := pair[0].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected XAUTOCLAIM entry ID %v", pair[0])
		}
		fields, ok := pair[1].([]any)
		if !ok || len(fields)%2 != 0 {
			return nil, fmt.Errorf("unexpected XAUTOCLAIM entry fields %v", pair[1])
		}
		values := make(map[string]any, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if !ok {
				return nil, fmt.Errorf("unexpected XAUTOCLAIM field name %v", fields[i])
			}
			values[key] = fields[i+1]
		}
		msgs = append(msgs, redis.XMessage{ID: id, Values: values})
	}
	return msgs, nil
}

// isLeaseLostErr reports whether err is the fenced-script rejection of an
// operation whose lease was taken over by another sink instance.
func isLeaseLostErr(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), leaseLostErrorPrefix)
}

// keepAliveContentKey returns the rmap content key of the sink keep-alive map
// (see sinkKeepAliveMapName).
func keepAliveContentKey(sinkName string) string {
	return fmt.Sprintf("map:sink:%s:keepalive:content", sinkName)
}

// keepAliveChannelKey returns the rmap pubsub channel of the sink keep-alive
// map.
func keepAliveChannelKey(sinkName string) string {
	return fmt.Sprintf("map:sink:%s:keepalive:updates", sinkName)
}
