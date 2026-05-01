// Package pool keeps its cross-map coordination scripts close to the pool
// admission code. These scripts preserve the rmap notification contract while
// making singleton job admission one atomic Redis operation.
package pool

import redis "github.com/redis/go-redis/v9"

const (
	dispatchClaimed int64 = iota + 1
	dispatchAlreadyPending
	dispatchAlreadyRunning
	dispatchMalformedPending
)

var (
	// luaClaimDispatch atomically admits a new external dispatch. A job key may
	// be claimed only when no durable payload exists and no active pending guard
	// exists. Stale pending guards are replaced by the caller's new guard and
	// published through the pending rmap channel; malformed guards are rejected
	// because they indicate corrupted coordination state.
	luaClaimDispatch = redis.NewScript(`
local payload = redis.call("HGET", KEYS[1], ARGV[1])
if payload then
   return {3, ""}
end

local function all_digits(value)
   return string.match(value, "^%d+$") ~= nil
end

local function active_until(value, now)
   if not all_digits(value) then
      return false
   end
   if string.len(value) ~= string.len(now) then
      return string.len(value) > string.len(now)
   end
   return value >= now
end

local pending = redis.call("HGET", KEYS[2], ARGV[1])
if pending then
   if not all_digits(pending) then
      return {4, pending}
   end
   if active_until(pending, ARGV[2]) then
      return {2, pending}
   end
end

redis.call("HSET", KEYS[2], ARGV[1], ARGV[3])
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "set")
local msg = struct.pack("ic0ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(ARGV[3]), ARGV[3], string.len(rev), rev)
redis.call("PUBLISH", KEYS[3], "set:" .. msg)
return {1, ARGV[3]}
`)

	// luaReleaseDispatch removes the pending guard only if it still belongs to
	// this dispatch attempt. It publishes a delete notification so rmap replicas
	// converge without relying on a later cleanup sweep.
	luaReleaseDispatch = redis.NewScript(`
local pending = redis.call("HGET", KEYS[1], ARGV[1])
if pending ~= ARGV[2] then
   return 0
end

redis.call("HDEL", KEYS[1], ARGV[1])
local rev = tostring(redis.call("HINCRBY", KEYS[1], "=rev", 1))
redis.call("HSET", KEYS[1], "=kind", "del")
local msg = struct.pack("ic0ic0", string.len(ARGV[1]), ARGV[1], string.len(rev), rev)
redis.call("PUBLISH", KEYS[2], "del:" .. msg)
return 1
`)
)
