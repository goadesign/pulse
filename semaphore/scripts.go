// Package semaphore keeps its Redis scripts next to the admission code so slot
// acquisition, expiry cleanup, and wakeups stay atomic.
package semaphore

import "github.com/redis/go-redis/v9"

var (
	luaAcquire = redis.NewScript(`
local holders = KEYS[1]
local channel = KEYS[2]
local waiters = KEYS[3]
local waiter_deadlines = KEYS[4]
local waiter_seq = KEYS[5]

local token = ARGV[1]
local limit = tonumber(ARGV[2])
local lease_ttl_ms = tonumber(ARGV[3])
local waiter_ttl_ms = tonumber(ARGV[4])

local function now_ms()
   local t = redis.call("TIME")
   return (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()
local expired_holders = redis.call("ZREMRANGEBYSCORE", holders, "-inf", now)

if redis.call("ZSCORE", waiters, token) then
   redis.call("HSET", waiter_deadlines, token, now + waiter_ttl_ms)
end

local expired_waiters = 0
local queued = redis.call("ZRANGE", waiters, 0, -1)
for _, waiter in ipairs(queued) do
   local deadline = redis.call("HGET", waiter_deadlines, waiter)
   if (not deadline) or tonumber(deadline) <= now then
      redis.call("ZREM", waiters, waiter)
      redis.call("HDEL", waiter_deadlines, waiter)
      expired_waiters = expired_waiters + 1
   end
end

if not redis.call("ZSCORE", waiters, token) then
   local seq = redis.call("INCR", waiter_seq)
   redis.call("ZADD", waiters, seq, token)
   redis.call("HSET", waiter_deadlines, token, now + waiter_ttl_ms)
end

if expired_holders > 0 or expired_waiters > 0 then
   redis.call("PUBLISH", channel, "expired")
end

local count = redis.call("ZCARD", holders)
local available = limit - count
local rank = redis.call("ZRANK", waiters, token)
if available > 0 and rank and rank < available then
   redis.call("ZREM", waiters, token)
   redis.call("HDEL", waiter_deadlines, token)
   redis.call("ZADD", holders, now + lease_ttl_ms, token)
   redis.call("PUBLISH", channel, "acquired")
   return {1, 0}
end

local next = redis.call("ZRANGE", holders, 0, 0, "WITHSCORES")
if #next == 0 then
   return {0, 1000}
end
local delay = tonumber(next[2]) - now
if delay < 1 then
   delay = 1
end
return {0, delay}
`)

	luaRelease = redis.NewScript(`
local holders = KEYS[1]
local channel = KEYS[2]
local waiters = KEYS[3]
local waiter_deadlines = KEYS[4]
local waiter_seq = KEYS[5]
local token = ARGV[1]

local removed = redis.call("ZREM", holders, token)
if redis.call("ZCARD", holders) == 0 and redis.call("ZCARD", waiters) == 0 then
   redis.call("DEL", waiter_deadlines)
   redis.call("DEL", waiter_seq)
end
if removed == 1 then
   redis.call("PUBLISH", channel, "released")
end
return removed
`)

	luaRenew = redis.NewScript(`
local holders = KEYS[1]
local channel = KEYS[2]
local token = ARGV[1]
local lease_ttl_ms = tonumber(ARGV[2])

local function now_ms()
   local t = redis.call("TIME")
   return (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()

local score = redis.call("ZSCORE", holders, token)
if not score then
   return 0
end
if tonumber(score) <= now then
   redis.call("ZREM", holders, token)
   redis.call("PUBLISH", channel, "expired")
   return 0
end
redis.call("ZADD", holders, now + lease_ttl_ms, token)
return 1
`)

	luaCancelWaiter = redis.NewScript(`
local waiters = KEYS[1]
local waiter_deadlines = KEYS[2]
local channel = KEYS[3]
local holders = KEYS[4]
local waiter_seq = KEYS[5]
local token = ARGV[1]

local removed = redis.call("ZREM", waiters, token)
redis.call("HDEL", waiter_deadlines, token)
if redis.call("ZCARD", holders) == 0 and redis.call("ZCARD", waiters) == 0 then
   redis.call("DEL", waiter_deadlines)
   redis.call("DEL", waiter_seq)
end
if removed == 1 then
   redis.call("PUBLISH", channel, "canceled")
end
return removed
`)
)
