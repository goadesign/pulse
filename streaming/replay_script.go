// The replay script owns the atomic lifecycle/anchor/prefix check. It executes
// no writes or repairs. COUNT bounds returned entries, not the raw bytes or
// internal Redis work needed to materialize one entry before measuring it.
package streaming

import redis "github.com/redis/go-redis/v9"

var (
	replayReadScript = redis.NewScript(`
-- Canonical positive/zero decimal metadata, compared without float conversion.
local function decimal(value, maximum)
    if not value or not string.match(value, "^%d+$") then return false end
    if #value > 1 and string.sub(value, 1, 1) == "0" then return false end
    return #value < #maximum or (#value == #maximum and value <= maximum)
end

local state = redis.call("HGET", KEYS[1], "state")
local generation = redis.call("HGET", KEYS[1], "generation")
local physical = redis.call("HGET", KEYS[1], "physical_key")
local deadline = redis.call("HGET", KEYS[1], "deadline_ms")
local retention = redis.call("HGET", KEYS[1], "retention_config")
local ttl_owned = redis.call("HGET", KEYS[1], "ttl_owned")
if redis.call("EXISTS", KEYS[1]) == 0 then
    return redis.error_reply("STREAMNOTFOUND")
end
if not decimal(generation, "9223372036854775807") or generation == "0" then
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
if state == "destroyed" then return redis.error_reply("STREAMDESTROYED") end
if state ~= "active" then return redis.error_reply("STREAMCONFIGMISMATCH") end
if not physical or not retention then
    return redis.error_reply("STREAMCONFIGMISSING")
end
local expected_physical = ARGV[1]
if generation ~= "1" then
    expected_physical = expected_physical .. ":generation:" .. generation
end
if physical ~= expected_physical then return redis.error_reply("STREAMCONFIGMISMATCH") end
if ARGV[2] ~= "" and (generation ~= ARGV[2] or physical ~= ARGV[3]) then
    return redis.error_reply("STREAMDESTROYED")
end

local version, maximum, mode, value, sliding = string.match(
    retention, "^v=(%d+)|max=(%d+)|mode=(%a+)|value=(%d+)|sliding=(%a+)$")
if version ~= ARGV[11] or not decimal(maximum, "9223372036854775807")
    or not decimal(value, "9223372036854775807") then
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
if mode == "none" then
    if value ~= "0" or sliding ~= "false" or deadline or ttl_owned then
        return redis.error_reply("STREAMCONFIGMISMATCH")
    end
elseif mode == "ttl" then
    if value == "0" or not decimal(value, "9223372036854")
        or (sliding ~= "true" and sliding ~= "false") or deadline or ttl_owned ~= "1" then
        return redis.error_reply("STREAMCONFIGMISMATCH")
    end
elseif mode == "deadline" then
    if value == "0" or sliding ~= "false" or deadline ~= value or ttl_owned then
        return redis.error_reply("STREAMCONFIGMISMATCH")
    end
else
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
if ARGV[4] ~= "" and retention ~= ARGV[4] then
    return redis.error_reply("STREAMCONFIGMISMATCH")
end
if deadline then
    local clock = redis.call("TIME")
    local now = string.format("%.0f", tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000))
    if #now > #deadline or (#now == #deadline and now >= deadline) then
        return redis.error_reply("DEADLINEELAPSED")
    end
end

-- Positive int64 budgets arrive as two exact 32-bit words. Subtraction preserves
-- inclusive byte/count admission even when the configured limit exceeds 2^53.
local function subtract(high, low, cost)
    local cost_high = math.floor(cost / 4294967296)
    local cost_low = cost % 4294967296
    if high < cost_high or (high == cost_high and low < cost_low) then return nil, nil end
    high = high - cost_high
    low = low - cost_low
    if low < 0 then
        high = high - 1
        low = low + 4294967296
    end
    return high, low
end

-- Validate the complete Pulse field set before charging any field. Duplicate
-- names must not disappear through conversion to a Go map.
local function charge(entry, high, low)
    local fields = entry[2]
    if #fields ~= 4 and #fields ~= 6 then return false, nil, nil end
    local seen = {}
    for i = 1, #fields, 2 do
        local key = fields[i]
        if (key ~= "n" and key ~= "p" and key ~= "t") or seen[key] then
            return false, nil, nil
        end
        seen[key] = fields[i + 1]
    end
    if not seen["n"] or seen["n"] == "" or not seen["p"] then
        return false, nil, nil
    end
    high, low = subtract(high, low, #entry[1])
    if high then
        for i = 2, #fields, 2 do
            high, low = subtract(high, low, #fields[i])
            if not high then break end
        end
    end
    return true, high, low
end

local count_high, count_low = tonumber(ARGV[7]), tonumber(ARGV[8])
local byte_high, byte_low = tonumber(ARGV[9]), tonumber(ARGV[10])
local anchor = {}
local position = ARGV[5]
if position ~= "0-0" then
    local retained = redis.call("XRANGE", physical, position, position, "COUNT", 1)
    if #retained ~= 1 then return redis.error_reply("REPLAYPOSITIONUNAVAILABLE") end
    local valid, high = charge(retained[1], byte_high, byte_low)
    if not valid then return redis.error_reply("REPLAYMALFORMED") end
    if not high then return redis.error_reply("REPLAYEVENTTOOLARGE") end
    if ARGV[6] == "1" then anchor = retained end
end

local events = {}
-- The maximal uint64 pair has no successor. Redis rejects an exclusive XRANGE
-- start there instead of returning an empty range; keep this valid tail open.
while (count_high ~= 0 or count_low ~= 0)
    and position ~= "18446744073709551615-18446744073709551615" do
    local candidate = redis.call("XRANGE", physical, "(" .. position, "+", "COUNT", 1)
    if #candidate == 0 then break end
    local valid, high, low = charge(candidate[1], byte_high, byte_low)
    if not valid then return redis.error_reply("REPLAYMALFORMED") end
    if not high then
        if #events == 0 then return redis.error_reply("REPLAYEVENTTOOLARGE") end
        break
    end
    events[#events + 1] = candidate[1]
    byte_high, byte_low = high, low
    count_high, count_low = subtract(count_high, count_low, 1)
    position = candidate[1][1]
end
return {generation, physical, deadline or "", retention, anchor, events}
`)
)
