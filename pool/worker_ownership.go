// An accepted handler can outlive a missing durable ownership entry. Recovery
// restores that entry from the running job without starting its handler again,
// but never replaces another worker, pending dispatch, or different payload.
package pool

import (
	"context"
	"errors"
	"fmt"

	redis "github.com/redis/go-redis/v9"
)

// restoreRunningJobScript checks every conflicting owner before writing either
// map. Worker and node registration checks prevent a removed process from
// recreating ownership after another process has taken over its cleanup.
var restoreRunningJobScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local worker = ARGV[3]
local job = ARGV[4]
local registration = redis.call("HGET", KEYS[7], worker)
if not registration or registration == "-"
or redis.call("HGET", KEYS[8], worker) then
    return redis.error_reply("WORKERCLEANUPLOST")
end
local payload = redis.call("HGET", KEYS[4], job)
if payload and payload ~= ARGV[5] then
    return redis.error_reply("JOBPAYLOADCONFLICT")
end
local pending = redis.call("HGET", KEYS[6], job)
if pending and pending ~= ARGV[6] then
    return redis.error_reply("JOBDISPATCHCONFLICT")
end

local own_jobs = {}
local owned = false
local owners = redis.call("HGETALL", KEYS[2])
for i = 1, #owners, 2 do
    local owner = owners[i]
    if string.sub(owner, 1, 1) ~= "=" then
        local ok, jobs = pcall(cjson.decode, owners[i + 1])
        if not ok or type(jobs) ~= "table" then
            return redis.error_reply("INVALIDJOBOWNERSHIP")
        end
        if owner == worker then
            own_jobs = jobs
        end
        for _, key in ipairs(jobs) do
            if key == job then
                if owner ~= worker then
                    return redis.error_reply("JOBOWNERSHIPCONFLICT")
                end
                owned = true
            end
        end
    end
end

local function set_value(content, channel, key, value)
    redis.call("HSET", content, key, value)
    local rev = tostring(redis.call("HINCRBY", content, "=rev", 1))
    redis.call("HSET", content, "=kind", "set")
    local message = struct.pack("ic0ic0ic0",
        string.len(key), key, string.len(value), value, string.len(rev), rev)
    redis.call("PUBLISH", channel, "set:" .. message)
end
if not owned then
    table.insert(own_jobs, job)
    set_value(KEYS[2], KEYS[3], worker, cjson.encode(own_jobs))
end
if not payload then
    set_value(KEYS[4], KEYS[5], job, ARGV[5])
end
return 1
`)

// restoreRunningJob repairs only missing entries for this accepted handler.
// The caller holds jobsLock. A failed repair leaves the handler and its local
// record intact; Redis failures keep the delivery pending for another attempt.
func (w *Worker) restoreRunningJob(ctx context.Context, job *Job) error {
	err := restoreRunningJobScript.Run(ctx, w.node.rdb, []string{
		fmt.Sprintf("pulse:stream:%s:lifecycle", w.node.poolStream.Name),
		rmapContentKey(w.node.resources.jobs),
		rmapUpdateChannel(w.node.resources.jobs),
		rmapContentKey(w.node.resources.jobPayloads),
		rmapUpdateChannel(w.node.resources.jobPayloads),
		rmapContentKey(w.node.resources.jobPending),
		rmapContentKey(w.node.resources.workers),
		rmapContentKey(w.node.resources.workerCleanup),
		rmapContentKey(w.node.resources.nodeKeepAlive),
	}, "active", w.node.resources.generation, w.ID, job.Key, job.Payload, job.dispatchID,
		w.node.ID, nodeCleanupField(w.node.ID)).Err()
	if err == nil {
		return nil
	}
	if redis.HasErrorPrefix(err, "JOBPAYLOADCONFLICT") ||
		redis.HasErrorPrefix(err, "JOBDISPATCHCONFLICT") ||
		redis.HasErrorPrefix(err, "JOBOWNERSHIPCONFLICT") {
		return fmt.Errorf("%w: cannot restore running job %q: %v", ErrJobExists, job.Key, err)
	}
	return errors.Join(ErrRequeue, fmt.Errorf("restore running job %q: %w", job.Key, poolBoundaryError(err)))
}
