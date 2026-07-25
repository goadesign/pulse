// Scheduler transitions use one Redis-time lease from due-time claim through
// Plan, job mutations, canonical ownership scans, and next-time commit. Every
// mutating script verifies the exact pool generation, owner token, fence, and
// unexpired lease so a paused predecessor cannot apply work after takeover.
package pool

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type (
	// schedulerClaim is the Redis-owned result of attempting one due transition.
	schedulerClaim struct {
		owned   bool
		stopped bool
		fence   string
		wait    time.Duration
	}
)

var (
	// claimSchedulerTransitionScript initializes canonical timing or acquires a
	// due transition after verifying the pool generation.
	claimSchedulerTransitionScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[3] .. "stopped") == "1" then
    return {2, "", 0}
end
local interval = redis.call("HGET", KEYS[2], ARGV[3] .. "interval_ms")
if interval and interval ~= ARGV[5] then
    return redis.error_reply("SCHEDULERCONFIGMISMATCH")
end
local next_at = tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "next_ms") or "0")
if next_at == 0 then
    next_at = now + tonumber(ARGV[5])
    redis.call("HSET", KEYS[2],
        ARGV[3] .. "interval_ms", ARGV[5],
        ARGV[3] .. "next_ms", tostring(next_at))
    return {0, "", tonumber(ARGV[5])}
end
local owner = redis.call("HGET", KEYS[2], ARGV[3] .. "owner")
local lease_until = tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "lease_until") or "0")
if owner and owner ~= ARGV[4] and lease_until > now then
    return {0, "", math.max(1, math.min(lease_until - now, math.max(1, next_at - now)))}
end
if next_at > now then
    return {0, "", math.max(1, next_at - now)}
end
local fence = tostring(redis.call("HINCRBY", KEYS[2], ARGV[3] .. "fence", 1))
redis.call("HSET", KEYS[2],
    ARGV[3] .. "owner", ARGV[4],
    ARGV[3] .. "active_fence", fence,
    ARGV[3] .. "lease_until", tostring(now + tonumber(ARGV[6])))
return {1, fence, 0}
`)

	// renewSchedulerTransitionScript extends only the exact live transition.
	renewSchedulerTransitionScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[3] .. "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], ARGV[3] .. "active_fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
redis.call("HSET", KEYS[2], ARGV[3] .. "lease_until", tostring(now + tonumber(ARGV[6])))
return 1
`)

	// commitSchedulerTransitionScript advances canonical time and releases only
	// the exact live transition after every planned mutation completed.
	commitSchedulerTransitionScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[3] .. "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], ARGV[3] .. "active_fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
local interval = tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "interval_ms"))
local next_at = tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "next_ms"))
repeat
    next_at = next_at + interval
until next_at > now
redis.call("HSET", KEYS[2], ARGV[3] .. "next_ms", tostring(next_at))
redis.call("HDEL", KEYS[2],
    ARGV[3] .. "owner",
    ARGV[3] .. "active_fence",
    ARGV[3] .. "lease_until")
return math.max(1, next_at - now)
`)

	// stopSchedulerTransitionScript removes canonical scheduling state only
	// after the exact owner has stopped every scheduler-owned job.
	stopSchedulerTransitionScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[3] .. "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], ARGV[3] .. "active_fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
redis.call("HDEL", KEYS[2],
    ARGV[3] .. "interval_ms",
    ARGV[3] .. "next_ms",
    ARGV[3] .. "owner",
    ARGV[3] .. "active_fence",
    ARGV[3] .. "lease_until")
redis.call("HSET", KEYS[2], ARGV[3] .. "stopped", "1")
return 1
`)

	// releaseSchedulerTransitionScript leaves the due time unchanged after a
	// failed attempt so another owner can retry the same transition.
	releaseSchedulerTransitionScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[3] .. "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], ARGV[3] .. "active_fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
redis.call("HDEL", KEYS[2],
    ARGV[3] .. "owner",
    ARGV[3] .. "active_fence",
    ARGV[3] .. "lease_until")
return 1
`)

	// claimSchedulerJobScript stores a scheduler dispatch capability only while
	// the transition lease is current and publishes the matching rmap update.
	claimSchedulerJobScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[3] .. "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], ARGV[3] .. "active_fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
local current = redis.call("HGET", KEYS[2], ARGV[6])
if current then
    return current
end
redis.call("HSET", KEYS[2], ARGV[6], ARGV[7])
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "set")
local message = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[6]), ARGV[6],
    string.len(ARGV[7]), ARGV[7],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[3], "set:" .. message)
return ""
`)

	// dispatchScheduledJobScript admits the exact persisted scheduler dispatch
	// only while the transition owner/fence is live.
	dispatchScheduledJobScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[3] .. "owner") ~= ARGV[4]
or redis.call("HGET", KEYS[2], ARGV[3] .. "active_fence") ~= ARGV[5]
or tonumber(redis.call("HGET", KEYS[2], ARGV[3] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
local identity = redis.call("HGET", KEYS[6], "identity")
if identity then
    if identity ~= ARGV[12] then
        return redis.error_reply("DISPATCHIDEMPOTENCYCONFLICT")
    end
    local state = redis.call("HGET", KEYS[6], "state")
    return {
        state == "terminal" and 2 or 1,
        redis.call("HGET", KEYS[6], "event"),
        redis.call("HGET", KEYS[6], "result") or "",
        redis.call("HGET", KEYS[6], "error") or ""
    }
end
if redis.call("HGET", KEYS[3], ARGV[6]) then
   return {4, "", "", ""}
end
if redis.call("HGET", KEYS[4], ARGV[6]) then
   return {3, "", "", ""}
end
local count = 0
for _, key in ipairs(redis.call("HKEYS", KEYS[4])) do
    if string.sub(key, 1, 1) ~= "=" then
        count = count + 1
    end
end
if count >= tonumber(ARGV[8]) then
    return {5, "", "", ""}
end
local stream = redis.call("HGET", KEYS[1], ARGV[11])
if not stream then
    return redis.error_reply("POOLGENERATIONLOST")
end
local event_id = redis.call("XADD", stream, "*", "n", ARGV[9], "p", ARGV[10])
redis.call("HSET", KEYS[6],
    "id", ARGV[7],
    "identity", ARGV[12],
    "key", ARGV[6],
    "event", event_id,
    "state", "pending",
    "result", "",
    "error", "")
redis.call("SADD", KEYS[7], KEYS[6])
redis.call("HSET", KEYS[4], ARGV[6], ARGV[7])
local rev = tostring(redis.call("HINCRBY", KEYS[4], "=rev", 1))
redis.call("HSET", KEYS[4], "=kind", "set")
local msg = struct.pack(
    "ic0ic0ic0",
    string.len(ARGV[6]), ARGV[6],
    string.len(ARGV[7]), ARGV[7],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[5], "set:" .. msg)
return {1, event_id, "", ""}
`)
)

// claimTransition claims one due transition or returns the Redis-derived wait.
func (sched *scheduler) claimTransition(ctx context.Context) (schedulerClaim, error) {
	raw, err := claimSchedulerTransitionScript.Run(
		ctx,
		sched.node.rdb,
		sched.transitionKeys(),
		"active",
		sched.node.resources.generation,
		sched.transitionPrefix,
		sched.owner,
		strconv.FormatInt(sched.interval.Milliseconds(), 10),
		strconv.FormatInt(sched.lease.Milliseconds(), 10),
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Slice()
	if err != nil {
		return schedulerClaim{}, poolBoundaryError(err)
	}
	if len(raw) != 3 {
		return schedulerClaim{}, fmt.Errorf("scheduler claim returned %d fields", len(raw))
	}
	status, ok := raw[0].(int64)
	if !ok || status < 0 || status > 2 {
		return schedulerClaim{}, fmt.Errorf("scheduler claim returned invalid status %T(%v)", raw[0], raw[0])
	}
	fence, ok := raw[1].(string)
	if !ok || (status == 1 && fence == "") {
		return schedulerClaim{}, fmt.Errorf("scheduler claim returned invalid fence %T", raw[1])
	}
	waitMillis, ok := raw[2].(int64)
	if !ok || waitMillis < 0 {
		return schedulerClaim{}, fmt.Errorf("scheduler claim returned invalid wait %T(%v)", raw[2], raw[2])
	}
	return schedulerClaim{
		owned:   status == 1,
		stopped: status == 2,
		fence:   fence,
		wait:    time.Duration(waitMillis) * time.Millisecond,
	}, nil
}

// runTransition renews ownership while Plan and all side effects run, then
// commits canonical next time or releases the unchanged due transition.
func (sched *scheduler) runTransition(ctx context.Context, fence string) (bool, error) {
	transitionCtx, cancel := context.WithCancel(ctx)
	var renewWG sync.WaitGroup
	renewErr := make(chan error, 1)
	renewWG.Add(1)
	go sched.renewTransition(transitionCtx, cancel, fence, renewErr, &renewWG)

	stop, applyErr := sched.applyTransition(transitionCtx, fence)
	cancel()
	renewWG.Wait()
	select {
	case err := <-renewErr:
		applyErr = errors.Join(applyErr, err)
	default:
	}
	if applyErr != nil {
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		releaseErr := sched.releaseTransition(releaseCtx, fence)
		releaseCancel()
		return false, errors.Join(applyErr, releaseErr)
	}
	if stop {
		return true, sched.stopTransition(ctx, fence)
	}
	return false, sched.commitTransition(ctx, fence)
}

// renewTransition keeps a slow cancellable Plan and its apply phase fenced.
func (sched *scheduler) renewTransition(
	ctx context.Context,
	cancel context.CancelFunc,
	fence string,
	result chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	renewEvery := sched.lease / 3
	if renewEvery < time.Millisecond {
		renewEvery = time.Millisecond
	}
	ticker := time.NewTicker(renewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sched.renewTransitionLease(ctx, fence); err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case result <- err:
				default:
				}
				cancel()
				return
			}
		}
	}
}

// renewTransitionLease extends the exact owner/fence using Redis time.
func (sched *scheduler) renewTransitionLease(ctx context.Context, fence string) error {
	return poolBoundaryError(renewSchedulerTransitionScript.Run(
		ctx,
		sched.node.rdb,
		sched.transitionKeys(),
		"active",
		sched.node.resources.generation,
		sched.transitionPrefix,
		sched.owner,
		fence,
		strconv.FormatInt(sched.lease.Milliseconds(), 10),
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Err())
}

// commitTransition advances canonical next time and releases this transition.
func (sched *scheduler) commitTransition(ctx context.Context, fence string) error {
	return poolBoundaryError(commitSchedulerTransitionScript.Run(
		ctx,
		sched.node.rdb,
		sched.transitionKeys(),
		"active",
		sched.node.resources.generation,
		sched.transitionPrefix,
		sched.owner,
		fence,
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Err())
}

// stopTransition removes canonical timing after all owned jobs are stopped.
func (sched *scheduler) stopTransition(ctx context.Context, fence string) error {
	return poolBoundaryError(stopSchedulerTransitionScript.Run(
		ctx,
		sched.node.rdb,
		sched.transitionKeys(),
		"active",
		sched.node.resources.generation,
		sched.transitionPrefix,
		sched.owner,
		fence,
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Err())
}

// releaseTransition makes a failed due transition immediately retryable.
func (sched *scheduler) releaseTransition(ctx context.Context, fence string) error {
	return poolBoundaryError(releaseSchedulerTransitionScript.Run(
		ctx,
		sched.node.rdb,
		sched.transitionKeys(),
		"active",
		sched.node.resources.generation,
		sched.transitionPrefix,
		sched.owner,
		fence,
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Err())
}

// claimJobOwnership persists one exact scheduler dispatch under the transition
// lease. Existing ownership is returned and never overwritten.
func (sched *scheduler) claimJobOwnership(
	ctx context.Context,
	fence, field, proposed string,
) (string, error) {
	return claimSchedulerJobScript.Run(
		ctx,
		sched.node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", sched.node.poolStream.Name),
			rmapContentKey(sched.node.resources.schedulerJobs),
			rmapUpdateChannel(sched.node.resources.schedulerJobs),
			rmapContentKey(sched.node.resources.nodeKeepAlive),
		},
		"active",
		sched.node.resources.generation,
		sched.transitionPrefix,
		sched.owner,
		fence,
		field,
		proposed,
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Text()
}

// dispatchJob publishes and waits for one exact scheduler-owned dispatch while
// the transition renewer keeps the admission fence live.
func (sched *scheduler) dispatchJob(
	ctx context.Context,
	fence, dispatchID string,
	job *Job,
) (string, error) {
	waiter := sched.node.acquireDispatchWaiter(dispatchID)
	defer sched.node.releaseDispatchWaiter(dispatchID, waiter)
	record, err := sched.publishDispatchRecord(ctx, fence, dispatchID, job)
	if err != nil {
		return "", err
	}
	if record.status == dispatchTerminal {
		return record.eventID, dispatchTerminalError(record)
	}
	identity, err := dispatchIdentity(job.Key, job.Payload)
	if err != nil {
		return record.eventID, err
	}
	record, err = sched.node.awaitDispatch(ctx, waiter, dispatchID, identity, record)
	if err != nil {
		return record.eventID, err
	}
	return record.eventID, dispatchTerminalError(record)
}

// publishDispatchRecord atomically verifies transition ownership and admits
// the exact scheduler dispatch.
func (sched *scheduler) publishDispatchRecord(
	ctx context.Context,
	fence, dispatchID string,
	job *Job,
) (dispatchRecord, error) {
	identity, err := dispatchIdentity(job.Key, job.Payload)
	if err != nil {
		return dispatchRecord{}, err
	}
	raw, err := dispatchScheduledJobScript.Run(
		ctx,
		sched.node.rdb,
		[]string{
			fmt.Sprintf("pulse:stream:%s:lifecycle", sched.node.poolStream.Name),
			rmapContentKey(sched.node.resources.schedulerJobs),
			rmapContentKey(sched.node.resources.jobPayloads),
			rmapContentKey(sched.node.resources.jobPending),
			rmapUpdateChannel(sched.node.resources.jobPending),
			dispatchRecordKey(sched.node.resources.dispatches, dispatchID),
			dispatchActiveKey(sched.node.resources.dispatches),
			rmapContentKey(sched.node.resources.nodeKeepAlive),
		},
		"active",
		sched.node.resources.generation,
		sched.transitionPrefix,
		sched.owner,
		fence,
		job.Key,
		dispatchID,
		sched.node.maxQueuedJobs,
		evStartJob,
		marshalJob(job),
		"physical_key",
		identity,
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Result()
	if err != nil {
		if redis.HasErrorPrefix(err, "DISPATCHIDEMPOTENCYCONFLICT") {
			return dispatchRecord{}, fmt.Errorf("%w: dispatch %q", ErrDispatchConflict, dispatchID)
		}
		return dispatchRecord{}, poolBoundaryError(err)
	}
	record, err := parseDispatchRecord(raw)
	if err != nil {
		return dispatchRecord{}, err
	}
	switch record.status {
	case dispatchClaimed, dispatchTerminal:
		return record, nil
	case dispatchAlreadyPending, dispatchAlreadyRunning:
		return dispatchRecord{}, fmt.Errorf("%w: job %q", ErrJobExists, job.Key)
	case dispatchCapacityReached:
		return dispatchRecord{}, fmt.Errorf(
			"%w: maximum %d pending jobs",
			ErrPoolCapacity,
			sched.node.maxQueuedJobs,
		)
	default:
		return dispatchRecord{}, fmt.Errorf("unexpected scheduler dispatch status %d", record.status)
	}
}

// transitionKeys returns the lifecycle and generation-owned scheduler state.
func (sched *scheduler) transitionKeys() []string {
	return []string{
		fmt.Sprintf("pulse:stream:%s:lifecycle", sched.node.poolStream.Name),
		rmapContentKey(sched.node.resources.schedulerJobs),
		rmapContentKey(sched.node.resources.nodeKeepAlive),
	}
}
