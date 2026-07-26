package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"

	"goa.design/pulse/pulse"
)

type (
	// JobComputeFunc is the function called by the scheduler to compute jobs.
	// It returns the list of jobs to start and job keys to stop.
	JobProducer interface {
		// Name returns the name of the producer. Schedule calls Plan on
		// only one of the producers with identical names across all
		// nodes.
		Name() string
		// Plan computes the list of jobs to start and job keys to stop.
		// Returning ErrScheduleStop indicates that the recurring
		// schedule should be stopped. Legacy Plan calls cannot be cancelled;
		// implement ContextJobProducer when planning may block.
		Plan() (*JobPlan, error)
	}

	// ContextJobProducer adds cancellable planning without breaking the v1
	// JobProducer contract. The scheduler prefers PlanContext when implemented;
	// Node.Close cancels its context and joins the in-flight transition.
	ContextJobProducer interface {
		JobProducer
		PlanContext(ctx context.Context) (*JobPlan, error)
	}

	// JobPlan represents a list of jobs to start and job keys to stop.
	JobPlan struct {
		// Jobs to start.
		Start []*JobParam
		// Job keys to stop.
		Stop []string
		// StopAll indicates that all jobs not in Jobs should be
		// stopped.  Stop is ignored if StopAll is true.
		StopAll bool
	}

	// JobParam represents a job to start.
	JobParam struct {
		// Key is the job key.
		Key string
		// Payload is the job payload.
		Payload []byte
	}

	// scheduler implements a scheduler that starts and stops jobs on a
	// recurring basis.
	scheduler struct {
		// name is the name of the scheduler.
		name string
		// interval is the interval at which the scheduler runs.
		interval time.Duration
		// producer is the job producer.
		producer JobProducer
		// node is the node running the scheduler.
		node *Node
		// keyPrefix scopes ownership records in the generation-owned scheduler
		// map.
		keyPrefix string
		// transitionPrefix scopes Redis-owned due time and lease fields.
		transitionPrefix string
		// owner is this local schedule's unique transition owner token.
		owner string
		// lease is the renewable Redis-time transition lease.
		lease time.Duration
		// logger is the logger used by the scheduler.
		logger pulse.Logger
	}
)

// ErrScheduleStop is returned by JobProducer.Plan or
// ContextJobProducer.PlanContext to stop the corresponding schedule.
var (
	ErrScheduleStop = fmt.Errorf("stop")

	// stopSchedulerJobScript atomically verifies exact scheduler ownership,
	// publishes the durable stop request, and removes the ownership record.
	stopSchedulerJobScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[7] .. "owner") ~= ARGV[8]
or redis.call("HGET", KEYS[2], ARGV[7] .. "active_fence") ~= ARGV[9]
or tonumber(redis.call("HGET", KEYS[2], ARGV[7] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
if redis.call("HGET", KEYS[2], ARGV[3]) ~= ARGV[4] then
    return 0
end
local stream = redis.call("HGET", KEYS[1], "physical_key")
if not stream then
    return redis.error_reply("POOLGENERATIONLOST")
end
redis.call("XADD", stream, "*", "n", ARGV[5], "p", ARGV[6])
redis.call("HDEL", KEYS[2], ARGV[3])
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "del")
local message = struct.pack(
    "ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[3], "del:" .. message)
return 1
`)

	// releaseSchedulerOwnershipScript removes only the exact scheduler
	// capability, without publishing a stop request for a foreign job.
	releaseSchedulerOwnershipScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "state") ~= ARGV[1]
or redis.call("HGET", KEYS[1], "generation") ~= ARGV[2] then
    return redis.error_reply("POOLGENERATIONLOST")
end` + nodeLivenessFenceLua + `
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if redis.call("HGET", KEYS[2], ARGV[5] .. "owner") ~= ARGV[6]
or redis.call("HGET", KEYS[2], ARGV[5] .. "active_fence") ~= ARGV[7]
or tonumber(redis.call("HGET", KEYS[2], ARGV[5] .. "lease_until") or "0") <= now then
    return redis.error_reply("SCHEDULERLEASELOST")
end
if redis.call("HGET", KEYS[2], ARGV[3]) ~= ARGV[4] then
    return 0
end
redis.call("HDEL", KEYS[2], ARGV[3])
local rev = tostring(redis.call("HINCRBY", KEYS[2], "=rev", 1))
redis.call("HSET", KEYS[2], "=kind", "del")
local message = struct.pack(
    "ic0ic0",
    string.len(ARGV[3]), ARGV[3],
    string.len(rev), rev
)
redis.call("PUBLISH", KEYS[3], "del:" .. message)
return 1
`)
)

// Schedule starts a distributed schedule. The shared ticker establishes
// per-producer ownership before every Plan call, including the initial call, so
// only one node computes or applies each transition. The node owns the schedule
// goroutine and ticker: caller cancellation stops this schedule, and Node.Close
// cancels and joins every remaining schedule before returning.
func (node *Node) Schedule(ctx context.Context, producer JobProducer, interval time.Duration) error {
	node.lock.RLock()
	defer node.lock.RUnlock()
	if node.closing {
		return fmt.Errorf("schedule %q: pool %q is closed", producer.Name(), node.PoolName)
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		return fmt.Errorf("schedule %q: %w", producer.Name(), err)
	}
	if interval < time.Millisecond {
		return fmt.Errorf("schedule %q: interval must be at least 1ms", producer.Name())
	}
	name := node.PoolName + ":" + producer.Name()
	encodedName := hex.EncodeToString([]byte(producer.Name()))
	sched := &scheduler{
		name:             name,
		interval:         interval,
		producer:         producer,
		node:             node,
		keyPrefix:        encodedName + ":",
		transitionPrefix: "=transition:" + encodedName + ":",
		owner:            "scheduler-transition-" + ulid.Make().String(),
		lease:            node.workerTTL,
		logger:           node.logger,
	}
	scheduleCtx, cancel := context.WithCancel(ctx)
	stopNodeCancellation := context.AfterFunc(node.scheduleCtx, cancel)
	node.scheduleWG.Add(1)
	pulse.Go(sched.logger, func() {
		defer node.scheduleWG.Done()
		defer stopNodeCancellation()
		defer cancel()
		sched.scheduleJobs(scheduleCtx)
	})
	return nil
}

// scheduleJobs claims, renews, and commits one canonical transition at a time.
func (sched *scheduler) scheduleJobs(ctx context.Context) {
	var wait time.Duration
	for {
		if !waitForScheduler(ctx, wait) {
			return
		}
		claim, err := sched.claimTransition(ctx)
		if err != nil {
			sched.logger.Error(err, "scheduler", sched.name)
			wait = min(sched.interval, time.Second)
			continue
		}
		if claim.stopped {
			return
		}
		if !claim.owned {
			wait = claim.wait
			continue
		}
		stop, err := sched.runTransition(ctx, claim.fence)
		if err != nil {
			sched.logger.Error(err, "scheduler", sched.name)
			wait = min(sched.interval, 100*time.Millisecond)
			continue
		}
		if stop {
			return
		}
		wait = 0
	}
}

// applyTransition computes and fully applies one owner-held scheduler
// transition. A failure leaves canonical ownership records unchanged wherever
// the corresponding side effect did not complete.
func (sched *scheduler) applyTransition(ctx context.Context, fence string) (bool, error) {
	var (
		plan *JobPlan
		err  error
	)
	if producer, ok := sched.producer.(ContextJobProducer); ok {
		plan, err = producer.PlanContext(ctx)
	} else {
		plan, err = sched.producer.Plan()
	}
	if err != nil {
		if errors.Is(err, ErrScheduleStop) {
			if err := sched.clearJobs(ctx, fence); err != nil {
				return false, fmt.Errorf("clear scheduler jobs: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("compute schedule: %w", err)
	}
	sched.logger.Info(
		"scheduling jobs",
		"scheduler",
		sched.name,
		"start",
		len(plan.Start),
		"stop",
		len(plan.Stop),
		"stopAll",
		plan.StopAll,
	)
	if err := sched.startJobs(ctx, fence, plan.Start); err != nil {
		return false, fmt.Errorf("start jobs: %w", err)
	}
	if err := sched.stopJobs(ctx, fence, plan); err != nil {
		return false, fmt.Errorf("stop jobs: %w", err)
	}
	return false, nil
}

// startJobs dispatches the given jobs.
func (sched *scheduler) startJobs(ctx context.Context, fence string, jobs []*JobParam) error {
	for _, job := range jobs {
		field := sched.keyPrefix + job.Key
		dispatchID, err := sched.schedulerOwnership(ctx, field)
		if err != nil {
			return fmt.Errorf("read job %q ownership: %w", job.Key, err)
		}
		// A pre-existing record proves this scheduler already dispatched the
		// key; a fresh proposal means any occupant of the key is foreign.
		owned := dispatchID != ""
		if !owned {
			proposed := "scheduler-" + ulid.Make().String()
			previous, err := sched.claimJobOwnership(ctx, fence, field, proposed)
			if err != nil {
				return fmt.Errorf("store job %q ownership: %w", job.Key, err)
			}
			dispatchID = proposed
			if previous != "" {
				dispatchID = previous
				owned = true
			}
		}
		dispatched := &Job{
			Key:        job.Key,
			Payload:    job.Payload,
			CreatedAt:  time.Now(),
			NodeID:     sched.node.ID,
			dispatchID: dispatchID,
		}
		if _, err := sched.dispatchJob(ctx, fence, dispatchID, dispatched); err != nil {
			if errors.Is(err, ErrJobExists) {
				// The scheduled key is already running, which is the state
				// this transition wanted. An occupant this scheduler
				// dispatched keeps its ownership so later transitions retry
				// the same exact dispatch instead of proposing a new one; a
				// foreign occupant must never become scheduler-owned, so the
				// speculative claim is dropped and the key is retried on the
				// next transition. Either way the remaining planned jobs
				// still run.
				if owned {
					continue
				}
				if releaseErr := sched.releaseOwnership(ctx, fence, field, dispatchID); releaseErr != nil {
					return fmt.Errorf("release foreign job %q ownership: %w", job.Key, releaseErr)
				}
				sched.logger.Debug(
					"scheduled job key held by a foreign job",
					"job", job.Key,
					"scheduler", sched.name,
				)
				continue
			}
			identity, identityErr := dispatchIdentity(job.Key, job.Payload)
			if identityErr != nil {
				return fmt.Errorf("encode job %q identity: %w", job.Key, identityErr)
			}
			record, readErr := sched.node.readDispatchRecord(ctx, dispatchID, identity)
			if readErr == nil && record.status == dispatchTerminal {
				// The run this scheduler owns already settled: drop ownership
				// so the next transition dispatches a fresh run.
				if releaseErr := sched.releaseOwnership(ctx, fence, field, dispatchID); releaseErr != nil {
					return errors.Join(
						fmt.Errorf("dispatch job %q as %q: %w", job.Key, dispatchID, err),
						releaseErr,
					)
				}
			}
			return fmt.Errorf("dispatch job %q as %q: %w", job.Key, dispatchID, err)
		}
	}
	return nil
}

// stopJobs stops jobs according to the given schedule.
func (sched *scheduler) stopJobs(ctx context.Context, fence string, plan *JobPlan) error {
	var toStop []string
	if plan.StopAll {
		ownership, err := sched.jobOwnership(ctx)
		if err != nil {
			return err
		}
		toStop = make([]string, 0, len(ownership))
		for key := range ownership {
			toStop = append(toStop, key)
		}
		for _, j := range plan.Start {
			for i, k := range toStop {
				if k == j.Key {
					toStop = append(toStop[:i], toStop[i+1:]...)
					break
				}
			}
		}
	} else {
		toStop = plan.Stop
	}
	for _, key := range toStop {
		field := sched.keyPrefix + key
		dispatchID, err := sched.schedulerOwnership(ctx, field)
		if err != nil {
			return fmt.Errorf("read job %q ownership: %w", key, err)
		}
		if dispatchID == "" {
			continue
		}
		stopped, err := sched.stopOwnedJob(ctx, fence, field, key, dispatchID)
		if err != nil {
			return fmt.Errorf("stop job %q: %w", key, err)
		}
		if !stopped {
			continue
		}
	}
	return nil
}

// jobOwnership scans Redis rather than the eventually consistent local rmap
// cache. Each later mutation rechecks the exact field and dispatch ID.
func (sched *scheduler) jobOwnership(ctx context.Context) (map[string]string, error) {
	ownership := make(map[string]string)
	var cursor uint64
	for {
		values, next, err := sched.node.rdb.HScan(
			ctx,
			rmapContentKey(sched.node.resources.schedulerJobs),
			cursor,
			sched.keyPrefix+"*",
			100,
		).Result()
		if err != nil {
			return nil, fmt.Errorf("scan scheduler ownership: %w", err)
		}
		for index := 0; index < len(values); index += 2 {
			ownership[strings.TrimPrefix(values[index], sched.keyPrefix)] = values[index+1]
		}
		cursor = next
		if cursor == 0 {
			return ownership, nil
		}
	}
}

// clearJobs durably stops every Redis-authoritative job owned by this schedule.
func (sched *scheduler) clearJobs(ctx context.Context, fence string) error {
	ownership, err := sched.jobOwnership(ctx)
	if err != nil {
		return err
	}
	for key, dispatchID := range ownership {
		if _, err := sched.stopOwnedJob(ctx, fence, sched.keyPrefix+key, key, dispatchID); err != nil {
			return err
		}
	}
	return nil
}

// schedulerOwnership reads one exact ownership capability from Redis.
func (sched *scheduler) schedulerOwnership(ctx context.Context, field string) (string, error) {
	value, err := sched.node.rdb.HGet(
		ctx,
		rmapContentKey(sched.node.resources.schedulerJobs),
		field,
	).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return value, nil
}

// stopOwnedJob publishes a stop request only if the exact scheduler dispatch
// capability remains current at the Redis linearization point.
func (sched *scheduler) stopOwnedJob(
	ctx context.Context,
	fence, field, key, dispatchID string,
) (bool, error) {
	stopped, err := stopSchedulerJobScript.Run(
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
		field,
		dispatchID,
		evStopJob,
		marshalJobKey(key),
		sched.transitionPrefix,
		sched.owner,
		fence,
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Int64()
	if err != nil {
		return false, poolBoundaryError(err)
	}
	return stopped == 1, nil
}

// releaseOwnership removes only the exact persisted scheduler capability.
func (sched *scheduler) releaseOwnership(
	ctx context.Context,
	fence, field, dispatchID string,
) error {
	_, err := releaseSchedulerOwnershipScript.Run(
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
		field,
		dispatchID,
		sched.transitionPrefix,
		sched.owner,
		fence,
		sched.node.ID,
		nodeCleanupField(sched.node.ID),
	).Int64()
	return poolBoundaryError(err)
}

// waitForScheduler waits without allocating a ticker for the zero-delay path.
func waitForScheduler(ctx context.Context, wait time.Duration) bool {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
