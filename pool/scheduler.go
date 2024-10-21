package pool

import (
	"context"
	"fmt"
	"time"

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
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
		// schedule should be stopped.
		Plan() (*JobPlan, error)
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
		// ticker is the ticker used to run the scheduler.
		ticker *Ticker
		// jobMap is the map of jobs keyed by job key.
		jobMap *rmap.Map
		// logger is the logger used by the scheduler.
		logger pulse.Logger
	}
)

// ErrScheduleStop is returned by JobProducer.Plan to indicate that the
// corresponding schedule should be stopped.
var ErrScheduleStop = fmt.Errorf("stop")

// Schedule calls the producer Plan method on the given interval and starts and
// stops jobs accordingly. The schedule stops when the producer Plan method
// returns ErrScheduleStop. Plan is called on only one of the nodes that
// scheduled the same producer.
func (node *Node) Schedule(ctx context.Context, producer JobProducer, interval time.Duration) error {
	name := node.Name + ":" + producer.Name()
	jobMap, err := rmap.Join(ctx, name, node.rdb, rmap.WithLogger(node.logger))
	if err != nil {
		return fmt.Errorf("failed to join job map %s: %w", name, err)
	}
	ticker, err := node.NewTicker(ctx, producer.Name(), interval)
	if err != nil {
		return fmt.Errorf("failed to create ticker %s: %w", name, err)
	}
	sched := &scheduler{
		name:     name,
		interval: interval,
		producer: producer,
		node:     node,
		ticker:   ticker,
		jobMap:   jobMap,
		logger:   node.logger,
	}
	plan, err := producer.Plan()
	if err != nil {
		return fmt.Errorf("failed to compute schedule: %w", err)
	}
	if err := sched.startJobs(ctx, plan.Start); err != nil {
		return fmt.Errorf("failed to start jobs: %w", err)
	}
	if err := sched.stopJobs(ctx, plan); err != nil {
		return fmt.Errorf("failed to stop jobs: %w", err)
	}
	pulse.Go(ctx, func() { sched.scheduleJobs(ctx, ticker, producer) })
	pulse.Go(ctx, func() { sched.handleStop(ctx) })
	return nil
}

// scheduleJobs calls Plan on ticks and starts and stops jobs as needed.
func (sched *scheduler) scheduleJobs(ctx context.Context, ticker *Ticker, producer JobProducer) {
	for range ticker.C {
		plan, err := producer.Plan()
		if err != nil {
			if err == ErrScheduleStop {
				if err := sched.jobMap.Reset(ctx); err != nil {
					sched.logger.Error(err, "failed to reset job map", "scheduler", sched.name)
					continue
				}
				return
			}
			sched.logger.Error(err, "failed to compute schedule", "scheduler", sched.name)
			continue
		}
		sched.logger.Info("scheduling jobs", "scheduler", sched.name, "start", len(plan.Start), "stop", len(plan.Stop), "stopAll", plan.StopAll)
		if err := sched.startJobs(ctx, plan.Start); err != nil {
			sched.logger.Error(err, "failed to start jobs", "scheduler", sched.name)
		}
		if err := sched.stopJobs(ctx, plan); err != nil {
			sched.logger.Error(err, "failed to stop jobs", "scheduler", sched.name)
		}
	}
}

// startJobs dispatches the given jobs.
func (sched *scheduler) startJobs(ctx context.Context, jobs []*JobParam) error {
	for _, job := range jobs {
		err := sched.node.DispatchJob(ctx, job.Key, job.Payload)
		if err != nil {
			sched.logger.Error(err, "failed to dispatch job", "job", job.Key)
			continue
		}
		if _, err := sched.jobMap.Set(ctx, job.Key, time.Now().String()); err != nil {
			sched.logger.Error(err, "failed to store job", "job", job.Key)
			continue
		}
	}
	return nil
}

// stopJobs stops jobs according to the given schedule.
func (sched *scheduler) stopJobs(ctx context.Context, plan *JobPlan) error {
	var toStop []string
	if plan.StopAll {
		toStop = sched.jobMap.Keys()
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
		err := sched.node.StopJob(ctx, key)
		if err != nil {
			sched.logger.Error(err, "failed to stop job", "job", key)
			continue
		}
		if _, err := sched.jobMap.Delete(ctx, key); err != nil {
			sched.logger.Error(err, "failed to delete job", "job", key)
		}
	}
	return nil
}

// handleStop handles the scheduler stop signal.
func (sched *scheduler) handleStop(_ context.Context) {
	ch := sched.jobMap.Subscribe()
	for ev := range ch {
		if ev == rmap.EventReset {
			sched.logger.Info("stopping scheduler", "scheduler", sched.name)
			sched.ticker.Stop()
			return
		}
	}
}
