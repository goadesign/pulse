package pool

import (
	"fmt"
	"time"

	"goa.design/pulse/pulse"
)

type (
	// NodeOption is a worker creation option.
	NodeOption func(*nodeOptions)

	nodeOptions struct {
		workerTTL               time.Duration
		requeueTimeout          time.Duration
		maxQueuedJobs           int
		clientOnly              bool
		jobSinkBlockDuration    time.Duration
		dispatchTimeout         time.Duration
		dispatchResultRetention time.Duration
		recoveryGrace           time.Duration
		cleanupLease            time.Duration
		logger                  pulse.Logger
	}
)

// WithWorkerTTL sets the immutable pool-generation threshold for stale workers
// and nodes. Every node must configure the same value before joining. The
// default is 30s. A lower number causes more frequent keep-alive updates.
func WithWorkerTTL(ttl time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.workerTTL = ttl
	}
}

// WithRequeueTimeout sets the timeout for one local worker's concurrent
// requeue handoff attempt during graceful removal or Close. It does not control
// remote stale-worker detection or cleanup ownership; those use WithWorkerTTL
// and a renewable Redis-time lease. The default is 2 minutes.
func WithRequeueTimeout(timeout time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.requeueTimeout = timeout
	}
}

// WithWorkerShutdownTTL is the deprecated v1 name for WithRequeueTimeout.
// Deprecated: use WithRequeueTimeout.
func WithWorkerShutdownTTL(ttl time.Duration) NodeOption {
	return WithRequeueTimeout(ttl)
}

// WithJobSinkBlockDuration sets the duration to block when reading from the
// job stream. The default is 5s. This option is mostly useful for testing.
func WithJobSinkBlockDuration(d time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.jobSinkBlockDuration = d
	}
}

// WithMaxQueuedJobs sets the maximum number of jobs that can be queued in the pool.
// The default is 1000.
func WithMaxQueuedJobs(max int) NodeOption {
	return func(o *nodeOptions) {
		o.maxQueuedJobs = max
	}
}

// WithClientOnly sets the pool to be client only. A client-only pool only
// supports dispatching jobs to workers and does not start background goroutines
// to route jobs.
func WithClientOnly() NodeOption {
	return func(o *nodeOptions) {
		o.clientOnly = true
	}
}

// WithDispatchTimeout sets how long DispatchJob waits for a definitive worker
// response. Timing out removes only the local waiter.
func WithDispatchTimeout(timeout time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.dispatchTimeout = timeout
	}
}

// WithDispatchResultRetention sets how long a settled DispatchJobOnce record
// remains replayable. After expiry the dispatch ID may be admitted as new. The
// value is immutable for a pool generation and must exceed both DispatchTimeout
// and RecoveryGrace. The default is five minutes.
func WithDispatchResultRetention(retention time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.dispatchResultRetention = retention
	}
}

// WithRecoveryGrace sets the sink idle-recovery and orphan convergence grace.
func WithRecoveryGrace(grace time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.recoveryGrace = grace
	}
}

// WithAckGracePeriod is the deprecated v1 name for WithRecoveryGrace.
// Deprecated: use WithRecoveryGrace.
func WithAckGracePeriod(grace time.Duration) NodeOption {
	return WithRecoveryGrace(grace)
}

// WithCleanupLease sets the immutable Redis-time lease for takeover of
// interrupted pool-generation cleanup. Worker and node stale reaping use
// WithWorkerTTL instead.
func WithCleanupLease(lease time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.cleanupLease = lease
	}
}

// WithLogger sets the handler used to report temporary errors.
func WithLogger(logger pulse.Logger) NodeOption {
	return func(o *nodeOptions) {
		o.logger = logger
	}
}

// parseOptions parses the given options and returns the corresponding
// options.
func parseOptions(opts ...NodeOption) *nodeOptions {
	o := defaultPoolOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// validateNodeOptions rejects values that would cause immediate lease expiry,
// Redis retry churn, or invalid stream limits.
func validateNodeOptions(o *nodeOptions) error {
	replayWindow := o.dispatchTimeout
	if o.recoveryGrace > replayWindow {
		replayWindow = o.recoveryGrace
	}
	switch {
	case o.workerTTL < 2*time.Millisecond:
		return fmt.Errorf("pool worker TTL must be at least 2ms")
	case o.requeueTimeout < time.Millisecond:
		return fmt.Errorf("pool worker requeue timeout must be at least 1ms")
	case o.jobSinkBlockDuration < time.Millisecond:
		return fmt.Errorf("pool job sink block duration must be at least 1ms")
	case o.maxQueuedJobs <= 0:
		return fmt.Errorf("pool maximum queued jobs must be greater than zero")
	case o.dispatchTimeout < time.Millisecond:
		return fmt.Errorf("pool dispatch timeout must be at least 1ms")
	case o.recoveryGrace < time.Millisecond:
		return fmt.Errorf("pool recovery grace must be at least 1ms")
	case o.cleanupLease < time.Millisecond:
		return fmt.Errorf("pool cleanup lease must be at least 1ms")
	case o.dispatchResultRetention < time.Millisecond:
		return fmt.Errorf("pool dispatch result retention must be at least 1ms")
	case o.dispatchResultRetention <= replayWindow:
		return fmt.Errorf(
			"pool dispatch result retention must exceed dispatch timeout and recovery grace",
		)
	default:
		return nil
	}
}

// defaultPoolOptions returns the default options.
func defaultPoolOptions() *nodeOptions {
	return &nodeOptions{
		workerTTL:               30 * time.Second,
		requeueTimeout:          2 * time.Minute,
		jobSinkBlockDuration:    5 * time.Second,
		maxQueuedJobs:           1000,
		dispatchTimeout:         40 * time.Second,
		dispatchResultRetention: 5 * time.Minute,
		recoveryGrace:           20 * time.Second,
		cleanupLease:            30 * time.Second,
		logger:                  pulse.NoopLogger(),
	}
}
