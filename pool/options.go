package pool

import (
	"time"

	"goa.design/ponos/ponos"
)

type (
	// PoolOption is a worker creation option.
	PoolOption func(*poolOption)

	// WorkerOption is a worker creation option.
	WorkerOption func(*workerOption)

	poolOption struct {
		workerTTL            time.Duration
		pendingJobTTL        time.Duration
		workerShutdownTTL    time.Duration
		maxQueuedJobs        int
		clientOnly           bool
		jobSinkBlockDuration time.Duration
		logger               ponos.Logger
	}
)

// WithWorkerTTL sets the duration after which the worker is removed from the pool in
// case of network partitioning.  The default is 10s. A lower number causes more
// frequent keep-alive updates from all workers.
func WithWorkerTTL(ttl time.Duration) PoolOption {
	return func(o *poolOption) {
		o.workerTTL = ttl
	}
}

// WithPendingJobTTL sets the duration after which a job is made available to
// other workers if it wasn't started. The default is 20s.
func WithPendingJobTTL(ttl time.Duration) PoolOption {
	return func(o *poolOption) {
		o.pendingJobTTL = ttl
	}
}

// WithWorkerShutdownTTL sets the maximum time to wait for workers to
// shutdown.  The default is 2 minutes.
func WithWorkerShutdownTTL(ttl time.Duration) PoolOption {
	return func(o *poolOption) {
		o.workerShutdownTTL = ttl
	}
}

// WithMaxQueuedJobs sets the maximum number of jobs that can be queued in the pool.
// The default is 1000.
func WithMaxQueuedJobs(max int) PoolOption {
	return func(o *poolOption) {
		o.maxQueuedJobs = max
	}
}

// WithClientOnly sets the pool to be client only. A client-only pool only
// supports dispatching jobs to workers and does not start background goroutines
// to route jobs.
func WithClientOnly() PoolOption {
	return func(o *poolOption) {
		o.clientOnly = true
	}
}

// WithJobSinkBlockDuration sets the duration to block when reading from the
// job stream. The default is 5s. This option is mostly useful for testing.
func WithJobSinkBlockDuration(d time.Duration) PoolOption {
	return func(o *poolOption) {
		o.jobSinkBlockDuration = d
	}
}

// WithLogger sets the handler used to report temporary errors.
func WithLogger(logger ponos.Logger) PoolOption {
	return func(o *poolOption) {
		o.logger = logger
	}
}

// defaultPoolOptions returns the default options.
func defaultPoolOptions() *poolOption {
	return &poolOption{
		workerTTL:            10 * time.Second,
		pendingJobTTL:        20 * time.Second,
		workerShutdownTTL:    2 * time.Minute,
		maxQueuedJobs:        1000,
		jobSinkBlockDuration: 5 * time.Second,
		logger:               ponos.NoopLogger(),
	}
}
