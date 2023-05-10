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
		leaseTTL             time.Duration
		maxQueuedJobs        int
		maxShutdownDuration  time.Duration
		clientOnly           bool
		jobSinkBlockDuration time.Duration
		logger               ponos.Logger
	}

	workerOption struct {
		jobChannelSize int
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

// WithLeaseTTL sets the duration after which the job is made available to other
// workers if it wasn't acked. The default is 5s.
func WithLeaseTTL(ttl time.Duration) PoolOption {
	return func(o *poolOption) {
		o.leaseTTL = ttl
	}
}

// WithMaxQueuedJobs sets the maximum number of jobs that can be queued in the pool.
// The default is 1000.
func WithMaxQueuedJobs(max int) PoolOption {
	return func(o *poolOption) {
		o.maxQueuedJobs = max
	}
}

// WithMaxShutdownDuration sets the maximum time to wait for workers to
// shutdown.  The default is 2 minutes.
func WithMaxShutdownDuration(max time.Duration) PoolOption {
	return func(o *poolOption) {
		o.maxShutdownDuration = max
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
		leaseTTL:             5 * time.Second,
		maxQueuedJobs:        1000,
		maxShutdownDuration:  2 * time.Minute,
		jobSinkBlockDuration: 5 * time.Second,
		logger:               ponos.NoopLogger(),
	}
}

// WithJobChannelSize sets the size of the job channel. The default is 100.
func WithJobChannelSize(size int) WorkerOption {
	return func(o *workerOption) {
		o.jobChannelSize = size
	}
}

// defaultWorkerOptions returns the default options.
func defaultWorkerOptions() *workerOption {
	return &workerOption{
		jobChannelSize: 100,
	}
}
