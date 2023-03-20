package pool

import (
	"time"

	"goa.design/ponos/ponos"
)

type (
	// PoolOption is a worker creation option.
	PoolOption func(*poolOption)

	poolOption struct {
		workerTTL     time.Duration
		leaseTTL      time.Duration
		maxQueuedJobs int
		logger        ponos.Logger
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

// WithErrorReporter sets the handler used to report temporary errors.
func WithErrorReporter(logger ponos.Logger) PoolOption {
	return func(o *poolOption) {
		o.logger = logger
	}
}

// defaultPoolOptions returns the default options.
func defaultPoolOptions() *poolOption {
	return &poolOption{
		workerTTL:     10 * time.Second,
		leaseTTL:      5 * time.Second,
		maxQueuedJobs: 1000,
		logger:        &ponos.NilLogger{},
	}
}
