package pool

import (
	"time"

	"goa.design/pulse/pulse"
)

type (
	// NodeOption is a worker creation option.
	NodeOption func(*nodeOptions)

	nodeOptions struct {
		workerTTL         time.Duration
		workerShutdownTTL time.Duration
		maxQueuedJobs     int
		clientOnly        bool
		ackGracePeriod    time.Duration
		logger            pulse.Logger
	}
)

// WithWorkerTTL sets the duration after which the worker is removed from the pool in
// case of network partitioning.  The default is 10s. A lower number causes more
// frequent keep-alive updates from all workers.
func WithWorkerTTL(ttl time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.workerTTL = ttl
	}
}

// WithWorkerShutdownTTL sets the maximum time to wait for workers to
// shutdown.  The default is 2 minutes.
func WithWorkerShutdownTTL(ttl time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.workerShutdownTTL = ttl
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

// WithAckGracePeriod sets the duration after which a job is made available to
// other workers if it wasn't started. The default is 20s.
func WithAckGracePeriod(ttl time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.ackGracePeriod = ttl
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

// defaultPoolOptions returns the default options.
func defaultPoolOptions() *nodeOptions {
	return &nodeOptions{
		workerTTL:         30 * time.Second,
		workerShutdownTTL: 2 * time.Minute,
		maxQueuedJobs:     1000,
		ackGracePeriod:    20 * time.Second,
		logger:            pulse.NoopLogger(),
	}
}
