package pool

import (
	"github.com/google/uuid"
)

type (
	// WorkerOption is a worker creation option.
	WorkerOption func(*workerOption)

	workerOption struct {
		group          string
		jobChannelSize int
	}
)

// WithWorkerGroup sets the worker group.  Ponos generates a random group ID if
// not set.
func WithWorkerGroup(group string) WorkerOption {
	return func(o *workerOption) {
		o.group = group
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
		group:          uuid.NewString(),
		jobChannelSize: 100,
	}
}
