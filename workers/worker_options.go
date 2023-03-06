package ponos

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type (
	// WorkerOption is a worker creation option.
	WorkerOption func(*option)

	// Interface used by Ponos to report temporary errors.
	ErrorHandler interface {
		HandleError(context.Context, error)
	}

	option struct {
		wid       string
		workerTTL time.Duration
		leaseTTL  time.Duration
		handler   ErrorHandler
	}

	// noopHandler is a no-op error handler.
	noopHandler struct{}
)

// WithWorkerTTL sets the duration after which the worker is removed from the pool in
// case of network partitioning.  The default is 10s.
func WithWorkerTTL(ttl time.Duration) WorkerOption {
	return func(o *option) {
		o.workerTTL = ttl
	}
}

// WithLeaseTTL sets the duration after which the job is made available to other
// workers if it wasn't acked. The default is 5s.
func WithLeaseTTL(ttl time.Duration) WorkerOption {
	return func(o *option) {
		o.leaseTTL = ttl
	}
}

// WithErrorHandler sets the handler used to report temporary errors.
func WithErrorHandler(handler ErrorHandler) WorkerOption {
	return func(o *option) {
		o.handler = handler
	}
}

// defaultWorkerOptions returns the default options.
func defaultWorkerOptions() option {
	return option{
		wid:       uuid.New().String(),
		workerTTL: 10 * time.Second,
		leaseTTL:  5 * time.Second,
		handler:   &noopHandler{},
	}
}

// no-op Error logging.
func (l *noopHandler) HandleError(_ context.Context, _ error) {}
