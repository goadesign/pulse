package pool

import "errors"

// ErrRequeue indicates that a worker failed to process a job's start or stop operation
// and requests the job to be requeued for another attempt.
var ErrRequeue = errors.New("requeue")
