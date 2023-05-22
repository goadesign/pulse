package pool

import "fmt"

// ErrRequeue is the error returned by a worker when starting a job it wants to
// requeue.
var ErrRequeue = fmt.Errorf("requeue")
