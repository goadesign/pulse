package pool

import "fmt"

// errRequeue is the error returned by a worker when starting a job it wants to
// requeue.
var errRequeue = fmt.Errorf("requeue")
