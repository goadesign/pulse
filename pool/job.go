package pool

import (
	"context"
	"fmt"
	"time"
)

type (
	// Job is a job that can be added to a worker.
	Job struct {
		// Key is used to identify the worker that handles the job.
		Key string
		// Payload is the job payload.
		Payload []byte
		// CreatedAt is the time the job was created.
		CreatedAt time.Time
		// w is the worker that handles the job.
		w *Worker
	}
)

// Ack acknowledges the job so that it is not requeued.
func (job *Job) Ack(ctx context.Context) {
	pendingJobs := job.w.Pool.pendingJobsMap.Map()
	var pendingJobID string
	var pendingJob *pendingJob
	for id, pj := range pendingJobs {
		upj := unmarshalPendingJob(pj)
		if upj.Key == job.Key {
			pendingJobID = id
			pendingJob = upj
			break
		}
	}
	if pendingJobID == "" {
		job.w.logger.Error(fmt.Errorf("job %s not found in pending jobs map", job.Key))
		return
	}
	pendingJob.Done = true
	mjob := string(marshalPendingJob(pendingJob))
	if _, err := job.w.Pool.pendingJobsMap.Set(ctx, pendingJobID, mjob); err != nil {
		job.w.logger.Error(fmt.Errorf("failed to ack job %s (will be redelivered): %w", job.Key, err))
	} else {
		job.w.logger.Info("acked", "job", job.Key)
	}
}
