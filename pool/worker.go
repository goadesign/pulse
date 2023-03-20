package pool

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"goa.design/ponos/ponos"
	"goa.design/ponos/replicated"
	"goa.design/ponos/streams"
)

type (
	// Worker is a worker that handles jobs with a given payload type.
	Worker struct {
		// Unique worker ID
		ID string
		// Name of worker pool
		Pool string
		// Group is the name of the worker group the worker belongs to.
		Group string
		// C is the channel the worker receives jobs on.
		C <-chan *Job
		// CreatedAt is the time the worker was created.
		CreatedAt time.Time

		c         chan *Job
		lastSeen  *replicated.Map
		workerTTL time.Duration
		jobTTL    time.Duration
		logger    ponos.Logger
	}

	// Job is a job that can be added to a worker.
	Job struct {
		// Key is used to identify the worker that handles the job.
		Key string
		// Payload is the job payload.
		Payload []byte
		// CreatedAt is the time the job was created.
		CreatedAt time.Time
	}
)

// run is the worker loop.
// Cancel ctx to stop the worker.
func (w *Worker) run(ctx context.Context, sink *streams.Sink) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go w.keepAlive(ctx)
	for {
		select {
		case msg, ok := <-sink.C:
			if !ok {
				return
			}
			switch msg.EventName {
			case eventNewJob:
				job, err := unmarshalJob(msg.Payload)
				if err != nil {
					w.logger.Error(fmt.Errorf("failed to unmarshal job: %w", err))
					msg.Ack(ctx)
					continue
				}
				if job.CreatedAt.Add(w.jobTTL).After(time.Now()) {
					w.logger.Error(fmt.Errorf("job %s expired (created %s, payload: %s)", job.Key, job.CreatedAt.Round(time.Second), string(job.Payload)))
					msg.Ack(ctx)
					continue
				}
				w.c <- job
				msg.Ack(ctx)
			case eventShutdown:
				if err := sink.Destroy(ctx); err != nil {
					w.logger.Error(fmt.Errorf("failed to destroy sink: %w", err))
				}
				return
			default:
				w.logger.Error(fmt.Errorf("unexpected event %s", msg.EventName))
			}
		case <-ctx.Done():
			return
		}
	}
}

// keepAlive keeps the worker registration up-to-date until ctx is cancelled.
func (w *Worker) keepAlive(ctx context.Context) {
	ticker := time.NewTicker(w.workerTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := w.lastSeen.Set(ctx, w.ID, strconv.FormatInt(time.Now().UnixNano(), 10)); err != nil {
				w.logger.Error(err)
			}
		case <-ctx.Done():
			return
		}
	}
}
