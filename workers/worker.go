package ponos

import (
	"context"
	"fmt"
	"hash"
	"hash/crc64"
	"time"

	redis "github.com/go-redis/redis/v8"
)

type (
	// Worker is a worker that handles jobs with a given payload type.
	Worker struct {
		// Name of worker pool.
		PoolName string
		// Unique worker ID.
		ID string
		// c is the channel the worker receives work on.
		c <-chan *Job

		workerTTL    time.Duration
		jobTTL       time.Duration
		errorHandler ErrorHandler
		rdb          *redis.Client
		h            jumpHash
	}

	// Job is a job that can be added to a worker.
	Job struct {
		// Key is used to identify the worker that handles the job.
		Key string
		// Payload is the job payload.
		Payload []byte
	}

	// workerData is the data stored in the Redis workers set.
	workerData struct {
		ID        string
		ExpiresAt int64
	}

	// jumpHash implement Jump Consistent Hash.
	jumpHash struct {
		h hash.Hash64
	}
)

// Jobs queue key suffix.
const jobsKeySuffix = "jobs"

// Join creates a new worker and adds it to the pool with the given name.
// Cancel ctx to stop the worker.
func Join(ctx context.Context, poolName string, rdb *redis.Client, opts ...WorkerOption) (*Worker, error) {
	options := defaultOptions()
	for _, option := range opts {
		option(&options)
	}
	w := &Worker[T]{
		PoolName:     poolName,
		ID:           options.wid,
		workerTTL:    options.workerTTL,
		jobTTL:       options.jobTTL,
		errorHandler: options.handler,
		rdb:          rdb,
		h:            jumpHash{crc64.New(crc64.MakeTable(crc64.ECMA))},
	}
	if err := w.register(ctx); err != nil {
		return nil, err
	}
	go w.keepAlive(ctx)
	go w.subscribe(ctx)
	return w, nil
}

// HandlesJob returns true if the worker should handle the job with the given key.
func (w *Worker[T]) HandlesJob(ctx context.Context, key string) bool {
	return w.handlesJob(ctx, w.ID, key)
}

// register adds the worker ID to the pool's worker set.
func (w *Worker[T]) register(ctx context.Context) error {
	z := &redis.Z{Score: float64(time.Now().UnixNano()), Member: w.ID}
	if err := w.rdb.ZAdd(ctx, w.PoolName, z).Err(); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}
	return nil
}

// unregister removes the worker ID from the pool's worker set.
func (w *Worker[T]) unregister(ctx context.Context) error {
	if err := w.rdb.ZRem(ctx, w.PoolName, w.ID).Err(); err != nil {
		return fmt.Errorf("failed to unregister worker: %w", err)
	}
	return nil
}

// keepAlive keeps the worker registration up-to-date.
func (w *Worker[T]) keepAlive(ctx context.Context) {
	ticker := time.NewTicker(w.workerTTL / 2)
	defer ticker.Stop()
	z := &redis.Z{Score: float64(time.Now().UnixNano()), Member: w.ID}
	for {
		select {
		case <-ticker.C:
			z.Score = float64(time.Now().UnixNano())
			if err := w.rdb.ZAdd(ctx, w.PoolName, z).Err(); err != nil {
				w.errorHandler.HandleError(ctx, fmt.Errorf("failed to refresh worker registration: %w", err))
			}
		case <-ctx.Done():
			if err := w.unregister(ctx); err != nil {
				w.errorHandler.HandleError(ctx, err)
			}
			return
		}
	}
}

// subscribe subscribes to the jobs Redis channel.
func (w *Worker[T]) subscribe(ctx context.Context) {
	sub := w.rdb.Subscribe(ctx, w.PoolName)
	defer sub.Close()
	for {
		select {
		case msg := <-sub.Channel():
			jobID := msg.Payload
			if !w.HandlesJob(ctx, jobID) {
				return
			}
			if err := w.process(ctx, jobID); err != nil {
				w.errorHandler.HandleError(ctx, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// process handles a job.
func (w *Worker[T]) process(ctx context.Context, jobID string) error {
	res, err := w.rdb.SetNX(ctx, jobID, w.ID, w.jobTTL).Result()
	if err != nil {
		return fmt.Errorf("worker %s failed to handle job %s: %w", w.ID, jobID, err)
	}
	if !res {
		return nil
	}
	var job Job[T]
	if err := w.rdb.Get(ctx, jobID).Scan(&job); err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}
	return nil
}

// handlesJob returns true if the consistent hash of the job key falls into the
// range of this worker.
func (w *Worker[T]) handlesJob(ctx context.Context, workerID, key string) bool {
	if key == BroadcastKey {
		// Broadcast message
		return true
	}
	pipe := w.rdb.TxPipeline()
	rank := pipe.ZRank(ctx, w.PoolName, workerID)
	numWorkers := pipe.ZCard(ctx, w.PoolName)
	_, err := pipe.Exec(ctx)
	if err != nil {
		w.errorHandler.HandleError(ctx, fmt.Errorf("failed to compute if worker %s should handle job %s: %w", w.ID, key, err))
		return false
	}

	return rank.Val() == w.h.Hash(workerID, numWorkers.Val())
}
