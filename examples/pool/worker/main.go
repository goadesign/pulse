package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/pulse/pool"
	"goa.design/pulse/pulse"
)

type (
	// JobHandler is the worker implementation.
	JobHandler struct {
		// lock protects the fields below.
		lock sync.Mutex
		// executions stores the job executions.
		executions map[string]*Execution
		// node is the node the worker is registered with.
		node *pool.Node
		// logctx is the logger context.
		logctx context.Context
	}

	// Execution represents a single execution.
	Execution struct {
		// c is the channel used to stop the execution.
		c chan struct{}
	}
)

func main() {
	// Setup Redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	// Setup clue logger.
	ctx := log.Context(context.Background())
	log.FlushAndDisableBuffering(ctx)

	var logger pulse.Logger
	if len(os.Args) > 1 && os.Args[1] == "-v" {
		logger = pulse.ClueLogger(ctx)
	}

	// Create node for pool "example".
	node, err := pool.AddNode(ctx, "example", rdb,
		pool.WithJobSinkBlockDuration(100*time.Millisecond), // Shutdown faster
		pool.WithLogger(logger),
	)
	if err != nil {
		panic(err)
	}

	// Create a new worker for pool "example".
	handler := &JobHandler{executions: make(map[string]*Execution), node: node, logctx: ctx}
	if _, err := node.AddWorker(ctx, handler); err != nil {
		panic(err)
	}

	// Check if a timeout is set
	var timeout time.Duration
	if t := os.Getenv("TIMEOUT"); t != "" {
		timeout, err = time.ParseDuration(t)
		if err != nil {
			panic(err)
		}
	} else {
		timeout = 10 * time.Minute
	}
	log.Infof(ctx, "timeout set to %s", timeout)

	// Wait for CTRL-C or timeout.
	log.Infof(ctx, "Waiting for jobs... CTRL+C to stop.")
	sigc := make(chan os.Signal, 1)
	defer close(sigc)
	signal.Notify(sigc, os.Interrupt)
	select {
	case <-sigc:
		log.Infof(ctx, "interrupted")
	case <-time.After(timeout):
		log.Infof(ctx, "timeout")
	}

	// Shutdown node.
	if err := node.Shutdown(ctx); err != nil {
		panic(err)
	}
}

// Start starts an execution.
func (w *JobHandler) Start(job *pool.Job) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	exec := &Execution{c: make(chan struct{})}
	w.executions[job.Key] = exec
	go exec.Start(w.logctx, job)
	return nil
}

// Stop stops an execution.
func (w *JobHandler) Stop(key string) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	exec, ok := w.executions[key]
	if !ok {
		return fmt.Errorf("execution for job key %s not found", key)
	}
	close(exec.c)
	delete(w.executions, key)
	return nil
}

// Print notification.
func (w *JobHandler) HandleNotification(key string, payload []byte) error {
	log.Info(w.logctx, log.Fields{"msg": "notification", "key": key, "payload": string(payload)})
	return nil
}

// Start execution.
func (c *Execution) Start(ctx context.Context, job *pool.Job) {
	log.Info(ctx, log.Fields{"msg": "job started", "worker-id": job.Worker.ID, "job": job.Key})
	defer log.Info(ctx, log.Fields{"msg": "job done", "worker-id": job.Worker.ID, "job": job.Key})
	i := 1
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.c:
			return
		case <-ticker.C:
			i++
			log.Info(ctx, log.Fields{"msg": "tick", "worker-id": job.Worker.ID, "job": job.Key, "i": i})
		}
	}
}
