package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"goa.design/ponos/pool"
)

type (
	// JobHandler is the worker implementation.
	JobHandler struct {
		// lock protects the fields below.
		lock sync.Mutex
		// executions stores the job executions.
		executions map[string]*Execution
	}

	// Execution represents a single execution.
	Execution struct {
		// c is the channel used to stop the execution.
		c chan struct{}
	}
)

// Start starts an execution.
func (w *JobHandler) Start(ctx context.Context, job *pool.Job) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	exec := &Execution{c: make(chan struct{})}
	w.executions[job.Key] = exec
	go exec.Start(job)
	return nil
}

// Stop stops an execution.
func (w *JobHandler) Stop(ctx context.Context, key string) error {
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
func (w *JobHandler) HandleNotification(ctx context.Context, key string, payload []byte) error {
	fmt.Printf(">> Notification: %s\n", string(payload))
	return nil
}

// Start execution.
func (c *Execution) Start(job *pool.Job) {
	defer fmt.Printf("Worker %s, Job %s, Done\n", job.Worker.ID, job.Key)
	i := 1
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.c:
			return
		case <-ticker.C:
			i++
			fmt.Printf(">> Worker %s, Job %s, Iteration %d\n", job.Worker.ID, job.Key, i)
		}
	}
}
