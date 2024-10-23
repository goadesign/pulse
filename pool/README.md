# Dedicated Worker Pool

The `pool` package builds on top of the Pulse [rmap](../rmap/README.md) and
[streaming](../streaming/README.md) packages to provide scalable and reliable
dedicated worker pools.

## Overview

A *dedicated* worker pool uses a consistent hashing algorithm to assign long
running jobs to workers. Each job is associated with a key and each worker with
a range of hashed values. The pool hashes the job key when the job is dispatched
to route the job to the proper worker.

Workers can be added or removed from the pool dynamically. Jobs get
automatically re-assigned to workers when the pool grows or shrinks. This makes
it possible to implement auto-scaling solutions, for example based on queueing
delays.

Pulse uses the [Jump Consistent Hash](https://arxiv.org/abs/1406.2294) algorithm
to assign jobs to workers which provides a good balance between load balancing
and worker assignment stability.

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    A[Job Producer]
    subgraph Pool["<span style='margin: 0 10px;'>Routing Pool Node</span>"]
        Sink["Job Sink"]
    end
    subgraph Worker[Worker Pool Node]
        Reader
        B[Worker]
    end
    A-->|Job+Key|Sink
    Sink-.->|Job|Reader
    Reader-.->|Job|B

    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef pulse fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;

    class A,B userCode;
    class Pool,Sink,Reader,Worker pulse;

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

## Usage

Job producer:
```go
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	node, err := pool.AddNode(ctx, "example", rdb, pool.WithClientOnly())
	if err != nil {
		panic(err)
	}
	if err := node.DispatchJob(ctx, "key", []byte("payload")); err != nil {
		panic(err)
	}
```

Worker:
```go
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	node, err := pool.AddNode(ctx, "example", rdb)
	if err != nil {
		panic(err)
	}
	handler := &JobHandler{}
	_, err := node.AddWorker(context.Background(), handler)
  if err != nil {
		panic(err)
	}
```

Job handler:
```go
type JobHandler struct {
	// ...
}

// Pulse calls this method to start a job that was assigned to this worker.
func (h *JobHandler) Start(ctx context.Context, key string, payload []byte) error {
	// ...
}

// Pulse calls this method to stop a job that was assigned to this worker.
func (h *JobHandler) Stop(ctx context.Context, key string) error {
	// ...
}
```

### Creating A Pool

The function `AddNode` is used to create a new pool node. It takes as input a
name, a Redis client and a set of options.

[![Pool AddNode](../snippets/pool-addnode.png)](../examples/pool/worker/main.go#L43-L47)

The `AddNode` function returns a new pool node and an error. The pool node
should be closed when it is no longer needed (see below).

The options are used to configure the pool node. The following options are
available:

* `WithClientOnly` - specifies that this node will only be used to dispatch jobs to
  workers in other nodes, and will not run any workers itself.
* `WithLogger` - sets the logger to be used by the pool node.
* `WithWorkerTTL` - sets the worker time-to-live (TTL). This is the maximum duration
  a worker can go without sending a health check before it's considered inactive
  and removed from the pool. If a worker doesn't report its status within this
  time frame, it will be removed, allowing the pool to reassign its jobs to other
  active workers. The default value is 30 seconds.
* `WithWorkerShutdownTTL` - specifies the maximum time to wait for a worker to
  shutdown gracefully. This is the duration the pool will wait for a worker to
  finish its current job and perform any cleanup operations before forcefully
  terminating it. If the worker doesn't shut down within this time, it will be
  forcefully stopped. The default value is 2 minutes.
* `WithMaxQueuedJobs` - sets the maximum number of jobs that can be queued
  before the pool starts rejecting new jobs. This limit applies to the entire
  pool across all nodes. When this limit is reached, any attempt to dispatch
  new jobs will result in an error. The default value is 1000 jobs.
* `WithAckGracePeriod` - sets the grace period for job acknowledgment. If a
  worker doesn't acknowledge starting a job within this duration, the job
  becomes available for other workers to claim. This prevents jobs from being
  stuck if a worker fails to start processing them. The default value is 20
  seconds.

### Closing A Node

The `Close` method closes the pool node and releases all resources associated
with it. It should be called when the node is no longer needed.

[![Pool Close](../snippets/pool-close.png)](../examples/pool/producer/main.go#L31-L36)

Note that closing a pool node does not stop remote workers. It only stops the
local pool node. Remote workers can be stopped by calling the `Shutdown` method
described below.

### Shutting Down A Pool

The `Shutdown` method shuts down the entire pool by stopping all its workers
gracefully. It should be called when the pool is no longer needed.

[![Pool Shutdown](../snippets/pool-shutdown.png)](../examples/pool/worker/main.go#L62-L64)

See the [Data Flows](#data-flows) section below for more details on the
shutdown process.

### Creating A Worker

The function `AddWorker` is used to create a new worker. It takes as input a job
handler object.

[![Worker AddWorker](../snippets/pool-addworker.png)](../examples/pool/worker/main.go#L55-L57)

The job handler must implement the `Start` and `Stop` methods used to start and
stop jobs. The handler may also optionally implement a `HandleNotification`
method to receive notifications.

[![Worker JobHandler](../snippets/worker-jobhandler.png)](worker.go#L59-L71)

The `AddWorker` function returns a new worker and an error. Workers can be
removed from pool nodes using the `RemoveWorker` method.

### Dispatching A Job

The `DispatchJob` method is used to dispatch a new job to the pool. It takes as
input a job key and a job payload.

[![Pool DispatchJob](../snippets/pool-dispatchjob.png)](../examples/pool/producer/main.go#L39-L42)

The job key is used to route the job to the proper worker. The job payload is
passed to the worker's `Start` method.

The `DispatchJob` method returns an error if the job could not be dispatched.
This can happen if the pool is full or if the job key is invalid.

### Notifications

Nodes can send notifications to workers using the `NotifyWorker` method. The method
takes as input a job key and a notification payload.  The notification payload
is passed to the worker's `HandleNotification` method.

### Stopping A Job

The `StopJob` method is used to stop a job. It takes a job key as input and
returns an error if the job could not be stopped. This can happen if the job key
is invalid, the node is closed or the pool shutdown.

## Scheduling

The `Schedule` method of the `Node` struct can be used to schedule jobs to be
dispatched or stopped on a recurring basis. The method takes as input a job
producer and invokes it at the specified interval. The job producer returns
a list of jobs to be started and stopped.

`Schedule` makes it possible to maintain a pool of jobs for example in a
multi-tenant system. See the [examples](../examples/pool) for more details.
