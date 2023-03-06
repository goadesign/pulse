# Tenanted Worker Pools

Ponos builds on top of its streaming capabilities to provide a scalable and
reliable tenanted worker pool.

Each Job enqueued on a tenanted worker pool is associated with a key and each
worker is assigned a range of job keys. The matching algorithm uses a consistent
hash scheme which distributes job keys to workers.

Workers can be added or removed from the pool dynamically. Ponos minimizes the
number of job re-assignments when the worker pool grows or shrinks.  This makes
it possible to implement auto-scaling solutions, for example based on queueing
delays.

## Example

A worker pool named "fibonacci" is created and a worker is added to it. The
worker is assigned a range of job keys and starts consuming jobs.

`worker.go`

```go
package main

import (
    "encoding/binary"

    redis "github.com/go-redis/redis/v8"
    "goa.design/ponos"
)

func main() {
    rdb := redis.NewSink()
    ctx := context.Background()

    // Connect to or create worker for pool "fibonacci".
    pool, err := ponos.Pool("fibonacci", rdb)
    if err != nil {
        panic(err)
    }

    // Create a new worker for pool "fibonacci", worker IDs must be unique.
    worker, err := pool.Worker(ctx, "my-worker")
    if err != nil {
        panic(err)
    }

    // Handle jobs
    for job := range worker.Jobs(ctx) {
        n := binary.BigEndian.Uint64(job.Payload)
        fmt.Printf("fib(%d)=%d\n", n, fib(n))
    }
}

func fib(n uint64) uint64 {
    if n <= 1 {
        return n
    }
    return fib(n-1) + fib(n-2)
}
```

A new job is enqueued on the pool. The job is assigned to the worker created
above.

`client.go`

```go
package main

import (
    "encoding/binary"

    redis "github.com/go-redis/redis/v8"
    "goa.design/ponos"
)

func main() {
    rdb := redis.NewClient()

    // Get pool named "fibonacci"
    pool, err := ponos.Pool("fibonacci", rdb)
    if err != nil {
        panic(err)
    }

    // Queue new job with key "key" and payload 42 
    payload := make([]byte, 8)
    binary.BigEndian.PutUint64(payload, 42)
    if err := pool.Enqueue(context.Background(), "key", payload); err != nil {
        panic(err)
    }
}
```

## Custom Job Assigment Logic

Ponos provides a default job assignment algorithm which uses a consistent hash
scheme to distribute job keys to workers. This algorithm is suitable for most
use cases but it is possible to provide a custom assignment algorithm.

```go
pool, err := ponos.Pool("fibonacci", rdb, ponos.WithAssignment(
    func(key string, workers []*ponos.Worker) uint {
        // Always assign job to the first worker
        return 0
    },
))
```

The list of workers provided to the assignment function is sorted by worker
creation time (oldest first).

The worker ID is available in the assignment function:

```go
pool, err := ponos.Pool("fibonacci", rdb, ponos.WithAssignment(
    func(key string, workers []*ponos.Worker) uint {
        // Always assign job to "my-worker"
        for i, w := range workers {
            if w.ID == "my-worker" {
                return i
            }
        }
        return 0
    },
))
```

## Use Cases

In general Ponos tenanted worker pools are useful whenever workers need to
be stateful and their state is dependent on the jobs. For example when workers
need to maintain a connection to a remote service or when workers need to
maintain a local cache.

### Multitenant Background Jobs

In this use case a multitenant system needs to maintain a set of background
jobs that run in each tenant. Such a system can create a Ponos worker pool and
create one job per tenant using the unique tenant identifier as job key. This
guarantees that one and only one worker performs the background job for a given
tenant at a given time. Jobs can be added or removed as new tenants are
created or old tenants are deprovisioned. Workers can be added or removed as
well depending on performance requirements. A "controller" job can also be
created which periodically ensures that all tenants have a dedicated job (and
removes any stale job) for eventual consistency.

See the background job example for more details.

### Reverse Request Routing

Another interesting use case is one where many clients create persistent
connections to a remote service (via WebSocket, long polling, gRPC
bi-directional streaming etc.) and where the service needs to route "requests"
to specific clients (depending on the underlying transport these could actually
be responses, e.g. with long polling). The service exposes a load-balanced set
of endpoints that clients connect to, each endpoint registers a worker in a
Ponos worker pool. When the service needs to make a request to a specific client
it creates a job that uses the unique client id as key. In this case the Ponos
worker pool is configured to assign jobs to workers based on the set of client
IDs each worker is connected to rather than the default consistent hashing
algorithm.

See the reverse request routing example for more details.

## Key Data Flows

This section describes the data flows that occur when a worker joins a pool and
when a client enqueues a job.

### Worker

When a worker joins a pool, it:

1. adds its ID to the sorted sets named `ponos:pool:<pool-name>:workers` and
   `ponos:pool:<pool-name>:keep-alives` using the current time as score.
2. starts a background goroutine that periodically (every 5 seconds) updates
   the score of its ID in the keep alives sorted set to the current time.
3. subscribes to the stream `ponos:pool:<pool-name>:jobs-stream`
4. subscribes to the channel `ponos:pool:<pool-name>:rebalance`
5. sends a message to the channel `ponos:pool:<pool-name>:rebalance`.

Upon receiving a message from the jobs stream, the worker:

1. retrieves the cardinality of the workers set and its rank in it then uses the
   Jump Consistent Hash algorithm to determine if it should handle the job.
2. if so, the worker attempts to lease the job by setting the `worker` field of
   the job hash to its ID and the `lease` field to the current time. The worker
   may fail to lease the job if another worker has already leased it less than
   10 seconds ago.
3. if the job is leased by the worker, it sends the job to the `Jobs` channel
4. then removes the job from the jobs stream.

Upon receiving a message from the rebalance channel, the worker:

1. iterates through all the jobs in the stream and determines if it should
   handle the job. 
2. if so, the worker attempts to lease the job using the same algorithm as
   above.
3. if the job is leased by the worker, it sends the job to the `Jobs` channel.
4. once the job is acked by the client, the worker deletes the job hash.

Notes:

* The `rebalance` messages are sent by workers when they join a pool or by clients
  when they remove inactive workers from the pool. They are necessary to account
  for race conditions between workers receiving job notifications via the jobs
  channel and the worker pool growing and shrinking (two workers receiving the
  notification may read different values for the workers set cardinality and
  rank).
* The Jump Consistent Hash algorithm is used to ensure that the number of jobs
  that need to be rebalanced is minimized when the worker pool grows or shrinks.

### Client

When a client joins a pool, it starts a background goroutine that periodically
(every 5 seconds) removes workers from the sorted set that have not been updated
in the last 10 seconds. Upon removal, the client sends a `rebalance` message to
the `ponos:pool:<pool-name>:rebalance` channel.

When a client enqueues a job, it sends the job to the stream named
`ponos:pool:<pool-name>:jobs-stream`
