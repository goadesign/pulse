# Tenanted Worker Pools

Ponos builds on top of its streaming and replicated map capabilities to provide
a scalable and reliable tenanted worker pools.

## Overview

A *tenanted* worker pool is a collection of workers that process jobs where each
worker is assigned a range of job keys. Jobs are distributed to workers based on
the job key and a consistent hashing algorithm.

Workers can be grouped into worker groups. Each worker group is identified by a
unique name and is assigned a range of job keys. This makes is possible to scale
dynamically the number of workers assigned to a given job key range.

Workers and worker groups can be added or removed from the pool dynamically.
Ponos minimizes the number of job re-assignments when the worker pool grows or
shrinks.  This makes it possible to implement auto-scaling solutions, for
example based on queueing delays.

## Example

A worker pool named "fibonacci" is created and a worker is added to it. The
worker starts consuming jobs.

`worker.go`

```go
package main

import (
    "encoding/binary"

    redis "github.com/redis/go-redis/v9"
    "goa.design/ponos"
)

func main() {
    rdb := redis.NewClient()

    // Stop processing jobs after 10 seconds.
    ctx := context.WithDeadline(contdxt.Background(), time.Now().Add(10*time.Second))

    // Connect to or create pool "fibonacci".
    pool, err := ponos.Pool("fibonacci", rdb)
    if err != nil {
        panic(err)
    }

    // Create a new worker for pool "fibonacci".
    worker, err := pool.NewWorker(ctx)
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

    redis "github.com/redis/go-redis/v9"
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
    if err := pool.NewJob(context.Background(), "key", payload); err != nil {
        panic(err)
    }
}
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
