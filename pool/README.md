# Tenanted Worker Pools

The `pool` package builds on top of the Ponos `streams` and `replicated`
packages to provide a scalable and reliable dedicated worker pools.

## Overview

A *dedicated* worker pool is a collection of workers that process jobs where each
worker is assigned a range of job keys. Jobs are distributed to workers based on
the job key and a consistent hashing algorithm.

Workers can be added or removed from the pool dynamically. Jobs get
automatically re-assigned to workers when the pool grows or shrinks. This makes
it possible to implement auto-scaling solutions, for example based on queueing
delays.

Ponos uses the [Jump Consistent Hash](https://arxiv.org/abs/1406.2294) algorithm
to assign jobs to workers which provides a good balance between load balancing
and worker assignment stability.

## Usage and Trade-offs

In general Ponos dedicated worker pools are useful whenever workers need to be
stateful and their state is dependent on the jobs. For example when workers need
to maintain a connection to a remote service or when they need to maintain a
local cache.

The target use case is one where the number of jobs is larger than the number of
workers and where the job payloads should be small as they are stored in memory.

## Example

The following example creates a worker pool named "fibonacci", the job payloads
consists of a single 64-bit unsigned integer. The worker computes the
[Fibonacci number](https://en.wikipedia.org/wiki/Fibonacci_number) of the
payload and prints it to stdout.

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

    // Stop processing jobs after 5 seconds.
    ctx := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))

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

`producer.go`

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

## Data Flows

The following diagram illustrates the data flow involved in adding a new job to
a Ponos worker pool:
* The producer calls `DispatchJob` which adds an event to the pool job stream.
* The pool job stream is read by the pool sink which creates an entry in the
   pending jobs map and adds the job to the dedicated worker stream.
* The dedicated worker stream is read by the worker which adds the job to its
  channel.
* The worker user code reads the job from its channel and processes it.
* The worker marks the job as completed in the pending jobs map.
* The pool sink gets notified of the completed job, acks the job from the
  pool job stream and removes it from the pending jobs map.


```mermaid
%%{ init: { 'flowchart': { 'curve': 'monotoneX' } } }%%
%%{init: {'themeVariables': { 'edgeLabelBackground': '#7A7A7A'}}}%%
flowchart LR
    subgraph p[Producer Process]
        pr[User code] --1. DispatchJob--> po[Pool]
        ps
    end
    subgraph w[Worker Process]
        r[Reader] --7. Add Job--> c[[Worker Channel]]
        c -.Job.-> u[User code]
    end
    subgraph rdb[Redis]
        r --8. Mark Job Complete--> pj
        pj -.9. Notify.-> ps
        ps --11. Delete Pending Job--> pj
        po --2. Add Job--> js([Pool Job Stream])
        ps --10. Ack Job--> js
        js -.3. Job.-> ps[Pool Sink]
        ps --4. Set Pending Job--> pj[(Pending Jobs <br/> Replicated Map)]
        ps --5. Add Job--> ws(["Worker Stream (dedicated)"])
        ws -.6. Job.-> r
    end
    
    classDef userCode fill:#9A6D1F, stroke:#D9B871, stroke-width:2px, color:#FFF2CC;
    classDef producer fill:#2C5A9A, stroke:#6B96C1, stroke-width:2px, color:#CCE0FF;
    classDef ponos fill:#25503C, stroke:#5E8E71, stroke-width:2px, color:#D6E9C6;
    classDef background fill:#7A7A7A, color:#F2F2F2;

    class pr,u userCode;
    class pj,js,ws ponos;
    class po,ps,r,c producer;
    class p,w,rdb background; 

    linkStyle 0 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 1 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 2 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 3 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 4 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 5 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 6 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 7 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 8 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 9 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 10 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
    linkStyle 11 stroke:#DDDDDD,color:#DDDDDD,stroke-width:3px;
```

The worker pool uses a job stream so that jobs that do not get acknowledged in time
are automatically re-queued. This is useful in case of worker failure or
network partitioning. The pool sink applies the consistent hashing algorithm
to the job key to determine which worker stream the job should be added to. This
ensures that unhealthy workers are properly ignored when requeuing jobs.

## Failure Modes And Recovery

### Worker Failure

When a worker fails, the jobs it was processing are re-assigned to other
workers. The worker will automatically reconnect to the pool and resume
processing jobs.

### Redis Failure

When Redis fails, the worker pool will automatically reconnect to Redis and
resume processing jobs.


