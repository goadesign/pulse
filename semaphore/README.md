# Leased Semaphores

The `semaphore` package provides Redis-backed leased semaphores for bounding
concurrent work across a fleet of processes.

Use a semaphore when work is allowed to run on any node, but only a fixed number
of workers should run it at the same time. Each successful acquire returns a
lease. Releasing that lease frees the slot and wakes another waiter. If the
process holding a lease dies, the lease expires in Redis and the slot becomes
available again.

## Usage

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

sem, err := semaphore.New(rdb, "billing-sync", 3)
if err != nil {
	panic(err)
}

lease, err := sem.Acquire(ctx)
if err != nil {
	panic(err)
}
defer func() {
	if err := lease.Release(context.Background()); err != nil {
		panic(err)
	}
}()

// Run work while the lease is held. If the lease is lost, its context is
// canceled so the protected work can stop promptly.
select {
case <-lease.Context().Done():
	return lease.Err()
default:
	return runBillingSync(ctx)
}
```

## Lease Behavior

Semaphore leases are renewed in the background until `Release` is called or
Redis no longer recognizes the holder token. `Lease.Context` is canceled when
the caller's acquire context is canceled, the lease is released, or heartbeat
renewal fails.

Tune `WithLeaseTTL` for the maximum crash-recovery delay and
`WithHeartbeatInterval` for how often active holders renew their lease. The
heartbeat interval must be shorter than the lease TTL.
