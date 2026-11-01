// Sync makes a replica's local copy catch up with writes made through other
// replicas, and waitForRevision is the revision wait it shares with
// SetAndWait.
//
// Every write to a replicated map runs a Lua script that increments the map's
// revision counter (the "=rev" field of its Redis hash) and publishes the
// change with that revision. Each replica applies published changes in
// revision order and records the highest revision applied, so "this copy has
// applied revision R" means it includes every write numbered R or lower.
// Waiting for a revision therefore needs no coordination with other replicas.

package rmap

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Sync waits until the local copy includes every write that completed before
// Sync was called, including writes made through other replicas, so the reads
// that follow (Get, GetValues, Keys, Len, Map) reflect them. It reads the
// map's current revision from Redis once and waits until the local copy has
// applied it; it writes nothing.
//
// Use Sync before a decision that must not act on a value another replica has
// already replaced, for example when a request that follows a completed write
// may be served by a different process. Writes still in progress when Sync
// starts may or may not be included; Sync orders reads after completed
// writes, it does not lock the map.
//
// Sync returns immediately when the map has never been written. It returns an
// error when the context ends first, the map is stopped, or Redis cannot be
// read. A replica that lost its subscription reloads the whole map when it
// reconnects, which also completes a pending Sync.
func (sm *Map) Sync(ctx context.Context) error {
	sm.lock.RLock()
	closing := sm.closing
	sm.lock.RUnlock()
	if closing {
		return fmt.Errorf("pulse map: %s is stopped", sm.Name)
	}
	raw, err := sm.rdb.HGet(ctx, sm.hashkey, revField).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("pulse map: %s failed to read revision: %w", sm.Name, err)
	}
	rev, ok := parseRevisionString(raw)
	if !ok {
		return fmt.Errorf("pulse map: %s has invalid revision %q", sm.Name, raw)
	}
	return sm.waitForRevision(ctx, rev)
}

// waitForRevision blocks until the local copy has applied rev or a later
// revision. It returns immediately when the copy is already there, and an
// error when ctx ends or the map stops first. The run loop and the reconnect
// path release waiters through notifyWaitersUpTo as they apply revisions.
func (sm *Map) waitForRevision(ctx context.Context, rev uint64) error {
	notifyCh := make(chan struct{}, 1)
	waitCtx, cancel := context.WithCancel(ctx)
	waiter := &revWaiter{
		ch:     notifyCh,
		rev:    rev,
		ctx:    waitCtx,
		cancel: cancel,
	}
	defer func() {
		cancel()
		sm.removeWaiter(waiter)
	}()
	if sm.registerWaiter(waiter) {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sm.done:
		return fmt.Errorf("pulse map: %s is stopped", sm.Name)
	case <-notifyCh:
		return nil
	}
}
