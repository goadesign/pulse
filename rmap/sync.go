// Keeping a replica's local copy current.
//
// Every write runs a Lua script that stamps the map with a generation and a
// revision and publishes the change with both (see package rmapwire). A replica
// applies published changes of its generation in revision order. It reloads
// the whole map from Redis instead when a change comes from another
// generation (the map's data expired and was recreated) or skips a revision
// (a notification was lost), when go-redis resubscribes after a lost
// connection, and whenever Sync or SetAndWait is called.

package rmap

import (
	"context"
	"fmt"
)

// Sync makes the local copy include every write that completed before Sync
// was called, including writes made through other replicas, so the reads that
// follow (Get, GetValues, Keys, Len, Map) reflect them. It reads the map from
// Redis once and replaces the local copy when Redis holds a newer revision or
// another generation; an expired map becomes empty. Subscribers are notified
// when the copy changes. Sync writes nothing.
//
// Use Sync before a decision that must not act on a value another replica has
// already replaced, for example when a request that follows a completed write
// may be served by a different process. Sync orders reads after completed
// writes; it does not stop other processes writing to Redis, so a write still
// in progress when Sync starts may or may not be included. While it reads,
// Sync holds this replica's lock, so local reads wait for it. Sync costs one
// read of the whole map.
//
// Sync returns an error when the map is stopped or Redis cannot be read.
func (sm *Map) Sync(ctx context.Context) error {
	return sm.refresh(ctx)
}

// refresh replaces the local copy with the map's current content in Redis
// unless the copy already holds exactly that generation and revision. It holds
// the map lock from before the Redis read until the copy is replaced, so no
// published change can be applied in between: every change the copy holds was
// already in Redis when the snapshot was read, and the snapshot is never older
// than the copy. A snapshot with a lower revision in the same generation means
// the map's data is gone (for example a map written by an older Pulse, whose
// hash carries no generation, that then expired), so it is applied too.
func (sm *Map) refresh(ctx context.Context) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	if sm.closing {
		return fmt.Errorf("pulse map: %s is stopped", sm.Name)
	}
	content, err := sm.rdb.HGetAll(ctx, sm.hashkey).Result()
	if err != nil {
		return fmt.Errorf("pulse map: %s failed to read content: %w", sm.Name, err)
	}
	rev, kind, gen := extractState(content)
	if gen == sm.gen && rev == sm.rev {
		return nil
	}
	if gen != sm.gen || rev != sm.rev+1 {
		// The stored kind describes only the last change. When the copy moves
		// to another generation, skips several changes, or loses data, the
		// content is replaced wholesale, which subscribers see as a reset.
		kind = EventReset
	}
	sm.content, sm.rev, sm.gen = content, rev, gen
	sm.notifySubscribers(kind)
	return nil
}

// reload refreshes the local copy for the run loop. When Redis cannot be
// read the copy keeps what it has: if changes were missed, the next published
// change skips a revision and triggers another reload; if none were, the next
// change applies normally.
func (sm *Map) reload(reason string) {
	if err := sm.refresh(sm.closectx); err != nil {
		sm.logger.Error(fmt.Errorf("failed to reload after %s: %w", reason, err))
		return
	}
	sm.logger.Info("reloaded", "reason", reason)
}
