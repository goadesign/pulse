# rmap redesign proposal (WIP)

This document proposes a redesign of `rmap` driven by a careful review of the current implementation and its use in `pool` and `streaming`.

The goal is to preserve the spirit of `rmap` (fast local reads, Redis-backed writes, change notifications) while making the system *correct under concurrency*, *robust under Redis restarts / disconnects*, and *explicit about semantics*.

## Executive summary

`rmap` today is best described as a **local cache kept warm by Redis Pub/Sub**, not a fully reliable replicated state machine.

The largest issues are:
- **Correctness/liveness bugs** that can hang `Close()` or deadlock with reconnects.
- **Event loss → permanently stale local state** because Pub/Sub is not durable and reconnect does not resync.
- **Redis script fragility** (`EVALSHA` without `NOSCRIPT` fallback).
- **Ambiguous APIs and inconsistent validation** (notably: empty string vs missing key).
- **Broken “array” semantics** and unsafe CSV encoding for list/set helpers.

The proposal is split into phases:
1. Fix correctness/liveness bugs and Redis-script robustness with minimal behavior change.
2. Make replication semantics defensible: resync on reconnect, and optionally add a monotonic revision to detect/avoid stale state.
3. Improve API surface: return explicit booleans (`existed`, `swapped`, `deleted`) and offer structured events.
4. Replace CSV list encoding with a robust representation (prefer Redis `cjson`-encoded arrays) or move list/set helpers into dedicated replicated types.

## Current behavior (as implemented)

- State is stored in Redis as a hash: `map:<name>:content`.
- Updates are broadcast via Pub/Sub: `map:<name>:updates`.
- Writes are executed via Lua scripts that perform `HSET/HDEL/DEL` and `PUBLISH` atomically.
- Each node keeps an in-memory `map[string]string` updated only by consuming Pub/Sub messages.
- `Subscribe()` provides a buffered “something changed” signal (no deltas).
- `SetAndWait()` registers an internal waiter and waits until the local consumer loop observes the corresponding `set` event.

## Critique (problems to address)

### 1) Liveness and shutdown correctness

1. **`Close()` can hang forever** because `run()` does not guarantee `sm.wait.Done()` on all exit paths.
   - `run()` calls `sm.wait.Done()` only in the `<-sm.done` case (`rmap/map.go:688`).
   - There is an unconditional `return` in the waiter notification path (`rmap/map.go:675-677`) that bypasses cleanup and `wait.Done()`.
   - Any panic inside `run()` (including type assertion panics) will be recovered by `pulse.Go` and will also bypass `wait.Done()`, hanging `Close()`.

2. **Reconnect holds `sm.lock` while doing network I/O** (`rmap/map.go:736-750`).
   - This blocks all readers (`Get/Map/Keys/Len/Subscribe`) for the duration of dial timeouts/retry loops.
   - More importantly, it can deadlock `Close()`: `Close()` needs `sm.lock` to set `sm.closing=true` (`rmap/map.go:529`), but reconnect can hold the lock while blocked in `Receive()`.

### 2) Consistency: Pub/Sub loss → stale local state

Redis Pub/Sub is best-effort. If a node disconnects, is slow, or the client drops messages, **events can be lost**.

Today, on reconnect `rmap` only resubscribes (`rmap/map.go:741-750`) and continues. It does **not** resync the hash content, so a node can remain permanently stale.

This is especially risky because other packages (notably `pool`) use local `Get/Map/Keys` results for coordination and correctness (e.g. job existence checks).

### 3) Redis-script robustness (`NOSCRIPT`)

`rmap` pre-loads scripts (`rmap/map.go:558-575`) but executes with `EvalSha` (`rmap/map.go:722`).

If Redis restarts, `SCRIPT FLUSH` is run, or a failover happens, the server script cache is lost and subsequent writes fail with `NOSCRIPT`. `rmap` does not retry with `EVAL` or reload.

### 4) API semantics and validation mismatches

1. Methods returning “previous value” conflate **missing key** with **empty string value** (`Set/TestAndSet/Delete/TestAndDelete` return `""` when Redis returned `nil`).
   - `rmap` explicitly supports storing empty strings (see `rmap/map_test.go:269`), so callers cannot safely infer existence.
   - For CAS-style operations this makes “did it swap?” ambiguous when `test == ""`.

2. `TestAndReset` documentation claims per-key validation (“any key is empty/contains '='”), but implementation validates only the sentinel `"*"` (`rmap/map.go:510-523` → `runLuaScript` validates `args[0]` only).
   - It also does not check `len(keys) == len(tests)`, which can produce incorrect behavior in Lua (`rmap/scripts.go:157-170`).

3. `Join` does not validate `rdb != nil` (`rmap/map.go:93`).

### 5) “Array” helpers are unsafe and semantically broken

`AppendValues`, `AppendUniqueValues`, `RemoveValues`, `GetValues` operate on comma-separated strings.

Problems:
- **No escaping**: values containing `,` cannot round-trip (this affects `pool` job keys and any user-provided strings).
- `luaRemove` uses a set (`curr` map) and `pairs()` iteration (`rmap/scripts.go:90-121`), so it **reorders** values and **drops duplicates**, contradicting the “array/list” API name.
- `luaRemove` publishes a `set` even when `removed == false` and nothing changed, due to always re-writing the value (`rmap/scripts.go:110-121`).
- `luaAppend` produces a leading comma when the existing value is `""` (`rmap/scripts.go:12`), creating surprising results.
- Empty `items` arguments lead to odd behavior (e.g. creating a key with `""` for unique append).

### 6) Encoding portability: `struct.pack("i")` vs Go decoding

Lua uses `struct.pack("ic0...")` and Go decodes lengths with `binary.LittleEndian.Uint32` (`rmap/map.go:763-774`).
This assumes 4-byte little-endian `i`, which is very likely on common platforms, but it is not a contract. It is better to make the encoding explicit and self-describing.

## Design goals

1. **No deadlocks / no hangs**: `Close()` must always return; reconnect must not block unrelated readers indefinitely; goroutine lifecycle must be well-defined.
2. **Defensible consistency**:
   - Best-effort Pub/Sub is acceptable only if the implementation *self-heals* (resync) after loss.
   - Optionally, provide stronger detection/ordering via a monotonic revision.
3. **Script execution robustness** across Redis restarts/failovers (`NOSCRIPT`).
4. **Clear, explicit API semantics**: distinguish “missing” from “empty”; CAS operations should return “swapped?”; delete should return “deleted?”.
5. **Correct collection helpers** or remove them from `rmap`:
   - If kept, they must round-trip arbitrary strings and have stable, documented semantics.
6. **Minimal overhead and ergonomic usage** for existing call sites (`pool`, `streaming`).

## Proposal

### Phase 1: correctness + robustness fixes (minimal behavior change)

1. **Make `run()` lifecycle safe**
   - Ensure `sm.wait.Done()` is executed exactly once on any exit path by using `defer sm.wait.Done()` at the top of `run()`.
   - Remove the `return` inside the waiter notification select (`rmap/map.go:675-677`). If `sm.done` is closed, break out to the normal shutdown path so cleanup always runs.
   - Consider making shutdown cleanup idempotent and guarded so it can run from a deferred function.

2. **Fix reconnect to avoid deadlocks**
   - Do not hold `sm.lock` during `Subscribe()/Receive()` calls.
   - Use a dedicated `subMu` (or atomics) for swapping `sm.sub`/`sm.msgch`, separate from the content lock.
   - Add a cancellation context derived from map lifetime so reconnect attempts stop promptly on `Close()`.

3. **Handle `NOSCRIPT`**
   - Replace `EvalSha` with `script.Run` from `go-redis`, or explicitly retry on `NOSCRIPT` by calling `script.Eval` / `Load`.
   - Reload scripts after reconnect as a defensive measure (still handle `NOSCRIPT` at call sites).

4. **Validate inputs consistently**
   - `Join`: error if `rdb == nil`.
   - `TestAndReset`: validate `len(keys) == len(tests)`; validate each key (non-empty, no `'='`, and whatever additional constraints are desired).
   - Decide and document key/value constraints for list helpers (see Phase 4).

5. **Clarify docs**
   - Update `rmap/README.md` language: Pub/Sub does not guarantee delivery; the guarantee is “eventual consistency while connected; self-healing after reconnect”.
   - Document that `Set()` does not make local reads immediately reflect the write (unless `SetAndWait` is used).

### Phase 2: self-healing replication (Pub/Sub + resync)

Add a `resync()` method that fetches the full hash and atomically replaces `sm.content`:

- On initial join: keep current ordering (subscribe → HGETALL → start loop).
- On reconnect: **resubscribe → resync → notify subscribers**.

Key behaviors:
- **Resync always happens after reconnect**. Any missed Pub/Sub messages are repaired by the snapshot.
- After resync, emit `EventReset` (or a new `EventResync`) so callers that care can react.
- Consider adding a periodic resync option (e.g., every N minutes) to self-heal from silent message drops.

This phase makes the existing architecture *operationally reliable* without switching away from Pub/Sub.

### Phase 3: add a monotonic revision (optional but recommended)

To make ordering/idempotency explicit and reduce subtle race risk around resync:

1. Introduce a Redis key: `map:<name>:rev` (string integer).
2. Modify write scripts to:
   - increment `rev` (e.g., `INCR`) inside the script,
   - include the revision in the published message (e.g., `set:<rev>:<packed>` or binary-packed `{rev,key,value}`).
3. In Go:
   - track `sm.rev` in memory,
   - ignore messages with `rev <= sm.rev` (duplicate/out-of-order),
   - in `resync()`, fetch both `HGETALL` and `GET rev` (ideally in one Lua script) and set `(content, rev)` atomically under lock.

This yields a simple invariant:
> Local state corresponds to a known revision; processing is monotonic.

It also enables a general `WaitForRevision(ctx, rev)` which can replace ad-hoc per-key waiter lists and makes `SetAndWait` more robust.

### Phase 4: fix or replace list/set helpers

Two viable approaches:

#### A) Keep helpers but change encoding to JSON arrays (recommended)

Use Redis Lua `cjson` to store array values as JSON arrays (`["a","b"]`) instead of CSV.

- Update Lua scripts:
  - `append`: decode current JSON (or treat missing as `[]`), append items (allow duplicates if desired), encode back.
  - `appendUnique`: append only new items.
  - `remove`: remove occurrences (preserve relative order), delete key if resulting array empty, and **publish only if changed**.
- Backward compatibility:
  - If the stored value is not valid JSON, fall back to legacy CSV parsing, then re-encode as JSON when writing back.
  - Update `GetValues` to detect JSON vs CSV and return the correct slice.

This immediately fixes:
- comma safety,
- ordering stability,
- duplicate handling (by definition),
- spurious notifications on no-op removals.

#### B) Split into dedicated replicated types

Move these helpers out of `rmap.Map`:
- `rset` backed by Redis sets,
- `rlist` backed by Redis lists (or streams),
each with their own replication strategy and explicit semantics.

This is cleaner long-term but a larger change for call sites (`pool`, `streaming`).

### Phase 5: improve API semantics (non-breaking additions)

Avoid breaking existing APIs abruptly; add “v2” methods and gradually migrate call sites.

Suggested additions:
- `SetEx(ctx, key, value) (prev string, existed bool, err error)`
- `DeleteEx(ctx, key) (prev string, existed bool, err error)`
- `TestAndSetEx(ctx, key, test, value) (prev string, existed bool, swapped bool, err error)`
- `TestAndDeleteEx(ctx, key, test) (prev string, existed bool, deleted bool, err error)`
- `SubscribeEvents() <-chan Event` where `Event` includes `{Kind, Key, Value, Revision}` (Value optional).

Keep existing methods as thin wrappers for compatibility, but document their ambiguity.

### Phase 6: (optional) durable event transport via Redis Streams

If consumers ever need “no missed transitions” semantics (not just “eventual state”), Pub/Sub is the wrong transport.

A potential `rmap` v3 would:
- `XADD` every change to a stream `map:<name>:events` (capped with `MAXLEN ~`),
- have each node track its own last-seen ID (in memory and optionally persisted),
- use `XREAD` to catch up after reconnect (no missed events as long as retention is sufficient),
- still store the authoritative state in the hash for snapshotting.

This is more complex operationally and should be introduced only if required.

## Migration and rollout plan

1. Land Phase 1 fixes (safe, mostly internal).
2. Add Phase 2 resync-on-reconnect; emit a `EventReset` on resync.
3. Add Phase 3 revision support behind an option (e.g., `WithRevisioning()`), measure overhead.
4. Migrate list helpers to JSON encoding with backwards compatibility (Phase 4A).
5. Introduce new API methods (Phase 5), migrate internal packages first (`pool`, `streaming`), then deprecate ambiguous methods over time.

## Test plan

Add targeted tests in `rmap`:
- `Close()` does not hang even if:
  - called while processing a message,
  - called while `SetAndWait` waiters exist,
  - called during reconnect loops.
- Reconnect resync:
  - simulate disconnect, perform writes while disconnected, reconnect, assert local state matches Redis.
- `NOSCRIPT` behavior:
  - `SCRIPT FLUSH`, then verify writes still succeed (via fallback).
- List helpers:
  - values containing commas and quotes round-trip,
  - remove preserves order and duplicates as specified,
  - no-op remove does not publish.

Where possible, isolate Redis-dependent tests behind build tags or integration test harness so unit tests can run without Redis.

## Open questions

- Should `Reset()` remain callable after `Close()`? If kept, it should be clearly documented as “remote cleanup” (not affecting the point-in-time snapshot).
- What key/value constraints should `rmap` enforce globally? (today: map name regex, key forbids `=`, list items unrestricted).
- Do any consumers require durable transition events, or is “eventual state” sufficient?

