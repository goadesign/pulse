# Handoff prompt: Pulse pool + goa-ai admission hardening

Paste the prompt below to a fresh agent session to pick up the parked
pool/admission redesign work. Both source branches already build against
today's merged `main` in each repo; only the docs commit sits on top of the
parked implementation commit.

---

## Prompt

You are picking up parked hardening work in two sibling repos:
`~/src/pulse` (Redis-backed distributed coordination library) and
`~/src/goa-ai` (tool registry/runtime). Both repos already shipped the
narrow incident fixes from this line of work to `main`:

- Pulse `main` has the `streaming-recovery` fix (lossless `NOGROUP`
  recovery, jittered retry, fenced sink close, etc.).
- goa-ai `main` has the `provider-reregistration` fix (health tracker
  recovers from Redis state loss, providers re-register after registry
  state loss, idempotent `StartPingLoop`).

A second, larger redesign for each repo was implemented but never landed,
and is parked on a `wip/full-hardening` branch in each repo:

- `~/src/pulse` branch `wip/full-hardening`, commit `91dd0a4` (docs commit
  `2b462a7` on top) — the **pool exact-dispatch redesign**.
- `~/src/goa-ai` branch `wip/full-hardening`, commit `9772e29` (docs commit
  `d04980d` on top) — the **registry admission-generation redesign**,
  including a gRPC design change (`registry/design/design.go` +
  regenerated `registry/gen/`).

Read `HARDENING_BACKLOG.md` at the root of each branch first
(`git show wip/full-hardening:HARDENING_BACKLOG.md`). It is the
authoritative inventory of what shipped, what's parked, and what the last
audit (2026-07-23) found still blocking publication. Do not re-discover
this from scratch — the backlog doc already enumerates every open finding.

### Your task

For each repo, land the parked redesign as one or more dedicated,
reviewed changes rebased onto current `main` (do not merge the branch
wholesale — it predates the incident fixes now on `main` and needs a clean
rebase/cherry-pick). Before landing:

1. **Fix every P1 finding in each repo's backlog doc.** These are
   publication blockers, not backlog nice-to-haves:
   - Pulse: stale workers mutating after cleanup fencing (heartbeat
     precedes dispatch claim; `claimWorkerStartScript` only validates pool
     generation) and stale nodes dispatching/scheduling after node cleanup
     (heartbeat preflight and publication are separate Redis ops; dispatch
     and scheduler scripts don't check the node-cleanup field).
   - goa-ai: result-stream identity collision risk (`tool_call_id` reused
     as global transport ID — derive from `run_id + tool_call_id` instead),
     and bounded provider overload silently losing calls via approximate
     `MAXLEN` (needs atomic call admission with overload retry).
2. **Apply the Design Adjudication Protocol** (see each repo's `AGENTS.md`)
   to the gRPC design change in goa-ai before landing it — it crosses a
   contract-shape boundary and needs every registry consumer's rollout
   plan spelled out, not just the schema diff.
3. Address the P2/documentation items opportunistically but don't let them
   block: exact-dispatch worker claiming should validate payload identity
   against the dispatch ID, `poolResources.mapNames()` should include the
   legacy dispatch map, README overstatements about fencing/cleanup
   guarantees need correcting, and both repos are missing contract comments
   on several changed core files.
4. Follow the "Process contract for landing the parked work" section at
   the bottom of each backlog doc: dedicated changes with written
   contracts, diff-scoped reviews against `main` (pre-existing defects are
   backlog, not blockers), at most two fix/review cycles per change, and
   acceptance requires a live-Redis `-race` run plus targeted
   fault-injection tests (forced `XGROUP DESTROY`, workers/nodes paused
   past TTL).
5. For goa-ai's registry gRPC change specifically, write the staged
   rollout plan across every registry consumer (this includes Aura's
   `chat-agent` and any other goa-ai tool provider) before merging —
   registration becoming a required `Serve` argument and schema admission
   moving to registry-owned CAS is a breaking contract change for every
   provider process.

### Non-blocking backlog (do not chase unless trivially in-scope)

Both backlog docs list "Backlog candidates from the ship review" sections
with pre-existing-on-main findings (package-level `max`/`delay` shadowing
in `pulse/streaming/testing.go`, `EnsureGroup` BUSYGROUP string matching,
`Health()` using `context.Background()`, etc.). These are logged, not
blocking; fix opportunistically if a change already touches that file, but
do not scope-creep into them.

### Constraints

- Do not weaken the admission/dispatch contracts to make a test pass; if a
  P1 finding reveals the fencing model itself needs to change, fix the
  model and update every caller/test together (see each repo's
  `AGENTS.md` Design Adjudication Protocol).
- goa-ai's `wip/full-hardening` branch builds against the **published**
  `goa.design/pulse` module, not an unpublished local Pulse checkout — so
  the Pulse pool redesign and the goa-ai admission redesign can land on
  independent timelines; goa-ai does not need to wait on Pulse's pool work
  unless a specific admission fix depends on a Pulse API pulse doesn't yet
  export.
- Aura (`~/src/aura`) is a downstream consumer of both `goa-ai` and, via
  Flows, Pulse-backed coordination. Do not change Aura in this task unless
  a landed contract change requires a version bump; if so, bump
  `go.mod`/`go.sum` only, run `./scripts/lint`, and call that out
  separately rather than folding Aura changes into the Pulse/goa-ai
  review.
