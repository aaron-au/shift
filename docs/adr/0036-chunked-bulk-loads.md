# ADR-0036: Chunked bulk loads — restartable ETL over the existing queue

Date: 2026-08-04

Status: **Designed**

## Context

The engine already handles ETL-sized data. Measured (`docs/bench-M1.md`):
10 GiB transformed at **24.30 MiB** peak RSS — byte-identical to the 1 GiB
figure — with zero spill; 10 GiB of CSV at 12.97 MiB. RSS is a function of
watermark and batch sizing, not of how much data flows through. Join and
aggregate both spill and stay bounded. Reading dominates wall time (63–78%),
so transform depth is close to free.

What is missing is not throughput. It is **restartability**, and a grep of
`runner/`, `engine/` and `pkg/` for checkpoint or resume returns nothing.

Three consequences, all disqualifying for real ETL:

1. A six-hour load that fails at hour five restarts from zero.
2. Dispatch is at-least-once (ADR-0002), so a lost lease re-runs the load
   *in its entirety*. Step idempotency keys make small tasks safe; they do
   not make a 500 GB load safe.
3. Runners are ephemeral and disposable by design (ADR-0002). For a
   long-running load that design property becomes a liability — the longer
   the task, the likelier the runner disappears mid-flight.

This is the gap between "handles ETL volumes" and "is an ETL tool". Nobody
buys the latter without restart.

The project owner raised the shape of the problem directly (2026-08-04): if
runners are ephemeral, resumption state has to live somewhere, and a central
store where runners stage data "seems risky on data exposure". That instinct
is correct and this ADR records why, then routes around it.

## Decision

**A bulk load is not one long task. It is N ordinary tasks over the queue
that already exists.**

### 1. Chunking, not checkpointing

A job declares a partitionable source. A **planner** phase enumerates
chunks — key ranges, object keys, file names, date partitions — and each
chunk is enqueued as an ordinary hub task with idempotency key
`job:<job_id>:chunk:<n>`. The `job:` prefix becomes reserved alongside
`sched:` (ADR-0012).

Nothing new is invented for durability. The hub already owns a Postgres
queue with `SKIP LOCKED` leases, attempt history, at-least-once dispatch and
idempotency-keyed deduplication, all proven in e2e crash-recovery tests. A
chunk is just a task.

The consequences follow from that reuse rather than from new machinery:

- **A dead runner loses one chunk, not the load.** The hub re-dispatches
  that chunk exactly as it re-dispatches any task.
- **Parallelism is free.** Chunks spread across the fleet, so a large load
  uses the whole licensed core budget (ADR-0033) instead of pinning one
  runner for hours. This is the same property that makes ephemeral runners
  an asset here instead of a hazard.
- **Restart granularity is a tuning knob** (chunk size), not a redesign.

### 2. No shared payload store — explicitly rejected

Runners do **not** stage payload anywhere shared, and the hub does not hold
it. That would break three commitments at once:

- `CLAUDE.md` doctrine: no shared filesystems for runner clustering;
- ADR-0016's two-plane split: payload never touches the control plane;
- the spill store's deliberate design — single unlinked file, ephemeral,
  never a durable artifact.

It would also create a new data-at-rest surface carrying encryption, key
management, retention and residency obligations that the platform currently
does not have and sells the absence of.

Only **metadata** moves: chunk boundaries are a few bytes, and they travel
the control plane like any other task field.

### 3. Per-chunk atomic commit

A chunk's sink write must be atomic at chunk granularity: stage, then
publish. The pattern already exists — the sftp connector does atomic put
(write to a temp path, rename on completion), and object stores make this
natural.

Re-dispatch of a chunk therefore either produces its output exactly once or
produces nothing to clean up. Combined with the injected `idempotency_key`
this is what makes at-least-once dispatch safe at volume rather than merely
safe in principle.

### 4. Job state is control-plane metadata

The hub tracks a job: its chunks, each chunk's terminal state, and progress
derived from them. That is counts and states — the same class of data the
task queue already stores. Completion is "all chunks terminal"; failure
policy is a job-level decision (below).

### 5. Cursor-resume within a chunk is deferred

An additional `Checkpoint() []byte` on a source action — an opaque,
connector-defined cursor (last primary key, object key plus byte offset)
reported on the heartbeat and handed back on re-dispatch — is a genuine
refinement for individually huge chunks. It is **not** needed first: chunk
size already bounds lost work, and it is the cheaper dial. Adding both at
once would mean designing a second resumption mechanism before the first has
met a real workload.

## Consequences

- **Ephemeral runners stop being a liability for long work** — the property
  that made six-hour loads fragile becomes the property that makes chunked
  loads parallel.
- **The licensing story improves.** A big load now consumes the core budget
  it was sold (ADR-0033) rather than one runner's worth of it.
- **Placement composes.** Chunks are ordinary tasks, so runner groups
  (ADR-0030) constrain them unchanged: an on-prem-only extract stays
  on-prem.
- **Sources must be partitionable** — a range query, an object list, a file
  list. This is a *weaker* requirement than full resumability and is met by
  the sources ETL actually uses. A non-partitionable source (a webhook body,
  a non-deterministic query) simply runs as a single chunk, which is exactly
  today's behaviour, so nothing regresses.
- **Stragglers are a real risk.** Uneven partitions leave one chunk running
  long after the rest finish. Adaptive sizing is an open question below.
- **Global aggregation does not decompose.** A `GROUP BY` across the whole
  dataset cannot be chunked naively. Two-phase (partial aggregate per chunk,
  then a reduce chunk) is the known answer and is deliberately out of scope
  here — record-wise extract/transform/load is the 80% and should not wait
  for it.
- **New surface:** a partition capability in the connector SDK, a job
  concept in the hub, and a planner phase in the flow model. Each is
  additive; existing single-task flows are unaffected.
- **Ordering across chunks is not guaranteed.** Chunks run concurrently, so
  a flow that depends on global input order must stay single-chunk. This
  matches ADR-0029's existing stance that branching is concurrent.

## Alternatives considered

**Central/shared staging store.** Rejected in §2. Breaks the no-shared-
filesystem doctrine and the two-plane split, and creates a data-at-rest
surface whose absence is currently a selling point.

**Checkpoint the single long task.** A cursor plus periodic state snapshot,
resuming one task in place. Rejected as the *first* mechanism: it needs
sink-side dedup for the records already written (per-record idempotency is
expensive at volume), it leaves the load pinned to one runner so nothing
parallelises, and it invents durability machinery next to a queue that
already has it. Kept as a refinement (§5).

**Re-extract everything on failure.** Correct but wasteful, and it makes
failure probability compound with load size — the longer the job, the more
likely a restart, and each restart is as long as the original.

**Push it to the source system** (let the database do the chunking via its
own export tooling). Works for one vendor at a time and abandons the
platform's job of being the thing that moves data between systems.

## Open questions

1. **Chunk sizing.** Fixed count, fixed row/byte target, or adaptive from
   observed chunk durations? Adaptive handles skew but needs a feedback path
   the queue does not currently have.
2. **Job-level failure policy.** Fail the job on the first terminal chunk
   failure, or complete what can complete and report partial success?
   Leaning configurable per job, defaulting to fail-fast, since a
   half-loaded warehouse table is usually worse than none.
3. **Two-phase aggregation** for global `GROUP BY` — a reduce chunk that
   consumes partial aggregates. Needs its own ADR.
4. **Whether the planner is a step type or a connector action.** A connector
   knows how to partition its own source; the flow model needs to know a
   plan phase happened. Probably a source-action capability surfaced as a
   plan phase, but the descriptor implications (ADR-0018) need working
   through.
5. **Progress reporting granularity** — per chunk is trivially available;
   within-chunk progress needs the deferred cursor (§5).
