# ADR-0029: Flow model v3 — branching and merging (the executable DAG)

Date: 2026-07-28
Status: **Accepted; implemented** — model (`pkg/flowdoc/dag.go`, #32), engine
primitives (`engine/stream/fanout.go` + `merge.go`, #34), keyed join (#36) with
grace-hash spill (#41), runner execution (`service_multipath.go`), studio
nodes (`ui.html`). Three gaps remain open, each with an issue:

- **Nested / mixed topologies are not executable.** `executeMulti` runs one
  fan-out **or** one fan-in; a graph combining them (`source → tee → enrich →
  merge`, the enrichment shape) validates at the hub and draws on the canvas
  but fails at run time with a clear error. Issue #59.
- **Test-mode capture (ADR-0014) is dropped on DAG flows.** `executeMulti`
  discards the `*captureSampler`, so `?capture=1` silently yields nothing for
  exactly the multi-path flows where per-branch inspection matters. Issue #60.

Fan-out spill (§2) is deliberately absent: the bounded branch queues block
instead, which is memory-bounded and matches this ADR's own "the tee runs at
the pace of its slowest branch". Spill would decouple a slow branch — an
optimization, not a correctness fix.

## Context

ADR-0013 made the flow document a *step graph*, but only for **outcome
routing** (green/red try-catch edges). The **data** path it lowers to is still
strictly linear: one `Source` → operators → one `Sink`, a single batch pulled
sink-ward at a time (ADR-0003, `engine/stream`). ADR-0013 said so explicitly
and deferred the rest to "a later chunk with its own ADR: … true branching
needs a multi-branch scheduler." This is that ADR.

Aaron asked for the two capabilities every real multi-path integration needs
and that the linear model cannot express:

1. **Branching (fan-out).** The *same* record stream flows to *multiple*
   downstream paths — e.g. write every order to SFTP **and** POST it to an API
   **and** aggregate a running total. A tee.
2. **Merging (fan-in / join).** Records from *multiple* inputs combine on a
   **linked element** (a join key) — e.g. enrich each `order` with its
   `customer` row keyed by `customer_id`; or concatenate two feeds into one
   stream.

The studio (ADR-0019) already *draws* multi-port nodes and multi-input merge
nodes in `ui.html`; the model validates and the engine executes only the linear
subset, so those shapes are undrawable-into-runnable today. This ADR closes
that gap. It is the **engine centerpiece**: the change that turns SHIFT from a
one-line pipe into an executable integration DAG.

Everything here evolves **ADR-0013's model** (the `Step`/edge/`Plan`
vocabulary) and preserves ADR-0003/0004/0005 doctrine: bounded memory, spill to
one store above the watermark, resource-governed flow control, no
`map[string]interface{}` on the hot path, and the batch-lifetime contract.

### Why this is hard (and why it was deferred)

A pull pipeline has exactly one consumer pulling one batch through one chain.
Two properties break the moment there is more than one path:

- **Batch lifetime.** A batch from `Source.Next` is valid only until the next
  `Next`/`Close` (CLAUDE.md engine contract). A tee that hands the *same*
  batch to two consumers that pull at different rates violates this instantly:
  consumer B still reading batch _n_ while consumer A pulls _n+1_ mutates
  B's data out from under it. Fan-out therefore needs **owned copies** or a
  **hand-off discipline**, and copying is the thing the engine exists to
  avoid.
- **Backpressure across branches.** With two consumers of one producer, a slow
  consumer must not force the fast one to buffer the whole stream in memory —
  that is exactly v0's fully-buffered failure mode. Flow control between
  branches must be **explicit and bounded** (ADR-0005: per-stream buffer bounds
  are flow control *within* a task, spill only above the watermark).

## Decision

Introduce **flow model v3**: the data path becomes a **directed acyclic graph**
of steps with two new structural node kinds — **fan-out** (branch) and
**fan-in** (merge) — plus the engine machinery to execute them under bounded
memory. Linear and outcome-edge (v2) documents remain valid and lower onto the
same DAG unchanged; v3 is a superset.

### 1. Model changes (`pkg/flowdoc`)

ADR-0013 gave every step a single happy successor (`onSuccess` XOR
`onComplete`). v3 relaxes that on two explicit node kinds and keeps the
single-successor rule everywhere else (so v2 flows are unchanged and the graph
stays legible).

**Data edges vs outcome edges.** v2's `onSuccess`/`onFailure`/`onComplete` are
**outcome** edges — they route on a step's *terminal* result (EOF vs error).
v3 adds **data** edges that carry the *record stream itself* to more than one
place. The two are orthogonal and coexist: a fan-out step still has one
`onFailure`. To keep the wire model additive, data fan-out is expressed as a
step whose happy output names **a list of** successors rather than one.

Two new step types:

- **`tee` (unconditional fan-out).** A source/transform step declares
  `branches: [stepA, stepB, …]` (≥2 targets). *Every* record is delivered to
  *every* branch. Role: pass-through — it neither adds nor drops records; it
  duplicates the stream. Sugar: any transform step may carry `branches`
  directly instead of a single happy edge, which the plan normalizes into an
  implicit `tee` (keeps the canvas to one node when the author just wants "send
  this two places").

- **`router` (conditional fan-out / switch).** Declares ordered
  `routes: [{when: <predicate>, to: <stepID>}, …]` plus an optional
  `default: <stepID>`. Each record is evaluated against the predicates **in
  order** and sent to the **first** matching branch only (or `default`, or
  dropped if neither). Predicates reuse the existing `filter` expression
  grammar over `record.Path` — **no new expression language**. A `router` is a
  *partition*: record counts across branches sum to the input (minus dropped),
  unlike `tee` which multiplies.

  > `tee` = every record to all branches. `router` = each record to one
  > branch by predicate. The distinction is load-bearing for idempotency
  > (below) and for the studio's port rendering.

One new **fan-in** step type:

- **`merge` (fan-in / join).** Declares `inputs: [stepA, stepB, …]` (≥2, the
  step ids whose happy output feeds in) and a `mode`:
  - **`concat`** — unordered union of all inputs' records into one stream
    (append/interleave). No key. Streaming, non-blocking.
  - **`join`** — a **keyed** relational join. Declares `on: {left: <path>,
    right: <path>}` (the *linked element*), `type: inner | left`, and which
    input is the **build (right) side** vs the **probe (left) side**. Output
    records carry the probe record's fields plus the matched build record's
    fields under a configurable `as: <field>` (nested, so field names never
    collide and the no-`map[string]interface{}` builder stays simple).

    `join` requires exactly two inputs in v3 (n-ary joins are a chained pair of
    `merge` steps; see Open questions). `right | left` naming, not
    `inner/outer` on both sides, because the engine builds a hash table of one
    side — the model names which.

`Plan` (ADR-0013) grows from `Main []*Step` (a list) to a validated DAG:

```
type Plan struct {
    Nodes map[stepID]*Step   // every data + outcome node
    Data  map[stepID][]stepID // happy data edges (tee/router/linear successors)
    Catch map[stepID]*Step   // unchanged: onFailure scoping (ADR-0013)
    Sources []stepID          // ≥1 roots (multiple only via independent inputs to a merge)
    Sinks   []stepID          // ≥1 terminals
}
```

The linear/​v2 lowering (`linearPlan`, `buildPlan`) produces a DAG whose `Data`
adjacency is a single chain — byte-for-byte the same execution as today. **v2
is the degenerate DAG.**

### 2. Engine changes (`engine/stream`)

The pull model is preserved *within* each linear segment; fan-out/fan-in are
the only points that need a scheduler. A v3 `Plan` compiles to a set of linear
**segments** (a maximal run of one-in/one-out ops) joined at `tee`/`router`/
`merge` **nodes**. Each segment is exactly today's fused `Pipeline`. The new
work is at the node boundaries.

#### Fan-out: a bounded, spillable tee

A `tee`/`router` node is driven by **one** goroutine pulling its upstream
segment (the single producer — no double-pull, no re-execution of the source).
For each output branch it owns a **bounded queue** of batches; each downstream
branch runs as its own consumer goroutine pulling from its queue (ADR-0005:
every task gets its own goroutine; a coordinator orchestrates but never
executes).

- **Ownership, not aliasing.** The tee driver hands each branch its **own
  batch** so the batch-lifetime contract holds independently per branch. To
  avoid N full copies on the hot path, batches are **reference-counted and
  arena-pooled**: a branch that only *reads* (filter/project/sink-serialize)
  shares the immutable arena under a refcount; the arena returns to the pool
  when the last branch releases it. A branch that *mutates* in place (the
  common transform case, which shares allocators) triggers **copy-on-write** of
  just that batch via `record.CopyValue` into the branch's own batch — the
  existing deep-copy primitive, now the fan-out seam. `router` never copies:
  each record goes to exactly one branch, so it *partitions* the input batch
  into per-branch batches (moves, not copies).

- **Backpressure is bounded and symmetric.** Each branch queue has a small
  fixed depth (a few batches). When a branch's queue is full the tee driver
  **blocks** on that branch — it does not skip ahead and it does not grow the
  queue. So the tee runs at the pace of its *slowest* branch, and the fast
  branch cannot force unbounded buffering of the slow one: at most `depth`
  batches per branch are in flight. This is the same "a task waits only when
  genuinely out of resources" rule (ADR-0005) applied *within* a task —
  branch queues are flow control, not gates.

- **Spill above the watermark.** The bounded queues account their held bytes
  against the pipeline `mem.Governor`. If a genuinely slow branch (e.g. a
  rate-limited API sink) would push held bytes over the watermark,
  `Gov.TryReserve` fails and the queue **spills** overflow batches to the
  single `spill.Store` (the aggregate operator's exact pattern: reserve →
  on-fail spill → release), draining back to memory as the slow branch
  catches up. Never many small files; never unbounded RAM. This is the one
  place fan-out can touch disk, and only under sustained backpressure.

- **Error + close propagation.** A branch error becomes an `OpError` tagged
  with the branch's step id (ADR-0013 routing is unchanged), cancels the shared
  `ctx`, and tears down sibling branches and the driver. `Close` fan-in-joins
  all branch goroutines before the node reports done, so no goroutine outlives
  the task (the runner's disposability contract).

#### Fan-in: streaming concat, blocking keyed join

- **`concat`** — the merge node pulls from its input segments and forwards
  batches as they arrive. A simple **fair, non-blocking multiplex**: poll ready
  inputs, forward whichever has a batch, apply the *same* bounded-queue
  backpressure so a fast input can't outrun the downstream sink. Fully
  streaming, O(batch) memory, no key, order unspecified (interleaved). Records
  are **moved** (not copied) into the output stream.

- **`join`** — a **blocking multi-input operator**, built as a sibling of
  `Aggregate` and reusing its exact spill machinery:
  1. **Build phase.** Fully consume the **right (build) input**, hashing each
     record by its `on.right` key into an in-memory **hash table**
     (`map[encodedKey][]record`), keys encoded with the `spill.Encoder` value
     codec (same as aggregate group keys — no `map[string]interface{}`,
     partition-hashed for bounded merge). Each inserted row reserves bytes via
     `mem.Governor.TryReserve`; on watermark hit the table **spills partitions
     to `spill.Store`** exactly as `aggSource.spillAll` does. The build side
     should be the *smaller* input; the model names it, and validation can warn
     when descriptor cardinality hints suggest otherwise.
  2. **Probe phase.** Stream the **left (probe) input** one batch at a time.
     For each probe record, encode its `on.left` key, look up the matching
     build bucket (loading the relevant spilled partition on demand, partition
     by partition — merge memory bounded by the largest partition, not total
     cardinality), and emit joined records: probe fields + matched build record
     nested under `as`. `type: left` emits probe records with a null `as` when
     no match; `type: inner` drops them. Output is built with `record.Builder`
     into the flowing batch — the standard contract.

  The join is **blocking on the build side only**; the probe side streams, so a
  large-left / small-right join (the common enrichment shape) stays
  memory-bounded by the right input's spillable table. Batch lifetime holds:
  build rows the table retains are `record.CopyValue`'d in (they must outlive
  their source batch); probe rows are read within their batch and emitted
  immediately.

### 3. Studio (`ui.html`, ADR-0019)

The canvas already renders the *shapes*; v3 makes them mean something. No new
windowed-shell work, only node/port semantics:

- **Fan-out node** renders **multiple right (output) ports** — one per branch —
  each a snap target for a downstream edge. A `router` node additionally shows a
  **predicate field per port** (reusing the filter expression editor) and a
  `default` port; a `tee` node's ports are unlabeled (every record to all).
  Port role follows ADR-0024 (the connector/verb model is unchanged; a fan-out
  is a structural node, not a connector).
- **Merge node** renders **multiple left (input) ports** and a body config for
  `mode`. In `join` mode it exposes the **join-key / linked-element** config —
  a left-path picker, a right-path picker, `type` (inner/left), the
  build-side selector, and the `as` output field — the "linked element" Aaron
  described. `concat` mode hides the key config.
- The builder keeps surfacing hub **422s** verbatim (ADR-0019): it never
  re-implements the acyclicity / join-key validation below. Node positions ride
  the existing presentational `Document.Layout`, ignored by the plan.

### 4. Validation rules (`pkg/flowdoc`, authoritative)

Added to the existing graph validator; all are structural (hub validates
without touching payload, ADR-0013 doctrine):

- **Acyclic.** The data-edge graph must be a DAG. A cycle is a 422
  (`cycle through step "x"`). Outcome (`onFailure`) edges are excluded from the
  cycle check (they already terminate).
- **Typed, resolvable edges.** Every `branches[]` / `routes[].to` / `default` /
  `inputs[]` target must name an existing step; a `merge` input must actually
  flow to that merge (no dangling producers). Fan-out branch count ≥ 2; merge
  input count ≥ 2 (`join` == exactly 2).
- **Roles.** A `tee`/`router` is pass-through (not a sink; must have ≥2
  outgoing data edges). A `merge` is a transform (has one happy successor like
  any op, plus its multiple inputs). Every source root is a `source`/config-
  driven source; every terminal is a sink (`@discard`/`@response`/connector
  sink) — the ADR-0024 "happy path ends at a sink" rule now reads "**every**
  terminal path ends at a sink," and the studio auto-appends `@discard` to any
  dangling branch.
- **Join-key presence.** `mode: join` requires `on.left`, `on.right`, a
  `type`, and a build-side selection; both paths must be parseable
  `record.Path`s (validated with `engine/record`, the only engine surface the
  hub imports). `mode: concat` forbids `on`.
- **Router predicates** must parse under the filter grammar; an unreachable
  route (predicate after a broader earlier one) is a lint **warning**, not a
  rejection.

### 5. Idempotency & at-least-once (ADR-0002/0013)

Fan-out multiplies the risk that a re-dispatched task double-applies side
effects, so v3 makes the rule explicit:

- **Per-branch idempotency keys.** The hub injects one `idempotency_key` per
  task (stable across re-dispatch). Under fan-out, each side-effecting sink
  derives its own **stable per-branch key** — `<task_key>:<branchStepID>` (the
  step id is stable in the plan). So a tee to "SFTP put" **and** "API POST" that
  re-runs after a runner death replays each sink under *its own* stable key; a
  correctly-idempotent sink dedupes and no write doubles. Two branches writing
  the *same* target with the *same* key would collapse — which is why branch
  step ids (distinct by construction) seed distinct keys.
- **Tee re-run must not double.** Because the whole task is the unit of
  at-least-once redelivery, a tee re-running replays *all* branches; correctness
  rests on each terminal sink honoring its per-branch key (the existing sink
  contract, now normative under fan-out). The engine does not attempt
  partial-branch resumption — a task is atomic-at-least-once, all branches or
  retry-all.
- **`router` and counts.** Because a router sends each record to exactly one
  branch, its branches partition the side effects; re-run replays the same
  partition (predicates are pure functions of record content), so keys and
  targets are stable across attempts.

### Doctrine held

- **Bounded memory / spill-to-one-store.** Fan-out queues and the join build
  table both account against `mem.Governor` and spill to the single
  `spill.Store` on watermark — the aggregate pattern, reused. Nothing buffers a
  whole stream in RAM; disk only above the watermark, one file.
- **Batch lifetime + no `map[string]interface{}`.** Fan-out uses refcount +
  copy-on-write via `record.CopyValue`; join build rows are `CopyValue`'d;
  everything is built through `record.Builder`. No hot-path maps.
- **Resource-governed concurrency (ADR-0005).** Branches/inputs are goroutines
  with bounded queues; a branch blocks only when its bounded queue is full
  (genuine local flow control), never on an arbitrary cap. A coordinator drives
  the node but never executes a branch's work.
- **Hub never touches payload.** All new validation is structural over
  `flowdoc` + `record` path parsing; teeing, joining, and per-branch keys are
  runner-side. Telemetry keys on step ids (ADR-0013) — a branch/merge is just
  more `OpStats.Name`s.

## Consequences

- SHIFT gains the two shapes that make it a *real* iPaaS engine rather than a
  linear pipe: fan-out (tee/router) and fan-in (concat/join). Multi-path
  integrations authored on the canvas become executable.
- The engine grows its **first multi-goroutine data topology**. Until now a
  task was a single pull chain; v3 adds a coordinator + branch/input goroutines.
  This is the largest engine change since M1 and the most concurrency-sensitive
  (backpressure, teardown, ctx cancellation, `-race` coverage of the tee/join).
- **Additive and backward-compatible.** v2/linear documents are the degenerate
  single-chain DAG; they compile and run unchanged, with the same
  zero-copy/zero-goroutine-overhead happy path. No hub migration (the DAG rides
  the existing flow-version JSON; new fields are optional).
- **Sequencing.** This lands **after the base connector fleet** (SFTP is the
  M6 base-connector track; more connectors give fan-out/fan-in real endpoints
  to wire). It is the engine centerpiece of the milestone that follows connector
  breadth — there is little value in branching to two connectors that don't
  exist yet. It also **subsumes** the ADR-0013 deferral ("parallel fan-out +
  merge + multi-sink data DAG") and complements ADR-0024 Phase 2 (mid-flow
  request-reply): a `router`→enrich→`merge` graph is the natural home for
  request-reply enrichment once both ship.
- Costs paid: copy-on-write and refcounting add complexity to the batch/arena
  layer; the join is a second blocking operator to keep spill-correct and
  fuzz-tested; the studio's port/predicate/join-key UI is real builder work.

## Open questions

- **N-ary joins.** v3 caps `join` at two inputs (chain `merge` steps for more).
  Is a native n-way hash join worth it, or is chaining sufficient and clearer?
- **Ordered concat / sort-merge.** `concat` is interleaved (unordered). Some
  integrations want deterministic order or a **sort-merge join** (both inputs
  sorted on the key, fully streaming, no build table). Defer until a workload
  demands it; sort-merge would be a third join strategy chosen by the planner.
- **Router "fall-through" vs "first-match".** v3 is first-match (like a
  `switch`). Do we ever want a record to match *multiple* routes (that is just
  `tee` + per-branch `filter`)? Keep them distinct unless a use case forces a
  hybrid.
- **Fan-out copy threshold.** Copy-on-write assumes most branches read-only.
  Do we need a heuristic/flag for the all-branches-mutate case (N deep copies
  unavoidable), or accept it as the honest cost of that (rare) topology?
- **Cross-branch backpressure fairness.** A single pathologically slow branch
  throttles the whole tee (correct for memory, but a "best-effort, drop-on-lag"
  branch — e.g. metrics — might prefer to be shed rather than block siblings).
  A per-branch `onOverflow: block | spill | drop` policy is a candidate, but
  "drop" collides with ADR-0005's "never silently dropped" — needs its own
  decision.
- **Merge + outcome edges.** How does `onFailure` scope across a fan-in (which
  step "owns" the failure when two inputs converge)? v3 scopes the handler to
  the failing *upstream* step (unchanged ADR-0013 rule); a handler on the
  `merge` itself covers the merge operator only. Confirm this reads naturally on
  the canvas.
