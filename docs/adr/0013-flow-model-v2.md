# ADR-0013: Flow model v2 — step graph with outcome edges

Date: 2026-07-20
Status: Accepted

## Context

Through M4b the flow document was linear: `Document{Source, Ops[], Sink}`,
step order = slice index, no edges, and the first error aborted the whole
run (`engine/stream` returned the error, the runner marked the task failed).
M5 ("Flow model & studio API") needs a richer shape — the artifact that
studio authors, the runner compiles, telemetry keys on, and custom-code
steps (M5b) plug into. M5a delivers that shape.

Two scope decisions were confirmed with the product owner:

1. **Build the flow model first** (before custom code, test mode, studio) —
   it is the keystone everything else rides on; building them against the
   linear model would force a later migration.
2. **Outcome edges only** — each step exits on **success** or **failure**
   (Boomi try/catch: green path / red path), with a single **complete**
   out-edge for steps that have no success/failure distinction. **Parallel
   fan-out, merge, and multi-sink data DAGs are deferred** to a later chunk
   with its own ADR: the engine is a single-batch, sink-ward pull pipeline,
   and true branching needs a multi-branch scheduler. Outcome-edge error
   routing, by contrast, fits the pull model with a tiny additive change.

## Decision

### The model

A document is a **graph of steps**. Every node — connector or transform —
is a `Step` (`pkg/flowdoc`). `Step` embeds `Op`, so transform steps reuse
the exact fields, validation, and runner lowering (`applyOp`) as before.

Type namespace:
- connector: `source | sink`
- transform: `filter | project | coerce | flatten | aggregate`
- reserved: `wasm | python | subflow` — parsed but rejected ("not yet
  supported") so the schema is forward-declared for M5b.

Edges name step ids:
- `onSuccess` / `onComplete` — the single **happy-path** successor. They are
  structurally identical (both name the next step); the distinction is
  authoring intent (whether the step also declares an `onFailure`).
  Validation requires exactly one happy edge per non-terminal step
  (`onSuccess` XOR `onComplete`).
- `onFailure` — an **error handler**: a `sink`-type step, off the main path,
  with no outgoing edges.

### Two authoring forms, one execution plan

The linear form (`Source/Ops/Sink`) is kept as ergonomic, AI-friendly
**sugar**. Both forms lower to one normalized `Plan{Main []*Step, Catch
map[stepID]*Step}` (`flowdoc/graph.go`, `Document.Plan()`): the linear form
is synthesized (ids `source`, `op0`…, `sink`, chained by the happy path, no
handlers); the graph form is validated as it is built. The hub validates
through the plan at deploy; the runner compiles from it. One code path, one
telemetry model. The two forms are mutually exclusive within a document.

### Streaming semantics (why handlers are terminal actions)

In a pull pipeline a step processes the *whole* stream, so a step's
success/failure is terminal — evaluated at EOF or first error, never
per-record. Therefore:

- The **happy path** compiles to today's fused pull pipeline exactly as
  before — **zero engine change on the happy path**.
- On any error (source, transform, or sink), the runner identifies the
  failing step and runs the **nearest covering `onFailure` handler** (the
  step's own, or the nearest preceding main step's — try/catch scoping). The
  handler receives **one payload-free error record** `{flow, step, error,
  at}` and the task ends `failed` with `handled=true`. This is dead-letter /
  alerting — the top iPaaS error-handling need — without a branch scheduler.
- **No covering handler ⇒ behaves exactly as before** (task fails with the
  error). The change is strictly additive and backward-compatible.

`onFailure` is *not* stream resumption and *not* per-record content routing
(a different concept, deferred).

### Engine change: one typed error

`engine/stream` gains `OpError{Op, Err}` (with `Unwrap`), replacing the two
`fmt.Errorf("%s: %w", name, err)` wrap sites (transform and sink). `Op`
carries the operator name; the runner stamps each operator with its **step
id** (`Pipeline.RenameLastOp`, and `New`/`Run` name the source/sink), so a
run failure is routed via `errors.AsType[*stream.OpError]` — no message
parsing. The `Error()` string is unchanged. Per-step telemetry
(`OpStats.Name`) is now the step id too (`OpStat.StepID` mirrors it through
the runner and hub result).

### Doctrine held

- **Hub never touches payload.** Validation stays in `pkg/flowdoc` using only
  `engine/record`; the hub imports no new engine surface. The error record is
  produced and delivered runner-side; the hub sees only the metadata failure
  message.
- **Secrets never in logs (ADR-0010).** The service redacts every resolved
  secret value from the error string before it reaches `task.Error` or the
  handler record. The runner passes the resolved plaintexts to the service
  solely to build the redactor (`SubmitOpts.SecretValues`); they are never
  stored. Redaction happens at the single point errors are recorded, so the
  hub-facing `Fail` message inherits it.

## Consequences

- Existing linear flows deploy and run unchanged (proven: all pre-M5a flow
  tests pass through the lowering).
- No hub migration: the handled-failure outcome rides the existing `error`
  string ("… (handled by onFailure step \"dead\")"); per-step ids ride the
  opaque `result` JSONB.
- **Deferred, to be superseded by later ADRs:** parallel fan-out + merge +
  multi-sink data DAG; per-record content routing; sub-flows; multi-step
  handler sub-pipelines (M5a handler = one sink action); custom-code step
  types (`wasm`/`python`, reserved here, decided in ADR-0014).

## Proof

`pkg/flowdoc` graph validation matrix + linear-lowering golden;
`engine/stream` `OpError` recovery via `errors.AsType`; runner
`TestApplyNamesOpsByStepID` (op names == step ids); runner
`TestErrorRoutingAndRedaction` (real connectors: a mid-pipeline coerce
failure routes to the dead-letter handler, task ends failed+handled, and the
secret value embedded in the error is masked to `***`),
`TestNoHandlerFailsAsBefore` (regression), `TestErrorRecordIsPayloadFree`.
