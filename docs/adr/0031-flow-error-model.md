# ADR-0031: Flow error model — canonical error, default dead-letter, and forced termination

Date: 2026-07-29
Status: **Designed (build deferred)**

## Context

The flow engine now has three error-shaped mechanisms that grew independently
and do not yet share a coherent model:

- **Try/catch handlers (ADR-0013).** Every step carries typed outcome edges —
  `onSuccess`/`onComplete` (happy path) and `onFailure` (error handler /
  dead-letter). A step's *effective* handler is its own `onFailure` or the
  nearest preceding step's. The engine tags a failure `OpError{Op: <stepID>,
  Err}` and the runner routes via `errors.As`, never string parsing. This is
  the try/catch **primitive that already exists**; this ADR extends it, it does
  not reinvent it.
- **At-least-once durability (ADR-0002).** The hub re-dispatches a task whose
  runner died; side effects dedupe through injected idempotency keys.
  `Document.Delivery` is `at_least_once` (default, retried up to
  `DefaultMaxAttempts = 3`) or `at_most_once` (caps `max_attempts` at 1; a
  trigger can never raise it). Delivery is a **control-plane dispatch policy**;
  the engine ignores it.
- **Concurrent fan-out (ADR-0029).** A tee/router spawns one goroutine per
  branch, each a full pipeline into its own sink. A branch error cancels the
  shared context and tears the whole topology down ("atomic-at-least-once: all
  branches or retry-all"). The executor already reports `firstRealError` — the
  first non-cancellation error — so teardown's `context.Canceled`, raised in
  innocent sibling branches, never masks the true cause.

The gap: there is no single statement of **what one execution reports**, no way
to say "handle any unhandled error rather than fail the task", and no way for a
developer to **deliberately end a flow early as a success** (distinct from an
error). Conflating these produces wrong retry behaviour — a deliberate early
exit that gets retried, or a clean stop that gets dead-lettered. This ADR fixes
the model as a whole.

## Decision

### 1. Canonical single error per execution

A flow execution surfaces **exactly one real error** — the root cause — and
never the `context.Canceled` cascade that teardown or cancellation induces in
sibling branches or downstream stages. Generalize the ADR-0029 `firstRealError`
fix into a platform principle: the **canonical error** is resolved by

1. prefer the first **non-cancellation** error observed across all goroutines
   (source, transforms, sink, every fan-out branch); a `context.Canceled` /
   `context.DeadlineExceeded` produced *by teardown* is a symptom, never the
   report;
2. report a cancellation error **only when cancellation is itself the genuine
   cause** — a real client disconnect, a caller-initiated abort, or a
   flow/step deadline firing — i.e. no non-cancellation error exists and the
   cancel originated outside the executor's own teardown path.

The executor distinguishes teardown-cancel from genuine-cancel by ownership: the
context it cancels on first-error is its own; a cancel arriving on the *parent*
context (caller/deadline) with no recorded step error is genuine. This canonical
error is the **reporting contract** for the runner's execution report to the hub
— metadata only, keyed by the failing step id (ADR-0016 control plane, ADR-0020
telemetry). One run, one cause, one `OpError`.

### 2. Default / global dead-letter ("handle, don't blow up")

Extend ADR-0013 so an **unhandled** error can route to a **flow-level default
handler** instead of failing the task: a global `onError` default that catches
any step lacking its own (or a covering) `onFailure`. Resolution order stays
try/catch-scoped: step's own `onFailure` → nearest covering `onFailure` →
**flow `onError` default** → unhandled (task fails).

- **Opt-in, default OFF.** The safe, visible default is unchanged: an unhandled
  error **fails the task** (ADR-0013). A flow enables the default DLQ explicitly
  (`Document.OnError`, a `sink`-type terminal like any handler). An operator may
  set a **hub-configurable default DLQ** turned on **hub-wide**, so a deployment
  can choose "nothing is ever unhandled" as policy — but that is an operator
  decision, never an engine default.
- **Observable, always (ADR-0005).** "Never blow up" without visibility is worse
  than failing. Every dead-lettered error is **counted** (a per-flow,
  per-step-id metric — `shift_flow_dead_letters_total` — and an increment on the
  task result) and **inspectable** (the payload-free error record `{flow, step,
  error, at}` from ADR-0013, delivered to the DLQ sink). Records are never
  silently swallowed.
- **Task outcome.** A run whose error was caught by a handler or the default DLQ
  ends **`succeeded_with_dead_letters`** — a terminal success carrying a
  non-zero dead-letter count — *not* `failed`. (An uncaught error still ends
  `failed`.) This distinction is what the retry table below keys on.
- **Retry implication: a handled error is terminal.** The handler ran; the
  dead-letter was recorded; re-running would re-execute side effects and
  re-dead-letter the same record. **No retry.** Handling is a deliberate
  "we dealt with it" outcome, exactly as `onFailure` is today.

### 3. Forced early termination (`@stop`)

Introduce a reserved built-in terminal — **`@stop`** — a sibling to `@discard`
and `@response` (ADR-0024) that lets a developer **deliberately end the flow
early as a SUCCESS**, distinct from any error.

- **Reserved name / role.** `@stop` joins the reserved built-in terminals
  (`@discard`, `@response`, `@webhook` source). Sink-like, role-locked
  (terminal only), needs no action, exempt from capability policy and signing
  (connector-free, side-effect-free — like `@discard`).
- **Engine mapping.** `@stop` maps to a **clean cancellation sentinel** —
  `stream.ErrStopRequested` — that the executor treats as **normal completion**,
  never a failure. Reaching `@stop` cancels the surrounding topology the same
  mechanical way a branch teardown does, but the sentinel is classified as
  success: rule (1) above never promotes it to the canonical error, and sibling
  `context.Canceled` from its teardown is likewise not reported.
- **Telemetry / outcome.** The task ends **`succeeded`** (or
  `succeeded_with_dead_letters` if a DLQ also fired) with a `stopped=true`
  marker and the step id that requested the stop, counted as
  `shift_flow_stops_total`. It is a first-class, observable outcome, not an
  error swallow.
- **Retry.** A deliberate stop is a **terminal success — NO retry.**

### 4. Retry semantics

Retry is a control-plane dispatch decision (ADR-0002); the engine only reports
the outcome that keys it. The mapping:

| Execution outcome | Task result | Retried? |
|---|---|---|
| Unhandled error (no covering `onFailure`, no flow `onError`) | `failed` | **Yes** — re-dispatched up to `max_attempts` |
| Error routed to `onFailure` handler | `succeeded_with_dead_letters` | **No** — handled, terminal |
| Error routed to flow `onError` default DLQ | `succeeded_with_dead_letters` | **No** — handled, terminal |
| Forced `@stop` | `succeeded` (`stopped=true`) | **No** — deliberate success, terminal |
| Runner died mid-flight (any of the above, in flight) | re-leased by hub | **Yes** — at-least-once re-dispatch (ADR-0002) |
| `at_most_once` delivery | caps `max_attempts` at 1 | a lost runner **fails terminally** rather than risk a double side effect |

**Why "force stop" must not be modelled as an error.** If `@stop` raised an
error it would take one of two wrong paths: with retry enabled it would **retry
a deliberate stop** (re-running the flow the developer explicitly ended), and
with a default DLQ enabled it would **dead-letter a clean exit** (alerting on a
success). Both corrupt the outcome. `@stop` is therefore a distinct **success**
sentinel, resolved by rule (1) before any error classification. Symmetrically,
a handled error is terminal-**not-retried** because the handler already ran —
retrying re-executes its side effects.

### 5. Concurrency caveat under fan-out (load-bearing)

Branches are **concurrent** (ADR-0029). An early `@stop` — or an error tearing
the topology down — in one branch does **NOT roll back or halt sibling side
effects already in flight**. There is **no distributed transaction across
branches**; `@stop` cancels contexts, it does not un-write a file already
written or un-POST a request already sent. This is stated honestly as a
**documented limitation**, not a bug.

If a flow needs ordering — "wait until application 1 succeeds before touching
application 2" — that is **explicit sequencing**: a **linear chain** (sink of A
feeds source of B), or a **cross-branch barrier / dependency node** (§6), **not
two parallel branches** that happen to want ordering. The concurrent tee must
stay **free of hidden inter-branch gating**: any implicit "branch B waits on
branch A" would silently serialize the tee and destroy the parallelism that
fan-out exists for. Ordering is always an **explicit construct the author
chose**, visible in the graph, never an emergent property of a condition.

### 6. Conditional branching / "fan-out conditions" (model recorded, mostly deferred)

Three explicit forms; only the first two are fan-out:

- **router** — predicate on record **data** (per-record content routing).
  **Exists today** (ADR-0029): concurrent, one goroutine per matched branch.
- **variable-gate** — predicate on a flow **variable** (`var("x") == …`).
  **DEFERRED** — depends on a future flow-variables feature. Still concurrent
  fan-out once the variable exists; the gate decides *whether* a branch runs,
  never *when* relative to a sibling.
- **cross-branch barrier / dependency** — "run branch B after branch A's
  terminal outcome". This is **sequencing expressed as an explicit DAG edge /
  await node**, **not parallel fan-out**. **DEFERRED** — needs a cross-branch
  scheduler (the multi-branch scheduler ADR-0013 flagged) and belongs to the
  sequencing model, not the tee.

Conditions are **explicit constructs**. None of them is an implicit property
that secretly serializes a concurrent tee — a barrier is drawn as a barrier, a
router branches in parallel, and the author always sees which they chose.

## Doctrine held

- **Hub never touches payload (ADR-0016).** The canonical error, dead-letter
  count, and `stopped` marker are **metadata** in the runner's execution report.
  The DLQ error record is produced and delivered **runner-side**; the hub sees
  only the outcome string and counts.
- **Never silently dropped (ADR-0005).** Every dead-lettered record is counted
  and inspectable; `@stop` is counted; the canonical-error rule surfaces the
  real cause rather than hiding it behind teardown noise.
- **Secrets never in logs (ADR-0010/0013).** The existing redactor masks
  resolved secret values in the canonical error and every DLQ record before it
  leaves the runner.
- **Additive, backward-compatible.** Default OFF preserves ADR-0013 exactly: no
  `onError`, no `@stop` ⇒ identical behaviour (unhandled fails, handled is
  terminal). Telemetry keys stay per-step-id (ADR-0020).
- **Engine ignores delivery (ADR-0002).** The engine emits outcomes; the hub
  maps outcome → retry. `at_most_once` still caps attempts at 1.

## Consequences

- One coherent vocabulary: **canonical error** (one cause per run),
  **dead-letter** (handled, terminal, observable), **`@stop`** (success,
  terminal), **unhandled** (fail, retried). The runner's report and the hub's
  retry decision read from the same small outcome set.
- New reserved terminal `@stop` and new optional `Document.OnError` default
  handler; both default OFF, both flow through the existing ADR-0013 handler
  routing and telemetry — a small additive change, not a new subsystem.
- New task result state `succeeded_with_dead_letters` (distinct from `succeeded`
  and `failed`) and a `stopped` marker; the hub schema carries them in the
  opaque result JSONB, matching the ADR-0013 precedent (no migration for the
  outcome strings).
- Two new metrics: `shift_flow_dead_letters_total`, `shift_flow_stops_total`
  (both per-flow, per-step-id).
- Honest limitation documented: no cross-branch rollback; ordering is explicit
  sequencing, never a hidden gate on a concurrent tee.

## Open questions

- **Hub-wide default DLQ config surface.** Where the operator toggle lives
  (deployment config vs. per-account policy) and whether a flow may *opt out* of
  a hub-wide default. Interacts with ADR-0015 capability policy.
- **`@stop` semantics under fan-out.** Whether an author can scope `@stop` to a
  single branch (end this branch cleanly, let siblings run) vs. whole-flow stop.
  The default here is **whole-flow** (matches the shared-context teardown);
  per-branch stop needs the deferred sequencing model.
- **Multi-step handler sub-pipelines.** ADR-0013 handlers are one sink action;
  a default DLQ that itself transforms before writing reopens that deferral.
- **`succeeded_with_dead_letters` and downstream automation.** Whether a
  partial-success result should be alertable distinctly from clean success
  (likely yes, via the count metric) — a dashboards/alerting question, not an
  engine one.
- **Variable-gate + barrier** both block on unbuilt features (flow variables,
  cross-branch scheduler); their ADRs supersede §6 when picked up.
