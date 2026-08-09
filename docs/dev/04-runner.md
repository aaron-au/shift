# 04 — The Runner (`runner/`)

The runner executes integration flows: stateless by design (ADR-0002),
resource-governed (ADR-0005), local-intake-first (ADR-0008). Start it and
look at it:

```bash
make build
bin/runnerd -connector-dir bin        # dashboard on http://127.0.0.1:8340
```

## Package map

```
cmd/runnerd            flags/env wiring, HTTP server, hub registration, SIGTERM drain
internal/flow          compiles flow documents onto engine pipelines
                       (the document model itself lives in pkg/flowdoc,
                        shared with the hub for deploy-time validation)
internal/connpool      connector subprocess pool (reuse, health, idle-reap)
internal/task          task model + in-memory result ring (dashboard state)
internal/service       THE core: admission → pool → pipeline → results; benchmark
internal/api           HTTP API + embedded dashboard (go:embed ui.html)
internal/hubclient     HTTP client for the hub control API (ADR-0009);
                       M4b: secret resolve, artifact resolve/fetch, trusted keys,
                       CA trust (SHIFT_HUB_CA_FILE), persisted credentials
internal/leaseloop     hub intake (M3b): lease → submit → heartbeat → report;
                       M4b: per-task {"$secret":…} resolution before Submit
internal/connstore     M4b (ADR-0011): fetch signed connector artifacts from the
                       hub registry, verify Ed25519+SHA-256 fail-closed, cache
                       content-addressed, re-hash on every use
```

## M4b: secrets and signed connectors

- **Binding (`internal/bind`)**: documents arrive carrying inert
  `{"$secret":"name"}` refs and, since ADR-0034, references to reusable
  **Connections**. `bind.Binder.Apply` does both in one step — merge each
  referenced connection into its node's config, then substitute every
  secret reference — and returns a COPY; the caller's document is never
  mutated (the webhook registry shares one parsed document across
  concurrent invocations).

  Order matters: connections merge **first**, because a connection's own
  config can carry refs the document never did, and substituting before
  merging leaves a live `{"$secret":…}` in the merged config.

  **One hub round trip per task**, not one per secret and not one per
  concern: `POST /api/v1/task-config/resolve` returns the connections AND
  every secret needed, including the ones those connections reference —
  which the runner cannot name until it has them. Two sequential calls
  would double hub latency on exactly the request-reply and webhook paths
  that exist to avoid it (ADR-0035 §3). No cache: revocation stays
  immediate and the runner stays stateless.

  Resolution failures fail the task with **names only**; values never
  appear in logs or reports (e2e: `TestSecretsNeverAtRest`).

  Binding runs on **all four execution paths**: the hub-queued lease loop
  and the three runner-direct ones (`POST /hooks/{name}`,
  `/api/flows/execute`, `/api/flows/run`). Secret resolution lived only in
  the leaseloop until 2026-08-03, so runner-direct triggers shipped the
  literal reference object to the connector — a webhook flow using a
  credential simply did not work. Keeping connections and secrets in ONE
  call on ONE code path is deliberate: splitting them would recreate that
  failure mode one step later. A runner with no hub attached fails such a
  document with `ErrNoResolver` rather than passing references through,
  because the alternative error describes a bad password instead of a
  missing hub.

  The connector-vs-connection mismatch check runs here as well as at
  deploy: connections are not versioned (ADR-0034 open question 3), so a
  connection can be re-pointed after a deploy passed.
- **Signed connectors**: with `-hub` set, `connstore.Ensure` becomes the
  pool's locator. Order: operator `-connector-dir` first (local trust,
  unchanged dev workflow), registry second. `SHIFT_REQUIRE_SIGNED=1`
  disables the Dir fallback — everything must come verified from the
  registry (the compose bundle runs this way). Trust root = the hub's
  key list over the authenticated TLS channel; `SHIFT_TRUSTED_KEYS`
  (comma-separated base64) pins keys and disables hub fetch.
- **Restart identity**: registration tokens are single-use, so
  `SHIFT_HUB_CRED_FILE` persists the issued secret; `SHIFT_HUB_REG_TOKEN_FILE`
  reads the token from a file (compose hands it over that way); bounded
  registration retry (~60s) tolerates hub boot ordering.

## Flow documents

Declarative JSON — deliberately plain data (AI/developer-friendly, no DSL):

```json
{
  "name": "orders-rollup",
  "source": {"connector": "http", "action": "get",
             "config": {"url": "https://api.example.com/orders"}},
  "ops": [
    {"type": "filter",  "path": "$.active", "op": "eq", "value": true},
    {"type": "coerce",  "rules": [{"field": "amount", "to": "decimal"}]},
    {"type": "flatten", "sep": "_"},
    {"type": "project", "fields": [{"path": "$.id"}, {"out": "city", "path": "$.address_city"}]},
    {"type": "aggregate", "key": "$.region",
     "aggs": [{"op": "count", "out": "n"}, {"op": "sum", "path": "$.amount", "out": "total"}]}
  ],
  "sink": {"connector": "http", "action": "post",
           "config": {"url": "https://internal/rollups"}}
}
```

Validation is **eager** (submit time): paths compile, op shapes check,
filter values must be scalars. Compilation (`flow.Apply`) maps ops 1:1
onto engine operators; filter comparisons are `eq/ne` on scalars
(`EqualScalar`) and `gt/gte/lt/lte` via `record.Compare`; a path miss fails the
predicate (missing ≠ null), and so does an unorderable pair (a number against a
string, or a NaN) rather than an invented ordering.

### The `starlark` step (ADR-0052)

Where the declarative layer runs out — arithmetic, string functions, per-field
conditionals, anything touching a list — a step runs a small script:

```json
{"type": "starlark", "id": "calc", "script": "def transform(rec):\n    return {\"total\": rec.qty * rec.price}\n"}
```

`transform(rec)` returns a record, or `None` to **drop** it, so a script is also
a filter. The contract, and why each part is the way it is:

| Property | Why |
|---|---|
| In-process `go.starlark.net`, not wasm | The runner is already the trust boundary; a wasm crossing per record is the cost that would make the step unusable. ADR-0017's "type names the language, not the sandbox" keeps the move reversible. |
| Fuel, not wall-clock | Deterministic — a script always fits its budget or never does, so nothing depends on machine load. |
| No I/O, no `load()` | Nothing to import means **no supply chain**: nothing to pin, vendor, audit or revoke. |
| No clock, no randomness | Tasks are at-least-once (ADR-0002), so a transform that varies between attempts is a *correctness* bug, not an inconvenience. |
| Globals frozen after load | State cannot cross records, so results do not depend on batch boundaries. |
| Values adapted, not materialised | A script reading two fields of a fifty-field record pays for two. `map[string]interface{}` never appears — the reason JSONata was rejected as a runtime. |
| Decimals are exact in-script | `qty * price` on money is exact; mixing a decimal with a float is **refused**, and division points at `.rescale(n)` rather than rounding silently. |
| Errors carry no payload to the hub | The interpreter backtrace quotes values; that text would travel in an execution report. The hub gets step id and error class, detail goes to the sampler. |
| Config and secrets are out of scope | Otherwise a flow author gains an exfiltration path for every secret the node uses. |
| **Off unless `SHIFT_ALLOW_CODE_STEPS=1`** | The sandbox bounds blast radius, not *authority*. While the hub is open-access (RBAC = issue #16), anyone who can author a flow could otherwise run code on a runner. Fail closed; the refusal names the setting. |

The evaluator lives in `runner/internal/starlarkop` and attaches through the
engine's public `Pipeline.Apply`, so `engine` stays stdlib-only and never learns
Starlark exists.

Coerce targets are `flowdoc.CoerceKinds`:
`int|float|bool|string|decimal|timestamp|date|time`. The exact kinds are
ADR-0051's opt-in — the example above declares `amount` a **decimal**, so the
`sum` below it is exact to the cent rather than accumulating float error. The
name set and the runner's `kindOf` must agree, and a test asserts they do: a
name legal in one but not the other is a flow that validates at publish and
fails at run time.

A fractional filter literal (`{"op":"gt","value":100.50}`) is itself parsed as
an exact decimal, so a money threshold compares exactly against a decimal
column. Against a float column the comparison still goes through `float64`, so
no existing result changes.

**Flow model v2 (M5a, ADR-0013).** A document is a **graph of steps**.
The linear `source/ops/sink` above is kept as sugar; the graph form uses
`steps[]` + `start` with typed outcome edges: `onSuccess`/`onComplete`
(the happy path) and `onFailure` (an error handler — a `sink` step off the
main path). Both forms lower to one validated `Plan{Main, Catch}`
(`Document.Plan()`), so there is a single compile + telemetry path. Each
operator is labeled by its **step id** (telemetry `OpStats.Name`, and the
`stream.OpError` tag), so a run failure is routed via
`errors.AsType[*stream.OpError]` to the nearest covering `onFailure`
handler. The handler is fed **one payload-free error record** `{flow,
step, error, at}` and the task ends failed with `handled=true`; with no
handler the task fails exactly as before. Any resolved secret value is
redacted from the error text before it reaches `task.Error` or the handler
record (ADR-0010).

**Flow model v3 — the executable DAG (ADR-0029).** v2's outcome edges route
on a step's *terminal result*; v3 adds **data** edges that carry the record
stream itself to more than one place. Three step types:

- `tee` — `branches: [a, b, …]`, every record to every branch.
- `router` — ordered `routes: [{when, to}, …]` + optional `default`, each
  record to the **first** match only. Predicates reuse the filter grammar
  above; there is no second expression language.
- `merge` — `inputs: [a, b, …]` with `mode: concat` (unordered union) or
  `mode: join` (keyed, `on: {left, right}`, `type: inner|left`, matched
  build record nested under `as` so field names never collide).

`Document.Plan()` returns `Multi: true` with `Nodes`/`Data`/`Sources`/`Sinks`
populated and `Main` nil. Linear and v2 documents leave those zero-valued and
execute through `Main` exactly as before — **v2 is the degenerate DAG**, same
zero-copy, zero-goroutine happy path. The engine primitives are described in
`docs/dev/02-engine.md`.

`Service.executeMulti` (`internal/service/service_multipath.go`) compiles the
plan. It supports two topology families end to end:

```
fan-out:  source → ops → (tee|router) → { ops → sink } × N
fan-in:   { source → ops } × N → merge(concat|join) → ops → sink
```

Those two are the common shapes, not the limit. `dag.go` compiles **any**
validated DAG: it walks the plan's adjacency into linear segments joined at
tee/router/merge nodes (ADR-0029 §2), so nested fan-outs and mixed
fan-out/fan-in graphs run too. `source → tee → enrich → merge` — the
enrichment shape, and the most common real integration — is the case that
motivated it.

The join between stages is `stream.Pipe`: a bounded Sink↔Source pair. A
fan-out branch that runs straight to a sink keeps its real sink and its
zero-copy path; a branch that feeds a merge or another fan-out ends at a pipe
instead, and the node downstream reads the other half. Each stage is a
goroutine, because a merge cannot read until the branch upstream of it is
running — sequencing them would deadlock on the first full queue. Queue depth
is flow control inside one task, never a gate between tasks (ADR-0005), and
closing a pipe's reader releases a blocked writer so a failing stage cannot
wedge the one feeding it.

**Per-branch idempotency keys (ADR-0029 §5).** A flow with more than one
side-effecting sink has each sink's injected key derived as
`<task_key>:<stepID>`. One key across two sinks writing the *same* target
means the second write dedupes against the first and is silently lost —
nothing fails, nothing is logged, the record is simply not there. Step ids are
distinct by construction and stable in the plan, so a re-dispatched attempt
derives the same keys and an idempotent receiver still dedupes the retry.
Single-sink flows keep the bare task key: that is what §5 specifies, and
changing the key a sink sees is itself a hazard for any task in flight across
the upgrade. Built-in terminals (`@discard`, `@stop`, `@response`) have no
side effect and do not count toward the threshold.

### The error model: one cause, and a deliberate stop (ADR-0031)

Once an execution is more than one goroutine, "what went wrong" stops being
obvious. A fan-out cancels its shared context the instant any branch fails, so
the innocent siblings and the driver all return `context.Canceled`. Reporting
one of those buries the real cause; picking whichever finished first makes the
report a race.

`stream.Canonical` resolves the single error worth reporting, in order:

1. A deliberate `@stop` wins outright and resolves to **no error**.
2. The first **non-cancellation** error is the cause. Order is the caller's
   (the fan-out passes branches in declaration order), so it is deterministic.
3. A cancellation is the cause only when the **parent** context is done — the
   caller aborted, a client disconnected, a deadline fired. The executor's own
   teardown cancel is a derived context, so it never satisfies this.
4. Anything left over is still returned rather than swallowed: a cancellation
   with nothing to explain it is worth seeing (ADR-0005).

`stream.Classify` wraps that and additionally reports whether the run ended on
a stop and which step requested it, so a caller cannot check one half and miss
the other.

**`@stop`** is the third built-in terminal, alongside `@discard` and
`@response`. Routing a record into it ends the whole execution **as a
success** — the usual shape is one arm of a router, "if this holds, we're
done". The distinction from `@discard` is the point: `@discard` drops records
and lets the stream run to its natural end; `@stop` terminates.

It must not be modelled as an error, and the reason is concrete: an error would
be **retried** by the hub — re-running a flow the author deliberately ended —
and, once a default dead-letter handler exists, would be **dead-lettered**,
alerting on a clean exit. So the sentinel is classified before any error
classification runs, and the task ends `completed` with `stopped=true` plus the
stopping step id, counted as `shift_flow_stops_total` (a **subset** of
completed, not a peer — `completed + failed` still totals every terminal task).

A `@stop` that is never reached costs nothing: its `Write` is simply never
called.

Honest limitation (ADR-0031 §5): branches are concurrent, so a stop **does not
roll back sibling side effects already in flight**. It cancels contexts; it
does not un-write a file or un-POST a request. Ordering across branches is
explicit sequencing (a linear chain), never an implicit gate on a tee.

### Resume: a cursor is control metadata (ADR-0037)

A re-dispatched task restarts from where the previous attempt's sink got to,
rather than from the beginning — and, because runners are replaceable by
design, usually on a *different* runner. Three pieces:

1. **The source reports a position.** Optional connector capability
   (`sdk.ResumableSource`); see `docs/dev/03-connector-protocol.md`. A source
   that does not implement it reports nothing and nothing is recorded.
2. **The engine reports it only when the sink confirms.**
   `Pipeline.WithCheckpoint` fires after each successful `sink.Write`, which
   is the only moment the source's position and "everything delivered" agree.
   A position taken on *source* progress would, on resume, skip records that
   were read but never written — silent data loss, strictly worse than the
   duplicate work resume exists to avoid.
3. **The hub stores it.** Sent on the heartbeat, gated by the same lease check
   as the heartbeat itself — a runner whose lease expired has been superseded,
   and letting it write a cursor would let a zombie rewind the attempt that
   replaced it. Returned on the next `Claim`.

**Eligibility is a property of the plan** (`flow.Resumable`), decided before
execution, and it is a correctness constraint rather than an optimisation:

| Plan | Resumable | Why |
|---|---|---|
| source → filter/project/coerce/flatten → sink | yes | streaming; each confirmed write covers a known prefix |
| any plan containing `aggregate` or a `merge` join | **no** | blocking: the whole input is drained before anything is emitted, so the first confirmed write already reports end-of-input — and rebuilding from a suffix loses the state for the skipped prefix, making the output quietly *wrong* |
| multi-path (v3 DAG) | **no** | several sources with independent positions and a per-branch confirm point; there is no single cursor to record |

**The cursor is pinned to the connector build that produced it.** A cursor is
opaque bytes only its producer understands, so a v0.3 cursor read under v0.4
could resolve to a *different* position and resume at the wrong place with
nothing downstream able to notice. The runner refuses to resume on a mismatch
(and on a cursor with no recorded identity) and replays from the start
instead — slower, and correct.
### Finding a bottleneck (ADR-0039)

Every execution records enough to answer "where did the time go" without
turning anything on.

**Per step** (`task.OpStat`, on the task and in the hub execution report):

| Field | Reads as |
|---|---|
| `records_in` / `records_out` | how many |
| `bytes` | how much — the scale dimension record counts hide |
| `batches` | batching health; a count approaching the record count means per-record overhead |
| `seconds` | this step's OWN work (exclusive) |
| `wall_seconds` | this step INCLUDING everything upstream (inclusive) |

The last pair is the one that changes what is diagnosable. `seconds` alone says
how expensive a step is. **`wall_seconds` minus `seconds` is time spent
waiting** — and in a pull pipeline a step blocked on a slow sink or a full
fan-out queue is starved, not expensive. Without both numbers those look
identical.

`bytes` is exact at the source and **approximate downstream**: operators mutate
the flowing batch in place and share its arena, so a step that rebuilds records
also counts bytes written upstream. Read the source's figure as the flow's true
input size.

**Per execution** (`task.Phases`) — a fixed handful of numbers, so it rides the
report at any throughput rather than needing a log pipeline:

- `admission_ms` — queued on resource capacity before running. **The span
  nothing else can show**: a runner at its memory ceiling (ADR-0005) is
  otherwise indistinguishable from a slow integration.
- `bind_ms` — connector checkout/spawn plus pipeline build, separating "the
  connector was slow to start" from "the data was slow".
- `run_ms` — the pipeline itself.
- `total_ms` — submit to terminal state.

Not built yet, and specified in ADR-0039: log levels and per-flow log capture,
the `task_steps` table for cross-execution queries, OTel span export, and a
trace id spanning gateway → hub → runner → connector.

## Task lifecycle and admission (ADR-0005 in practice)

```
Submit → validate → task recorded (waiting) → admission → running → completed|failed
```

Admission is the **only queueing**: each task reserves
`task-watermark + overhead` (defaults 64 MiB + 16 MiB) against the
runner-wide governor (`-mem-budget`, default 1 GiB). If the reservation
fails, the task waits for a release broadcast — there is no task-count cap
anywhere, and never will be. The waiter captures the release channel
**before** re-testing capacity (condvar order); the reverse would drop a
release that fired in the gap and strand the task (lost wakeup). Inside a
task, stateful operators (aggregate) get their **own** engine governor with
the task watermark as budget and spill beyond it, so one task's heavy
group-by can't starve its siblings.

Each task runs under its own context derived from a service base context.
`-task-timeout` (env `SHIFT_RUNNER_TASK_TIMEOUT`, default 0 = off — streaming
workloads are legitimately long) bounds a single task; and on drain the base
context is cancelled, so a hung connector's RPC stream is aborted and its
admission reservation is freed rather than stranded for the process lifetime.
Task goroutines also `recover`: a panicking plan fails that one task, never
the shared process (defense in depth behind flow validation).

Every task records honest per-operator stats (records in/out, seconds of
that operator's own work) visible in the API and dashboard. The result
ring (`internal/task.Store`, last 500) is **not durable** — restart loses
history by design; durable truth arrives with the hub.

Draining: SIGTERM stops intake, waits for running tasks (30 s bound); if the
bound elapses it cancels every task context to force-abort stragglers (with a
short grace), then closes the connector pool.

## Connector pool (`internal/connpool`)

One live subprocess per connector name (`<dir>/shift-connector-<name>`;
names validated against `^[a-z0-9][a-z0-9-]{0,63}$`). First use spawns
(via `host.Launch`); reuse health-checks first and relaunches crashed
processes transparently; the reaper closes processes idle past
`IdleTTL` (5 m). `Launches()` counts spawns for observability/tests.

## The capacity benchmark (ADR-0008)

`POST /api/benchmark {"records": N, "streams": K}` (defaults: 1M records,
K = GOMAXPROCS clamped to the memory ceiling). It runs the **production
path** — real gen-connector subprocess, representative ops, real sink —
first as one stream, then K concurrent streams, all as visible tasks that
respect admission. The report:

| Field | Meaning for the admin |
|---|---|
| `single_stream_rec_s` | best-case per-flow throughput on this box |
| `aggregate_rec_s` | measured whole-runner throughput at K streams |
| `scaling_efficiency` | aggregate ÷ (single × K); low ⇒ CPU-bound here |
| `max_concurrent_by_mem` | admission ceiling — beyond this, tasks wait |

The dashboard's "waiting for capacity" counter plus this report is the
add/subtract-compute signal; the same numbers are the intended input for
hub-side placement later. Estimates never extrapolate beyond what was
measured.

**Tiered workload benchmark (M5e).** `POST /api/benchmark/tiers` sweeps
graded process shapes and reports `single_stream_rec_s` /
`aggregate_rec_s` / `scaling_efficiency` **per tier**, so throughput is
never one number hiding the shape it was measured on:

| Tier | Shape |
|---|---|
| simple | passthrough (source → sink) |
| standard | filter + coerce + project |
| complex | flatten + aggregate (high-cardinality, spill-capable) |
| extreme | filter + flatten + project + aggregate (very high cardinality) |

Every tier runs the production path (gen source → discard sink) and lowers
through the v2 flow Plan like any flow, so the numbers are reproducible on
any runner with no external target — the honest basis for incumbent
comparison (M6 collateral). An http-sink "extreme" profile is deferred: a
live endpoint under load would couple the figures to an unrelated network
target.

## Trigger-path throughput (`internal/api`, Go benchmarks)

The capacity benchmark above measures **records/sec** through one flow.
What it does not answer is **invocations/sec** through the public HTTP
surface — the number webhook and request-reply workloads live on, and the
one any "N tps" claim rests on. `throughput_bench_test.go` measures it end
to end over a real `httptest` server and a real connector subprocess:

```
GOMAXPROCS=4 go test ./internal/api/ -run '^$' -bench Throughput -benchmem
```

Indicative (Apple M4 Max, `GOMAXPROCS=4`, one record per invocation, so
this is per-invocation OVERHEAD rather than engine work):

| Benchmark | tps @ 4 cores | Path |
|---|---|---|
| `SyncRunThroughput` | ~4,300 | `POST /api/flows/run` — caller waits for the output |
| `WebhookThroughput` | ~7,500 | `POST /hooks/{name}` — 202 then async, timed to completion |
| `SyncRunReportOverhead` | ~4,270 | as sync, with a 5 ms stand-in hub reporter |

A fast laptop core is not a cloud vCPU — treat the ratios as the durable
part and re-measure on target hardware before quoting a figure.

`SyncRunReportOverhead` tracking `SyncRunThroughput` is the regression
guard for ADR-0035 §3: the hub execution report must stay **off** the
response path. Reported inline it would pin the sync path near
`in-flight ÷ hub-latency` instead.

Two traps the file documents, because both produced badly wrong numbers
before they were found:

- **Cold start.** The first connector subprocess launch costs ~1s, so
  without a warm-up the sync benchmarks settle at `b.N=1` and report ~1
  tps — the process launch, not the path.
- **Measuring the instrument.** The async path must be closed-loop
  (bounded in flight) or `b.N` just measures how fast a queue fills. And
  completion must be read from the service's lifetime totals, not the
  execution-report callback: the old `reportWhenDone` polled at 200 ms, so
  gating on it capped the result at `in-flight ÷ 200 ms` — 140 tps for a
  path that actually does ~7,500.

**Between-run drift is larger than it looks.** Five samples in one session
land within ±5%, but the same benchmark measured in a different session
moved ~20% on unchanged code. Any before/after claim has to A/B in ONE
session (`git stash`, measure, pop, measure) or it is measuring the
machine, not the change. Two plausible-looking wins evaporated under that
test — see below.

### Does tps change with integration complexity?

`SyncRunThroughput` measures one record through source → sink, which is the
cheapest flow that can exist — per-invocation **overhead** and nothing else.
`shape_bench_test.go` sweeps the two axes separately (shapes mirror the
tiers above, so invocations/sec sits beside the tiered records/sec figure
for the same workload).

**Operator depth barely matters.** At one record, passthrough → the
four-operator "extreme" shape costs about 10%, which is barely outside
run-to-run noise:

| Shape @ 1 record | tps @ 4 cores |
|---|---|
| simple (passthrough) | ~5,200 |
| standard (filter + coerce + project) | ~5,360 |
| complex (flatten + aggregate) | ~4,910 |
| extreme (filter + flatten + project + aggregate) | ~4,680 |

**Records per invocation is the whole story:**

| Records | standard | extreme |
|---|---|---|
| 1 | ~5,000 tps | ~4,310 tps |
| 100 | ~3,450 tps | ~3,000 tps |
| 1,000 | ~1,090 tps | ~1,080 tps |
| 10,000 | ~271 tps | ~285 tps |

Two things fall out of this.

**A simple cost model fits both series**: roughly **200 µs fixed per
invocation plus ~350 ns per record**, near enough independent of shape. The
crossover — where payload starts costing more than the trigger machinery —
lands around **500–600 records**. Below that you are measuring overhead;
above it you are measuring the engine.

**Standard and extreme converge as payload grows** (1,090 vs 1,080 at 1,000
records; the heavier shape is even nominally *faster* at 10,000). Operators
are close to free next to moving records across the connector boundary,
which is the streaming thesis showing up as a measurement. The aggregate in
the heavier shapes also *reduces* what reaches the sink, paying back some of
its own cost.

Practical reading: for trigger-rate sizing, payload size is the question to
ask, not flow complexity. Even 10,000-record invocations sustain ~275/sec on
four cores.

### What the execution-report rewrite did and did not buy

`reportWhenDone`'s 200 ms poller is gone, replaced by
`service.SubmitOpts.OnDone` — a completion callback the service invokes
once the task is terminal. Likewise the webhook trigger no longer re-parses
its flow document per request (`webhook.Hook.Parsed`, built by
`webhook.NewHook` at registration / sync).

Measured honestly, neither changes **throughput**:

- Report latency drops from ~200 ms to immediate. That is the real win, and
  it is asserted by `TestDirectExecutionReportsWithoutPolling`, whose bound
  is deliberately set to fail against the old implementation (it measured
  201 ms).
- A goroutine per in-flight direct execution, sleeping on a 200 ms ticker
  for the task's whole life, is gone. Resource churn, not throughput —
  a sleeping goroutine costs little CPU.
- Re-parsing cost is negligible for the small documents these benchmarks
  use. It matters in proportion to document size, which is where real
  flows differ from the fixture.

A benchmark that pairs the webhook path with a reporter appears to make the
NEW code ~11% slower. It does not: the old poller deferred its report work
past the 200 ms tick and therefore outside the measured window, while the
callback does it inside. The comparison is invalid across versions, which
is why no such benchmark is committed — the latency assertion is the honest
guard.

## Test-only behaviour (ADR-0048 §5)

Capture tells you what a flow DID; it does not stop a test run writing to a
real sink. Sampling an SFTP `put` does not stop the file landing on the
customer's server.

The answer is an option on the REAL step, never a node that stands in for it:

| | Where | In test | Deployed |
|---|---|---|---|
| `mock` | connector **sink** step | records what would have been written | inert — the connector writes |
| `testInput` | connector **source** step | emits the configured records | inert — the connector reads |
| `probe` | a step type of its own | taps the stream and reports into the capture | compiles to **nothing** |

The connector, its config and its version pin stay in the document in every
case, so a deployed flow is complete by construction — and the practical
consequence is that **a mock never has to be removed before shipping**. The
flow running in production is the same document that was tested, with the
diversion simply not taken.

`SubmitOpts.Test` is what makes them live, and it is the HUB's statement,
carried on the lease. A runner that decided for itself could stop a sink
writing by calling its own work a test.

Two details are load-bearing:

- **A deployed probe is not an operator that does nothing — it is no operator
  at all.** `applyTransforms` skips it, so a probe left on a canvas costs a
  deployed flow no batch hand-off, no rename and no telemetry row. "Strictly
  inert" is a claim about the compiled pipeline, not just its output.
- **An unchecked mock drives the real sink even in a test run**, which is how
  an author says "hitting the real system IS the test".

The tests use a connector that is deliberately **not installed**, so every
assertion is unambiguous: a run that completes proves the diversion held and
the connector was never touched; a run that fails naming the connector proves
the real one was used. Nothing depends on which binaries happen to exist.

## Test-mode data capture (M5c, ADR-0014)

When a task is submitted with capture on (`POST /api/flows/execute?capture=1
[&capture_max=N]`, or `SubmitOpts.Capture`), the engine's `Sampler` hook
records a **bounded sample** (default 20 records) of **each stage's output**
— the source and every operator, keyed by step id. It is:

- **payload, so runner-only** — held on the in-memory task, read back via
  `GET /api/tasks/{id}/capture`; the hub never sees it;
- **redacted** — serialized to NDJSON and passed through the same secret
  redactor as error text (ADR-0010), at the text layer so all values mask;
- **ephemeral** — evicted with the task from the ring; no store, no TTL, no
  encryption (nothing at rest);
- **best-effort** — never fails or stalls a task; stops at the bound.

**On a v3 DAG every path samples** (#60, fixed). Capture used to come back
empty for anything using tee/router/merge because `executeMulti` discarded the
sampler — and an empty capture reads as *"no records flowed"*, not *"nobody was
watching"*. The sampler is now wired onto the upstream pipeline, **each
branch**, each merge input and the downstream, which is where capture earns
most: "which branch did this record take" and "did the join match" are
unanswerable from an upstream sample alone.

That makes the sampler genuinely concurrent — branches run in their own
goroutines (ADR-0029) — so its mutex is load-bearing rather than a guard
against a later reader, and it covers the shared scratch batch. The bound is
per step, so a fan-out does not multiply the sample by the number of paths.

Off by default; the lease path leaves it off (hub-driven test runs land with
the studio, M5d). The dashboard shows samples inline in the task detail.
Durable/encrypted payload storage + TTL + erasure + OTel/Splunk push is a
deferred enterprise layer (ADR-0014 Consequences).

## Webhooks / direct execution (M5d-2, ADR-0016)

Beyond leased (pull) work, a runner accepts **direct** (push) execution: an
inbound `POST /hooks/{name}` runs a registered flow with the **request body
as its source**. The flow's source is the built-in `@webhook` connector
(`pkg/flowdoc`, reserved, source-only, exempt from the registry/capability
policy); at bind time the runner wraps the body as an NDJSON source instead
of spawning a source subprocess. Payload stays entirely on the runner — it
never reaches the hub (the whole point: the hub holds no payload).

- **Async by default:** the body is buffered (bounded, 8 MiB), the flow is
  submitted, and the caller gets `202 + task_id` — poll `/api/tasks/{id}`
  (and `.../capture`) for status/results. A per-execution sync toggle rides
  the same machinery (later stage).
- **Synchronous run (ADR-0024):** `POST /api/flows/run` runs a posted flow
  **inline** and returns its result in the **same** response — the
  request-reply call. `Service.RunSync` executes under normal admission (a
  busy runner holds the call until capacity frees), then: a flow terminating
  at the built-in **`@response`** sink streams its output as
  `application/x-ndjson` (200, headers `X-Shift-Records` + `X-Shift-Task-Id`),
  buffered into a bounded (8 MiB) `boundedBuffer` so a clean status precedes
  the body; a non-`@response` terminal returns the task summary JSON; a failed
  task returns `422` + the redacted error. `@response` (`pkg/flowdoc`,
  reserved, sink-only, exempt from registry/capability/signing) is the egress
  twin of `@webhook`: **payload never reaches the hub.** It degrades to a
  counting drop when no writer is supplied (a `@response` flow on the async
  path). Basic-test recipe: `gen` source → `@response` sink returns the
  generated document.
- **Auth:** hook endpoints authenticate by a per-webhook token
  (`X-Webhook-Token` or `Authorization: Bearer`, constant-time). The
  **control surface** (`/api/*`, dashboard) is guarded separately —
  see below.
- **Registration:** two sources into one in-memory registry. Hub-attached
  runners **sync** their hooks from the hub every 30s (`GET
  /api/v1/webhooks/sync` → name + token hash + published document), replacing
  the registry — the hub is authoritative. Standalone runners register hooks
  locally (`PUT /api/webhooks/{name}` with `{document, token}`). Tokens are
  held only as SHA-256 hashes and an incoming token is hashed to verify.
- **Hub load visibility:** direct executions (webhook and local
  `/api/flows/execute`) never enter the hub queue, so when attached to a hub
  the runner reports each finished one as **metadata** — flow, outcome,
  record counts, timing, never payload — to `POST /api/v1/executions`. A
  best-effort watcher (`reportWhenDone`) fires once the task is terminal.
  Leased tasks keep reporting through the normal complete/fail path.

## Control-surface auth (M5d-2, ADR-0016)

`internal/auth` guards the control surface once runners are public. **Opt-in:**
with no users configured the surface is open (loopback dev, all existing
callers keep working); `SHIFT_RUNNER_USERS="user:bcrypt-hash:role;…"` turns
it on. Method today is **HTTP Basic** (bcrypt-hashed passwords) behind an
`Authenticator` interface, so bearer/OIDC/mTLS drop in later.

**Roles → permissions:** `admin` (read+execute+manage), `operator`
(read+execute), `viewer` (read). A single middleware (`Guard.Wrap`) derives
the permission per request — GET ⇒ read, PUT/DELETE ⇒ manage, other writes ⇒
execute — authenticates once, and enforces. `/healthz` and `/hooks/*` are
unguarded by user auth (the latter uses its per-hook token). Browsers get a
native Basic prompt (`WWW-Authenticate`), so the dashboard just works. A
non-loopback bind with no users logs a loud warning.

## HTTP surface

| Route | Purpose |
|---|---|
| `GET /` | embedded dashboard (poll-based, no external assets) |
| `GET /healthz` | liveness |
| `GET /api/status` | governor, totals, pool, latest capacity report, hub intake stats |
| `POST /api/flows/execute` | submit a flow document → `{task_id}` (202) |
| `GET /api/tasks[?limit=]`, `GET /api/tasks/{id}` | results + per-op stats |
| `GET /api/tasks/{id}/capture` | per-step INPUT/OUTPUT samples (test mode; runner-only, redacted) |
| `PUT/GET/DELETE /api/webhooks[/{name}]` | register/list/remove direct-execution hooks (runner-local, this stage) |
| `POST /hooks/{name}` | trigger a hook: request body → flow `@webhook` source; 202 + task_id (per-hook token) |
| `POST /api/benchmark`, `GET /api/benchmark` | run/read capacity reports |
| `POST /api/benchmark/tiers`, `GET /api/benchmark/tiers` | run/read tiered workload reports (M5e) |

**Security posture:** binds `127.0.0.1` by default and is unauthenticated
— hub-issued identity (M4) must land before any non-local bind ships.
Config: flags or `SHIFT_*` env vars (see `runnerd -h`).

## Hub intake (M3b)

`runnerd -hub <url>` (+ `SHIFT_HUB_REG_TOKEN`, single-use) registers the
runner and starts `internal/leaseloop` alongside the local API — a second
intake over the same `service.Submit` path, exactly as ADR-0008 promised:

- **Capacity-gated claiming (ADR-0005):** the loop leases only while the
  governor has headroom for another task; a busy runner leaves work on
  the hub queue for idle runners instead of hoarding it.
- **Heartbeats at TTL/3.** A lost lease (409) stops reporting but lets
  the local task finish — the injected idempotency key (stable across
  attempts; sinks like `http` emit it as `Idempotency-Key` per batch)
  keeps the duplicate side-effect-free.
- **SIGTERM drain:** stop leasing, finish + report in-flight tasks, then
  shut down. SIGKILL needs no cooperation at all — the lease expires and
  the hub re-dispatches (`hub/e2e/crash_recovery_test.go`).

**Credential: mTLS or bearer (ADR-0044).** `-identity-dir` (`SHIFT_RUNNER_IDENTITY_DIR`)
switches the runner to a client certificate:

- First boot generates a **P-256 key in-process**, sends a CSR with the
  single-use registration token, and writes the issued bundle
  (`runner.pem`, `runner-key.pem`, `ca.pem`, `runner-id`, all 0600) into the
  directory. The private key never leaves the process; only a public key
  inside a CSR does. There is no bearer secret at all.
- The hub assigns the subject, ignoring whatever the CSR asked for — a runner
  that could name itself could name another runner.
- **Renewal is the runner's own job**, unlike the gateway (ADR-0041 §4, where
  the hub must push): `RenewLoop` renews at half the *remaining* lifetime with
  a one-minute floor, using a **fresh key every time**, authenticating with the
  current certificate — no operator token, or a fleet needs a human in the loop
  every day. On repeated failure the runner **keeps working and keeps
  retrying**, more often as expiry approaches, and logs at ERROR inside the
  last hour: a renewal outage must not become a work stoppage before the
  certificates have actually expired.
- The certificate is swapped through an atomic pointer read by
  `GetClientCertificate`, so one transport and one connection pool survive a
  renewal with no window in which a caller holds a stale certificate.
- Server trust is the **system store plus the control-plane CA**, and
  `-hub-ca` adds to it. Pinning to the control CA alone would break every hub
  fronted by a public or corporate certificate.

Without `-identity-dir` the runner keeps the ADR-0009 bearer secret in
`-cred-file`, which is what a deployment terminating TLS at a proxy needs.

See [06-hub.md](06-hub.md) for the hub side of the protocol.

## Gateway intake (`internal/gwclient`, ADR-0038)

With a hub attached, gateway intake needs no configuration at all: the runner
asks `GET /api/v1/gateways/sync` which gateways to poll and starts a **third**
intake over the same task service — one long-poll group per gateway, added and
withdrawn as the hub's answer changes. `-gateways <url,url>` adds static
addresses to that list (and is the whole list without a hub); an address given
locally is never withdrawn by a remote answer.

The direction is the whole point. The gateway sits in a DMZ and never dials
inward — the runner reaches **out** to it, so a runner behind NAT, a firewall,
or a Kubernetes policy that denies all ingress is still serving inbound HTTP
traffic. It is not reachable; it is a client.

```
poll (park) → gateway hands over a request → look up the flow by NAME
            → RunSync with the body as @webhook, output to @response
            → POST it back to /api/v1/gw/deliver/{id}
```

Four properties worth stating because each is load-bearing:

- **The gateway sends a flow NAME, never a document.** A DMZ component that
  could hand a runner arbitrary code to execute would be a remote-execution
  primitive with extra steps. The name resolves against the hub-synced webhook
  registry; an unknown name is a 404 delivered back, not a hang.
- **Every exit path delivers something.** A caller is blocked on the gateway,
  so a runner that returns nothing costs them the full delivery timeout.
  Unknown flow, secret-resolution failure and execution failure all deliver a
  status.
- **Delivery survives shutdown.** It runs on `context.WithoutCancel` with its
  own deadline: abandoning it on SIGTERM would turn a completed execution into
  a 504 for someone who is still waiting.
- **A gateway being down must not take the runner with it.** Poll failures back
  off exponentially per gateway; the hub lease loop and the other gateways
  carry on.

- **A discovery failure is not an empty list.** An unreachable hub says nothing
  about which gateways exist, so a failed pass keeps the current set. The
  control plane must not be able to take the data plane down with it.

Labels are what the runner **is**, and the hub asserts them (ADR-0041 §3) — the
runner tells a gateway nothing about itself. The same labels decide both halves:
which gateways it is told to poll, and which routes those gateways will pick it
for.

Because capacity is checked when the runner decides to poll rather than when
work lands, a runner parked on several gateways can be handed several requests
at once. That overshoot falls through to the existing ADR-0005 admission path —
tested behaviour, not a new failure mode.

## What's deliberately NOT here yet

- Step-level error routing (`onFailure` handlers) landed in M5a (ADR-0013);
  task cancellation API and per-flow retry policy remain future work.
- Webhook/custom-API triggers on the runner → M5.
- Dashboard auth → M4b hub-issued identity (until then: loopback bind only).
