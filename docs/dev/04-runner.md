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

- **Secrets**: documents arrive carrying inert `{"$secret":"name"}` refs.
  `internal/secretref` resolves them per task via the runner-realm
  `POST /api/v1/secrets/resolve` (no cache — revocation is immediate, the
  runner stays stateless) and substitutes into a copy of the document.
  Resolution failures fail the task with **names only**; values never
  appear in logs or reports (e2e: `TestSecretsNeverAtRest`).
  One round trip per task, not per secret — every ref goes in one
  batched call.
  Resolution runs on **all four execution paths**: the hub-queued lease
  loop and the three runner-direct ones (`POST /hooks/{name}`,
  `/api/flows/execute`, `/api/flows/run`). It lived only in the leaseloop
  until 2026-08-03, so runner-direct triggers shipped the literal
  reference object to the connector — a webhook flow using a credential
  simply did not work. A runner with no hub attached fails such a
  document with `ErrNoResolver` rather than passing the ref through,
  because the alternative error describes a bad password instead of a
  missing hub.
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
    {"type": "coerce",  "rules": [{"field": "amount", "to": "float"}]},
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
(`EqualScalar`) and `gt/gte/lt/lte` numeric-only; a path miss fails the
predicate (missing ≠ null).

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
record (ADR-0010). DAG data-branching (parallel fan-out/merge, multi-sink)
is deferred to a later M5 chunk.

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

See [06-hub.md](06-hub.md) for the hub side of the protocol.

## What's deliberately NOT here yet

- Step-level error routing (`onFailure` handlers) landed in M5a (ADR-0013);
  task cancellation API and per-flow retry policy remain future work.
- Webhook/custom-API triggers on the runner → M5.
- Dashboard auth → M4b hub-issued identity (until then: loopback bind only).
