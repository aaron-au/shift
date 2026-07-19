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

- **Secrets**: documents arrive from the hub carrying inert
  `{"$secret":"name"}` refs. The leaseloop resolves them per task via
  the runner-realm `POST /api/v1/secrets/resolve` (no cache — revocation
  is immediate, the runner stays stateless) and substitutes into a copy
  of the document. Resolution failures fail the task with **names only**;
  values never appear in logs or reports (e2e: `TestSecretsNeverAtRest`).
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
filter values must be scalars. Compilation (`Document.Apply`) maps ops 1:1
onto engine operators; filter comparisons are `eq/ne` on scalars
(`EqualScalar`) and `gt/gte/lt/lte` numeric-only; a path miss fails the
predicate (missing ≠ null). v1 is linear source→ops→sink; DAG shapes are
M5.

## Task lifecycle and admission (ADR-0005 in practice)

```
Submit → validate → task recorded (waiting) → admission → running → completed|failed
```

Admission is the **only queueing**: each task reserves
`task-watermark + overhead` (defaults 64 MiB + 16 MiB) against the
runner-wide governor (`-mem-budget`, default 1 GiB). If the reservation
fails, the task waits for a release broadcast — there is no task-count cap
anywhere, and never will be. Inside a task, stateful operators (aggregate)
get their **own** engine governor with the task watermark as budget and
spill beyond it, so one task's heavy group-by can't starve its siblings.

Every task records honest per-operator stats (records in/out, seconds of
that operator's own work) visible in the API and dashboard. The result
ring (`internal/task.Store`, last 500) is **not durable** — restart loses
history by design; durable truth arrives with the hub.

Draining: SIGTERM stops intake, waits for running tasks (30 s bound), then
closes the connector pool.

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

## HTTP surface

| Route | Purpose |
|---|---|
| `GET /` | embedded dashboard (poll-based, no external assets) |
| `GET /healthz` | liveness |
| `GET /api/status` | governor, totals, pool, latest capacity report, hub intake stats |
| `POST /api/flows/execute` | submit a flow document → `{task_id}` (202) |
| `GET /api/tasks[?limit=]`, `GET /api/tasks/{id}` | results + per-op stats |
| `POST /api/benchmark`, `GET /api/benchmark` | run/read capacity reports |

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

- Task cancellation API, per-flow retry/error routing → M5 flow model.
- Webhook/custom-API triggers on the runner → M5.
- Dashboard auth → M4b hub-issued identity (until then: loopback bind only).
