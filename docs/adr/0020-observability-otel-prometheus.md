# ADR-0020: Observability — OpenTelemetry traces + Prometheus metrics

Date: 2026-07-20
Status: Accepted. Metrics implemented (M6a stage 1). **Tracing deferred**
(2026-07-20) — see "Tracing" below.

## Context

M6 (enterprise hardening) opens with observability: it is the foundation the
rest of M6 leans on — billing aggregates from telemetry, rate limiting reacts
to it, and operators need traces/metrics to run the platform. The engine
already produces **honest per-op metrics** (real allocation accounting, not
wall-clock-as-CPU or global MemStats deltas — doctrine), and the hub already
has per-step-id telemetry (ADR-0013) and an audit log. What is missing is a
standard way to **export** it: scrape-able metrics and distributed traces that
cross the hub↔runner boundary.

Constraints from doctrine:
- **Engine stays stdlib-only.** No telemetry library may be imported by
  `engine/`. The engine keeps emitting its own metric structs; the runner
  reads them and translates. Same for `pkg/flowdoc`/`pkg/consign`.
- **Two planes (ADR-0016).** The control plane carries metadata only; the data
  plane never touches the hub. Telemetry must honor this: **spans and metrics
  carry metadata only — step ids, connector names, record/byte counts,
  durations, outcomes — never payload, never secret values** (secret-redacted
  like all error text, ADR-0010).
- **Honest metrics.** CPU/allocation come from the engine's real per-op
  accounting, never wall-clock-as-CPU or process-wide MemStats deltas.
- **No heavy frameworks** generally — but a telemetry standard is worth a
  vetted dependency; see Decision.

## Decision

Adopt **OpenTelemetry (Go SDK)** as the single telemetry stack, exporting
**traces via OTLP** and **metrics via a Prometheus scrape endpoint** (the
OTel Prometheus exporter, so one library covers both). OTel is the vetted
control-plane dependency here, alongside `go-oidc`/`robfig-cron` (ADR-0009/12);
it lives **only in `hub/` and `runner/`**, never in `engine/` or `pkg/`.

### Metrics (Prometheus)

- `GET /metrics` on **hubd** and **runnerd** (own handler; loopback/scrape).
- **Hub:** queue depth by state, oldest-queued age, lease count/age, task
  outcomes, scheduler pass/fired counts + last-error, connector-registry
  size, HTTP request latency/status by route+realm.
- **Runner:** admission signals (memory-watermark headroom, active streams,
  admission waits — ADR-0005), connector-pool size/reuse/idle-reap,
  per-step throughput (records/s, bytes/s) and **allocation** sourced from the
  engine's existing per-op metrics, task durations, webhook/direct-exec counts.
- **Cardinality is bounded by construction:** labels are flow name, step id,
  connector, action, state, route, realm — all low-cardinality. **Task id and
  trace id are never metric labels** (unbounded) — they live on spans.

### Traces (OpenTelemetry / OTLP) — DEFERRED 2026-07-20

**Deferred after metrics shipped.** Rationale: much of the per-task causal
story tracing would provide **already exists in the hub** — the
`task_attempts` history (attempt trail, outcomes, lease-expiry → re-dispatch)
and the per-step `OpStats` captured on completed tasks (records in/out +
per-op nanos, ADR-0013/0014). Metrics (stage 1) cover the aggregate/alerting
need. So tracing's *unique* marginal value is narrower than metrics': (1)
**cross-service timing** not measured today (queue-sit and lease-wait
durations), and (2) integration with standard trace tooling (Jaeger/Tempo) —
against the cost of instrumenting the request path, a collector dependency,
and sampling. Not worth it right now versus other M6 work (audit log, rate
limiting).

**Revisit when** any of: operators need queue-sit / lease-wait tail latency
per task; a support workflow wants to drill a single slow/failed execution in
standard trace tooling; or multi-hop flows (sub-flows/fan-out) make the causal
graph too complex for the attempts table. A lighter first step is available —
add only the timing spans that don't already exist (queue-sit, lease-wait,
per-step durations), OTLP off-by-default, without elaborate attribute
modelling. The design below stands for whenever it is picked up.

- Spans model a task's life: hub `enqueue` → `lease-claim` → runner
  `execute` → per-**step** spans → `report`. Trace context propagates across
  the **control plane** (injected into the lease/dispatch + execution-report
  metadata, ADR-0016) so a task's hub and runner spans join one trace.
- Span attributes are metadata only (step id, connector/action, record/byte
  counts, outcome, idempotency key hash) — **no payload, no secret values.**
- OTLP endpoint + sampling configured by flag/env; **off by default** (no
  endpoint ⇒ a no-op tracer, zero overhead) so the "just runs" bundle and
  loopback dev stay dependency-light.

### Engine boundary

`engine/` gains nothing. It already exposes per-op metrics structs; the runner
reads them after each stage and records OTel metrics/span attributes. The
translation layer lives in `runner/internal` (e.g. a `telemetry` package).

## Consequences

- New vetted dependency (`go.opentelemetry.io/otel` + OTLP + Prometheus
  exporter) in hub and runner only. `make check`/govulncheck cover it; engine
  and `pkg/` dependency hygiene is preserved (enforced by their being separate
  modules that do not require it).
- Telemetry is **safe by construction** re: the two-plane split — a reviewer
  checks that no payload/secret is ever added to a span/metric; the data-free
  discipline already applied to the studio graph view applies here.
- Off-by-default keeps loopback/dev and the bundle unchanged until an operator
  sets an OTLP endpoint / scrapes `/metrics`.
- Billing (M6) can aggregate from the same metric/telemetry substrate rather
  than a bespoke counter; rate limiting (M6) can export its own metrics
  through the same registry.

## Open questions (resolve at build)

- OTLP transport (gRPC vs HTTP) default; sampling strategy (parent-based +
  ratio) defaults.
- Whether `/metrics` needs its own auth/allowlist or relies on network
  posture (loopback + scrape-network), consistent with the ADR-0009 realms.
- Exact metric names/units — settle a naming convention doc under `docs/dev/`
  before wide instrumentation to avoid churn.
- Exemplars linking metrics↔traces (nice-to-have; only if the exporter path
  makes it cheap).
