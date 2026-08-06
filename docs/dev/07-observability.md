# Observability (M6a, ADR-0020)

OpenTelemetry is the single telemetry stack. It lives **only in `hub/` and
`runner/`** — `engine/` and `pkg/` stay telemetry-free (the engine emits its
own metric structs; the runner translates them). Everything exported is
**metadata only** — never payload, never secret values (two-plane split,
ADR-0016; honest metrics from the engine's real per-op accounting, not
wall-clock-as-CPU).

## Metrics — Prometheus `/metrics`

Both `hubd` and `runnerd` serve `GET /metrics` (OTel meter → Prometheus
exporter). Unauthenticated like the dashboard root / `/healthz` — gate by
network posture (loopback + scrape network). Values are read live per scrape
via async observable callbacks:

- **hub** — `hub/internal/telemetry`, sourced from `store.PlatformStats`
  (platform-wide, un-tenant-scoped — the scrape has no auth context).
- **runner** — `runner/internal/telemetry`, sourced from `service.Status()`
  (in-memory governor + task totals + connector pool; no I/O).

**Per-request HTTP metrics (hub, issue #7):** synchronous instruments
`shift_hub_http_requests_total` + `shift_hub_http_request_duration_seconds`,
recorded by the API's `observe` middleware and labelled `method` / `route` /
`status`. The `route` is the **matched mux pattern** (`GET /api/v1/flows/{name}`),
read from `r.Pattern` after routing — bounded cardinality, never the raw path.
The middleware records via a func on `api.Options` (`RecordHTTP`) so the `api`
package stays free of the telemetry dependency.

## Operational logging (ADR-0046)

All three binaries emit **structured records on stdout**. The operator decides
whether that becomes a file, a pipe or a collector; a process that writes its
own log files has taken that decision away.

- `pkg/shiftlog.Setup` configures `hubd` and `runnerd` in one call: stdout,
  level from `SHIFT_LOG_LEVEL` (`SHIFT_HUB_LOG_LEVEL` / `SHIFT_RUNNER_LOG_LEVEL`
  still work), format from `SHIFT_LOG_FORMAT` — **JSON when stdout is not a
  terminal, text when it is**, so containers get machine-readable output and a
  local `make up` stays readable.
- **`gatewayd` does NOT import it.** Its `go.mod` has zero dependencies, an
  auditable property of the one component that may sit in a DMZ, so it keeps
  ~20 lines of its own setup. The contract is the output schema, enforced by
  `pkg/shiftlog`'s conformance test — which also fails if the gateway's
  `go.mod` ever grows a dependency, since the duplication would then have no
  justification.
- **The stdlib `log` package is bridged into slog**, so legacy `log.Print`
  call sites and any third-party library using the global logger cannot write
  prose into a JSON stream.

Every record carries `component` and `version`. Lifecycle records carry a
stable `event` name (`runner.registered`, `runner.cert.renewed`,
`task.completed`, `hub.started`, `gateway.config.applied`, …) — **alert on
`event`, never on `msg`**, which is prose for humans and free to change.
Context keys are spelled one way platform-wide: `runner`, `flow`, `task`,
`request`, `gateway`, `connector`, `account`, `duration_ms`, `error`.

Per-task records are DEBUG (`task.leased`) where a busy runner would otherwise
generate real volume; terminal ones are INFO/WARN.

Payload records, secret values, tokens and private keys never enter a log — a
credential may be *identified* (`cert_serial`) but never reproduced, and a test
greps a captured stream to keep that true.

### Correlation ids (hub, issue #7)

The `observe` middleware assigns each request a short correlation id, echoes it
as `X-Request-Id`, puts it on the request context (`api.RequestID(ctx)`), and
emits one access line (`event: hub.request`, plus `request`/`method`/`route`/
`status`/`duration_ms` — the platform-wide key names, so a request record
filters like any other). The gateway and
runner exchange a `request` id on the data plane. **Not yet done:** the runner
control API's mirror, and propagating the id into runner→hub reports for full
cross-plane correlation — a follow-up on #7.

### Naming convention (follow for all future metrics)

- `shift_<component>_<subject>[_<unit>]` — component is `hub` or `runner`.
  Counters end `_total`; byte gauges end `_bytes`; second gauges end
  `_seconds`.
- **Labels are low-cardinality only:** state, connector, action, route,
  realm. **Never** `task_id`, `trace_id`, flow-version, or any unbounded
  value as a metric label — those belong on spans (Stage 2), not metrics.
- Settle a metric's name before wide use; renames break dashboards.

### Current catalog

Hub (`shift_hub_*`): `tasks{state}`, `oldest_queued_seconds`,
`runners_active`, `runners_total`, `schedules`, `schedules_due`, `flows`.

Runner (`shift_runner_*`): `governor_budget_bytes`, `governor_used_bytes`,
`governor_peak_bytes`, `max_concurrent_by_mem`, `tasks_running`,
`tasks_waiting`, `tasks_submitted_total`, `tasks_completed_total`,
`tasks_failed_total`, `records_in_total`, `connector_in_use{connector}`,
`ratelimited_total{class}` (M6c).

Hub also exports `shift_hub_ratelimited_total{class}` (M6c, ADR-0021).

**Not a metric label:** per-account **usage** (M6d) is deliberately *not* on the
Prometheus surface — per-tenant labels blow cardinality. It lives in the
`usage_events` ledger, queried via `GET /api/v1/usage` (rollup) and pulled via
`GET /api/v1/usage/events` (cursor export for the external billing platform).
See [06-hub.md](06-hub.md) → Usage metering.

## Traces — OTLP (deferred)

Distributed tracing is designed (ADR-0020) but **deferred (2026-07-20)**, not
wired. Why: the per-task causal story is largely already available — the
`task_attempts` history (attempt trail, lease-expiry → re-dispatch) plus the
per-step `OpStats` captured on completed tasks — and metrics cover the
aggregate/alerting need. Tracing's unique marginal value is cross-service
**timing** not measured today (queue-sit, lease-wait) and standard trace
tooling; that doesn't yet justify instrumenting the request path + running a
collector + sampling.

Revisit when operators need per-task queue-sit/lease-wait tail latency, a
support flow wants to drill one execution in Jaeger/Tempo, or multi-hop flows
outgrow the attempts table. A lighter first step (just the missing timing
spans, OTLP off-by-default) is noted in ADR-0020.
