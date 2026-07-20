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
`tasks_failed_total`, `records_in_total`, `connector_in_use{connector}`.

## Traces — OTLP (Stage 2, pending)

Distributed traces across the control plane (enqueue → lease → execute →
per-step → report), trace context propagated in lease/report metadata, span
attributes metadata-only and secret-redacted. **Off by default** — no OTLP
endpoint ⇒ a no-op tracer, zero overhead. Not yet wired; see ADR-0020.
