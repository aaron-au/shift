# SHIFT Internal Developer Docs

Behind-the-scenes documentation for people (and agents) working **on** the
platform — how the pieces operate and fit together. Public/user docs are a
separate, later concern.

**Standing rule (also in PLAN.md):** every milestone lands with its dev doc
updated. If the code and these docs disagree, that's a bug — fix whichever
is wrong in the same PR.

## Reading order for a new developer

1. [01-architecture.md](01-architecture.md) — the system in one page:
   components, dependency direction, what talks to what and how.
2. [02-engine.md](02-engine.md) — the streaming data plane: records,
   batches, arenas, pipelines, spill. Includes the contracts you must not
   break.
3. [03-connector-protocol.md](03-connector-protocol.md) — how connectors
   are spawned and spoken to; how to write and test a new connector.
4. [04-runner.md](04-runner.md) — the runner: flow documents, task
   lifecycle, resource-governed admission, connector pooling, the capacity
   benchmark, and the dashboard/API.
5. [05-conventions.md](05-conventions.md) — dev workflow: the gate, make
   targets, module layout rules, ADR process, how to add things.
6. [06-hub.md](06-hub.md) — the hub: Postgres schema, the queue/lease
   lifecycle, auth realms, the runner lease intake, and the kill -9
   crash-recovery guarantee.
7. [07-observability.md](07-observability.md) — telemetry (M6a): the
   OpenTelemetry/Prometheus `/metrics` endpoints, the metric catalog +
   naming convention, and the (pending) OTLP tracing.

## Where decisions live

- `docs/adr/` — every locked architectural decision, with context and
  consequences. Deviating requires a superseding ADR (never a silent fork).
- `PLAN.md` — milestone map with exit criteria and what's deliberately
  deferred.
- `docs/bench-M*.md` — measured proof for each milestone's performance
  claims, with reproduction commands.
- `docs/REVIEW-2026-07.md` + `_archive/` — the 2025 prototype, why it was
  restarted, and the lessons register. Read §3 before "improving" anything
  back toward v0 patterns.
