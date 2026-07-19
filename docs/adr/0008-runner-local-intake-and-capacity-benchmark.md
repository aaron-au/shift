# ADR-0008: Runner ships local-intake-first; benchmarking is a runner feature

**Status:** Accepted — 2026-07-19
**Decider:** Aaron (visible results + benchmark-as-feature requirements); Claude (intake layering)

## Context
The hub (durable queue, lease API) is M4, but the runner must become
operational and *visible* now — results on a dashboard, not CLI logs.
Building a temporary local queue would repeat v0's fatal pattern
(throwaway state machinery that gets load-bearing). Separately, Aaron
requires benchmarking to be a first-class **runner feature**: each runner
establishes its own execution capacity so admins can size compute
(add/subtract runners) from data, not guesswork.

## Decisions

**1. Task intake is a thin layer over one task service.** The runner core
is a task service: admission (ADR-0005 governor) → connector pool → engine
pipeline → recorded result. Intakes feed it:
- *M3a (now):* HTTP submission (`POST /api/flows/execute`) — synchronous
  origin, in-memory result ring, dashboard visibility. Explicitly **not
  durable**; the ring is a convenience view.
- *M3b/M4:* the hub lease loop becomes a second intake feeding the same
  service; the ring keeps serving the dashboard while durable truth moves
  to the hub. No execution machinery changes.

**2. The capacity benchmark is built into the runner.** `POST
/api/benchmark` runs calibration flows through the *production* path
(real connector subprocesses, real pipelines, recorded as visible tasks):
single-stream throughput, then concurrent streams at hardware width. The
resulting capacity report — records/sec (single and aggregate), scaling
efficiency, and memory-derived max concurrent tasks (budget ÷ per-task
cost) — is stored and exposed via API/dashboard. This is the admin's
add/subtract-compute signal today, and the intended input for hub-side
placement and lease sizing later (feeds ADR-0005 admission signals).

**3. Per-task memory model:** admission reserves `task watermark + fixed
overhead` against the runner-wide governor; each task's stateful operators
get their own engine governor with the task watermark as budget (spill
beyond it). Waiting for capacity is the only queueing — resource-based,
never a count cap (ADR-0005).

## Consequences
- The runner dashboard (embedded, dependency-free) is the first visible
  surface of the platform; it reads only runner-local state.
- HTTP-submitted tasks are lost on runner restart *by design* — durability
  arrives with the hub (ADR-0002); the dashboard says so.
- Benchmark runs consume real capacity; they are visible tasks and respect
  admission like any other work.
