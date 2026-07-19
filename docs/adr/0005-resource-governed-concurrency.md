# ADR-0005: Resource-governed concurrency — no arbitrary limits

**Status:** Accepted — 2026-07-19
**Decider:** Aaron

## Context
JVM-based incumbents commonly serialize work behind global locks or fixed worker pools — an entire agent can stall because one task holds the runtime. Aaron's requirements: (1) no arbitrary limits on horizontal scaling — a customer running 10+ runners against a group must just work; (2) within a runner, a task must never wait on another task unless the machine is genuinely out of compute resources.

## Decision
**Vertical (within a runner):**
- Every task executes in its own goroutine (or goroutine set — one per pipeline stage where the DAG parallelizes). A single coordinator goroutine orchestrates lease/dispatch/reporting and never executes task work itself.
- **No fixed task-count caps.** Admission of new work is governed by real resource signals only: the engine memory watermark (the same accounting that drives spill), CPU saturation, and scratch-space headroom. When resources are available, work runs immediately; when they aren't, the runner stops leasing more (and sheds via lease-non-renewal if severely pressured) — it never queues work behind an artificial number.
- Per-stream buffer bounds inside the engine are **flow control** (backpressure between pipeline stages), not task blocking — they pace producers against consumers within one task, never gate one task on another.
- No global locks on the execution hot path. Shared runner state (connector registry, metrics) uses fine-grained synchronization or lock-free reads; nothing a task blocks on while another task holds it.

**Horizontal (across runners):**
- Group membership is dynamic and unbounded — runners join by registering and leasing; the hub's lease queue (ADR-0002) naturally load-balances because each runner leases only what its resource signals permit. Adding a runner adds capacity with no config ceiling, no rebalancing event, no coordination round.
- Platform tiers may meter capacity commercially (billing), but the architecture itself imposes no runner-count or task-count ceiling.

## Consequences
- Resource accounting must be trustworthy from M1 (honest allocation/RSS tracking is an engine deliverable, not an afterthought) — it is the admission controller.
- Lease batch sizing becomes adaptive (capacity-driven), which is more design work than a fixed prefetch count — accepted.
- Runaway flows are contained by per-task memory/scratch budgets (fair-share of the watermark), not by capping how many tasks run.
