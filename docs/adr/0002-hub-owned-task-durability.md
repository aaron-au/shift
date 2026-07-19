# ADR-0002: Hub owns task durability; runners are stateless workers

**Status:** Accepted — 2026-07-19
**Decider:** Aaron

## Context
The v0 prototype gave each runner a private SQLite queue pretending to be distributed — scheduled-flow dedup was broken and a crashed runner lost its tasks. Aaron's requirement: *"If the runners themselves have the tasks and they crash/die/restart, the task is lost"* — resilience demands the hub hold the work. Hosting is container-first; runners should be disposable.

## Decision
**Topology (supersedes the v0 "offline-capable spoke" doctrine):**
- The **hub is HA** (stateless hub services over HA Postgres) and hosts the design studio. Durable state — flow definitions, task queue, execution records, leases — lives only in the hub's Postgres.
- **Hubs are deployable anywhere**: cloud (multi-tenant SaaS) or local/on-prem (customer-managed, same images). Which hub a runner talks to is pure runner configuration. "Offline capability" is achieved by running a local hub, not by making runners autonomous.
- **Runners are stateless.** No local queue database. In-memory execution with scratch-only spill space (ADR-0004). A runner container can be killed at any moment with zero durable loss.

**Task lifecycle (at-least-once):**
1. Trigger (schedule/webhook/API) enqueues a durable task row in hub Postgres.
2. Runners lease work: `SELECT … FOR UPDATE SKIP LOCKED`, lease with expiry, heartbeat renewal while executing.
3. Completion/failure recorded transactionally; lease released.
4. Runner crash ⇒ heartbeats stop ⇒ lease expires ⇒ task re-dispatched to another runner in the group.
5. Scheduled-flow dedup is trivial: the schedule fires in the hub exactly once per tick (advisory-locked scheduler across HA hub replicas).

**Semantics:** at-least-once delivery. Therefore the engine contract includes step idempotency keys from day one, and mid-flow checkpointing for long-running flows is a planned engine feature (M3+), so re-dispatch resumes rather than replays where connectors support it.

**Data path stays off the hub:** only control metadata and queue rows transit the hub. Payload streams move directly between connectors within the runner (and later runner-to-runner if a flow spans runners). The hub is on the *dispatch* path, never the *data* path — the performance thesis is unaffected.

**Queue abstraction:** the lease/queue API is a narrow interface backed by Postgres SKIP LOCKED initially. Postgres with batching sustains thousands of dispatches/sec per group, which is far beyond MVP needs; if a tenant ever outgrows it, the backend can be swapped (e.g. NATS JetStream) without touching engine or SDK contracts.

## Consequences
- Runner→hub connectivity is required to *receive new work* (in-flight work survives hub blips; lease renewal tolerates transient disconnects with grace windows). Local-hub deployment covers autonomy requirements.
- Hub Postgres sizing/HA is the platform's availability keystone — operational docs must treat it accordingly.
- "Just runs" single-box variant = bundled hub + runner + single-node Postgres (one compose/stack, one data volume, no file sprawl).
