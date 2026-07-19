# SHIFT Rebuild Plan

> Supersedes `_archive/plan.md` (v0). Decisions backing this plan: `docs/adr/`. Review that triggered the rebuild: `docs/REVIEW-2026-07.md`.

## Topology (per ADR-0002)

- **Hub** — HA control plane (stateless services over HA Postgres). Owns: identity/tenancy, design studio, flow versions, connector registry (signed artifacts), the durable task queue, leases, execution records, telemetry, billing. Deployable cloud (multi-tenant SaaS) **or** local/on-prem — same images; runner config picks its hub.
- **Runner** — stateless, disposable execution container. Leases tasks from the hub, streams data through connector subprocesses, reports results/metrics. No durable local state; scratch-only spill.
- **Connectors** — signed standalone binaries, spawned by the runner, gRPC over UDS, streaming record batches (ADR-0001/0004).

## Milestones

### M0 — Scaffold
Monorepo, Go workspace, module boundaries, security/reliability gate (ADR-0006: govulncheck, gosec, staticcheck/errcheck via golangci-lint, gitleaks, `-race` tests, fmt — as `make check`, enforced by pre-push hook and CI), dev compose. Definition of done: `make check` green locally and in CI on an empty-but-real skeleton, pushed to github.com/aaron-au/shift.

```
shift/
  go.work
  engine/     # streaming core: records, batches, pipeline, spill, bench (no network deps)
  sdk/        # connector SDK: gRPC protocol, handshake, streaming interfaces, test kit
  runner/     # stateless worker: lease loop, connector lifecycle, engine host
  hub/        # control plane: API, OIDC, registry, queue/lease, scheduler, studio API
  pkg/        # shared primitives (config, logging, telemetry) — kept deliberately small
  proto/      # protobuf/gRPC definitions (connector protocol, hub control API)
  deploy/     # compose profiles: dev, local-hub "just runs" bundle
  docs/
```

### M1 — Streaming engine + proof (ADR-0003, ADR-0004) ✅ 2026-07-19
Record model, pull-based batch pipeline, pooled buffers, spill-over-watermark, JSON/NDJSON + CSV readers/writers, core transforms, `shift-bench` harness with buffered baseline.
**Exit met** (see `docs/bench-M1.md`): 1 GiB transformed at **24 MiB peak RSS** (bar was 100 MB), zero disk below watermark; spilling aggregate holds 1M groups at 164 MiB with a 337 MiB single-file spill; **80× less memory and 2× faster** than the buffered baseline.

### M1.5 — Format depth
XML streaming reader; EDI segment reader (X12/EDIFACT — deep domain, timeboxed spike first); DB cursor source with hub-persisted offsets.

### M2 — Connector runtime + SDK (ADR-0001, ADR-0007) ✅ 2026-07-19
gRPC connector protocol over UDS (opaque record-batch frames), handshake/versioning, spawn/health lifecycle, Go SDK + sdktest kit. Connectors: `gen` (bench/test) and `http` (streaming GET source, NDJSON POST sink, SSRF guard).
**Exit met** (see `docs/bench-M2.md`): engine pipelines run with subprocess source **and** sink at **1.32× the wall time of in-process** (~1.8M boundary-crossings/sec/core). Deferred deliberately: signature verification → M4 (registry owns signing); idle-reap/restart pooling → M3; SFTP connector → M3+.

### M3a — Runner, local intake (ADR-0008) ✅ 2026-07-19
Operational `runnerd`: flow documents (declarative JSON) compiled onto engine pipelines, connector subprocess pool (reuse/health/idle-reap), resource-governed admission (ADR-0005 — waiting is capacity-based, never a count cap), embedded dashboard + HTTP API, and the **capacity benchmark** as a runner feature (single + concurrent streams through the production path; reports throughput, scaling efficiency, and the memory-admission ceiling — the admin's add/subtract-compute signal).
**Exit met:** live smoke — flow with aggregate over 250k records executed via API with per-op stats; benchmark established 1.4M rec/s single / 4.0M rec/s across 12 streams on the dev box; admission serialization and concurrency proven under `-race`. Docs: `docs/dev/04-runner.md`.

### M3b — Runner, hub intake (with M4)
Lease-loop against the hub queue API as a second intake over the same task service, heartbeats, step idempotency keys, crash-recovery semantics (lease expiry → re-dispatch), durable execution records.
**Exit:** kill -9 a runner mid-flow; task completes on another runner; no duplicate side effects on idempotent steps.

### M4 — Hub control plane
Postgres schema (evolve `docs/reference/schema-v0.sql`; add audit + secrets/envelope-encryption tables), OIDC auth + tenancy enforcement, runner registration (single-use token → mTLS/signed-token channel), task queue + lease API (SKIP LOCKED), HA-safe scheduler (advisory locks), flow deploy/versioning/publish, connector registry with signing.
**Exit:** `docker compose up` the "just runs" bundle → login via OIDC → deploy a flow → schedule fires exactly once → runner executes → telemetry visible. All endpoints authenticated.

### M5 — Flow model & studio API
DAG flows (branch/merge, error handlers, parallel fan-out, sub-flows), mapping/transform authoring API (AI-friendly: flows and mappings are declarative JSON/YAML documents with a JSON-Schema), WASM (wazero) user transforms, webhook triggers with custom API endpoints on runners.

### M6 — Enterprise hardening
Observability (OpenTelemetry + Prometheus), audit log, billing aggregation from telemetry, rate limiting, connector marketplace plumbing, migration tooling (OpenAPI importer), benchmark-vs-incumbent collateral.

## Standing rules

- Every milestone lands with tests (`-race` mandatory) and updated docs/ADRs for decisions made in flight.
- **Internal dev docs (`docs/dev/`) are part of every milestone's definition of done** — the behind-the-scenes "how it all operates" documentation for new developers, kept in lockstep with the code (public/user docs are a separate, later concern).
- Any deviation from ADRs gets a superseding ADR, not a silent fork (v0's Kafka lesson).
- Benchmarks run in CI from M1 onward; a perf regression fails the build.
- Developer & AI friendliness is a feature: declarative flow documents, schema'd APIs, one-command dev environment, CLAUDE.md kept current.
