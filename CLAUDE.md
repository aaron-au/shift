# SHIFT — iPaaS platform (Go)

Hub-and-spoke Integration Platform as a Service. Goal: a provisionable, enterprise-grade iPaaS rivaling webMethods/Workato/Boomi, differentiated on **performance** (streaming, memory-efficient, disk-light transformations) and on being **developer- and AI-friendly**.

## Project status — read this first

**Clean rebuild in progress (decided 2026-07-19).** The 2025 prototype was reviewed (`docs/REVIEW-2026-07.md`), judged a successful learning exercise but not a foundation, and moved wholesale to `_archive/`. New code starts fresh in this repo following `PLAN.md` and the ADRs in `docs/adr/`. Nothing in `_archive/` should be extended or imported — it is reference-only.

**Locked decisions (see `docs/adr/` for full context):**
1. Connectors = gRPC subprocesses (go-plugin pattern), signed artifacts; WASM (wazero) for user transforms later. Never Go native `plugin`. (ADR-0001)
2. The HA hub owns task durability (Postgres queue + leases via SKIP LOCKED); runners are **stateless** disposable workers; hubs deploy cloud or local — "offline" means "local hub". At-least-once semantics ⇒ step idempotency keys in the engine contract. (ADR-0002)
3. Milestone 1 = streaming engine + `shift-bench` benchmark harness, before any distributed machinery. Exit: 1 GB stream at bounded ~100 MB RSS. (ADR-0003)
4. First-class workloads: JSON APIs, CSV/fixed-width, XML/EDI, DB sync/CDC ⇒ hierarchical typed record model, batch-based pull pipelines, streaming parsers, no `map[string]interface{}` on the hot path. (ADR-0004)

## Doctrine (non-negotiable for new code)

- **Hub-and-spoke:** the HA Hub is the control plane (identity, design studio, durable task queue, registry) and never touches payload data; stateless Runners lease work, execute streams, and are disposable at any moment.
- **Streaming data plane:** inter-step data moves as `io.Reader`-based streams / typed record batches. No whole-payload `map[string]interface{}` buffering. Spill to disk only above an explicit memory watermark, into a single spill store — never thousands of small files.
- **Container-first:** everything ships as OCI images; low disk footprint by default. A self-contained "just runs" mode may persist artifacts, but efficiently (single embedded store, not file sprawl).
- **Connectors are out-of-process:** never compiled into the runner binary, never Go native `plugin` `.so` (the prototype proved that a dead end — toolchain lock-in, no unload, RCE-shaped distribution).
- **Encrypted by default:** TLS everywhere, OIDC on the hub, token-based runner registration, secrets encrypted at rest and never echoed into payloads, results, or logs. Auth exists from the first commit, not bolted on.
- **Honest metrics:** per-step CPU/allocation profiling from real sources; never wall-clock-as-CPU or global MemStats deltas.
- **Resource-governed concurrency, no arbitrary limits (ADR-0005):** every task gets its own goroutine(s); a coordinator goroutine orchestrates but never executes; admission is governed by real resource signals (memory watermark, CPU, scratch headroom), never fixed task-count caps. A task must never wait on another task unless the machine is genuinely out of resources. Per-stream buffer bounds are flow control within a task, not gates between tasks. Horizontal runner scale is unbounded by design. Backpressure and send-buffer overflow are handled explicitly (never silently dropped).
- **Security gates on every push (ADR-0006):** `make check` = govulncheck + gosec/staticcheck/errcheck (golangci-lint) + gitleaks + `go test -race` + fmt — identical locally (pre-push hook), and in CI. Findings are fixed or suppressed inline with justification.
- **Tests from commit one.** The prototype's zero-test state is how simulation code and data races survived unnoticed. `-race` is always on in test runs.
- Go stdlib `net/http`; no heavy frameworks. Parameterized SQL only. No shared filesystems for runner clustering.

## Documentation map

| Doc | What it is |
|---|---|
| `PLAN.md` | The rebuild plan: topology, milestones M0–M6 with exit criteria, standing rules. |
| `docs/adr/` | Architecture Decision Records — locked decisions with context. Deviations require a superseding ADR. |
| `docs/REVIEW-2026-07.md` | Review of the prototype + viability study that triggered the restart. §5 lists what to carry forward and the decision sequence for the rebuild. |
| `docs/ARCHITECTURE.md` | As-implemented map of the **archived prototype** (now under `_archive/`). Reference for how things used to work. |
| `docs/reference/schema-v0.sql` | The prototype's 13-table Postgres schema — the best asset from v0; target data model for the hub (needs audit + secrets tables added). |
| `_archive/plan.md`, `_archive/agents.md`, `_archive/*_spec.md` | Original design docs. Directionally right, stale in specifics (RPC contract, library picks, Kafka never belonged). |

## Layout

```
_archive/   The complete 2025 prototype (hub, runner, scripts, compose, legacy docs). Read-only reference.
docs/       Review, prototype architecture map, reference schema, ADRs.
PLAN.md     Rebuild milestones. Target layout (from M0): go.work + engine/ sdk/ runner/ hub/ pkg/ proto/ deploy/
```

## Lessons already paid for (don't relearn)

- Go native `plugin` connectors: exact toolchain/dep/flag match required, Linux/macOS only, unloadable never — unusable for an ecosystem.
- A "distributed" queue backed by per-runner private SQLite coordinates nothing; scheduled-flow dedup must live in a genuinely shared medium.
- Kafka as a spoke dependency contradicts the lightweight self-hosted runner story.
- Buffer-in/buffer-out connector interfaces (`Execute(input []byte) ([]byte, error)`) force whole-payload buffering platform-wide — the streaming contract must be designed **before** the connector SDK.
- WebSocket hubs: never mutate client maps or close send channels under `RLock`; always implement reconnect-after-drop, not just connect-retry.
