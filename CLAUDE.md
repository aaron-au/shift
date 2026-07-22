# SHIFT — iPaaS platform (Go)

Hub-and-spoke Integration Platform as a Service. Goal: a provisionable, enterprise-grade iPaaS rivaling webMethods/Workato/Boomi, differentiated on **performance** (streaming, memory-efficient, disk-light transformations) and on being **developer- and AI-friendly**.

## Project status — read this first

**Clean rebuild in progress (decided 2026-07-19).** The 2025 prototype was reviewed (`docs/REVIEW-2026-07.md`), judged a successful learning exercise but not a foundation, and moved wholesale to `_archive/`. New code starts fresh in this repo following `PLAN.md` and the ADRs in `docs/adr/`. Nothing in `_archive/` should be extended or imported — it is reference-only.

**Locked decisions (see `docs/adr/` for full context):**
1. Connectors = gRPC subprocesses (go-plugin pattern), signed artifacts; WASM (wazero) for user transforms later. Never Go native `plugin`. (ADR-0001)
2. The HA hub owns task durability (Postgres queue + leases via SKIP LOCKED); runners are **stateless** disposable workers; hubs deploy cloud or local — "offline" means "local hub". At-least-once semantics ⇒ step idempotency keys in the engine contract. (ADR-0002)
3. Milestone 1 = streaming engine + `shift-bench` benchmark harness, before any distributed machinery. Exit: 1 GB stream at bounded ~100 MB RSS. (ADR-0003)
4. First-class workloads: JSON APIs, CSV/fixed-width, XML/EDI, DB sync/CDC ⇒ hierarchical typed record model, batch-based pull pipelines, streaming parsers, no `map[string]interface{}` on the hot path. (ADR-0004)
5. Hub control API = HTTP/JSON on stdlib mux, two auth realms (humans: OIDC — generic, any IdP — with a break-glass admin token; runners: single-use registration token → hashed bearer secret), long-poll lease claims with reap-at-claim, zombie-result rejection; runner lease intake is capacity-gated. Runner secrets stored as SHA-256 only. (ADR-0009)
6. Tenancy = `store.WithAccount(ctx)` set by every auth middleware; user secrets = envelope encryption (per-secret DEK, pluggable KEK) with **runner-pull** resolution of `{"$secret":"name"}` refs — plaintext never in the queue, task reads, or logs. (ADR-0010)
7. Connector registry: Ed25519 signatures over a canonical manifest (`pkg/consign`); publisher private keys never server-side; runners verify fail-closed (`connstore`, re-hash on every use); `SHIFT_REQUIRE_SIGNED=1` disables local-Dir trust. (ADR-0011)
8. Scheduler: DB-owned exactly-once — advisory lock + SKIP LOCKED + atomic tick advance + `sched:<id>:<tick>` idempotency keys (the `sched:` key prefix is reserved); Postgres `now()` is the only clock; UTC crons; only published versions fire. (ADR-0012)
9. Flow model v2: a **step graph** with typed outcome edges — `onSuccess`/`onComplete` happy path, `onFailure` error handler (dead-letter). Linear source/ops/sink kept as sugar; both lower to one validated `Plan`. Engine `OpError` tags failures by step id; per-step-id telemetry. (ADR-0013)
10. Connector capability policy: per-deployment allow/deny (cloud hubs hide dangerous connectors — rejected at deploy, invisible in list/resolve). Name-based, hub-wide. (ADR-0015)
11. Trigger & ingress: two planes — control (hub↔runner, metadata only: lease, config sync, execution reports) and data (ingress→runner, runner→source; **payload never touches the hub**). Webhooks are push triggers on a runner (`@webhook` source, async), reported to the hub as metadata; runner APIs are a public, authenticated surface (Basic first, pluggable). (ADR-0016)
12. Test-mode data capture: an engine `Sampler` hook takes a bounded, secret-redacted, runner-only, ephemeral sample of each stage's output; hub never sees payload. (ADR-0014)
13. Custom code (designed, build deferred): two tiers — `starlark` inline (fuel-metered, no I/O) + `python` out-of-process (connector subprocess, wheels-only, signed bundles). Step types `starlark`/`python`/`subflow` reserved. (ADR-0017)
14. Connector config-schema discovery: connectors declare a per-action JSON Schema (`sdk.Connector.Schemas`); it travels as a signed, opaque **descriptor** blob bound into `consign.Manifest` (v2 message; byte-identical v1 when absent). Connectors self-describe (`<binary> describe` + `Describe` RPC); the hub stores + serves it (`resolve`/`list`) so the studio builder renders config forms with **no runner online and no payload plane**. Hub never parses it; verify fail-closed. (ADR-0018)
15. Studio is a **canvas builder** in an "OS-lite" **windowed shell** (dock + draggable/resizable app windows; builder + tasks side by side), authored in **vanilla JS, no build step**. Node positions ride in an optional presentational `flowdoc.Document.Layout` (ignored by validation/`Plan`/engine). `pkg/flowdoc` validation stays authoritative — the builder surfaces 422s, never re-implements them. Visual polish is a deferred series. (ADR-0019)

## Doctrine (non-negotiable for new code)

- **Hub-and-spoke:** the HA Hub is the control plane (identity, design studio, durable task queue, registry) and never touches payload data; stateless Runners lease work, execute streams, and are disposable at any moment. The hub↔runner wire is **HTTP/JSON long-poll** (`POST /lease`, pull/lease model — ADR-0009), and the dashboard **polls**; there are **no WebSockets** in the control plane. This is deliberate (v0's WebSocket hub was the pain documented in "Lessons"); don't reintroduce WS without a superseding ADR. See `docs/reviews/2026-07-external-checklists.md`.
- **Streaming data plane:** inter-step data moves as `io.Reader`-based streams / typed record batches. No whole-payload `map[string]interface{}` buffering. Spill to disk only above an explicit memory watermark, into a single spill store — never thousands of small files.
- **Container-first:** everything ships as OCI images; low disk footprint by default. A self-contained "just runs" mode may persist artifacts, but efficiently (single embedded store, not file sprawl).
- **Connectors are out-of-process:** never compiled into the runner binary, never Go native `plugin` `.so` (the prototype proved that a dead end — toolchain lock-in, no unload, RCE-shaped distribution).
- **Encrypted by default:** TLS everywhere, OIDC on the hub, token-based runner registration, secrets encrypted at rest and never echoed into payloads, results, or logs. Auth exists from the first commit, not bolted on.
- **Honest metrics:** per-step CPU/allocation profiling from real sources; never wall-clock-as-CPU or global MemStats deltas.
- **Resource-governed concurrency, no arbitrary limits (ADR-0005):** every task gets its own goroutine(s); a coordinator goroutine orchestrates but never executes; admission is governed by real resource signals (memory watermark, CPU, scratch headroom), never fixed task-count caps. A task must never wait on another task unless the machine is genuinely out of resources. Per-stream buffer bounds are flow control within a task, not gates between tasks. Horizontal runner scale is unbounded by design. Backpressure and send-buffer overflow are handled explicitly (never silently dropped).
- **Security gates on every push (ADR-0006):** `make check` = govulncheck + gosec/staticcheck/errcheck (golangci-lint) + gitleaks + `actionlint` (GitHub Actions workflow lint, fail-closed — `make check` doesn't otherwise cover CI YAML) + `go test -race` (`test`) + fmt + the **per-package coverage gate** (`cover`, ADR-0022 — floors in `coverage.thresholds`, ratchet with `make cover-bump`; deterministic via `SHIFT_COVERAGE` skipping timing-flaky subprocess/e2e tests) — identical locally (pre-push hook), and in CI. Findings are fixed or suppressed inline with justification. **`depguard` enforces the module-boundary doctrine below as a build gate** (connectors → sdk → engine; hub/runner never import sdk or the engine payload plane; engine/pkg are leaves) — a forbidden import fails `make lint`. **Before pushing, run `golangci-lint cache clean && make check`** — a stale lint cache can hide findings the pre-push hook then rejects.
- **Tests from commit one.** The prototype's zero-test state is how simulation code and data races survived unnoticed. `-race` is always on in test runs.
- Go stdlib `net/http`; no heavy frameworks. Parameterized SQL only. No shared filesystems for runner clustering.

## Documentation map

| Doc | What it is |
|---|---|
| `docs/dev/` | **Internal developer docs** — how everything operates together (architecture, engine, connector protocol, runner, conventions). Read these first; keep them in lockstep with code (standing rule). |
| `PLAN.md` | The rebuild plan: topology, milestones M0–M6 with exit criteria, standing rules. |
| `docs/adr/` | Architecture Decision Records — locked decisions with context. Deviations require a superseding ADR. |
| `docs/REVIEW-2026-07.md` | Review of the prototype + viability study that triggered the restart. §5 lists what to carry forward and the decision sequence for the rebuild. |
| `docs/ARCHITECTURE.md` | As-implemented map of the **archived prototype** (now under `_archive/`). Reference for how things used to work. |
| `docs/reference/schema-v0.sql` | The prototype's 13-table Postgres schema — the best asset from v0; target data model for the hub (needs audit + secrets tables added). |
| `_archive/plan.md`, `_archive/agents.md`, `_archive/*_spec.md` | Original design docs. Directionally right, stale in specifics (RPC contract, library picks, Kafka never belonged). |

## Layout

```
engine/     Streaming data plane (M1, done — see docs/bench-M1.md for proven numbers):
  record/     hierarchical typed Values in arena-backed Batches; 0-alloc steady state; compiled Paths
  stream/     pull pipelines: Project/Filter/Coerce/Flatten + spillable Aggregate; per-op metrics
  format/     ndjson (hand-rolled tokenizer, differential-tested vs encoding/json), csvf
  spill/      single-file unlinked scratch store + compact binary Value codec
  mem/        watermark Governor (TryReserve fail == spill signal)
  cmd/shift-bench/  the proof harness; run with -max-rss to enforce exit criteria
sdk/        Connector SDK (M2, done — see docs/bench-M2.md: 1.32x subprocess overhead):
  sdk.go/server.go   author side: SourceAction/SinkAction + Serve (UDS, token auth, graceful stop);
                     M5.5: per-action config Schemas + Describe RPC + `describe` CLI mode → signed
                     descriptor (ADR-0018)
  host/              runner side: Launch/Attach, handshake-as-readiness, stream.Source/Sink adapters,
                     Describe/ExtractDescriptor (publisher-side schema extraction)
  sdktest/           in-process wire-protocol test harness for connector authors
  connectorpb/       generated from proto/connector/v1 (make proto to regenerate)
connectors/ Connector binaries: gen (bench/test), http (streaming GET source, NDJSON POST sink, SSRF guard),
            sftp (M6+ base-connector track: streaming get source + atomic put sink over SFTP, ndjson/csv
            via engine/format, mandatory host-key verification + network guard fail-closed)
proto/      gRPC contracts (ADR-0007: batches cross as opaque binary frames, never per-record proto)
runner/     runnerd (M3a+M3b+M4b, done — see docs/dev/04-runner.md): flow docs → engine pipelines,
  internal/{flow,connpool,task,service,api}   resource-governed admission (ADR-0005), connector pool,
                                              capacity benchmark (ADR-0008), embedded dashboard on :8340
  internal/{hubclient,leaseloop,connstore}    hub lease intake (M3b): capacity-gated claims, heartbeats;
                                              M4b: per-task secret resolution, signed-artifact fetch+verify
                                              (fail closed), persisted credentials (SHIFT_HUB_CRED_FILE)
hub/        hubd (M4a+M4b, done — see docs/dev/06-hub.md): Postgres store (schema v5, embedded
  internal/{store,api,pgtest}                 migrations), SKIP LOCKED queue + leases + attempt history,
  internal/{oidcauth,kek,secrets,scheduler}   runner registration, flow versions + publish workflow, OIDC
  cmd/{hubd,shift-bootstrap}                  realm + tenancy, envelope secrets, connector registry (signed),
                                              HA scheduler (exactly-once), embedded dashboard on :8400;
                                              M5.5: studio = canvas flow builder in an OS-lite windowed
                                              shell + connector config-schema discovery served for
                                              schema-driven config forms (ADR-0018/0019, vanilla no-build);
                                              e2e: crash recovery, exactly-once schedules, signed artifacts
                                              (v2 descriptor supply chain), secrets-never-at-rest (hub/e2e)
pkg/        flowdoc (flow document model + validation + {"$secret":...} refs — shared hub↔runner;
              M5a: step graph with outcome edges — onSuccess/onComplete happy path, onFailure
              error handler — linear source/ops/sink kept as sugar; both lower to one Plan; ADR-0013;
              M5.5: optional presentational Document.Layout for builder node positions, ADR-0019),
            consign (Ed25519 artifact signing + v2 descriptor digest — hub/runner/CLI; ADR-0018), buildinfo
deploy/     compose.dev.yml (dev Postgres), compose.yml + docker/ + dex/ (the M4b "just runs"
            bundle — `make up`; see deploy/README.md for the exit-criterion walkthrough)
_archive/   The complete 2025 prototype (hub, runner, scripts, compose, legacy docs). Read-only reference.
docs/       Review, prototype architecture map, reference schema, ADRs, bench results.
PLAN.md     Rebuild milestones.
```

**Engine contracts to preserve** (violating these reintroduces v0's failure mode):
- Batch lifetime: a batch from `Source.Next` is valid only until the next `Next`/`Close`; retaining data across batches requires `record.CopyValue` into your own batch.
- No `map[string]interface{}` on any hot path; build values via `record.Builder` into a batch.
- Operators mutate the flowing batch in place (they share its allocators); blocking operators (aggregate) account state via `mem.Governor` and spill to `spill.Store` when `TryReserve` fails.
- Paths (`record.ParsePath`) compile once at pipeline build, never per record.
- Connector actions mirror the same contracts (`sdk.SourceAction`/`SinkAction`); the spawn contract is two env vars (`SHIFT_CONNECTOR_SOCKET`, `SHIFT_CONNECTOR_TOKEN`) and every RPC carries the token. Dependency direction: connectors → sdk → engine; engine stays stdlib-only. The hub imports only `pkg/flowdoc` + `pkg/consign` (+ `engine/record` for path validation, `go-oidc` and `robfig/cron` as vetted control-plane deps) — it must never import stream/sdk or touch payload data.
- Hub tasks are at-least-once: any sink with side effects must honor the injected `idempotency_key` (stable across re-dispatched attempts). Results from a runner whose lease expired are rejected (409) — never "fix" that by loosening the `leased_by` check.

## Lessons already paid for (don't relearn)

- Go native `plugin` connectors: exact toolchain/dep/flag match required, Linux/macOS only, unloadable never — unusable for an ecosystem.
- A "distributed" queue backed by per-runner private SQLite coordinates nothing; scheduled-flow dedup must live in a genuinely shared medium.
- Kafka as a spoke dependency contradicts the lightweight self-hosted runner story.
- Buffer-in/buffer-out connector interfaces (`Execute(input []byte) ([]byte, error)`) force whole-payload buffering platform-wide — the streaming contract must be designed **before** the connector SDK.
- WebSocket hubs: never mutate client maps or close send channels under `RLock`; always implement reconnect-after-drop, not just connect-retry.
