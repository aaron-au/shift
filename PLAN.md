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

### M3b — Runner, hub intake (ADR-0009) ✅ 2026-07-19
Lease-loop against the hub queue API as a second intake over the same task service (`runner/internal/leaseloop`), capacity-gated claiming (ADR-0005), heartbeats at TTL/3, idempotency keys injected into sink config (http sink emits `Idempotency-Key`), crash-recovery semantics (lease expiry → re-dispatch), zombie-result rejection.
**Exit met:** `hub/e2e/crash_recovery_test.go` — kill -9 a runner mid-flow (real `runnerd` processes, real Postgres); task completes on the second runner (attempt trail: `lease-expired` → `completed`, full record count, idempotency key still maps to the one task).

### M4a — Hub queue core (ADR-0009) ✅ 2026-07-19
Postgres schema v1 (evolved from `docs/reference/schema-v0.sql`: accounts, runner identity, flow versions, task queue + leases + attempt history, audit log), migrations (advisory-locked, embedded), task queue + lease API (`FOR UPDATE SKIP LOCKED`), runner registration (single-use hashed tokens → hashed bearer secrets), flow deploy/versioning, idempotent enqueue, `hubd` (stateless over Postgres, HA-ready; TLS flags). Tests run against real Postgres everywhere: `SHIFT_TEST_PG` (CI service container / compose dev DB) or a throwaway `pg_ctl` cluster.

### M4b — Hub control plane, complete ✅ 2026-07-19
OIDC auth + tenancy enforcement (generic OIDC via go-oidc; Dex in the bundle; break-glass token retained — ADR-0010), envelope-encrypted secrets with runner-pull resolution (`{"$secret":...}` refs; plaintext never at rest — ADR-0010), connector registry with Ed25519 artifact signing + fail-closed runner verification (`pkg/consign`, `runner/internal/connstore` — ADR-0011), HA scheduler with layered exactly-once (advisory lock + SKIP LOCKED + atomic tick + `sched:` idempotency keys; periodic lease sweep — ADR-0012), flow publish workflow (drafts; version 0 = published), hub dashboard (embedded, OIDC login), "just runs" compose bundle (`make up`: postgres + dex + certgen + hubd TLS + bootstrap seeding + runner with `SHIFT_REQUIRE_SIGNED=1`).
**Exit met** (live walkthrough + e2e suite): compose up → Dex login → seeded demo flow → minutely schedule fired exactly once per tick → runner executed the registry-signed connector → telemetry on the dashboard. Proofs: `hub/e2e/{schedule,signed_artifact,secrets}_test.go` + two-account isolation and FireDue-contention store tests. Deferred deliberately: multi-tenant signup UX, connector version pinning in flow docs (M5), KMS KEK provider.

### M5 — Flow model & studio API ✅ 2026-07-20 (M5b build deferred, designed)
DAG flows (branch/merge, error handlers, parallel fan-out, sub-flows), mapping/transform authoring API (AI-friendly: flows and mappings are declarative JSON/YAML documents with a JSON-Schema), WASM (wazero) user transforms, webhook triggers with custom API endpoints on runners.

M5 is milestone-scale, split into sub-milestones (each lands green through `make check`). Build order (chosen 2026-07-20 — operational + visual first, custom code last since it is "another step, not needed right now"): **M5a → M5e → M5c → M5d → M5b**.
- **M5a — Flow model v2 (ADR-0013) ✅ 2026-07-20.** Step graph with typed outcome edges (`onSuccess`/`onComplete` happy path, `onFailure` error handler); linear form kept as sugar, both lower to one validated `Plan`; engine `OpError` for step-attributed error routing; per-step-id telemetry; runner routes a step failure to its dead-letter handler with a payload-free, secret-redacted error record. Deferred to later ADRs: parallel fan-out/merge/multi-sink, per-record routing, sub-flows.
- **M5b — Custom code (ADR-0017) — designed, build deferred.** Two tiers: `starlark` inline transform (deterministic, fuel-metered, no fs/network; Starlark-guest-in-WASM/wazero per ADR-0001, `go.starlark.net` fallback decided at build) + `python` out-of-process step (connector subprocess pattern; uv, wheels-only, hash-pinned lockfile, internal proxy index, signed bundles in the registry). Step types `starlark`/`python`/`subflow` are reserved in `pkg/flowdoc` (parsed, rejected until built). Lowest-priority M5 capability, "last option not the go-to" — fully specced so the build is a clean pick-up. Studio stays low/no-code first.
- **M5c — Test-mode per-step data capture (ADR-0014) ✅ 2026-07-20.** Live streaming was cut (product decision): test mode reads results from the runner on completion and overlays them (M5d canvas). Delivered the substrate — an engine `Sampler` hook capturing a bounded, secret-redacted sample of each stage's output; runner-side, ephemeral, opt-in (`?capture=1`); `GET /api/tasks/{id}/capture` + inline on the dashboard. Deferred (enterprise): durable **encrypted** payload store + retention TTL + right-to-erasure + Splunk/OTel push + prod-capture audit-gating.
- **M5d — Studio authoring API + webhook triggers + per-hub connector capability policy** (scope: API + read-only graph view, not a full drag-drop builder; order policy → webhooks → studio):
  - **M5d-1 — connector capability policy (ADR-0015) ✅ 2026-07-20.** Per-deployment allow/deny (`SHIFT_HUB_CONNECTOR_ALLOW`/`_DENY`); disallowed connectors rejected at deploy (422), hidden from list, 404 on resolve. Name-based (capability-metadata deferred); per-deployment (per-tenant deferred).
  - **M5d-2 — webhook triggers / direct execution (ADR-0016).** Two planes: control (hub↔runner, metadata — lease, config sync, execution reports) and data (ingress→runner, runner→source; payload never touches the hub). Webhook hits a runner API directly (k8s ingress), runs the flow with the request body as the `@webhook` source, async by default (`202 + task_id`, poll for status; per-execution sync toggle), and reports the direct task to the hub as metadata (load/history). Runner APIs become a public, authenticated surface — multi-user + per-API permissions, HTTP Basic first (pluggable). Config authored on the hub, synced to runners. Stages: (1) runner direct-exec + `@webhook` source ✅ 2026-07-20 (runner-local hook registry, per-hook token, async 202, body-as-source); (1b) report direct task to hub ✅ 2026-07-20 (runner posts direct-execution metadata to `POST /api/v1/executions` on completion; hub `direct_executions` table, metadata only — no payload; `GET /api/v1/executions`); (2) runner auth ✅ 2026-07-20 (opt-in HTTP Basic + roles admin/operator/viewer → read/execute/manage, one middleware, pluggable interface; open on loopback when unconfigured); (3) hub webhook-config store + runner sync ✅ 2026-07-20 (hub `webhooks` table + admin CRUD `PUT/GET/DELETE /api/v1/webhooks[/{name}]`, token stored as SHA-256 hash; runner-realm `GET /api/v1/webhooks/sync` returns enabled hooks on published flows with their document; runner sync loop replaces its registry — hub authoritative for attached runners, local PUT for standalone; unified token-hash verification). **M5d-2 complete; M5d complete.**
  - **M5d-3 — studio: read-only flow graph view ✅ 2026-07-20.** `flowdoc.GraphView()` (nodes + typed edges, data-free) → hub `GET /api/v1/flows/{name}/graph`; hub dashboard renders the step graph as SVG (green success/complete, red failure → dead-letter) in a modal, plus a direct-executions panel. `make up` demo is now a v2 graph so the view has content out of the box. Test-run overlay (per-step capture pulled from the runner onto the canvas) deferred — needs the studio→runner cross-service read.
- **M5e — Tiered benchmark suite ✅ 2026-07-20.** Graded process shapes — simple (passthrough), standard (filter+coerce+project), complex (flatten+aggregate/spill-capable), extreme (multi-stage, very high cardinality) — each measured single + concurrent through the production path (gen source → discard sink, lowered via the v2 Plan), reported per tier on the runner dashboard. `POST/GET /api/benchmark/tiers`. Http-sink "extreme" profile deferred (needs a live target; would couple the numbers to an unrelated endpoint).

**M5 exit met (ADRs 0013–0017).** Flow model v2 (step graph + outcome edges),
tiered benchmark, test-mode data capture, connector capability policy,
webhook/direct execution end-to-end (proven live on the bundle), hub-authored
webhook config synced to runners, and the read-only studio graph view.
Custom code (M5b) is designed (ADR-0017) with the build deferred as the
lowest-priority, "last option" capability.

**Deferred out of M5 (pick up as their own tasks, ADR-backed where noted):**
- Custom-code **build** — `starlark` + `python` tiers (ADR-0017).
- **Sub-flows** and a true **parallel-fan-out / merge / multi-sink DAG**
  (ADR-0013 deferred these; the current engine is single-path pull); also
  per-record content routing.
- **Studio**: full drag-drop authoring (M5d shipped read-only) + the
  **test-run overlay** (per-step capture pulled from the runner onto the
  canvas — needs a studio→runner cross-service read).
- **Durable payload storage** (enterprise): runner-local **encrypted** store
  + retention TTL + right-to-erasure + Splunk/OTel push + prod-capture
  audit-gating (ADR-0014 consequences).
- **Synchronous webhooks** + HMAC verification; connector **capability
  metadata** (vs name-based policy) + **per-tenant** capability scope
  (ADR-0015/0016); http-sink "extreme" benchmark profile.

### M5.5 — Studio Builder (ADR-0018, ADR-0019) — before M6
Promote the read-only studio (M5d-3) into a visual **builder**: canvas
drag-drop flow authoring inside an "operating-system-lite" windowed UI shell
(left dock of tools; each opens its own draggable app window, so the builder
canvas and the task-runner list sit side by side). Decided 2026-07-20: canvas
editing (not form-list), **schema-driven** config forms via connector
config-schema discovery baked into the signed manifest (ADR-0018, path A — hub
serves schema, builder needs no runner online), **stay vanilla / no build
step** (dependency-free `go:embed`'d JS/CSS — ADR-0019). Backend write path
already exists (`deployFlow`/`publish`/`execute` + JSONB draft→published), so
most work is UI + the schema chain.

Build order (each lands green through `make check`; A is the long pole and
unblocks D; B+C can run in parallel with A since they need no schema):
- **Phase 0 — ADRs + plan ✅ 2026-07-20.** ADR-0018 (config-schema discovery,
  signed manifest v2), ADR-0019 (canvas builder + windowed shell), this entry.
- **Phase A — schema in the manifest chain ✅ 2026-07-20.** `sdk.Connector.Schemas`
  per-action JSON Schema; proto `Describe` RPC (+ regen `connectorpb`);
  `consign` v2 signed message binding a descriptor digest (byte-identical v1
  when absent → old signatures valid); connectors self-describe (`<bin>
  describe` CLI + `host.ExtractDescriptor`); publisher (shift-bootstrap)
  extracts + signs + uploads the descriptor; hub registry stores `descriptor`
  (migration 0008) + serves it on `list`/`resolve`, verify fail-closed;
  `http`/`gen` reference schemas. e2e runs the full v2 supply chain.
- **Phase B — canvas foundation ✅ 2026-07-20.** Interactive canvas (drag-move);
  node positions persisted via optional presentational `flowdoc.Document.Layout`
  (ignored by validation/`Plan`/engine).
- **Phase C — graph editing ✅ 2026-07-20.** Add/delete nodes, drag-to-connect
  outcome edges (onSuccess/onComplete/onFailure), delete edges; model-based
  editor (linear docs lowered client-side); serialize → `deployFlow`; 422
  surfaced inline; validation stays server-authoritative.
- **Phase D — schema-driven config panel ✅ 2026-07-20.** Connector/action
  pickers + typed config forms from the served descriptor; `x-shift-secret`
  fields → secret picker inserting `{"$secret":...}`; raw-JSON fallback.
- **Phase E — windowed shell + publish/rollback ✅ 2026-07-20.** OS-lite dock +
  draggable/resizable singleton windows (Overview/Flows/Builder/Tasks/
  Executions/Runners/Connectors/Secrets; layout browser-only in localStorage);
  builder is a window (canvas + tasks side by side); Flows version picker for
  publish/rollback + run-now. Test-run overlay (ADR-0014 `Sampler`) deferred.
- **Phase F — docs ✅ 2026-07-20.** `docs/dev/03-connector-protocol.md` (Describe
  + descriptor + signing v2) and `docs/dev/06-hub.md` (builder + shell) updated
  in lockstep; ADRs 0018/0019; this plan. Browser-automation e2e of the JS
  builder is out of scope (no browser/build harness by doctrine); the
  serialize→deploy→publish→execute path is covered by the existing hub e2e +
  manual curl smoke of builder-shaped documents.

**M5.5 complete 2026-07-20.** Studio is a functional canvas builder in an
OS-lite windowed shell, on schema-driven config from signed connector
descriptors — vanilla, no build step.

**Deferred — Studio visual polish (its own series, later; not a blocker for
M6).** Functional-first was deliberate. Backlog: window snapping (left/right
+ middle drag bar) & maximise; merge close/minimise into one control; labels
+ helper text; per-window help button; styled scrollbars; a visible
resize-corner indicator; a proper **light + dark colour palette** (currently
dark-only). UI-only, still vanilla unless a bundler ADR supersedes.

**Related / future (not in M5.5):** persistent encrypted runner-local
credential store synced from the hub, with the hub sourcing KEK/secret material
from an external key vault (provider TBD) — an evolution of ADR-0010's
pluggable KEK, tracked separately (noted in ADR-0018).

### M6 — Enterprise hardening
Observability (OpenTelemetry + Prometheus), audit log, billing aggregation from telemetry, rate limiting, connector marketplace plumbing, migration tooling (OpenAPI importer), benchmark-vs-incumbent collateral.

Milestone-scale; split into sub-milestones (each lands green through `make check`).
Build order chosen 2026-07-20 — observability first (the substrate billing +
rate limiting lean on):
- **M6a — Observability (ADR-0020).** OpenTelemetry (Go SDK) as the single
  telemetry stack; OTel lives only in `hub/`+`runner/` (engine + `pkg/` stay
  telemetry-free); metadata only, secret-redacted (two-plane split, ADR-0016).
  - **Metrics ✅ 2026-07-20.** Prometheus `/metrics` on hubd + runnerd
    (`shift_hub_*` from `store.PlatformStats`; `shift_runner_*` from
    `service.Status()`). **Honest metrics** from the engine's real per-op
    accounting. Bounded cardinality (task/trace id never a metric label).
    Naming convention + catalog in `docs/dev/07-observability.md`.
  - **Tracing (OTLP) — deferred 2026-07-20.** The per-task causal story is
    largely already in the hub (`task_attempts` history + per-step `OpStats`);
    metrics cover the aggregate need. Tracing's unique value (cross-service
    queue-sit/lease-wait timing + standard trace tooling) doesn't yet justify
    the request-path instrumentation + collector + sampling versus other M6
    work. Design retained in ADR-0020; revisit triggers documented there.
- **M6b — Audit log completion ✅ 2026-07-21.** Every mutating admin action
  audits (`api/*.go` call sites: flow deploy/publish, task enqueue, runner
  token/register, secret put/delete + KEK rotate + per-secret access, schedule
  put/delete, webhook upsert/delete, connector publish, publisher-key add/revoke);
  runner hot-path endpoints (lease/heartbeat/complete/fail/report) deliberately
  excluded. `audit_log` tenant-scoped (migration 0009 `account_id`); read API
  `GET /api/v1/audit` (keyset pagination, action-family filter, `?format=csv`
  export with formula-injection guard); studio Audit window (`ui.html`).
- **M6c — Rate limiting ✅ 2026-07-20 (ADR-0021).** Token-bucket
  (`golang.org/x/time/rate`), per-process/per-key. Hub control API: keyed by
  identity (admin/runner class) in the auth wrappers + by client IP (public
  class) on unauthenticated routes. Runner ingress: `POST /hooks/{name}` keyed
  by `{hook, source IP}`. Over limit → 429 + Retry-After; `/healthz`/`/readyz`/
  `/metrics` exempt; idle buckets swept. Per-replica by design (overload/abuse
  protection, not global quota). Off by default (rps<=0). Rejections exported
  as `shift_{hub,runner}_ratelimited_total{class}`.
- **M6d — Usage metering substrate ✅ 2026-07-21** (telemetry substrate, M6a).
  **Boundary (2026-07-21, Aaron):** the hub is **task control**, NOT the central
  account-management / billing platform — that is a separate global system that
  does not exist yet. So M6d on the hub is *metering + an export point*, not a
  billing system of record: `account_id` here is a tenant key, and the future
  billing platform *pulls* usage from the hub. Scope: per-account **metering +
  visibility** — the runner already reports records-in/out + per-op seconds
  (today dumped opaquely into `tasks.result` JSONB) and `direct_executions`
  carries typed records for the push path; promote to an append-only
  `usage_events` ledger + aggregation API (studio Usage window) + a cursor-based
  raw-events export endpoint (the external billing platform's ingest). Deferred:
  **quota/plan enforcement** (belongs to the external account platform, not the
  hub) and **engine bytes-processed** (needs byte accounting added to
  `stream.OpStats` and threaded runner→hub — a hot-path change; strong billing
  signal, its own task).
- **M6e — Connector marketplace plumbing ✅ 2026-07-21.** On top of the signed,
  content-addressed, versioned registry (ADR-0011/0018): signed **discovery
  metadata** (`sdk.ConnectorMeta` — description/category/icon/tags — bound into
  the descriptor so it stays tamper-evident; hub still never parses it, studio
  decodes client-side; nil Meta keeps the descriptor byte-identical); a
  **version-list** API (`GET …/connectors/{name}/versions`, all os/arch incl.
  yanked) + **yank/restore** (`POST …/versions/{version}/yank`, fail-closed on
  resolve/download, audited); an **end-to-end publish tool** (`shift-consign
  publish` — v2 descriptor via `-describe`/`-descriptor` + upload, replacing the
  e2e-only path); and a searchable **Marketplace** studio window (cards with
  icon/description/category/tags + per-connector version history + yank/restore).
  Not built: a literal "install" step — connectors auto-resolve to runners on
  demand, so browse/discovery + use-in-builder is the whole surface.
- **M6f — Migration tooling** (OpenAPI importer) + **benchmark-vs-incumbent**
  collateral. **Deferred 2026-07-21** — migration/GTM-focused, least tied to the
  near-term usable-state UI; revisit as its own milestone after M6 closes on b/d/e.

**M6 substantially complete 2026-07-21.** Observability metrics (M6a), audit
(M6b), rate limiting (M6c), usage metering (M6d), and marketplace plumbing (M6e)
all shipped; each surfaces in the studio (Overview/Audit/Usage/Marketplace
windows) — the "initial usable state" the UI push was aiming at. Deliberately
deferred, each its own future task: OTLP tracing (M6a), OpenAPI importer +
incumbent-bench (M6f), plus the M6d follow-ups (quota enforcement on the external
account platform; engine bytes-processed metering).

**Validated live 2026-07-21** via a full `make up` bundle dry-run (signed
connectors, TLS, OIDC): published a connector with the new `shift-consign
publish -describe`, browsed the Marketplace + version history + yank/restore,
watched Usage metering + Audit — all confirmed on the real stack (+ studio
screenshots). One bug found+fixed (descriptor base64 UTF-8 mojibake in the
studio, `b64utf8`).

## Future milestones (post-M6, not yet scheduled)

- **Hub RBAC (issue #16) — milestone-sized, BLOCKED on the central identity/
  account platform.** The hub is **open access** today: a minimal `users.role
  ∈ {admin,viewer}` model exists but JIT-provisions every OIDC user as `admin`
  (migration 0002 `DEFAULT 'admin'`) and has only two coarse tiers. Real RBAC
  (what roles exist, who holds them, custom vs standard) is the central
  platform's responsibility — building it in the hub re-creates the
  account-management surface the hub explicitly is NOT ([[shift-hub-is-task-control]]).
  Likely shape: fixed standard tiers on the hub assigned from OIDC claims,
  least-privilege default; custom roles live in the central platform. Decision
  deferred (Aaron, 2026-07-21) — needs its own milestone + that platform.
- **Base connector library** — turn the platform into real integration
  workloads. Started 2026-07-21 (Aaron's call, after RBAC deferred). Shipped:
  - **sftp ✅** — streaming `get` source (remote file → ndjson/csv → batches)
    + atomic `put` sink (temp-then-`PosixRename`); mandatory SSH host-key
    verification (pinned `host_key`, or unverified only under `allow_local`);
    network guard fail-closed mirroring http's SSRF guard; creds are
    `{"$secret":…}`-resolved before spawn. Real in-process SSH+SFTP round-trip
    tests. Deps `pkg/sftp` + `x/crypto/ssh` (connectors module only).
  - Next candidates: filesystem, DB source/CDC + upsert sink, S3, SMTP,
    message queues. XML/EDI is adjacent engine-format work (M1.5).

### M7 — Testing & benchmark hardening (ADR-0022) ✅ 2026-07-20

Pivot (Aaron's call) from remaining M6 feature work to provable reliability —
the artifacts a buyer trusts. Delivered:

- **Hard per-package coverage gate.** `make cover` (`scripts/coverage.sh`):
  race + `-coverpkg` per module, correct count-merge, per-package aggregation,
  floors in `coverage.thresholds` (ratchet via `make cover-bump`, floors only
  rise). `check` runs both `test` (full `-race`, incl. subprocess integration
  for behavior) and `cover` (deterministic gate — SHIFT_COVERAGE skips the
  timing-flaky connector-subprocess + e2e tests so coverage never flakes).
  Total **68.5%** deterministic (was unmeasured); weak packages lifted:
  scheduler, hub/api, oidcauth, ratelimit (both), webhook, sdk/host,
  leaseloop, hubclient. Latent bug fixed: `sdk/host` `Process.Close` was not
  idempotent (second call blocked forever).
- **Visible results.** CI uploads coverage HTML/JSON + posts the per-package
  table to the job summary; README coverage badge (`badges/coverage.json`);
  engine benchstat base-vs-PR on PRs.
- **Configurable benchmark suite.** `shift-bench` gained `-json`/`-runs`/
  `-warmup`; `make bench-report` renders `docs/bench-M7/results.md`. RSS
  ceilings stay hard gates.
- **Full-stack e2e.** `hub/e2e` gained webhook ingress → runner exec →
  metadata-only hub report, asserting the payload never reaches the hub
  (ADR-0016), race-clean and repeatable.

## Standing rules

- Every milestone lands with tests (`-race` mandatory) and updated docs/ADRs for decisions made in flight.
- **Internal dev docs (`docs/dev/`) are part of every milestone's definition of done** — the behind-the-scenes "how it all operates" documentation for new developers, kept in lockstep with the code (public/user docs are a separate, later concern).
- Any deviation from ADRs gets a superseding ADR, not a silent fork (v0's Kafka lesson).
- Benchmarks run in CI from M1 onward; RSS ceilings are hard gates, benchstat
  regressions are informational (ADR-0022).
- **Coverage is gated per package (ADR-0022):** new code carries tests or it
  drags a package under its floor and fails `make check`. After adding tests,
  `make cover-bump` and commit the raised floors.
- Developer & AI friendliness is a feature: declarative flow documents, schema'd APIs, one-command dev environment, CLAUDE.md kept current.
