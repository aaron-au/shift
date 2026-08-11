# SHIFT — Release Test Plan (living document)

The checklist that gates a release. It has **two levels**, and both must be
green (or a deviation explicitly signed off) before a version ships:

1. **Level 1 — Automated operational e2e.** A script/test that drives the real
   running system end-to-end and asserts. No human. Runs in CI and locally.
   Source of truth for "does it work". See [`scripts/e2e-operational.sh`](../scripts/e2e-operational.sh)
   and the Go e2e suite under `hub/e2e/`.
2. **Level 2 — Verification / evidence capture.** For each feature, the manual
   (or scripted) steps a releaser runs to *prove* it works, and the **evidence**
   they capture (API response, screenshot, log excerpt, metric). This is the
   audit trail a buyer/QA signs. Evidence lands under `evidence/<version>/`.

> **How to use.** Per release: (a) run `make check` (the gate) + `make cover`
> (coverage floors) + `scripts/e2e-operational.sh` (Level 1) — all must pass;
> (b) walk the Level-2 checklist, capturing evidence; (c) run the benchmark
> matrix and confirm the numbers hold; (d) record the result in the sign-off
> table at the bottom. Keep this file in lockstep with features — a new feature
> is not "done" until it has a Level-1 test and a Level-2 evidence step here.

Legend: **A** = has an automated (Level-1) test · **V** = has a verification
(Level-2) evidence step · status ✅ done / 🟡 partial / ⬜ not yet / ➖ n/a.

---

## 1. Engine (streaming data plane)

| Feature | A | V | Evidence | Status |
|---|---|---|---|---|
| 1 GB stream at bounded ~100 MB RSS (transform) | `make bench` / `shift-bench -scenario transform -max-rss` | RSS number in bench report | ✅ |
| Spilling aggregate above watermark | `shift-bench -scenario aggregate ... -max-rss` | peak RSS + spill bytes | ✅ |
| 0-alloc steady-state record build | engine `record` benchmarks | allocs/op in bench report | ✅ |
| ndjson reader (differential vs encoding/json) | `engine/format/ndjson` tests + fuzz | test pass | ✅ |
| **JSON array/object reader** (REST bodies) | `engine/format/ndjson.JSONReader` tests | test pass | ✅ |
| csv reader/writer | `engine/format/csvf` tests | test pass | ✅ |
| **XML streaming reader** (`xmlf`) | `engine/format/xmlf` tests (PR #23) | test pass | 🟡 (PR open) |
| Operators: project/filter/coerce/flatten/aggregate | `engine/stream` tests | test pass | ✅ |
| No map[string]interface{} on the hot path | lint (depguard) + review | gate green | ✅ |
| Batch-lifetime contract (valid only until the next `Next`) | `engine/batchtest` over `Batch.Poison`, wired as `lifetime_test.go` in `engine/stream` + all five format readers | gate green | ✅ TC-009 |
| No goroutine leaks (tasks, branches, pipes, pools) | `engine/leaktest` wired as `TestMain` in 10 packages | gate green | ✅ TC-001 |

## 2. Connectors (each verb exercised)

Per connector: build, `describe` emits a valid signed descriptor, each verb's
happy path + guard/error path unit-tested (in-process fake/server, no live
service), `$secret` fields tagged and never logged, network guard fail-closed.

| Connector | Verbs | A | V | Status |
|---|---|---|---|---|
| gen | gen/discard | pkg tests | bundle demo flow | ✅ |
| http | get (ndjson/json), post | pkg tests + SSRF | live GET jsonplaceholder → @response | ✅ |
| sftp | get/put/list/delete/mkdir/rmdir/rename | in-proc SSH server tests | evidence: round-trip on a real server | ✅ |
| **fs** | get/put/list/delete/mkdir/rmdir | tests (PR #17) | round-trip in a temp dir | 🟡 |
| **soap** | call | httptest tests (PR #18) | live SOAP endpoint call | 🟡 |
| **smtp** | send | in-proc go-smtp tests (PR #19) | delivered mail on a real relay | 🟡 |
| **db** (Postgres) | query/upsert/exec | sqlmock tests + live-PG integration (PR #20) | `SHIFT_TEST_PG_DSN` run + row proof | 🟡 |
| **s3** | get/put/list/delete | fake-S3 tests (PR #21) | MinIO/S3 round-trip | 🟡 |
| **azureblob** | get/put/list/delete | fake tests (PR #25) | Azurite/live round-trip | 🟡 |
| **amqp** | publish/consume | fake tests (PR #26) | live RabbitMQ round-trip | 🟡 |
| **ftp** | get/put/list/delete/mkdir/rmdir/rename | fake tests (PR #27) | FTPS server round-trip | 🟡 |
| **redis** | get/set/delete | fake tests (PR #28) | live Redis round-trip | 🟡 |

> Level-2 for connectors = a real end-point round-trip captured once per release
> (the unit tests deliberately avoid live services for determinism). Provision
> throwaway services (MinIO, Azurite, RabbitMQ, Postgres, an FTPS server) and
> capture the request/response + the resulting object/row/message.

## 3. Flow model & execution

| Feature | A | V | Status |
|---|---|---|---|
| Linear form (source/ops/sink) → Plan | `pkg/flowdoc` tests | ➖ | ✅ |
| Graph form + typed outcome edges (onSuccess/onComplete/onFailure) | `pkg/flowdoc` tests | ➖ | ✅ |
| Error routing to onFailure handler (dead-letter) | runner service tests | evidence: failing step → handler record | ✅ |
| Per-step-id telemetry (OpError tags) | runner tests | task Ops in dashboard | ✅ |
| `@discard` / `@response` built-in sinks | flowdoc + runner tests | sync run body | ✅ |
| **Branching (fan-out) / Merging (join)** | — | — | ⬜ **ADR-0029 designed; engine build pending** |

## 4. Runner (data plane)

| Feature | A | V | Status |
|---|---|---|---|
| Resource-governed admission (ADR-0005) | service tests under `-race` | ➖ | ✅ |
| Async execute (`POST /api/flows/execute` → 202 + poll) | api tests | task lifecycle | ✅ |
| **Sync request-reply (`POST /api/flows/run` + @response)** | api tests | live: pickup→transform→deliver body | ✅ |
| Webhook / direct execution (`POST /hooks/{name}`) | hub e2e webhook test | payload-never-at-hub asserted | ✅ |
| Capacity benchmark | service tests | dashboard numbers | ✅ |
| Test-mode data capture (redacted, ephemeral) | capture tests | dashboard overlay | ✅ |
| Control-surface auth (Basic + roles) | auth tests | 401/403 evidence | ✅ |

## 5. Hub (control plane)

| Feature | A | V | Status |
|---|---|---|---|
| Postgres SKIP LOCKED queue + leases + attempt history | store tests (real PG) | ➖ | ✅ |
| Crash recovery (kill -9, task re-dispatch) | `hub/e2e/crash_recovery_test.go` | e2e log | ✅ |
| HA scheduler exactly-once | `hub/e2e` schedule test | e2e log | ✅ |
| Runner registration (single-use token → hashed secret) | store/api tests | ➖ | ✅ |
| OIDC human realm + tenancy (`WithAccount`) | oidcauth + store tests | login → /me evidence | ✅ |
| Envelope secrets, runner-pull, never-at-rest | `hub/e2e` secrets test | secret nowhere but destination | ✅ |
| Connector registry: Ed25519 signed, verify fail-closed | consign + connstore tests + e2e | signed publish | ✅ |
| **KMS-KEK secret provider (ADR-0026)** | — | — | ⬜ **designed; build pending** |
| Capability policy (allow/deny, cloud hides dangerous) | connpolicy tests | deploy-time reject | ✅ |
| Publish/rollback workflow (drafts, published_version) | api/store tests | version history | ✅ |
| Audit log (account-scoped, CSV export) | store/api tests | audit window/CSV | ✅ |
| Usage metering ledger + export | store/api tests | usage window | ✅ |
| **Connector introspection / Discover (ADR-0025)** | — | — | ⬜ **designed; build pending** |

## 6. Studio (builder)

| Feature | A | V | Status |
|---|---|---|---|
| Windowed shell (dock, draggable app windows) | ➖ (no browser harness) | screenshot | ✅ |
| Canvas builder: nodes, edges, schema-driven config forms | ➖ | screenshot + deploy 201 | ✅ |
| Serialize → deploy → publish → execute round-trip | hub api/e2e (backend path) | deployed flow runs | ✅ |
| **Multi-window (one builder per flow)** | ➖ | screenshot | 🟡 (PR pending) |
| **Snap-to-grid drag** | ➖ | screenshot | 🟡 |
| **Left-input / right-output ports + arrows** | ➖ | screenshot | 🟡 |
| **Node cards (colourised headers + config summary)** | ➖ | screenshot | 🟡 |
| Marketplace (browse/version/yank) | api tests | marketplace window | ✅ |

> Studio has no browser-automation harness by doctrine (vanilla, no build). Its
> Level-1 coverage is the backend serialize→deploy→publish→execute path (hub
> e2e); its Level-2 is captured screenshots per window + a built-and-run flow.
> A future browser-driven visual e2e (issue #15) would upgrade these to **A**.

## 7. Security invariants (must never regress)

| Invariant | A | V | Status |
|---|---|---|---|
| Payload never touches the hub | `hub/e2e/webhook_test.go` (distinctive payload absent from hub) | grep hub tables/logs | ✅ |
| Secrets never in queue/results/logs | `hub/e2e` secrets test + redactor | grep evidence | ✅ |
| Connector artifacts signed; runner verifies fail-closed | consign/connstore tests + e2e | tampered artifact rejected | ✅ |
| Parameterized SQL only (db connector) | quoteIdent injection tests | ➖ | ✅ |
| SSRF/network guards fail-closed (http/soap/s3/…) | per-connector guard tests | ➖ | ✅ |
| No secret in committed source | `make check` `leaks` (gitleaks) | gate green | ✅ |
| Supply-chain / SAST / image scan | `.github/workflows/supply-chain.yml` | scan report | ✅ |

## 8. Observability

| Feature | A | V | Status |
|---|---|---|---|
| Prometheus `/metrics` (hub + runner) | telemetry wiring | scrape output | ✅ |
| Honest per-step CPU/alloc metrics | engine per-op accounting | ➖ | ✅ |
| OTLP tracing | — | — | ⬜ deferred (ADR-0020) |

## 9. Benchmarks (numbers must hold each release)

Run `make bench` (hard RSS gates) and `make bench-report` (the visible matrix
→ `docs/bench-M7/results.md`). Record deltas vs the previous release.

| Benchmark | Target | Source |
|---|---|---|
| Transform 1 GB stream | peak RSS ≤ 100 MB | `shift-bench -scenario transform -max-rss 100MiB` |
| Aggregate w/ spill (1M groups / 64 MiB watermark) | bounded RSS, single spill file | `shift-bench -scenario aggregate ...` |
| Buffered baseline vs streaming | streaming ≫ less memory, faster | bench-report table |
| Connector transport parity (ADR-0007) | subprocess overhead ≤ ~1.5× | `shift-bench-remote -max-ratio` |
| Runner capacity (single / N streams) | rec/s scaling efficiency | runner `/api/benchmark` |
| **Per-connector throughput** (rec/s, MB/s) | baseline captured per connector | ⬜ to add — extend shift-bench with connector sources |
| **End-to-end integration latency** (sync run) | p50/p99 for pickup→transform→deliver | ⬜ to add — extend e2e script timing |

---

## Release sign-off

| Version | Date | Gate (`make check`) | Coverage | e2e (Level 1) | Verification (Level 2) | Bench | Signed |
|---|---|---|---|---|---|---|---|
| _next_ | | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | |

## Backlog for this plan (keep growing it)

- Connector Level-2 harness: docker-compose of throwaway services (MinIO,
  Azurite, RabbitMQ, Postgres, an FTPS/SMTP server) + a script that runs each
  connector's real round-trip and captures evidence.
- Per-connector throughput benchmarks (extend `shift-bench` with connector
  source/sink adapters).
- End-to-end latency percentiles in the operational e2e.
- Browser-driven studio visual e2e (issue #15) to turn Studio rows into **A**.
- Level-1 automation for branch/merge once flow-model v3 (ADR-0029) lands.
- Work the open rows in `docs/assurance/test-conformance.md`. That register
  asks a different question from this plan — not "does the feature have a
  test" but "would a test *fail* if the invariant broke" — and a ✅ here can
  legitimately sit above a ⬜ there. Rows referencing `TC-nnn` are linked.
