# External review checklists — 2026-07-21

Two external best-practice checklists were run past SHIFT (a Go quality-gate
review "GPT Terra", and a Go architecture/delivery checklist "OpenAI"). This
note records what we already do, what is deliberately **not applicable** to our
architecture, and the concrete gaps we turned into issues. It exists so future
agents don't re-litigate the whole checklist or paste generic advice that
doesn't match how SHIFT is built.

## Architecture guardrail (read this before "adding realtime")

**SHIFT's control plane is HTTP/JSON long-poll, not WebSockets.** The hub owns a
durable Postgres task queue; runners are stateless and **pull** work by leasing
(`FOR UPDATE SKIP LOCKED` + heartbeat), via `POST /lease {wait_seconds}`
long-poll (ADR-0002, ADR-0009). The dashboard **polls**. This is deliberate:

- Control traffic is small, low-rate, **metadata-only** (payload never touches
  the hub — ADR-0016), so there is no stream to justify a persistent socket.
- Pull matches **resource-governed admission** (ADR-0005): a runner leases only
  when it has capacity; push would ignore headroom.
- Stateless HA hub: any replica answers any lease over plain HTTP — no sticky
  sockets, no cross-replica client map (the exact v0 WebSocket pain), LB/proxy
  friendly, curl-able.
- Disposable runners: crash → lease expires → re-dispatch. No socket lifecycle.

The v0 prototype used a WebSocket hub; it was fiddly (client-map mutation under
RLock, close-channel races, reconnect-after-drop). **Do not reintroduce
WebSockets for hub↔runner or the dashboard without a superseding ADR.** Most of
the "WebSocket protocol / backpressure / reconnect-resync" sections of external
checklists are therefore N/A here.

## Already adopted / enforced (do not re-propose)
- Durable DB as source of truth; at-least-once + step idempotency keys; crash
  recovery via lease/reap; zombie-result rejection (ADR-0002, ADR-0009).
- Boundaries/ownership, command-vs-event split, control/data plane separation
  (ADR-0016). The queue *is* the outbox — the "event" (task) lives in the same
  Postgres as the state, so the DB→event crash gap is sidestepped by design.
- Tenancy isolation, deny-by-default authz, resource-level scoping, two auth
  realms, envelope secrets, TLS, gitleaks, audit log (ADR-0009/0010/0011, M6b).
- Migrations as production code (embedded, advisory-locked), parameterized SQL,
  FK/NOT NULL/tenancy constraints, clean-DB migration tests.
- Local dev: compose bundle + `make up`, real-Postgres integration (pgtest).
- Failure-mode tests: kill-9 crash recovery, exactly-once schedules,
  secrets-never-at-rest, payload-never-to-hub, fuzzing, `-race`, and (2026-07-21)
  `-shuffle=on -count=1`.
- Gate tooling (2026-07-21): govulncheck, gosec/staticcheck/errcheck + curated
  linters, depguard module boundaries, gitleaks, per-package coverage gate,
  `tidy-check`. Release/scheduled supply-chain tier stubbed (ADR-0006 amendment).

## Gaps turned into issues
| # | Gap | Source |
|---|---|---|
| [#6](https://github.com/aaron-au/shift/issues/6) | Machine-readable API error schema + versioning/deprecation policy | OpenAI §2 |
| [#7](https://github.com/aaron-au/shift/issues/7) | Structured logging + correlation IDs + per-route HTTP metrics | OpenAI §7 |
| [#8](https://github.com/aaron-au/shift/issues/8) | Postgres statement/lock timeouts, serialization retry, restore test, expand/contract migrations | OpenAI §4 |
| [#9](https://github.com/aaron-au/shift/issues/9) | Digest-pinned base images, staging path, rollback, runbooks, SBOM/signing | OpenAI §11 |
| [#10](https://github.com/aaron-au/shift/issues/10) | Failure-mode tests: DB exhaustion/restart, deadlock retry, control-plane load | OpenAI §12 |
| [#5](https://github.com/aaron-au/shift/issues/5) | SSRF guard: block RFC1918/ULA/CGNAT by policy | whole-repo review |

## Not adopted (with reason)
- **WebSocket protocol / backpressure / reconnect-resync** — N/A (see guardrail).
- **`build` as a gate step** — `go vet` + `go test ./...` already compile every
  package; marginal value is only release ldflags/trimpath.
- **`go mod verify`** — meaningless on a fresh CI checkout; `tidy-check` covers
  the real module-hygiene gap.
- Anything already enforced by `.golangci.yml` (goimports, gosec) — no duplicate
  gates.

## Source docs
The two checklists were pasted in the 2026-07-21 working session; this note is
the durable digest. Anything that becomes a real decision gets its own ADR
rather than living as a checklist paste.
