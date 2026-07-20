# 06 — Hub: the durable control plane

> Code: `hub/`. Decisions: ADR-0002 (topology/durability), ADR-0009
> (control API), ADR-0010 (OIDC/tenancy/secrets), ADR-0011 (registry),
> ADR-0012 (scheduler). Status: M4a + M4b complete.

## What the hub is (and is not)

The hub owns **all durable state**: runner identity, users, flow versions
(+ publish state), the task queue, attempt history, schedules, secrets
(encrypted), the connector registry, audit. It never touches payload
data — flow documents and result summaries are control-plane metadata;
records stream only through runners and their connector subprocesses.
Secrets are control-plane config: the hub decrypts them transiently for
the runner resolve path and nothing else.

`hubd` itself is **stateless over Postgres**. Run N replicas behind any
LB: the queue coordinates claims via `FOR UPDATE SKIP LOCKED`, migrations
serialize on advisory lock 823400, scheduler passes on 823401. HA
Postgres is the HA story (per ADR-0002).

## Module layout

```
hub/
  cmd/hubd/            flags/env, migrate-at-boot, HTTP(S) serving, scheduler loop
  cmd/shift-bootstrap/ compose-bundle one-shot: certs+KEK ("certs"), seed ("seed")
  internal/store/      pgx pool + embedded migrations + all SQL
    migrations/000{1..5}_*.sql   schema v1→v5 (see below)
    runners.go         registration tokens, runner identity, secret auth
    users.go           OIDC users (JIT upsert by issuer+subject), roles
    flows.go           flow upsert, monotonic versions, publish workflow
    queue.go           enqueue/claim/heartbeat/complete/fail + reaping (enqueueTx shared with FireDue)
    schedules.go       cron schedules + FireDue (the exactly-once core)
    secrets.go         envelope blob CRUD (no crypto here)
    registry.go        publisher keys, content-addressed blobs, versions
    stats.go           dashboard overview counters
  internal/api/        HTTP handlers; admin(OIDC/break-glass) + runner realms
    auth.go            identity context, middlewares, /auth/* browser login
    secrets.go registry.go schedules.go dashboard.go ui.html
  internal/oidcauth/   go-oidc wrapper (Verify + code-flow Exchange) + oidctest fake IdP
  internal/kek/        KEK Provider interface + local key-file impl
  internal/secrets/    envelope service (DEK seal/open, KEK rotate)
  internal/scheduler/  tick loop + robfig/cron parser isolation (ParseCron/NextAfter)
  internal/pgtest/     test-only: SHIFT_TEST_PG or throwaway pg_ctl cluster
  e2e/                 crash recovery, exactly-once schedules, signed artifacts,
                       secrets-never-at-rest (real processes, real Postgres)
```

Dependency direction: `hub → pkg/flowdoc + pkg/consign → engine/record`
(path parsing for deploy-time validation only), plus vetted control-plane
deps `go-oidc` (JWKS validation) and `robfig/cron` (parser only). The hub
never imports `sdk`, `stream`, or anything that moves records.

## Schema v5 (migrations/0001–0005)

v1: `accounts` → `runner_registration_tokens` / `runners` (secrets stored
as SHA-256 only) → `flows` + `flow_versions` → `tasks` → `task_attempts`
→ `audit_log`. v2: `users` (OIDC identity = UNIQUE(issuer,subject), role
admin|viewer), `secrets` (ciphertext + wrapped_dek + kek_id; AAD binds
ciphertext to the secret name). v3: `flow_versions.status`
draft|published + `flows.published_version` — **version 0 resolves to the
published version everywhere**. v4: `schedules` (cron, next_fire_at,
last_* bookkeeping; UNIQUE(flow_id)). v5: `publisher_keys`,
`connector_blobs` (content-addressed by SHA-256, deduped),
`connectors`/`connector_versions` (Ed25519 signature + key ref, yankable).

Task states: `queued → leased → completed | failed` (requeue loops
`leased → queued`). Partial indexes: the claim scan, idempotency
uniqueness, and `schedules_due (next_fire_at) WHERE enabled`.

## Tenancy

Every auth middleware calls `store.WithAccount(ctx, id)`; every store
query filters on it (`accountID(ctx)` falls back to the seed account for
unscoped callers like bootstrap). Runners carry their account from
`AuthRunner`; users from their row; break-glass maps to the default
account. Cross-account isolation is regression-tested (two-account store
test covering Claim/Tasks/GetTask/Runners/secrets).

## The lease lifecycle

```
enqueue      INSERT … ON CONFLICT (idempotency) DO NOTHING → existing id on replay
claim        reap expired ⭢ UPDATE oldest queued (same account) via SKIP LOCKED
             ⭢ attempt++, lease_expires_at = now()+TTL ⭢ task_attempts row
heartbeat    extend lease iff still held AND not expired (409 otherwise)
complete     terminal iff leased_by matches (zombie results → 409)
fail         requeue while attempt < max_attempts, else terminal
reap         at claim time AND every scheduler tick (crash visibility)
```

Timing knobs (api.Options / hubd flags): `LeaseTTL` 30s default —
runners heartbeat at TTL/3. Long-poll `wait_seconds` capped at 30s.

## Scheduler (ADR-0012)

Every replica ticks (`-sched-interval`, 5s default); `store.FireDue` is
one transaction: try-advisory-lock 823401 (liveness) → `SELECT … FOR
UPDATE SKIP LOCKED` on due rows → per row, enqueue with idempotency key
`sched:<schedule_id>:<stored tick RFC3339 UTC>` and advance
`next_fire_at = NextAfter(cron, dbNow)` atomically. Postgres `now()` is
the only clock. Missed ticks fire once, then jump forward. Unpublished
flows and unparseable crons park with `last_error` instead of wedging
the pass. The `sched:` idempotency prefix is reserved (422 on user keys).
Proof: FireDue contention test (N schedules, 2 stores, exactly N tasks)
+ two-replica e2e with a mid-race shutdown.

## Auth model (M4b)

- **Admin realm**: OIDC bearer JWT (or the session cookie set by the
  `/auth/login → IdP → /auth/callback` browser flow — the cookie IS the
  verified ID token; stateless, HA-safe). Users JIT-provision on
  (issuer, subject); `viewer` role is read-only (non-GET → 403).
  Break-glass `SHIFT_HUB_ADMIN_TOKEN` (≥16 chars, constant-time) stays
  for bootstrap/automation — audited as `admin:break-glass`, warn-logged
  when OIDC is also on, unset it in production.
- **Runner realm**: unchanged — single-use registration token →
  per-runner bearer secret, both stored as SHA-256.
- `adminOrRunner` guards the endpoints both need (artifact resolve/fetch,
  trusted keys).
- Unauthenticated: `/` (static dashboard page), `/healthz`, `/readyz`,
  `/api/v1/authinfo` (which login methods exist — nothing more).

## Secrets (ADR-0010)

`PUT /api/v1/secrets/{name} {"value":…}` seals under a fresh DEK
(AES-256-GCM, AAD = name), wraps the DEK with the active KEK
(`SHIFT_HUB_KEK_FILE`, 32 raw bytes, 0600 enforced). Flow docs reference
`{"$secret":"name"}`; deploy validates existence (422 with names);
**runners** resolve via `POST /api/v1/secrets/resolve` per task — the
value never lands in `tasks.document`, task reads, or logs (e2e-proven
with a sentinel). Every access = one `secret.access` audit row. KEK
rotation: new file active + old in `SHIFT_HUB_KEK_FILES_OLD` → restart →
`POST /api/v1/keys/rotate` (re-wraps DEKs only) → retire old file.
Without a KEK configured the secrets endpoints don't exist.

## Connector registry (ADR-0011)

Publish: `POST /api/v1/publisher-keys` (base64 Ed25519 public key), then
`PUT /api/v1/connectors/{name}/versions/{version}?os&arch` with
`X-Shift-Publisher-Key` + `X-Shift-Signature` headers and the raw binary
as body (cap 128 MiB). The hub verifies the consign manifest signature
**before** storing — 403 stores nothing. Runners resolve
(`…/resolve?version=latest`) and fetch (`…/artifact`), verifying
key-trust + signature + digest fail-closed (see 04-runner.md). Blobs
live in Postgres, content-addressed, deduped; "latest" = newest publish.

**Capability policy (M5d, ADR-0015).** A per-deployment allow/deny list
(`SHIFT_HUB_CONNECTOR_ALLOW` / `SHIFT_HUB_CONNECTOR_DENY`) restricts which
connectors flows may use: a deploy referencing a disallowed connector is
rejected (422), and disallowed connectors are hidden from listing and
return 404 on resolve — cloud hubs hide dangerous connectors "not even
visible." Empty (self-hosted default) allows everything. Name-based and
hub-wide today; capability metadata and per-tenant scope are later adds.

## Direct executions (M5d-2, ADR-0016)

Push-triggered runs (webhook / direct API) execute entirely on a runner and
never enter the queue, so the runner reports their **metadata** afterwards —
`POST /api/v1/executions` (runner realm) → the `direct_executions` table
(account, runner, flow name, trigger, state, record counts, timing; **no
document, no payload**). `GET /api/v1/executions` (admin) lists them. This
gives the hub fleet load + history for work it never queued, without ever
touching payload.

## Dashboard

`GET /` serves the embedded single-file page (runner pattern): overview
strip (`/api/v1/stats`), flows with publish buttons + schedule editor,
recent tasks with attempt/telemetry drill-down, runners, registry,
secret names. Login via OIDC redirect or break-glass token prompt
(sessionStorage). The page is static; every data call is authenticated.

## Testing

`pgtest.DSN(t)` gives every test a fresh database: against
`SHIFT_TEST_PG` when set, otherwise a private `initdb`/`pg_ctl` cluster.
Store tests cover queue semantics, publish resolution, schedule firing
(incl. two-store contention), registry, secrets CRUD, tenancy isolation.
`oidcauth` tests run against an in-process fake IdP (runtime-generated
RSA key; mints expired/wrong-aud/wrong-iss/foreign-key tokens). API
tests cover realm matrices, viewer read-only, secret leak-freedom.
`e2e/` proves the milestone exits with real binaries and real Postgres:
kill -9 crash recovery, two-replica exactly-once schedules, the signed
artifact supply chain (incl. DB-tamper fail-closed), and
secrets-never-at-rest.

## Operating it

```
docker compose -f deploy/compose.dev.yml up -d       # dev Postgres
head -c 32 /dev/urandom > kek.bin && chmod 600 kek.bin
SHIFT_HUB_ADMIN_TOKEN=$(openssl rand -hex 16) \
  bin/hubd -db postgres://shift:shift-dev-only@localhost:5432/shift \
           -kek-file kek.bin \
           [-oidc-issuer https://idp… -oidc-client-id shift-hub \
            -oidc-redirect-url https://hub…/auth/callback]
```

Deploy = `PUT /api/v1/flows/{name}`; **publish** =
`POST /api/v1/flows/{name}/versions/{v}/publish` (new deploys are
drafts); execute = `POST …/execute` (version 0 = published; 409 if none);
schedule = `PUT …/schedule {"cron":"*/5 * * * *"}`; observe =
`GET /api/v1/tasks/{id}` or the dashboard on :8400.

The full self-contained stack — Postgres, Dex, TLS certgen, seeded
registry, signed-only runner — is `make up` (see `deploy/README.md`).
