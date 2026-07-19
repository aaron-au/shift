# 06 — Hub: the durable control plane

> Code: `hub/`. Decisions: ADR-0002 (topology/durability), ADR-0009
> (control API). Status: M4a — queue core; OIDC/registry/scheduler are M4b.

## What the hub is (and is not)

The hub owns **all durable state**: runner identity, flow versions, the
task queue, attempt history, audit. It never touches payload data — flow
documents and result summaries are control-plane metadata; records stream
only through runners and their connector subprocesses.

`hubd` itself is **stateless over Postgres**. Run N replicas behind any
LB: the queue coordinates claims via `FOR UPDATE SKIP LOCKED`, and
migrations serialize on a Postgres advisory lock. HA Postgres is the HA
story (per ADR-0002).

## Module layout

```
hub/
  cmd/hubd/            flags/env, migrate-at-boot, HTTP(S) serving
  internal/store/      pgx pool + embedded migrations + all SQL
    migrations/0001_init.sql   schema v1 (see below)
    runners.go         registration tokens, runner identity, secret auth
    flows.go           flow upsert + monotonic versions
    queue.go           enqueue/claim/heartbeat/complete/fail + reaping
  internal/api/        HTTP handlers, admin/runner auth realms
  internal/pgtest/     test-only: SHIFT_TEST_PG or throwaway pg_ctl cluster
  e2e/                 crash-recovery exit test (real processes, kill -9)
```

Dependency direction: `hub → pkg/flowdoc → engine/record` (path parsing
for deploy-time validation only). The hub never imports `sdk`, `stream`,
or anything that moves records.

## Schema v1 (migrations/0001_init.sql)

`accounts` (seeded `default`; multi-tenant activates with OIDC) →
`runner_registration_tokens` / `runners` (secrets stored as SHA-256 only)
→ `flows` + `flow_versions` (documents are validated JSONB; deploy bumps
`latest_version`) → `tasks` (the queue) → `task_attempts` (one row per
lease: who, when, outcome ∈ completed|failed|lease-expired) →
`audit_log`.

Task states: `queued → leased → completed | failed` (requeue loops
`leased → queued`). Two partial indexes matter: the claim scan
(`state IN ('queued','leased')` by `enqueued_at`) and the idempotency
uniqueness (`(account_id, idempotency_key) WHERE … IS NOT NULL`).

## The lease lifecycle

```
enqueue      INSERT … ON CONFLICT (idempotency) DO NOTHING → existing id on replay
claim        reap expired ⭢ UPDATE oldest queued via SKIP LOCKED subselect
             ⭢ attempt++, lease_expires_at = now()+TTL ⭢ task_attempts row
heartbeat    extend lease iff still held AND not expired (409 otherwise)
complete     terminal iff leased_by matches (zombie results → 409)
fail         requeue while attempt < max_attempts, else terminal
reap         at claim time: expired+exhausted → failed; expired → queued
```

Reaping at claim time means no background scheduler exists yet — a hub
with zero lease traffic reaps nothing, which is fine because nothing is
waiting either. M4b's scheduler adds a periodic sweep for observability.

Timing knobs (api.Options / hubd flags): `LeaseTTL` 30s default —
runners heartbeat at TTL/3; the e2e test runs at 2s. Long-poll
`wait_seconds` capped at 30s, re-checking every 200ms.

## Auth model (M4a placeholder, M4b target)

- Admin realm: `SHIFT_HUB_ADMIN_TOKEN` (≥16 chars, constant-time
  compare). Replaced by OIDC in M4b; the handler split is what persists.
- Runner realm: single-use registration token → per-runner bearer secret;
  both stored as SHA-256, plaintext returned exactly once.
  `AuthRunner` doubles as liveness (`last_seen_at`).
- `healthz` (process) and `readyz` (DB ping) are unauthenticated.

## Testing

`pgtest.DSN(t)` gives every test a fresh database: against
`SHIFT_TEST_PG` when set (CI service container, compose dev DB),
otherwise a private `initdb`/`pg_ctl` cluster on a unix socket in a temp
dir (no TCP listener, destroyed with the test). Store tests cover the
queue semantics; api tests cover realms + protocol over HTTP; `e2e/`
proves the milestone exit: build real `runnerd` binaries, SIGKILL one
mid-flow, watch the task complete elsewhere with a clean attempt trail.

## Operating it

```
docker compose -f deploy/compose.dev.yml up -d       # dev Postgres
SHIFT_HUB_ADMIN_TOKEN=$(openssl rand -hex 16) \
  bin/hubd -db postgres://shift:shift-dev-only@localhost:5432/shift
# mint a runner token, then boot a runner against it:
curl -s -X POST -H "Authorization: Bearer $SHIFT_HUB_ADMIN_TOKEN" \
  localhost:8400/api/v1/runner-tokens         # → {"token":"srt_…"}
SHIFT_HUB_REG_TOKEN=srt_… bin/runnerd -hub http://127.0.0.1:8400 -connector-dir bin
```

Deploy = `PUT /api/v1/flows/{name}` with the flow document; execute =
`POST /api/v1/flows/{name}/execute {"idempotency_key":…}`; observe =
`GET /api/v1/tasks/{id}` (state, result, attempt history) or the runner
dashboard on :8340, which shows leased tasks like any local task.
