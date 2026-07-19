# SHIFT deploy bundles

## `compose.dev.yml` — dev database only

Postgres for local hub development and the pgtest suite:

```sh
docker compose -f deploy/compose.dev.yml up -d
export SHIFT_TEST_PG=postgres://shift:shift-dev-only@localhost:5432/shift
```

## `compose.yml` — the "just runs" bundle (M4b exit criterion)

Everything: Postgres, Dex (OIDC), certgen, hubd (TLS), bootstrap
seeding, and a runner that only accepts registry-signed connectors.

```sh
make up          # builds the three images, then compose up
```

### Exit-criterion walkthrough

1. `make up` — wait for `bootstrap` to log `seed: done` and `runnerd`
   to log `registered with hub`.
2. Open https://localhost:8400 (self-signed cert — accept once).
3. **Login via OIDC**: click "Login with your identity provider" →
   Dex → `admin@example.com` / `password`.
4. The `demo` flow is already deployed + published with a `* * * * *`
   schedule. **Within a minute** a task appears in Recent tasks and
   completes — executed by the runner via a **signed** `gen` connector
   fetched from the registry (no local binaries: `SHIFT_REQUIRE_SIGNED=1`).
5. Click the task row: attempts + records in/out + per-op telemetry.
6. Exactly-once check: every scheduled task's idempotency key is
   `sched:<schedule-id>:<tick>` — re-runs of the same tick collapse.

### What lives where

- `bootstrap` volume: dev CA + hub cert, KEK file, publisher private
  key, runner token + persisted runner credentials. Never bind-mounted.
- All secrets in `compose.yml`/`dex/config.yaml` are deliberately
  non-secret dev defaults (documented, same policy as compose.dev.yml).
  Deployments that leave a laptop must template their own and unset the
  break-glass `SHIFT_HUB_ADMIN_TOKEN` once OIDC login is verified.
- Production IdP: any OIDC-compliant provider — set
  `SHIFT_HUB_OIDC_ISSUER/CLIENT_ID/CLIENT_SECRET/REDIRECT_URL`.
  Provision it with its Terraform provider (e.g. `auth0/auth0`,
  `azuread`); nothing manual (standing rule).
