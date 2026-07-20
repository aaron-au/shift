# Handoff — SHIFT (2026-07-20)

Fresh-agent orientation. Read `CLAUDE.md` + `PLAN.md` first (authoritative);
this is the "what just happened / what's next / how to run" layer. Delete or
rewrite this file when you pick up.

## Where we are

Milestones **M0–M5 + M5.5 (Studio Builder)** shipped; **M6 (enterprise
hardening) in progress**. Just completed on `main` (7 commits, **NOT pushed**
— `main` is 7 ahead of `origin/main`; push when the user asks):

- **M5.5 Studio Builder** (ADR-0018/0019): canvas flow builder in an OS-lite
  windowed shell + connector config-schema discovery (signed descriptors).
  Vanilla JS, no build step, single embedded `hub/internal/api/ui.html`.
- **M6a Observability** (ADR-0020): Prometheus `/metrics` on hubd + runnerd.
  **OTLP tracing deferred** (rationale in ADR-0020 — attempt-history already
  covers the causal story).
- **M6b Audit log** (`GET /api/v1/audit` + CSV + studio window).
- **M6c Rate limiting** (ADR-0021): token-bucket on hub API + runner webhook
  ingress; off by default.

`make check` is green (fmt, vet, golangci-lint, govulncheck, gitleaks,
`-race` across all modules incl hub e2e). Postgres-backed tests need
`SHIFT_TEST_PG` — see below.

## What's next (M6 remaining — user's call which)

- **M6d** billing aggregation from the M6a telemetry substrate.
- **M6e** connector marketplace plumbing.
- **M6f** migration tooling (OpenAPI importer) + benchmark-vs-incumbent.
- **Deferred, own series:** studio **visual polish** (see memory
  `shift-studio-visual-polish` — snapping, maximise, light/dark palette,
  styled scrollbars, resize indicator, merge close/minimise). NOT a blocker.
- **Deferred:** M6a OTLP tracing (ADR-0020 has triggers + a lighter option).

## How to run the dev stack (throwaway hubd + runner, no compose/TLS)

The `make up` bundle is heavy (TLS + Dex). For eyeballing, run hubd over plain
HTTP against the dev Postgres. (Any leftover processes from the prior session
were killed; recreate as needed. `:8400` may be a running compose bundle —
use `:8410`.)

```
# pick any dev break-glass token (>=16 chars); keep it out of git
export ADMIN=$(openssl rand -hex 16)
# dev Postgres (compose.dev.yml) — container shift-postgres on :5432
export SHIFT_TEST_PG="postgres://shift:$PGPASS@127.0.0.1:5432/shift?sslmode=disable"  # PGPASS = compose.dev.yml POSTGRES_PASSWORD

# hubd on :8410, break-glass token, KEK for secrets, rate limits off
cd hub && go build -o /tmp/hubd ./cmd/hubd
SHIFT_HUB_DB="$SHIFT_TEST_PG" SHIFT_HUB_ADMIN_TOKEN="$ADMIN" \
  SHIFT_HUB_LISTEN="127.0.0.1:8410" SHIFT_HUB_KEK_FILE="/tmp/bootstrap/kek.bin" \
  /tmp/hubd
# dashboard: http://127.0.0.1:8410  (login: paste $ADMIN)
# metrics:   curl http://127.0.0.1:8410/metrics
```

To get connectors **with v2 descriptors** into that hub (so the builder's
config forms + connector list populate), seed via shift-bootstrap over HTTP:

```
mkdir -p /tmp/shiftconns /tmp/bootstrap
(cd connectors && go build -o /tmp/shiftconns/shift-connector-gen ./cmd/shift-connector-gen \
  && go build -o /tmp/shiftconns/shift-connector-http ./cmd/shift-connector-http)
(cd hub && go build -o /tmp/shift-bootstrap ./cmd/shift-bootstrap)
/tmp/shift-bootstrap certs -dir /tmp/bootstrap
SHIFT_HUB_ADMIN_TOKEN="$ADMIN" /tmp/shift-bootstrap seed \
  -dir /tmp/bootstrap -hub http://127.0.0.1:8410 -connectors /tmp/shiftconns
# (creates the KEK file above, publisher key, uploads gen+http v2-signed, mints a runner token)

# a runner (fresh single-use token each start):
cd runner && go build -o /tmp/runnerd ./cmd/runnerd
TOK=$(curl -s -XPOST http://127.0.0.1:8410/api/v1/runner-tokens -H "Authorization: Bearer $ADMIN" -d '{}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
SHIFT_HUB_REG_TOKEN="$TOK" /tmp/runnerd -hub http://127.0.0.1:8410 -listen 127.0.0.1:8341 \
  -connector-dir /tmp/shiftconns -connector-cache /tmp/conncache -name dev
```

Rate limits are flags (`-rl-admin-rps`/`-rl-runner-rps`/`-rl-public-rps` on
hubd, `-rl-webhook-rps` on runnerd); 0 = off.

## Key context / gotchas

- **Doctrine is strict** — read the doctrine + "lessons already paid for" in
  `CLAUDE.md`. Notably: hub module must **not** import `sdk`/`stream` (the
  publisher shells out to `<connector> describe` for that reason); engine +
  `pkg/` stay telemetry-free (OTel lives only in hub/runner; the ratelimit
  package is duplicated per-module to keep x/time out of connectors).
- **`pkg/flowdoc` validation is authoritative** — the studio builder surfaces
  422s, never re-implements validation.
- **Every milestone lands green through `make check` with tests + updated
  `docs/dev/` + an ADR for any decision.** Non-negotiable.
- Studio is `ui.html` only (vanilla, no build). Parse-check JS with
  `node -e` extracting the `<script>` block before restarting hubd.
- Full memory log: `shift-rebuild-decisions` (milestone-by-milestone),
  `shift-studio-visual-polish`, `shift-project-goals`, `shift-review-outcome`.
- ADRs 0001–0021 in `docs/adr/`. Dev docs `docs/dev/01–07`.
