# Release & operations (issue #9)

How a SHIFT release is cut and run. Complements the ADR-0006 amendment (the
release/scheduled **supply-chain tier**, `.github/workflows/supply-chain.yml`)
and the Postgres hardening in [06-hub.md](06-hub.md) (issue #8).

## Release checklist

Run in order; each is a gate, not a suggestion.

1. **Green gate on `main`** — `make check` passes in CI (the identical
   correctness/security gate, ADR-0006).
2. **Supply-chain tier** — trigger `supply-chain.yml` (workflow_dispatch or the
   release tag) and green-light its jobs (each currently carries a
   pin-before-enable TODO): proto-drift, Trivy image scan, hadolint, CodeQL.
3. **Digest-pin base images** — resolve every `FROM`/`image:` tag to a digest
   and pin it (see below). Tags are mutable; a release must be reproducible.
4. **Build + tag images** — `make images VERSION=<semver>`; tag immutably (the
   semver, not `latest`).
5. **SBOM + signing** — generate an SBOM per image and sign the artifacts
   (release-tier jobs, once enabled). Publish checksums.
6. **Staging deploy** — deploy the exact images to staging (below), run the
   smoke checks, watch `/metrics` + logs for one cycle.
7. **Promote** — deploy the same digests to production. Never rebuild between
   staging and prod; promote the artifact that was tested.

## Digest-pinning base images

Base images (`deploy/docker/Dockerfile`, `deploy/compose*.yml`) are tag-pinned
in-repo for dev ergonomics; a **release pins them by digest**. Obtain digests
with either:

```
crane digest postgres:18-alpine
docker buildx imagetools inspect gcr.io/distroless/static-debian12:nonroot
```

Then pin, e.g. `postgres:18-alpine@sha256:<digest>`. Do this at release time so
the pin matches what was scanned in step 2; don't commit stale digests that
drift from the dev tags. Images to pin: `golang:1.26-alpine`,
`gcr.io/distroless/static-debian12:nonroot`, `alpine:3.22`, `postgres:18-alpine`,
`dexidp/dex:vX.Y.Z`.

## Staging

Staging is the production compose stack (`deploy/compose.yml`) pointed at a
staging Postgres and IdP, running the release's pinned images. Smoke checks:

- `GET /healthz` and `/readyz` on hubd; `/healthz` on runnerd.
- Deploy → publish → execute a trivial flow; confirm it reaches `completed`.
- Register a runner; confirm it leases and heartbeats.
- Scrape `/metrics`; confirm `shift_hub_http_requests_total` and the queue
  gauges move.

## Rollback

- **Code/images**: redeploy the previous release's pinned digests. Images are
  immutable, so rollback is a re-point, not a rebuild.
- **Schema**: migrations are forward-only. Safe rollback depends on
  **expand/contract** discipline (06-hub.md #8): because release N only *adds*
  and N+1 only *drops* after nothing reads the old shape, rolling back code from
  N+1 to N still finds a compatible schema. Never ship a migration that a
  one-version rollback can't tolerate.

## Backup / restore

Operators run base backups + WAL archiving on the hub Postgres. **A backup is
not proven until a restore is tested** — schedule a periodic restore drill into
a scratch instance and verify the hub boots + migrates against it. The hub is
the availability keystone (ADR-0002); everything durable lives there.

## Incident basics

- **Metrics**: `/metrics` on hubd and runnerd (ADR-0020) — queue depth, oldest
  queued age, runner liveness, per-route HTTP latency/status (#7),
  rate-limit rejections.
- **Logs**: hubd emits structured JSON (`SHIFT_HUB_LOG_LEVEL`); each request
  carries a correlation id (`X-Request-Id`, on the access line as `id`) — grep
  it to follow one request (#7).
- **Runner loss**: a dead runner's task re-dispatches on lease expiry, or fails
  terminally for an `at_most_once` flow (06-hub.md #11); no operator action for
  the common case.

> This runbook is the starting point. Deepen the incident procedures (paging,
> severity levels, comms) as the deployment surface grows.
