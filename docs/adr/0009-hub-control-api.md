# ADR-0009: Hub control API — HTTP/JSON, bearer auth, lease protocol

Date: 2026-07-19
Status: Accepted

## Context

M4/M3b connect runners to the hub's durable queue (ADR-0002). The control
plane needs a wire protocol and an authentication story. The data plane
already has one (ADR-0007: gRPC/UDS between runner and connector
subprocesses) — but hub↔runner traffic is different in kind: small JSON
control messages (flow documents, lease claims, heartbeats, results), low
rate, crossing real networks.

## Decision

1. **HTTP/JSON, stdlib only.** The hub API is `net/http` + `encoding/json`
   (Go 1.22 method patterns). No gRPC for control: payload never flows
   here, throughput is irrelevant, and plain HTTP keeps the API curl-able,
   proxy/LB-friendly, and trivially consumable by the studio UI and
   third-party tooling later. gRPC stays where the volume is (data plane).
2. **Two auth realms from the first commit** (per doctrine: auth is never
   bolted on):
   - **Admin realm** (`/api/v1/flows*`, `/api/v1/tasks*`,
     `/api/v1/runner-tokens`, `/api/v1/runners`): bearer token from
     `SHIFT_HUB_ADMIN_TOKEN` (min 16 chars, constant-time compare). This
     is the M4a placeholder that OIDC replaces in M4b — the realm split,
     not the token, is the load-bearing decision.
   - **Runner realm** (`/api/v1/lease`, `/api/v1/tasks/{id}/…`): each
     runner registers by presenting a **single-use registration token**
     (admin-minted, TTL-bound, stored as SHA-256) and receives a bearer
     secret (also stored only as SHA-256). Every subsequent call
     authenticates by secret hash lookup. Each runner instance gets its
     own token — fits disposable containers; a provisioner mints tokens.
3. **Lease protocol** (all state transitions in Postgres, ADR-0002):
   - `POST /lease {wait_seconds}` long-polls: `Claim` runs
     `FOR UPDATE SKIP LOCKED` on the oldest queued task, stamps
     `leased_by` + `lease_expires_at = now() + TTL` (default 30s),
     increments `attempt`, and records a `task_attempts` row. 204 when
     the window closes empty.
   - Every claim first **reaps expired leases**: requeue while attempts
     remain, else fail terminally. Reaping at claim time needs no
     background scheduler and scales with demand.
   - `heartbeat` extends a live lease; an expired lease can never be
     resurrected (the zombie runner gets 409 and abandons reporting).
   - `complete`/`fail` verify `leased_by` — results from a runner that
     lost its lease are rejected (409). `fail` requeues or finishes per
     `max_attempts`.
   - **At-least-once ⇒ idempotency:** tasks accept an
     `idempotency_key` (unique per account; re-enqueue returns the
     existing task). The runner injects the key (or the stable task id)
     into the sink config; sinks with an idempotency notion (e.g. the
     http connector's `Idempotency-Key` header, keyed per batch ordinal)
     replay identically across attempts.
4. **Runner intake stays capacity-gated (ADR-0005):** the lease loop only
   claims when the memory governor has headroom for another task, so
   backlog queues at the hub — where any runner can take it — never
   inside a busy runner.
5. **Transport security:** hubd binds loopback by default and serves
   HTTPS when `-tls-cert/-tls-key` are set. Non-local plaintext binds are
   a deployment error; compose/K8s deployments terminate TLS at the hub
   or its ingress.

## M4a / M4b staging

M4a (shipped with this ADR): schema v1, queue/lease/heartbeat, runner
registration, flow deploy/versioning, execute, audit log, crash-recovery
exit test. M4b (next): OIDC + tenancy enforcement, connector registry +
artifact signing, HA scheduler (advisory locks), secrets envelope
encryption, hub dashboard, "just runs" compose bundle.

## Consequences

- A second wire idiom (HTTP/JSON) exists alongside gRPC — accepted; they
  serve different planes and the split is documented here.
- Long-poll leasing polls Postgres (200ms default) while a claim window
  is open; LISTEN/NOTIFY can cut latency later without protocol change.
- Registration tokens being single-use means restart ⇒ new token unless a
  provisioner supplies one; runner secret persistence is deliberately not
  offered (runners are stateless).
