# ADR-0021: Rate limiting — hub control API + runner ingress

Date: 2026-07-20
Status: Accepted (design); implementation in M6c

## Context

Two authenticated surfaces are exposed to clients and need abuse/overload
protection (M6c):

- The **hub control API** (ADR-0009) — humans (OIDC / break-glass admin) and
  runners (lease intake, execution reports). A misbehaving client, a runaway
  script, or credential-stuffing against the login/token paths can hammer it.
- The **public runner ingress** (ADR-0016) — `POST /hooks/{name}` webhooks are
  an internet-facing, authenticated surface. Unbounded inbound requests are a
  DoS vector and can exhaust runner admission.

Neither is rate limited today. This ADR fixes the approach. Constraints:

- **HA hub, stateless replicas (ADR-0002).** A *globally* precise limit would
  need shared state (Postgres/Redis) written on every request — Redis is a
  forbidden spoke dependency, and per-request Postgres writes are too costly
  for a limiter. So limits are **per-replica**; the effective global limit is
  `limit × replicas`. This is an explicit, documented tradeoff — the goal is
  overload/abuse protection, not exact quota accounting (billing owns quotas,
  M6d).
- **No heavy frameworks.** Use `golang.org/x/time/rate` (official, minimal
  token-bucket) — a small vetted dependency, not a gateway/middleware stack.
- **Runner stays stateless/disposable.** Its limiter is in-memory per process;
  a restart resets buckets (acceptable — it also drops all in-flight work).

## Decision

A **token-bucket** limiter (`golang.org/x/time/rate`) applied as HTTP
middleware on both surfaces, keyed per client, in-memory per process.

### Keying

- **Hub, authenticated:** key on the identity the auth middleware already
  resolves — OIDC subject, `admin` for the break-glass token, or the runner id
  — combined with the realm. Runners get a higher default (they poll leases).
- **Hub, unauthenticated** (login, `/auth/*`, `authinfo`): key on client IP
  (best-effort, honoring a configured trusted-proxy header) with a stricter
  limit — this is the credential-stuffing surface.
- **Runner ingress:** `POST /hooks/{name}` keyed on `{hook name, source IP}`;
  a per-hook ceiling protects one flow from starving the runner's admission.

### Behavior

- Over limit → **HTTP 429** with a `Retry-After` header. Never silently drop.
- **Exempt:** `/healthz`, `/readyz`, `/metrics` — liveness/scrape must never be
  throttled.
- **Bucket GC:** idle per-key buckets are swept on a timer so the map cannot
  grow unbounded (a per-IP key space is itself a memory vector).

### Configuration

Per-deployment via flags/env: requests/sec + burst per class (authenticated,
unauthenticated, runner-poll, webhook). Sensible defaults; **`0` disables** a
class (loopback dev / self-hosted single-user stays frictionless). Cloud
deployments set real numbers.

### Metrics

The limiter exports counters through the M6a registry:
`shift_hub_ratelimited_total{realm}` / `shift_runner_ratelimited_total{hook}`
so throttling is observable (and can be alerted on before it bites).

## Consequences

- New small dependency `golang.org/x/time/rate` in `hub/` and `runner/`.
- Limits are per-replica, not global — documented; precise cross-replica
  quota accounting is out of scope (billing/quota is M6d, and would use a
  shared medium if ever needed).
- The limiter middleware sits outside the auth wrappers for unauthenticated
  keys (IP) but needs the resolved identity for authenticated keys — so it
  composes *after* auth for the identity path; ordering is spelled out in the
  implementation (auth resolves identity into the request context, the limiter
  reads it).
- Off-by-default classes keep dev/self-hosted unchanged.

## Open questions (resolve at build)

- Trusted-proxy / `X-Forwarded-For` handling for the IP key (which hops to
  trust) — must not let a client spoof its key. Default: use the direct peer
  unless a trusted-proxy CIDR is configured.
- Whether webhook limits belong additionally on the hub-authored webhook
  config (a per-hook rps field synced to runners) vs runner-local flags only.
  Leaning: runner-local flags first, hub-authored per-hook limit later.
- Per-tenant limits (vs per-identity) once multi-tenant SaaS lands.
