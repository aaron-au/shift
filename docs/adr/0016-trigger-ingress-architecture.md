# ADR-0016: Trigger & ingress architecture — pull leases vs direct (webhook) execution

Date: 2026-07-20
Status: Accepted

## Context

Every trigger built so far is **pull**: the runner leases a task (metadata)
from the hub, then reaches out and pulls the data from a source connector.
The hub never sees payload — by design there is no payload channel to it
(ADR-0002).

Webhooks are the first **push** trigger: the payload arrives inbound in an
HTTP body and exists nowhere else. Something must catch that body and run a
flow with it. The question is whether the body may transit the hub. It must
not — keeping the hub payload-free is load-bearing doctrine (data residency,
tenant isolation, "control plane sees only metadata"). This ADR fixes the
model so webhooks (and later push triggers) fit hub-and-spoke without a hub
payload channel.

## Decision

**Two planes, cleanly separated:**

1. **Control plane — hub ↔ runner, metadata only.** Runners lease work,
   heartbeat, **sync config**, and **report executions**. The hub is the
   sole design-time authority: flows, schedules, webhook config, secrets,
   registry. Runners pull all of it (the same posture as leasing).
2. **Data plane — ingress → runner, runner → source.** Payload enters through
   a runner (inbound webhook) or is pulled by a runner (source connector),
   is transformed, and is delivered to a sink. **It never touches the hub.**

**Direct (push) execution:**
- A webhook hits a **runner API directly**, routed by a load balancer /
  Kubernetes ingress (container-first: the LB is just an ingress in front of
  the runner fleet). The runner runs the flow with the **request body as its
  source** (the `@webhook` built-in source), entirely in the data plane.
- It is **asynchronous by default**: the caller gets `202 + task_id` and
  polls for status/result/capture. **Async is the default everywhere** — the
  clean posture is components checking in via API rather than holding
  connections. So **every execution entry point** (leased execute, direct
  webhook, local execute) MUST expose a **job-status path** the caller can
  poll. A per-execution **sync toggle** is supported: when set, the entry
  point holds the request and returns the flow's terminal status/output
  instead of `202` — an opt-in convenience over the same async machinery.
- The runner then **reports the task to the hub as metadata** — id, flow,
  outcome, per-step telemetry — never payload. So the hub sees fleet load and
  keeps history even though it never queued the task. Direct tasks are
  distinct from leased tasks: not hub-queued, recorded post-hoc.

**Config distribution:** webhook definitions (path → flow, auth) are authored
on the hub (API now, studio UI in M5d-3) and **synced to runners
periodically**. Any runner behind the ingress can serve any webhook once
synced — so the fleet stays stateless and horizontally scalable.

**Runner APIs become a supported, authenticated public surface.** Until now
the runner's HTTP API was loopback-only and unauthenticated, with a standing
note that auth must land before any non-local bind (ADR-0008). This ADR is
that trigger: once webhooks route to runners through an ingress, the runner
surface is public and **must be authenticated**. Two kinds:
- **Hook endpoints** authenticate by a **per-webhook token**.
- **Control/dashboard endpoints** authenticate a **user**, and the surface is
  **multi-user with per-API permissions** (which endpoints a given user may
  call) — authorization, not just authentication.

**Auth starts as HTTP Basic and is pluggable.** The first cut is HTTP Basic
(user + secret) behind an auth interface, so more methods (bearer tokens,
OIDC — reusing the hub's realm, mTLS) drop in later without touching the
handlers. The permission model (roles → allowed endpoints) is designed in
from the first cut even while the credential type is basic.

## Consequences

- The hub doctrine is **preserved, not amended**: still no payload channel to
  the hub. Webhooks strengthen the story (payload stays where the runner is).
- **Durability differs by trigger kind, honestly:** leased (pull) tasks are
  hub-durable and exactly-once; a webhook body exists only in the inbound
  request, so a direct task is at-most-once per call — delivery guarantees
  are the caller's (retry) responsibility. Documented, not hidden.
- **Runner auth is now required** (closes the ADR-0008 deferral). Hook
  endpoints authenticate by per-webhook token; control/dashboard endpoints
  authenticate a user (HTTP Basic first, pluggable) with per-API permissions;
  the existing runner↔hub reporting authenticates by the runner's hub-issued
  bearer. Every execution entry point exposes a job-status path (async
  default); a per-execution sync toggle rides the same machinery.
- Kubernetes-native: runners behind an ingress, hub as a separate control
  service; no shared filesystem, no special networking.
- New surfaces: `@webhook` source (flowdoc + runner bind), runner hook server,
  runner→hub direct-task report endpoint, hub webhook-config store + runner
  sync. Built in stages (see PLAN M5d-2).

## Open (later)

- Richer runner auth methods behind the interface (bearer, OIDC, mTLS).
- HMAC/signature verification of inbound webhooks.
- Per-tenant ingress routing / custom domains.
