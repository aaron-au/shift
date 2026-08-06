# ADR-0041: Gateway identity, mutual TLS, and hub-asserted placement

Date: 2026-08-05

Status: **Accepted, build starting.** Supersedes the interim shared secret in
ADR-0038 §6a and closes the self-promotion gap in ADR-0038 §5.

## Context

Two problems, and they turn out to be one.

### 1. Runners assert their own labels

A route names the runners eligible to serve it by label set (ADR-0038 §5), and
the gateway matches that selector against labels the **runner sent in its own
poll body**:

```json
{"labels": {"environment": "production", "workload": "api"}}
```

ADR-0038 states the division as *"the hub decides eligibility, the poll set
decides availability."* That is currently only half true. Availability is
honest — a parked poll is a real runner with real capacity. **Eligibility is
not**: it is whatever the runner claims about itself.

The consequence is a privilege escalation with no audit trail (Aaron,
2026-08-05): a runner that is compromised, misconfigured, or simply started
with the wrong flag can claim `environment: production` and be handed
production traffic. Nothing in the system disagrees with it, because nothing
else has an opinion. **A component must not be able to promote itself into a
trust tier.**

### 2. The control listener authenticates one direction, weakly

The runner-facing endpoints currently carry a **shared secret** (ADR-0038 §6a,
built as an interim). It establishes that a caller knows a secret. It does not
establish:

- **which** runner is calling — every runner presents the same string, so
  nothing can be attributed, revoked, or scoped per runner;
- that the **gateway** is genuine — a runner has no way to tell a real gateway
  from anything that answered on that address, and a fake one is handed the
  runner's inbound payloads.

The second is the sharper one. The whole point of the DMZ topology is that the
runner reaches out; if it cannot verify what it reached, the topology protects
the network but not the data.

### Why these are one problem

Asserting labels *per runner* requires knowing *which runner is polling*. That
identity has to come from somewhere unforgeable, and the only unforgeable thing
on a TLS connection is the peer certificate. So label assertion is blocked on
mutual TLS, and mutual TLS is the thing that makes label assertion possible.
They ship together.

## Decision

### 1. Mutual TLS on the control listener, with a hub-issued PKI

The hub operates a **control-plane CA** (separate from any public-facing TLS
and from the ADR-0011 connector-signing keys, which are publisher-held and
must not be reused for transport identity).

- Every **gateway** gets a server certificate and the CA certificate.
- Every **runner** gets a client certificate at registration, alongside the
  bearer secret it already receives (ADR-0009).
- The control listener runs TLS with `ClientAuth: RequireAndVerifyClientCert`
  against the CA.
- The runner verifies the gateway's certificate against the same CA, which is
  the direction the shared secret never covered.

The certificate's subject carries the **runner id** and nothing else of
consequence — specifically **not** the labels (see §3).

### 2. The identity bundle is placed by hand, once

A gateway cannot dial the hub, so it cannot register with it. Bootstrap
therefore inverts: an administrator downloads an **identity bundle** from the
hub and places it on the gateway host. The bundle contains the gateway's
certificate and key, the CA certificate, and its gateway id.

The gateway loads it, binds, and **waits to be dialled**. The hub connects,
verifies the certificate it issued, and from that point the gateway is
configurable over the control channel like any other managed component.

One manual step, at install time only. Everything after it — including
certificate renewal (§4) — rides the channel the bundle establishes, the same
way an OAuth refresh token bootstraps once and then sustains itself (Aaron,
2026-08-04).

A copied bundle is **inert**: the hub dials a configured address, so a second
gateway holding the same identity is never contacted. It cannot steal traffic
by presenting a valid certificate, because nothing solicits it.

### 3. Labels are asserted by the hub, never by the runner

**The poll body no longer carries labels.** The gateway derives the polling
runner's identity from its client certificate and looks up its labels in a
**roster pushed by the hub**:

```json
{"runners": [{"id": "rnr-4f2a", "labels": {"environment": "production", "workload": "api"}}]}
```

Consequences, which are the point:

- **A runner cannot promote itself.** It has no way to state what it is; it can
  only prove who it is. Placement becomes a fact the control plane owns, which
  is what ADR-0038 §5 already claimed.
- **Attribution and revocation become possible.** "Which runner served this
  request" has an answer, and removing a runner from the roster removes its
  eligibility immediately, without touching the runner.
- **Misconfiguration stops being a security event.** A runner started with the
  wrong `-labels` flag is now simply a runner started with a flag that no
  longer exists.

Labels stay **out of the certificate** deliberately. Labels change (a runner is
moved between tiers, a workload class is added); identity does not. Encoding
them in the certificate would make every placement change a certificate
reissue, and would put a long-lived assertion where a mutable one belongs.

**A runner the gateway has no roster entry for is refused**, with a distinct
reason, and the refusal is logged. Failing closed is right: the alternative is
a runner silently treated as label-less, which matches every empty selector and
so receives traffic it was never granted. A brief window after a new runner
registers, before the roster push lands, is the cost — seconds, and visible.

### 4. Renewal is pushed before expiry

**A lapsed certificate strands a gateway permanently**, because renewing it
would require dialling the hub, which is precisely what a gateway cannot do.
So renewal is not a request the gateway makes; it is a push the hub performs,
and the hub must do it well before expiry.

The hub tracks expiry per gateway, renews at a configurable fraction of
lifetime (default: half), and surfaces "certificate expiring, push failed" as a
first-class alert rather than an ordinary retry. A gateway that has not been
successfully dialled within its renewal window is a **pending outage**, not a
warning.

Runner certificates carry the same requirement but not the same risk: a runner
*can* dial the hub, so it re-registers.

## Consequences

- **Self-promotion is structurally impossible**, rather than discouraged. This
  is the change that matters most, and it is why this ADR is not simply
  "add TLS".
- **The shared secret is removed**, not kept alongside. Leaving it in place as
  a fallback would preserve exactly the weakness this replaces — any client
  knowing one string, unattributable and unrevokable.
- **HTTP/2 arrives free.** Go negotiates h2 over TLS via ALPN, so the control
  plane gets multiplexing without a dependency (the gateway is stdlib-only by
  depguard rule). That directly relieves the connection pressure that caused
  the runner-side port exhaustion in `docs/bench-gateway.md`: N parked polls
  collapse onto one connection instead of N sockets. Worth re-measuring once it
  lands.
- **The poll body shrinks to a wait hint.** A wire simplification, and a
  reduction in what a DMZ component accepts from inside the network.
- **The hub gains a PKI to operate**: issuance, storage, renewal, revocation,
  and a CA key to protect. That is real operational weight and it is the honest
  price of per-component identity.
- **Bootstrap gains a manual step.** Accepted deliberately: the alternative is
  the gateway dialling inward, which is the one thing ADR-0038 exists to
  prevent.
- **A roster push is now required before a new runner can serve inbound
  traffic.** Scheduled and hub-queued work is unaffected.

## Alternatives considered

**Signed label assertions instead of a roster.** The hub signs a short-lived
`{runner id, labels, expiry}` token at registration; the runner presents it on
poll; the gateway verifies it with the hub's public key. This is genuinely
attractive under autoscaling — no per-runner push to every gateway, and no
window where a new runner is unknown. Rejected as the *primary* mechanism
because it adds a second signing key, clock-skew sensitivity, and a revocation
story that degrades to "wait for the TTL", to solve a freshness problem a
push already solves. **Kept explicitly as the escape hatch**: if runner churn
ever makes roster pushes expensive, this is the upgrade, and it composes with
everything above rather than replacing it.

**Labels in the client certificate** (SAN or a custom extension). Removes the
roster entirely and is unforgeable by construction. Rejected because labels are
mutable and identity is not: retiering a runner would mean reissuing its
certificate, and certificate lifetime would become placement lifetime.

**Keep self-asserted labels, and rely on mTLS alone.** Tempting, because mTLS
already stops an *outsider* joining the fleet. Rejected because the threat is
not only outsiders: a compromised or misconfigured insider is exactly the case
placement is supposed to contain, and mTLS without asserted labels authenticates
the claimant while still believing the claim.

**A gateway-held allowlist configured locally.** Rejected by ADR-0038 §6 — all
configuration comes from the hub, and a locally-defined allowlist is a second
source of truth whose failure mode is serving stale policy.

## Open questions

1. **CA rotation.** Rotating the control-plane CA means re-issuing every
   gateway and runner certificate. A cross-signed transition is the standard
   answer; whether it is worth building before the first rotation is due is
   not settled.
2. **Whether the hub's own API should move to the same CA** for runner
   authentication, replacing the bearer secret (ADR-0009) rather than sitting
   beside it. Attractive for uniformity; a larger change than this ADR.
3. **Roster scope.** Every runner to every gateway, or only runners whose
   labels can satisfy at least one of that gateway's routes? The latter is less
   to leak from a DMZ box and more to recompute on every route change.
4. **Whether `X-Shift-Principal` should also carry the serving runner id** so
   the hub's execution record ties caller, gateway and runner together without
   a join (relates to the ADR-0039 trace id).
