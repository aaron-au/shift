# ADR-0040: Gateway pass-through proxy routes

Date: 2026-08-05

Status: **Drafted, not built.** Proposed by Aaron 2026-08-05. The decision
below picks runner-mediated proxying as the default and direct proxying as an
explicit, separately-configured relaxation; neither is implemented.

## Context

The gateway (ADR-0038) exists to put inbound HTTP work onto a runner. A
customer deploying it will already have it terminating TLS, authenticating
callers, filtering by IP and stamping identity — and will immediately notice
that their *other* internal APIs need exactly those things and get none of
them.

The request is simple to state: let a route front an internal application
**as-is**. No flow, no runner, no transformation. The gateway authenticates
the caller with a SHIFT credential, attaches whatever credential the backend
expects, and passes the request and response through unchanged.

This is worth taking seriously rather than dismissing as scope creep. It is
the difference between "a component you deploy because SHIFT needs one" and
"the component you put your APIs behind", and the marginal code is small
because the hard parts — TLS, auth, allowlists, identity stamping, config push
— already exist for the flow path.

It also collides with the one rule ADR-0038 is built on, and that collision is
the entire content of this decision.

### The collision

ADR-0038 §2: **nothing in the DMZ ever initiates a connection inward.** It is
what makes a compromised gateway a dead end — it holds no credential that opens
a path onward, because it has none to hold, and a firewall review sees only
outbound-to-DMZ rules.

A proxy route inverts that twice over:

1. **The gateway dials the internal network.** A firewall rule now exists from
   the DMZ inward. That rule is what an attacker on the gateway inherits.
2. **The gateway holds a usable credential.** Today it holds token *digests* —
   it can verify a caller but cannot impersonate anyone. A backend credential
   is live secret material, in memory, on the box most exposed to the internet.
   That is a categorical change in what a gateway compromise costs, not a
   quantitative one.

Neither is a reason to refuse the feature. Both are reasons the default should
not silently be the weaker shape.

## Decision

### 1. Two route kinds, and the default keeps the invariant

A route gains a `kind`:

| `kind` | Behaviour | DMZ→internal connection | Credential in the DMZ |
|---|---|---|---|
| `flow` (default, built) | dispatch to a parked runner | none | none |
| `proxy` | pass through to an upstream | none — **via a runner** | none |
| `proxy_direct` | pass through to an upstream | **yes** | **yes** |

`proxy` is the recommended shape and it preserves everything ADR-0038 bought.
The insight is that the gateway does not need to make the outbound call itself:
a **parked runner already has an outbound path to the gateway**, and the runner
is inside the network with access to the hub's secret machinery. So a proxy
route dispatches exactly like a flow route, and the runner performs the
upstream call and streams the response back over the existing deliver endpoint.

What that buys, all of it for free:

- **Zero DMZ→internal connections.** The runner still polls outward. The
  firewall table does not change at all.
- **Backend credentials never leave the internal network.** They resolve
  runner-side through ADR-0010 envelope secrets, exactly as connector
  credentials already do — including `{"$secret":...}` refs, rotation and
  revocation, with no second secret-delivery path to build or audit.
- **Placement still applies.** A proxy route carries a label selector like any
  other, so "this API is fronted only by production runners in the AU region"
  is expressible on day one.
- **The observability story is already there.** It is a task: it has a request
  id, phase timings and an execution record (ADR-0039).

The cost is one extra hop. Measured on the flow path, gateway → parked runner →
back is **~0.5 ms** — against a backend API call that is, in practice, tens of
milliseconds. It is not the reason to choose `proxy_direct`.

`proxy_direct` exists for the case the extra hop genuinely does not suit: a
deployment with no runner fleet at all, or one where the gateway is wanted
purely as an API front door and SHIFT's runtime is not in the picture. It is a
**separate route kind rather than a flag** so that "this deployment has a
DMZ→internal path" is answerable by grepping the configuration, and so the hub
can refuse it per-deployment the way connector capability policy already
refuses dangerous connectors (ADR-0015).

### 2. Upstreams are named, never derived from the request

For both proxy kinds, the upstream is a **fully-specified URL in the pushed
configuration**. No part of it may come from the caller — not the host, not the
scheme, not a path segment interpolated from a parameter.

This is the whole difference between a proxy and an SSRF engine. A gateway that
would dial a caller-influenced address is a credentialed request-forger sitting
in the DMZ with a firewall rule inward, which is close to the worst object in
the system. `pkg/httpsec` (the connector SSRF guard) applies to `proxy_direct`
as a second line, but the primary defence is that the address is never
attacker-influenced in the first place.

Path *suffixes* may be forwarded (`/api/v1/orders/123` → upstream
`/orders/123`) because that is the point of a proxy, subject to normalisation
that rejects `..` traversal before matching.

### 3. Identity propagates the same way it does for flows

Strip-then-stamp (ADR-0038 §4b) is unchanged: every inbound `x-shift-*` header
is stripped, and `X-Shift-Principal` / `X-Shift-Route` / `X-Shift-Request-Id` /
`X-Shift-Client-Ip` are stamped. The backend therefore learns who called
without being able to be lied to about it, and without SHIFT having to
understand the backend's own auth model.

The caller's credential is **not** forwarded, matching the flow path. Credential
translation is the feature: the caller presents a SHIFT credential, the backend
receives its own.

### 4. What this is not

Not a WAF, not a plugin host, not a service mesh, and not a load balancer for
arbitrary backends — a proxy route names **one** upstream, and anything wanting
health-checked pools of backends should use the thing the customer already has
for that. ADR-0038's non-goals hold unchanged; this adds one route kind, not a
product category.

## Consequences

- **The gateway becomes worth deploying for its own sake**, which changes it
  from a SHIFT dependency into a component with independent value. That is a
  commercial argument as much as a technical one (ADR-0033).
- **`proxy` costs nothing architecturally.** It reuses dispatch, placement,
  identity, secrets and observability wholesale; the new code is a runner-side
  pass-through mode plus a route field.
- **`proxy_direct` is a real, stated weakening** of the DMZ posture, opt-in per
  route, refusable per deployment, and it must be documented in exactly those
  terms rather than as a performance option. A deployment that never configures
  one is bit-for-bit as isolated as today.
- **A new failure mode on the runner path:** the upstream is now a dependency
  of a caller-facing request, so upstream latency becomes gateway-visible
  latency and needs its own timeout distinct from the delivery timeout.
- The runner gains an outbound HTTP capability on a caller-triggered path. It
  already has one (the `http` connector), and the same SSRF guard applies, but
  the trigger being an internet caller rather than a scheduled flow is worth
  naming.

## Alternatives considered

**Express it as a flow instead of a route kind** — `@webhook` source → `http`
sink → `@response`. This works *today* with no new code at all, and is the
right answer for anyone who wants transformation, retries or routing logic. It
is rejected as the primary answer because the ask is explicitly "as-is": making
a customer author, validate, version and publish a flow document to forward a
request unchanged is ceremony that the feature exists to remove. It stays the
recommended path the moment anything non-trivial is wanted, and `proxy` should
be understood as sugar that lowers to roughly this.

**Gateway-direct only, no runner-mediated kind.** Simpler to implement and
lower latency, and it throws away ADR-0038's central property for a saving of
half a millisecond against a network call. Rejected as the default.

**Runner-mediated only, no direct kind.** Cleanest posture, and it makes the
gateway useless to a customer who wants an API front door without adopting the
runner fleet — which is precisely the adoption path this feature opens.
Rejected as the sole option.

## Open questions

1. **Whether `proxy` needs a flow document at all**, or whether the route
   configuration is enough for the runner to act on directly. The latter is
   cleaner for the user; the former reuses the existing execution path,
   including capture and per-step timings, with no new machinery.
2. **Where the backend credential lives for `proxy_direct`.** Pushed with the
   configuration is the obvious answer and puts live secret material in the
   DMZ. Sealed delivery (ADR-0035 §1) would at least keep it out of any
   TLS-terminating intermediary.
3. **Response buffering.** The flow path currently buffers a bounded response
   on the runner; a proxy of a large download wants true streaming, which is
   the same gap noted for the request body.
4. **Whether proxy routes should be counted against the licensed core budget**
   (ADR-0033). They consume no engine capacity in the direct form and one
   runner slot in the mediated form.
