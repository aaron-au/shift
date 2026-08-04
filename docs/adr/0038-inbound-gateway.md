# ADR-0038: The inbound gateway — a DMZ-deployable public front door

Date: 2026-08-04

Status: **Designed**

Supersedes the ingress half of **ADR-0016**: runners no longer host a publicly
reachable surface. The two-plane split, the payload/control separation, and the
hub↔runner lease model in ADR-0016 are unchanged and reaffirmed.

## Context

ADR-0016 put webhook ingress on the runner: an `@webhook` source, a hook server
on the runner, and "runner APIs are a public, authenticated surface". That was
the right call for getting triggers working; as a deployment model it is
backwards, and Aaron named why (2026-08-04): *runners can't / shouldn't have
publicly accessible components*.

**The runner is the highest-value target in the system.** It holds decrypted
secrets in memory (ADR-0010/0035), live payload, and connector subprocesses.
The hub holds ciphertext and metadata. Today's model exposes the valuable
component to the internet **N times over** — one endpoint per runner, each
needing DNS, a certificate and a firewall rule — while keeping the cheaper one
private. Scaling runners out means editing DNS or load-balancer config, which is
the opposite of "runners are disposable".

Two alternatives were considered and rejected before landing here.

**Runner ingress behind a customer-supplied load balancer.** Works, costs us
nothing, and is the Kubernetes-native answer — but it leaves every non-k8s
customer standing up nginx or HAProxy themselves, and it does not make runners
private, it just fronts them. It also cannot help a runner with no inbound
connectivity at all.

**The hub as the public front door.** Tempting, since the hub is already on the
critical path and a self-hosted hub is the customer's own machine anyway. Two
things kill it. First, it puts payload on the control plane, which is the one
line ADR-0016 exists to hold, and it does so permanently rather than for a
subset of deployments. Second, and more practically: the hub holds Postgres
credentials, KEK material, OIDC client secrets and the connector registry, so
it is categorically **not DMZ-approvable** under the network policy most
enterprises actually run. "Internet-facing hosts carry no credentials and no
database connections" is a rule we would be asking every customer to make an
exception to.

Aaron's proposal — a third application, purpose-built for inbound, deployable
in a DMZ — resolves both. It is also the shape the category already uses
(Boomi API Gateway, MuleSoft's gateway, Kong/Apigee as a product category), so
it is not a novel operational burden to explain.

## Decision

Introduce **`shift-gateway`** (binary `gatewayd`), a third, **optional**
application that is the sole publicly reachable component.

```
                    internet
                       │
                 ┌───────────┐
                 │  GATEWAY  │   TLS, authN, filtering, routing
                 └─────┬─────┘
        ─────────── DMZ ─┼──────────────
                         │
        ┌────────┐       │       ┌────────┐
        │  HUB   │◀──────┴──────▶│ RUNNER │
        └────────┘  control      └────────┘
                    + data hop
```

The gateway is **optional and absent by default**. A deployment with only
scheduled and polled flows never deploys it and carries **zero** inbound attack
surface — which is most deployments, and is the strongest argument for a
separate binary over a feature inside the hub, where every customer would carry
it forever.

### 1. What the gateway is

- **TLS termination** and the public listener. The only component with a
  public address.
- **Caller authentication**: per-webhook token (today's model, moved),
  mTLS, and HMAC signature verification for the shapes the common providers
  use (Stripe/GitHub-style `X-*-Signature` over the raw body).
- **Filtering**: IP/CIDR allowlists, required headers, body size caps,
  method and content-type constraints, and rate limiting — reusing the
  token-bucket already built for ADR-0021 rather than a second implementation.
- **Routing**: path → flow → an available runner, using configuration the hub
  supplies plus runner liveness and capacity.
- **A streaming proxy** to the chosen runner. Bounded memory, never persisted.

### 2. What the gateway is explicitly NOT

Scope discipline matters more here than anywhere else in the system, because
"the public-facing box" attracts features indefinitely.

- **Not a WAF.** Detection-style security — signature matching, anomaly
  scoring, bot management — is a multi-year product with a permanent
  maintenance and liability surface, and mature products already carry the
  insurance that goes with getting it wrong. Put Cloudflare, AWS WAF or
  ModSecurity in front. The gateway does **filtering and authentication**, not
  **detection**. (Aaron, 2026-08-04, agreed explicitly.)
- **Not a plugin host.** A plugin ABI in a new component is how the Go-`plugin`
  mistake (ADR-0001) repeats itself in a different costume. If extensibility is
  wanted later, the coherent shape is the one ADR-0001 already earmarks —
  **WASM request filters via wazero**, out-of-process by construction. Named as
  a deferred direction; not built.
- **Not an identity provider.** It authenticates callers against credentials
  the hub issued. It does not mint identities and is not a second auth system.
- **Not a queue.** No available runner means **503**, never buffer. A gateway
  that buffers is a gateway with durable state, and durable state in the DMZ is
  the thing this design exists to avoid.
- **Not a payload store.** It never writes payload to disk, database or log.
- **Not required.**

### 3. Why it is DMZ-safe

The property that makes a component DMZ-approvable is not "we hardened it" —
it is **what an attacker gets when it falls**:

| Component compromised | Attacker gains |
|---|---|
| Gateway | a proxy, its own hub credential, and traffic in flight |
| Hub | the control plane: DB, KEK, OIDC secrets, registry |
| Runner | plaintext secrets and live payload |

So the gateway holds, at rest: **its own hub credential, and an encrypted
last-known-good copy of the configuration that credential fetched.** No
database connection, no KEK, no user secrets, no durable payload, and — in
every mode except the `file` escape hatch — **no plaintext TLS private key on
disk** (§6). Everything else lives in memory and is re-fetched from the hub on
start.

### 4. Reaching the runner

Runners bind to a **private interface only, in every deployment**. That is the
security property this ADR exists to deliver, and it holds in all three
topologies:

| Topology | Data hop |
|---|---|
| Gateway in DMZ, runners internal | gateway → runner, one direction, specific hosts/port |
| Gateway and runners on one private network | gateway → runner directly |
| Runners with no inbound connectivity at all | runner long-polls the gateway; the gateway hands the request over |

DMZ→internal on a specific port to specific hosts is a normal, approvable
firewall rule in a way that internet→internal is not.

The third row reuses the pattern already proven in this codebase — runners
already long-poll `POST /lease` (ADR-0009) — rather than introducing
long-lived WebSocket-style connections, which is the failure mode ADR-0009 was
written to avoid. It also inherits a property for free: **the lease loop is
capacity-gated**, so ingress delivery respects admission (ADR-0005) without
needing its own backpressure mechanism.

**Gateway → runner is encrypted and mutually authenticated.** The end state is
mTLS with both sides holding hub-issued identities. The cheap correct start is
TLS plus the runner credential the hub already issues (ADR-0009), with mTLS as
the hardening step — recorded so it is a sequencing decision rather than an
omission.

### 5. Routing and placement reuse ADR-0030

The gateway picks a runner using **the placement model that already exists** —
runner groups and labels (ADR-0030): `prod` vs `non-prod`, region, capability.
A flow already declares its target group, and the scheduler already resolves
placement that way. The gateway consumes the same data rather than growing a
second targeting model, which would drift.

Within an eligible group, selection is by **liveness and capacity**, both of
which the hub already tracks (registration, heartbeat, capacity-gated lease
intake). Load balancing is therefore a query over data we already have, not a
new subsystem — which is what makes the gateway a genuine replacement for a
customer-supplied load balancer rather than an addition to one.

### 6. The hub is the single source of configuration

**Every piece of gateway configuration comes from the hub.** Domains, routes,
allowlists, header requirements, size caps, rate limits, trusted-proxy CIDRs,
TLS mode, and certificate material. An administrator prepares and updates it in
the hub; the gateway converges on it.

An earlier draft of this section made mounted local files the default for
purchased certificates, on the reasoning that keeping the private key off the
control plane is the strongest posture. Aaron rejected that (2026-08-04) —
*all* config should come from the hub — and on reflection the earlier position
was wrong on both counts:

- **It does not reduce blast radius.** The hub already holds the KEK, every
  user secret, database credentials and OIDC client secrets. A hub compromise
  is already total. Declining to put TLS keys in that store buys nothing while
  costing a whole parallel management path.
- **It is worse at rest, not better.** Hub-delivered material is held **in
  gateway memory**; a mounted file is on the DMZ host's disk. A stolen or
  imaged DMZ box yields a key in the file case and nothing in the hub case.
  That is the opposite of what the earlier draft claimed.
- **It costs the operational property that matters most.** A DMZ host is
  exactly the machine an ops team least wants to log into. Hub-delivered config
  means the gateway needs no local administrative access at all — rotation,
  route changes and allowlist edits are one action in one place, and they
  work identically for one gateway or five.

So configuration is hub-owned, and the four TLS arrangements become **modes
selected by that configuration** rather than four different management stories:

| Mode | Key material | For |
|---|---|---|
| `acme` | Gateway obtains and renews; hub supplies account, directory, DNS-01 credentials as `{"$secret":…}` | Self-managed public certs |
| `provided` | Hub delivers the bundle, encrypted; gateway holds it in memory | Purchased / enterprise-PKI certs |
| `upstream-tls` | None — the gateway serves plain HTTP behind an F5/ALB/Cloudflare | DMZs that already terminate TLS |
| `file` | Mounted path, hot-reloaded | Air-gapped or bootstrap only; **not** the default |

ACME stays gateway-executed because the gateway *is* the endpoint an HTTP-01 or
TLS-ALPN-01 challenge is served from — routing that through the hub would be
backwards. But its *configuration* is still hub-owned, so ACME is not an
exception to the rule; only the challenge response is local.

`upstream-tls` carries one non-negotiable: `X-Forwarded-For` and
`X-Forwarded-Proto` are honoured **only from the hub-configured trusted-proxy
CIDRs**, never from an arbitrary caller. A spoofable forwarded header would
defeat the IP allowlist in §1, so this is a correctness constraint on the mode,
not hardening to add later.

`file` survives only as an escape hatch for a deployment that genuinely cannot
reach a hub at start-up. It is documented as such rather than offered as a
peer.

### 6a. How configuration reaches the gateway

Three constraints shape this, and they are in tension.

**Bootstrap.** The gateway cannot fetch its hub address and its own identity
from the hub. Minimum local state is therefore a **hub URL plus a single-use
registration token**, which mints a hashed bearer credential — precisely the
runner registration flow (ADR-0009), reused rather than reinvented. That is the
*only* configuration a gateway is deployed with.

**Pull, not push.** Aaron described the hub sending config to the gateway; the
administrator experience is identical either way, and pull is the right
mechanism here. The established hub↔spoke wire is HTTP/JSON long-poll
(ADR-0009), deliberately not WebSockets and deliberately not the hub dialling
out. Pull also means the hub never needs to know a gateway's address or hold
open a connection into the DMZ — the connection direction stays outbound from
the DMZ, which is what a firewall review will expect to see.

**Restart availability.** Config held only in memory means a gateway that
restarts while the hub is unreachable cannot serve — the public front door
would inherit the control plane's availability. That is a real regression and
the mitigation is deliberate: the gateway persists a **last-known-good config,
encrypted**, unwrapped by a key it already holds locally (the same shape as the
runner's `SHIFT_HUB_CRED_FILE`). Honestly stated, that is one small piece of
local key material securing everything else — but it is one small piece rather
than a full parallel config path, and it converts a hard dependency into a
graceful one.

**The hub validates before it distributes.** A certificate that does not parse,
has expired, or does not match its declared domain; a malformed CIDR; a route
naming a flow that does not exist — all are rejected at configuration time, not
discovered when the public front door fails to start. This is the same posture
`pkg/flowdoc` already takes as the validation authority, and it matters more
here because the blast radius of a bad config is "the internet cannot reach
us".

### 7. Interaction with replay (ADR-0037)

The gateway does **not** solve where replay data lives, and must not be made
to. Giving a DMZ component write credentials for the customer's archive would
undo §3 in one step. The archive stays as ADR-0037 landed it: a destination the
customer already owns, written **by the runner**.

But this topology does surface a real wrinkle. For **ephemeral inbound**
payload the gateway keeps nothing, so if the runner dies mid-execution the body
is gone and there is nothing to replay. Reliable replay of webhook input
therefore requires the runner to **write the body to the archive before
processing it** — a write-ahead, and a round trip before execution begins.

That is acceptable for async webhooks and materially expensive for synchronous
request-reply. So replay-of-inbound is **opt-in per flow**, for a concrete
measured reason rather than a vague one, and a flow that does not opt in simply
cannot replay its inbound payload — which the API reports honestly rather than
silently degrading.

### 8. "Disposable" is a correctness contract, not a frequency prediction

Aaron, 2026-08-04, on an unrealism running through several of these designs:
runners are *designed* to be disposable, but in practice a healthy runner runs
for **months** between restarts.

Both are true and they answer different questions. "Nothing may be lost when a
runner dies" stays absolute — it is what makes at-least-once dispatch, lease
expiry and re-dispatch correct. But designing as though vanishing were *routine*
over-weights a rare event, and that shows up as real cost:

- **In-memory state is worth more than a strict reading suggests.** The
  opt-in secret cache (ADR-0035 §2) and the warm connector pool pay back over
  months, not seconds. Connector cold start (~6 ms) is a non-issue at that
  lifetime, which is why it was correctly left unfixed.
- **Resume's value shifts.** Less about routine crash recovery, more about the
  six-hour ETL where a *single* restart in months still costs six hours.
- **It argues against write-ahead as a default** (§7). Paying a round trip
  before every inbound execution to protect against something that happens a
  few times a year is a bad default, and reinforces opt-in.

Recorded here because the assumption was doing silent work in several
decisions, and a reader should see it stated rather than infer it.

## Doctrine held

- **Payload never touches the hub (ADR-0016).** Restored, not weakened. The
  earlier proposal to make the hub the front door would have broken this
  permanently; a dedicated data-plane component means the control plane stays
  out of the data path entirely.
- **Hub stays lightweight.** Inbound volume and control volume are unrelated
  and now scale independently. The hub gains gateway registration and config
  distribution — rows, not bytes.
- **Runners stay disposable (ADR-0002).** Scaling runners no longer touches
  DNS or load-balancer configuration; the gateway discovers capacity through
  the hub.
- **No WebSockets in the control plane (ADR-0009).** The no-inbound topology
  uses the existing long-poll pattern, not a persistent socket.
- **Resource-governed admission (ADR-0005).** Ingress delivery is
  capacity-gated by construction; overload is a 503, never a silent buffer.
- **Encrypted by default (ADR-0010).** Gateway→runner is TLS with mutual
  authentication; certificates and DNS credentials use the existing envelope
  machinery.

## Consequences

- **Runners become private in every deployment.** The single biggest security
  improvement available to this architecture, and it removes the per-runner
  DNS/TLS/firewall burden that made runner scale-out operationally awkward.
- **A third binary** to version, document, release and bundle. Real cost,
  bounded by being optional — and for a customer without Kubernetes it
  *replaces* standing up nginx/HAProxy rather than adding to it.
- **ADR-0016's ingress half is superseded.** `@webhook` as a flow source is
  unchanged; what changes is that the runner's hook surface binds privately and
  trusts only the gateway. The runner's own control surface (dashboard, direct
  execution) was already intended to be internal and now genuinely is.
- **One authentication surface instead of two.** The gateway authenticates the
  caller; the runner authenticates the gateway. Webhook token handling moves
  out of the runner.
- **Latency cost is expected to be small and must be measured.** The trigger
  path's fixed cost is ~200µs today (`docs/dev/04-runner.md`), and a streaming
  proxy on a private network should add a few hundred µs plus sub-millisecond
  RTT — well under 1ms on a LAN, not the tens of milliseconds a cross-region
  hop would cost. This is an estimate from existing measurements, not a
  measurement; a gateway benchmark is part of the build's definition of done,
  because it decides whether synchronous request-reply survives the extra hop.

## Open questions

- **Gateway HA.** Hub-owned configuration (§6) answers the shared-certificate
  half — every replica converges on the same bundle. What remains open is
  whether per-webhook rate limits are per-replica or coordinated. Per-replica
  is cheap and approximately right; coordinated needs shared state the DMZ
  should not hold.
- **Long-poll ingress latency under load.** In the no-inbound topology, a
  request arriving when no runner is currently polling waits for the next poll.
  Bounded by poll interval; whether that is acceptable for synchronous flows
  needs measuring before that topology is offered for request-reply.
- **Provider signature verification scope.** HMAC verification needs the raw
  body, which constrains how early the gateway may stream. Which providers ship
  in v1 should follow demand rather than a guessed list.
- **Does the gateway serve the runner dashboard?** Convenient (one entry point
  for operators) but it is a control surface, not ingress, and mixing them
  weakens the "gateway holds no control-plane authority" property. Leaning no.
- **Egress.** This ADR is inbound only. Whether outbound calls from private
  runners should route through an egress counterpart (fixed source IPs for
  customer allowlisting is a common enterprise requirement) is a separate
  decision.
