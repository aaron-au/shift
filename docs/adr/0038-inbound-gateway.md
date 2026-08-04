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
- **Routing**: path → flow → an available runner. Eligibility comes from
  hub-pushed configuration; availability is whichever eligible runner is
  currently holding a poll (§4).
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

So the gateway holds, at rest: **an identity bundle — its own certificate and
key, and the CA that lets it verify the hub. Nothing else.** No database
connection, no KEK, no user secrets, no durable payload, no routing table, and
— in every mode except the `file` escape hatch — no plaintext TLS private key
for a served domain (§6). Everything operational arrives pushed and lives only
in memory.

It is also, per §4, holding no credential that would let it *reach* anything
internal. An attacker who takes the gateway gets a proxy and traffic in flight,
and no path onward.

### 4. Direction: nothing in the DMZ ever initiates inward

**The gateway never opens a connection into the internal network. It only
answers.** Every connection crossing the DMZ boundary is initiated from the
internal side (Aaron, 2026-08-04).

| Connection | Direction | Mechanism |
|---|---|---|
| Hub → gateway (configuration) | internal → DMZ | HTTP POST on change + periodic reconcile |
| Runner → gateway (work delivery) | internal → DMZ | long-poll, the ADR-0009 pattern |
| Runner ↔ hub | internal → internal | unchanged (pull/lease) |
| Gateway → anywhere internal | **none** | — |

This is the security thesis of the whole component, and it is worth stating as
a property rather than a configuration detail: **a compromised gateway cannot
reach the internal network at all.** It holds no credential that opens an
inbound path, because it has none to hold. Contrast the arrangement an earlier
draft specified — gateway dialling the hub for config and dialling runners for
work — which needed two DMZ→internal firewall rules and made the DMZ box a
usable pivot.

Runner ↔ hub staying pull is deliberate: it is entirely internal, so the
direction buys nothing there, and changing it would churn the proven lease
model for no security gain.

Neither push is a persistent connection. Configuration is a plain POST when it
changes plus a periodic reconcile, so there is no long-lived socket to leak,
mutate under lock, or reconnect — the failure mode ADR-0009 was written to
avoid. Work delivery is the long-poll already proven in this codebase.

**mTLS in both directions.** Hub, runners and gateways each hold an identity,
so "who is the hub" and "who is a legitimate runner" are cryptographic
questions rather than network-position assumptions — which matters precisely
because a DMZ host cannot rely on being on a trusted network.

#### The property this buys for free: availability is who is listening

Because runners long-poll the gateway, **the set of runners currently polling
IS the set of available runners.** The gateway does not maintain a liveness
table, does not poll the hub for capacity, and cannot act on stale data: a
runner that has died is a runner that is no longer holding a poll.

And because the runner lease loop is already **capacity-gated** (ADR-0005), a
runner only polls when it can actually accept work. Admission control and load
balancing therefore fall out of the same mechanism, with no scheduler, no
health-check loop, and no round trip to the hub on the request path.

This is why the arrangement is not merely a firewall concession. It is
structurally simpler than the one it replaces.

### 5. Routing and placement reuse ADR-0030

*Which* runners may serve a given path still comes from the placement model
that already exists — runner groups and labels (ADR-0030): `prod` vs
`non-prod`, region, capability. A flow declares its target group and the
scheduler already resolves placement that way; the gateway consumes the same
data rather than growing a second targeting model that would drift.

The division is clean: **the hub decides eligibility, the poll set decides
availability.** Group membership arrives with the pushed configuration (§6a);
which eligible runner gets the next request is whichever is holding a poll. No
capacity query on the request path.

That is what makes the gateway a genuine replacement for a customer-supplied
load balancer rather than an addition to one — and it is a load balancer that
cannot route to a dead backend by construction.

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

Because the gateway never initiates inward (§4), the hub **pushes**
configuration to it: an HTTP POST when it changes, plus a periodic reconcile so
a missed push self-heals. Stateless, no long-lived socket.

**Bootstrap inverts.** A gateway that cannot dial the hub cannot register with
it either, so the runner flow (single-use token → bearer credential) does not
transfer. Instead the admin **creates the gateway record in the hub** — name,
address, group eligibility — and deploys the gateway with a small **identity
bundle**: its own certificate and key, plus the CA that lets it verify the hub.
The hub then dials it.

There is no registration exchange. The gateway presents its certificate during
the mTLS handshake and *that* is the identity assertion — the bundle IS the
registration, and placing it is the one manual step. Note also what the gateway
does **not** need: the hub's address. It never dials the hub, so it needs only
the CA and expected identity to *verify* the hub when the hub dials in.

Key custody has two shapes and both are offered: the hub generates the keypair
and the admin downloads the bundle (fewer steps), or `gatewayd bootstrap`
generates the key locally and emits a CSR the admin pastes into the hub (the
private key never leaves the host). The first is the default; the second exists
for deployments that require it.

That bundle is genuinely local material, and it is worth being straight about
why it is acceptable where the config cache was not. It is **small, static and
identity-only** — it authenticates the gateway and authenticates the hub to it,
and it grants nothing else. It does not contain routes, TLS certificates for
served domains, allowlists or any customer data, all of which arrive pushed and
live only in memory. And unlike the config cache, it does not exist to make the
gateway *keep working* while the hub is away; it exists so the two can identify
each other at all. It is the same class of artefact as a Kubernetes service
account token.

**Restart needs no cache, and that is the right answer.** An earlier draft had
the gateway persist an encrypted last-known-good configuration so it could
serve through a hub outage. Aaron pushed back that bootstrap already covers
restart, and following the failure through shows the cache was not merely
unnecessary but harmful:

> Request arrives → gateway routes it from cached config → runner receives it →
> **the runner needs the hub to resolve secrets and connections for that
> execution** (ADR-0035 §3) → the task fails.

So the cache does not buy availability. It converts a clean **503 at the front
door** into a **failed execution one hop later**, which is worse: a 503 tells
the sender to retry, while a failed execution looks like genuine processing
failure and may consume the attempt. **A 503 while the hub is unreachable is
correct behaviour, not a degradation to paper over**, and dropping the cache
removes the last piece of non-identity material from the DMZ.

A gateway that restarts holds no configuration and serves 503 until the hub's
next push. Its health endpoint reports "unconfigured" so the hub reconciles
immediately rather than waiting out the interval.

**A copied identity bundle is inert** — better than the "fails loudly"
rejection an earlier draft claimed, which under push cannot happen: the hub
dials one address for one record, so it never sees a second gateway at all.

It does not need to. Configuration flows only to the address in the record, and
a stolen bundle cannot ask for configuration because it cannot dial the hub. An
attacker holding one gets a gateway nobody ever configures — no routes, no
served-domain keys, no public listener.

The bundle is still worth protecting, but the threat is narrower and sharper
than a leaked registration token: a stolen bundle **plus** the ability to
intercept or redirect the hub's connection to that recorded address. That is
the ordinary mTLS threat model, and it is a much smaller target than "anyone
holding this token can register".

**Identity is two-tiered, and after enrollment it is ordinary configuration.**
Aaron's framing (2026-08-04): once the bundle is placed, why should the
identity not be hub-managed like everything else — the way an OAuth offline
refresh token mints access tokens? It should, and the analogy maps directly:

| | OAuth | Gateway |
|---|---|---|
| Obtained once, out of band | refresh token | **enrollment identity** (the manually placed bundle) |
| Short-lived, auto-rotated | access token | **operational certificate** (hub-pushed) |
| Renewal | client presents the refresh token | hub re-enrolls the gateway |

So the manual step happens exactly once. Thereafter the operational certificate
rotates on a schedule, pushed over the existing channel, and no human touches
the gateway again.

**One asymmetry inverts a usual intuition.** OAuth refresh is a *pull* — a
client that has been offline simply refreshes when it returns. The gateway
cannot pull, so renewal is push-only, which means **shorter operational
certificates are worse for availability here**: if the hub is unreachable past
expiry, the handshake fails, the gateway cannot request anything (requesting
means dialling), and it is stranded.

The fix is the direct analogue of the refresh exchange. **The hub accepts an
expired operational certificate for re-enrollment only** — never for serving —
provided the key matches and the gateway record is still valid and unrevoked.
Same identity, same key, provably issued by us, merely stale. The hub then
pushes a fresh operational certificate.

That converts stranding from "manual bundle replacement" into "self-heals after
an outage of any length", and revocation is unaffected because it is hub-side
record state rather than something the certificate carries. Renewal at ~50% of
lifetime (the kubelet/Vault pattern) becomes a hygiene target rather than a
hard deadline: missing it costs a re-enrollment round trip, not a site visit.

**One manual step is the floor.** The first trust anchor always requires an
out-of-band act; the only alternative is trust-on-first-use, which is not a
trade worth making for the one component sitting in the DMZ.

**An unconfigured gateway must be visible from the hub.** A passive component
that is never dialled — wrong address in the record, a missing firewall rule —
waits silently forever. The gateway logs `unconfigured, awaiting hub` on an
interval and reports it on health, but the diagnostic that matters is on the
**hub**: `last contact: never` against the gateway record, where the
administrator actually is.

**Local configuration is permitted, and the line is exact.** A gateway may
carry a small local file (ini or equivalent) for **facts about the host**:
public and control listen addresses, the bundle path, log level, timeouts,
resource limits. The machine knows its own addresses and the hub should not
have to.

What must never appear there is **policy about what we serve** — routes,
allowlists, rate limits, header rules, TLS mode, served-domain certificates,
group eligibility. The moment one of those lands in a local file it is the
config cache returning by another name, with two sources of truth and a failure
mode of serving stale policy instead of a clean 503. The line is worth stating
because it is the one that will erode quietly.

**The hub validates before it distributes.** A certificate that does not parse,
has expired, or does not match its declared domain; a malformed CIDR; a route
naming a flow that does not exist — all rejected at configuration time, not
discovered when the public front door fails to start. Same posture
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
- **No WebSockets in the control plane (ADR-0009).** Neither direction uses a
  persistent socket: configuration is a POST on change plus a reconcile, and
  work delivery is the long-poll already proven here.
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
- **Zero DMZ→internal firewall rules.** Every boundary-crossing connection is
  initiated from the internal side (§4), so a compromised gateway has no path
  onward — and a firewall review sees only outbound-to-DMZ rules, which is the
  easy approval. It also removes the liveness-tracking the gateway would
  otherwise need: the runners holding a poll are the available runners, and
  they poll only when capacity-gated admission lets them.
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
