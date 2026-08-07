# ADR-0049: Gateway adoption — a fingerprint, pasted once

Date: 2026-08-06

Status: **Designed, build pending.** Amends ADR-0038 §6a (how configuration
reaches the gateway) and resolves the bootstrap inversion recorded in
`docs/dev/09-gateway.md` § "Not built yet". Uses the PKI vocabulary of
ADR-0044.

## Context

ADR-0038 §4 is absolute: nothing in the DMZ ever initiates inward. The gateway
holds no hub credential, no database connection and no KEK, and the hub pushes
configuration to it. That property is the whole reason the gateway is safe to
put in a DMZ.

It also breaks the obvious bootstrap. Every other component in SHIFT registers
by dialling the hub — a runner presents a single-use registration token and
gets an identity back (ADR-0009, ADR-0044). A gateway cannot do that without
becoming a thing in the DMZ that holds a hub credential and initiates inward,
which is precisely what §4 forbids. So the current build ships a shared secret
on the control listener and a `-config` file loaded from disk, both marked
temporary.

Aaron, 2026-08-05, described the shape the real answer should take:

> gateway is deployed into the DMZ in a 'ready to adopt' state. Hub then comes
> along, starts the auth chain etc.. gets it up and running then deploys
> configuration

> So it would 'flip' the installation from admin downloads file(s) from hub and
> gives them to gateway to gateway generates a fingerprint, hub is given the URL
> and fingerprint to do the initial connection then configuration/mTLS etc..
> Assume the fingerprint is unguessable and anything connecting without it is
> just dropped traffic?

That inverts the credential flow to match the connection flow. Nothing secret
travels *to* the DMZ before trust exists; what travels *from* it is a public
value that is useless to anyone who intercepts it.

## Decision

### 1. A gateway starts unadopted and self-identifying

On first start with no persisted state, the gateway generates a long-lived key
pair and a self-signed certificate over it. It computes the SHA-256 of the
public key — the **fingerprint** — and does two things with it: logs it
(`event: gateway.awaiting_adoption`, ADR-0046) and serves it from an
unauthenticated bootstrap endpoint on the control listener.

The fingerprint is public by construction. Publishing it grants nothing: it
identifies a key, it does not authorise use of one.

In this state the gateway serves no ingress. It has no routes, no domains and
no certificates, because it has no configuration — and configuration only comes
from the hub (ADR-0038 §6). An unadopted gateway is inert.

### 1a. Adoption is a pairing on a one-time install token

An earlier draft of this ADR secured only one direction. A key fingerprint
proves the GATEWAY to the hub; nothing proved the HUB to the gateway, and an
unadopted gateway cannot tell the real hub from anyone else who can open a
socket to its control port. Adoption was a race won by whoever dialled first,
and the winner installed its own CA and pushed the gateway whatever routes it
liked.

The first fix was a second value — a code the gateway generated and the
operator carried back. Aaron rejected the ergonomics of that (2026-08-06):

> Couldn't simply providing the hub fingerprint as an envVar cover that instead
> of double handling and accessing the storage/shell back and forth?

He was right, and the better shape is a **pairing**. The hub mints a one-time
**install token** when the gateway record is created; the operator supplies it
at deploy time as `SHIFT_GATEWAY_INSTALL_TOKEN` (or `..._FILE`). One secret,
travelling one direction, at the moment a deployment is already being written.

The hub cannot pin a key that does not exist yet — the gateway generates it on
first start — so it does not try. It **learns** the fingerprint on the first
dial and pins it from then on. The token is what makes learning it safe.

**Both proofs are HMACs bound to the fingerprint of the certificate on the
wire**, not bare comparisons of the token, and that is the part that matters:

| Step | Proof |
|---|---|
| hub → gateway | `HMAC(T, "shift-gw-hub-hello" ‖ N ‖ F)` |
| gateway → hub | `HMAC(T, "shift-gw-gw-hello" ‖ N ‖ F ‖ sha256(csr))` |
| hub → gateway, install | `HMAC(T, "shift-gw-install" ‖ N ‖ F ‖ sha256(material))` |

`N` is a hub nonce; `F` is the fingerprint each side observes on *its* leg.

A machine-in-the-middle terminates TLS with its own key, so the hub computes
its proof over the interceptor's fingerprint and the gateway's check fails —
and the gateway's proof is over its own key, which the interceptor cannot
reproduce, so the hub's check fails too. Both sides detect it. A bare token
would simply be relayed and copied. The install proof additionally commits to a
digest of the delivered material, so a captured proof cannot be replayed with a
substituted CA.

The token is **burned at both ends** on success: the gateway clears it from
memory (it was never written to disk), and the hub clears the column in the
same statement that records the learned fingerprint, so it cannot be spent
twice. From then on the pinned key does everything the token did.

The construction is duplicated in `gateway/internal/adopt` and
`hub/internal/gwpush`, for the reason ADR-0046 §2 gives for duplicating the
logging setup: `gateway/go.mod` has zero dependencies, an auditable security
property of the one component that may sit in a DMZ. Duplicated crypto that
silently disagreed would be far worse than the dependency, so paired tests pin
both implementations to one fixed vector — a mismatch fails the build rather
than quietly making every gateway unadoptable.

**What this gives up:** a human no longer eyeballs the specific box. Whoever
holds the token becomes the gateway. That is the same trust model as runner
registration (ADR-0009), which the platform already accepts, and it buys an
install with no shell access to the DMZ host and nothing carried back.

### 2. Adoption is the hub dialling a pinned key

An administrator gives the hub three values: the gateway's URL, its
fingerprint, and its adoption code (§1a). The hub then dials the gateway, and validates the presented
certificate **by fingerprint alone** — no CA, no name check.

This is trust-on-first-use made safe by a secret that predates the connection,
which is the standard pairing shape. The fingerprint the hub learns is 256 bits
and is pinned for every later dial, so the window in which anything is
unverified is exactly one exchange, and even that exchange is authenticated
both ways.

Over that pinned connection, in one exchange:

1. The hub issues the gateway an identity certificate from the **gateway CA** —
   a third PKI, sharing no trust store with caller mTLS or runner mTLS
   (ADR-0044 §3).
2. The hub delivers the gateway CA bundle and its own control-plane identity,
   so the gateway can verify the hub on every subsequent push.
3. The hub delivers the initial configuration: domains, routes, allowlists,
   TLS mode and material.

Adoption then **closes**. The bootstrap endpoint stops answering, and the
control listener requires mutual TLS from that moment on. A second adoption
attempt against an adopted gateway is refused, not overwritten — one gateway,
one owner.

### 3. Adoption survives restart, because it is persisted

The gateway writes its key, its issued identity, the gateway CA, the hub's
identity and its adoption state to a state directory. On restart it reads them
and comes back **adopted**: it verifies the hub, accepts a config push, and
serves. No bootstrap window reopens, and no fingerprint is reprinted.

That is the answer to the question Aaron raised — a restart returns to the
adopted state; only a *lost state directory* is a new gateway.

Configuration itself is deliberately **not** persisted. It is held in memory,
as ADR-0038 §6 requires, so a stolen or imaged DMZ host yields no TLS private
key for a customer domain and no route table. On restart the gateway has an
identity but no config, and asks nothing — it waits, inert, for the hub's
reconcile push. The identity is what persists; the payload of configuration is
not.

### 4. Losing state means re-adoption, and that is correct

A gateway redeployed without its state directory comes up unadopted with a
**new** fingerprint. The hub, holding a record that names the old key, refuses
it. Nothing is silently re-trusted.

Recovery is an explicit, audited hub action — *rotate adoption* — which issues a
**fresh install token** for an existing gateway record, clears the pinned
fingerprint, and preserves the routes, domains and certificates. The friction is
one redeploy by an administrator who already knows they lost the box. Making it automatic would mean anything
that can reach the URL and present a fresh key inherits a live gateway's
configuration.

### 5. Version updates are full replacements, and do not re-adopt

Aaron, 2026-08-05:

> I think I prefer full version updates/replacements rather than dynamic
> patches its the testing (especially vs customer data) that scares me but as
> long as we have rollback plans I'm satisfied.

A gateway upgrade is a new image over the same state directory. The identity is
unchanged, so the new process comes up adopted and the hub reconciles config
into it. Rollback is the previous image over the same state directory, and is
symmetric.

That imposes one rule on the state format: it is **versioned and
additive-only** within a major. A newer gateway must never write state an older
one cannot read, or rollback — the thing that makes full replacement safe —
stops working at the worst possible moment. A conformance test pins this.

### 6. The pinned key is the permanent way back in

The dev-doc note said an expired identity strands a gateway permanently,
because requesting a new one would mean dialling. Pinning the gateway's
long-lived key at adoption removes that failure entirely.

The identity certificate is short-lived and renewed by push, well before expiry.
If a renewal is missed and the identity lapses, the gateway falls back to
serving its **anchor** certificate — it must, because a gateway still presenting
a dead identity could never be reached again — and the hub dials the fingerprint
it pinned at adoption and re-issues.

Neither side needs the install token for that, which is why it can be burned:
the hub authenticates the gateway by the pinned key, and the gateway
authenticates the hub by the client certificate it verifies against the gateway
CA it kept from adoption. The long-lived key is the anchor; the identity is a
lease against it.

So the failure mode degrades from *permanently stranded, requires a human in
the DMZ* to *self-healing on the next reconcile*. That, rather than convenience,
is why the fingerprint is retained after adoption instead of discarded.

### 7. Where the control listener has to be reachable from

Adoption is not a special path. It uses the same control listener as every
later push, so whatever route reaches it for adoption is the route the
reconcile loop uses every thirty seconds. The decision is made once, not twice.

Two ports, two very different exposures:

| Port | Who reaches it | Termination |
|---|---|---|
| Ingress | the public | may be terminated upstream (ADR-0038 §6 `upstream-tls`) |
| Control | the hub, and runners polling for work | **never terminated by anything in between** |

The control listener must be end-to-end TLS, because the trust *is* the key
pin. A proxy that terminates it means the hub pinned the PROXY's key and has
adopted the proxy; everything past it is unauthenticated. Worse, the channel
carries TLS private keys for customer domains, so a terminating appliance reads
them. Where an appliance genuinely sits in the path, the rule for that one port
is L4/TCP or SNI passthrough — not "disable inspection everywhere", just one
port forwarded rather than opened.

Where the hub lives changes the network conversation:

- **On-prem hub** — hub → gateway is internal → DMZ, ordinary management
  traffic. Nothing about the control listener is public.
- **Cloud hub** — the hub is on the internet, so the control listener must be
  internet-reachable too. ADR-0038's claim that the gateway is the sole
  publicly reachable component still holds, but it is now **two ports public,
  not one**, and that belongs on the network diagram rather than being
  discovered.

  The exposure is small and should be kept that way: the control listener
  accepts mutual TLS only, from a CA the hub controls, pinned to one identity;
  while unadopted it serves nothing but a public fingerprint; and it should be
  source-restricted to the hub's egress addresses.

## Consequences

- The hub gains gateway records (URL, install token, learned fingerprint,
  identity serial, adoption state, config generation), an adopt action, a rotate-adoption action, and the
  push/reconcile loop. The gateway CA is a third signer alongside the runner CA
  from ADR-0044.
- The gateway gains a state directory, a bootstrap endpoint that closes, and
  mutual TLS on the control listener. The shared-secret control listener is
  superseded and removed once this lands — including its fail-closed
  non-loopback guard, which exists only to make the temporary state safe.
- `gateway/go.mod` stays dependency-free. Everything here is
  `crypto/x509`, `crypto/tls` and `net/http`.
- Nothing in the DMZ initiates inward at any point, including during bootstrap.
  The property ADR-0038 §4 asserts now holds through the whole lifecycle rather
  than from the second boot onwards.
- **Open:** whether the adopt action should be scriptable for infrastructure-as-
  code deployments, where an administrator pasting a fingerprint is a manual
  step in an automated pipeline. A hub-issued pre-authorisation — "the next
  gateway to present itself at this URL with any key is gateway `X`" — is the
  obvious shape and is strictly weaker, because it trusts the URL rather than
  the key. It should be a deliberate opt-in if it exists at all.
