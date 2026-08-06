# ADR-0044: Mutual TLS on the runner→hub control plane

Date: 2026-08-06

Status: **Built.** Supersedes the runner-realm credential in
**ADR-0009** (single-use registration token → hashed bearer secret). The two
auth realms, the lease model, and the human/OIDC realm are unchanged.

## Context

ADR-0041 introduced a control-plane CA so a runner could prove *which* runner it
is to a gateway, and so a runner could verify the gateway in return. That closed
a real hole: labels used to be self-asserted, which was a privilege escalation.

It also left the system with **two credential types for one identity**. A runner
now holds a client certificate for gateways *and* a hashed bearer secret for the
hub. Aaron's question (2026-08-06): *any reason this isn't mTLS also?*

There is not a good one. The reason it is a bearer token is chronological — the
runner realm was built in M3b, and the CA did not exist until M6+.

**The bootstrap objection does not apply here**, which is worth stating because
it is exactly why the gateway's bundle is placed by hand. A gateway cannot dial
the hub, so it cannot request a certificate; a runner already dials the hub for
every lease. It can do a proper CSR.

## Decision

**Mutual TLS is the runner realm's credential**, issued from the ADR-0041
control-plane CA, with the bearer token retained only where TLS is terminated
before the hub.

### 1. Registration mints a certificate, not a secret

```
operator creates a single-use registration token   (unchanged)
        │
runner  POST /api/v1/runners/register  +  CSR
        │
hub     verifies the token, signs a client certificate whose SUBJECT is the
        runner id, records the runner, returns cert + CA
        │
runner  every subsequent call uses that certificate; the token is spent
```

The registration token does not disappear — it gets **shorter-lived and
single-use**, which it already is. What disappears is the long-lived shared
secret that follows it. That is the security win: a bearer credential sitting on
disk for the life of a runner is replayable by anything that reads it — a log
line, a proxy, a core dump — whereas a private key never leaves the TLS layer.

### 2. One identity, two relying parties

The runner presents the **same** certificate to a gateway's control listener and
to the hub. One identity, one renewal path, one revocation. A runner that is
decommissioned is revoked once and is refused everywhere.

This is the point of having a CA at all. Two certificates for one runner would
reintroduce, in PKI form, exactly the duplication this ADR removes.

### 3. The three PKIs never share a trust store

There are three distinct trust relationships in the system, and conflating any
two is a serious security bug:

| | Verifier | Trust root | Purpose |
|---|---|---|---|
| **Caller mTLS** | gateway PUBLIC listener | the customer's CA bundle | authenticate an external caller |
| **Control mTLS** | gateway control listener, hub | our control-plane CA | authenticate runners (and gateways to runners) |
| **Connector mTLS** | a connector, outbound | whatever the target system requires | reach a customer's SAP/API |

The rule: **the public listener and the control listener must never share a
trust store.** If the public listener trusted the control-plane CA, a runner
certificate would authenticate as a caller. If the control listener trusted a
customer CA, a customer certificate could park as a runner and receive real
inbound payloads — interception from one misconfigured line.

Caller certificates are terminated at the gateway and mapped to a principal via
the route's `AuthPrincipal` (ADR-0038 §4b), which exists precisely so that a
cert-authenticated caller is identified **without the runner touching any PKI**.
The runner is a client in the control plane and never a verifier of caller
certificates.

### 4. Bearer stays supported where TLS is terminated

Plenty of deployments front the hub with an ALB, nginx or an ingress controller
that terminates TLS. A client certificate does not survive that; a bearer token
does. Forwarding the verified identity in a header is possible but is a trust
decision about the proxy, and one we should not make on a customer's behalf.

So the hub accepts either, with mTLS preferred and bearer explicitly configured:
`SHIFT_HUB_RUNNER_AUTH=mtls|bearer|both` (default `both` during migration,
`mtls` once a deployment has cut over). A deployment that has cut over should
be able to refuse bearer entirely, because "we support both forever" is how the
weaker credential stays alive.

### 5. Account binding stays server-side

Today the runner's account comes from the runner record, looked up by the
hashed secret. Under mTLS it comes from the runner record looked up by the
certificate **subject**. In both cases the account is a fact the hub holds and
the runner cannot state — the same principle as ADR-0041 §3, where the runner
proves who it is and the hub says what it is.

## Consequences

**One credential type across the control plane.** Runner↔gateway and runner↔hub
authenticate identically, so revocation, renewal and audit are one mechanism
rather than two.

**The hashed-secret table goes away** once a deployment sets `mtls`, along with
`SHIFT_HUB_CRED_FILE`'s long-lived contents.

**Renewal becomes a shared concern.** A runner CAN dial the hub, so unlike the
gateway (ADR-0041 §4, where renewal must be pushed) a runner can renew itself
before expiry. That is strictly easier, but it must actually be built — a runner
whose certificate lapses stops leasing, and "the fleet went quiet overnight" is
a bad way to discover a renewal path was never written.

**Local development gets slightly heavier.** `make up` must mint a CA and a
runner certificate rather than echo a token. Acceptable, and the gateway bundle
already needs the same machinery.

## Alternatives considered

**Leave it as a bearer token.** Works, and is what ships today. Rejected because
it keeps two credential types for one identity, keeps a long-lived replayable
secret on disk, and leaves revocation split across two mechanisms.

**mTLS only, no bearer.** Cleaner, and wrong for deployments that terminate TLS
at a proxy — which is most enterprise Kubernetes. Refusing them would be
choosing purity over the deployment model this platform is meant to fit.

**A separate certificate per relying party** (one for gateways, one for the
hub). More isolation in theory; in practice two renewal paths, two revocation
lists and two ways to be half-configured, for an identity that is the same
runner either way.

## Resolved while building

1. **Renewal cadence and the failure mode.** Renew at half the **remaining**
   lifetime, floored at a minute — deriving it from what is left rather than
   from the original lifetime means a runner that has been asleep, or that has
   failed several attempts, converges on trying more often as the cliff
   approaches instead of waking on a schedule it has already missed.

   On repeated failure the runner **keeps working and keeps retrying**, and
   logs at ERROR inside the last hour. It does not refuse new work early. A
   renewal outage is already a problem; converting it into a fleet-wide work
   stoppage *before the certificates have actually expired* would make the
   platform's response to a hub hiccup worse than the hiccup.

   Every issuance uses a **fresh key**, including renewals: reusing one would
   mean a key compromised today stays useful for as long as the runner keeps
   renewing, which is exactly the property short lifetimes exist to remove.

2. **Revocation distribution.** Short lifetimes, no CRL. 24h by default, and
   `DELETE /runners/{id}` is the revocation: the certificate stays
   cryptographically valid and the **name it carries stops resolving**, so the
   next request it makes is refused and the certificate ages out on its own.
   There is nothing to publish, distribute, or fail to fetch.

3. **Sequencing.** Held: ADR-0042's status work landed first on the existing
   credential (#70), and this landed on its own branch (#71).

## Found while building

**Server trust is the system store plus the control-plane CA, not the control
CA alone.** The first implementation pinned the runner's `RootCAs` to the CA
that issued its client certificate, which would have failed every handshake
against a hub fronted by a public or corporate certificate — and the failure
would have looked like a runner problem. The control CA is *added* to the
system pool, and `-hub-ca` adds to it as well. `Identity.CA` — the control
plane's own root, used to verify gateways — is never widened by that flag,
because §3's rule is that the three PKIs do not share a trust store.
