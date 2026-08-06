# ADR-0045: Custom CA trust is configuration, not machine state

Date: 2026-08-06

Status: **Designed, build pending.** Extends ADR-0034 (connections) and
ADR-0010 (secrets). Third leg of the PKI story in ADR-0044 §3: the
**connector mTLS** column, where a connector reaches a customer's system.

## Context

Aaron, 2026-08-06:

> One thing I notice with Boomi is CACerts needing to be added to the java
> keystore which means an admin has to login to the backend, run java's
> keytool, import the certificate, restart the JVM … it's a mess.

It is, and it is worth naming exactly *why*, because the fix follows from the
diagnosis rather than from "have a nicer UI for it".

The Java model makes trust **machine state**: a mutable file on a host, edited
out of band by whoever has shell access, consumed at JVM start. Four
consequences, each independently bad:

1. **It needs a human with backend access.** The person who knows the
   integration needs the person who can SSH. Two people, a ticket, a window.
2. **It needs a restart**, so adding trust for one endpoint interrupts every
   integration on that box.
3. **It is global.** A CA imported for one customer's SAP is now trusted for
   *every* outbound connection that runtime makes. The blast radius of a bad
   import is the whole estate.
4. **It is invisible.** The keystore is not in source control, not in the
   platform's own model, and nothing reconciles it. A replacement host is
   missing trust nobody remembers adding.

Then the fifth, which is the one that actually bites: **because adding a CA is
painful, people disable verification instead.** That is not hypothetical for
us — `connectors/internal/ftpconn` has an `insecure_tls` option today, and the
honest reading is that it exists because the alternative was unavailable. A
platform that makes the safe path hard has chosen the unsafe path on its users'
behalf.

**Our architecture forecloses the Java model anyway.** Runners are stateless
and disposable (ADR-0002); anything installed into a runner's filesystem is
gone at the next restart, and there is no "the backend" to log into. So the
question is not whether to copy the keystore — it is what replaces it.

## Decision

**Trust is configuration data, scoped to a connection, delivered with the task,
and never installed anywhere.**

### 1. Trust material lives on the connection

A connection (ADR-0034) already carries what it takes to reach a system — host,
credentials by reference, options. TLS trust is more of the same:

```json
{
  "name": "customer-a-sap",
  "connector": "http",
  "config": {
    "baseUrl": "https://sap.customer-a.internal",
    "tls": {
      "caPem":   "-----BEGIN CERTIFICATE-----\n…",
      "certPem": {"$secret": "customer-a-client-cert"},
      "keyPem":  {"$secret": "customer-a-client-key"},
      "pinSha256": ["9f86d081…"],
      "serverName": "sap.customer-a.internal"
    }
  }
}
```

- `caPem` — extra roots for this connection. A CA certificate is public
  material, so it is ordinary config, not a secret.
- `certPem` / `keyPem` — outbound client certificates. The **key is a secret
  reference**: envelope-encrypted, resolved runner-side at task time, never at
  rest at the hub and never in a log (ADR-0010).
- `pinSha256` — pin a leaf or intermediate by fingerprint. This is the answer
  for a self-signed endpoint whose owner will not issue you a CA, and it is
  strictly better than what people do today, which is turn verification off.
- `serverName` — SNI/verification name, for the very common case of an IP
  address or an internal alias that does not match the certificate.

### 2. Scoped to the connection, never global

This is the substantive improvement over a keystore, not just an ergonomic one.
A CA added for `customer-a-sap` authenticates **that connection and nothing
else**. Trust cannot leak sideways into an unrelated integration, and the blast
radius of a wrong or malicious CA is one connection instead of an estate.

The system trust store is still the baseline — an ordinary public certificate
needs no configuration at all — and `caPem` is *added* to it rather than
replacing it. Replacing it is a separate, explicit choice (`"onlyCaPem": true`)
for the deployment that genuinely wants to talk to one internal CA and nothing
else.

### 3. No restart, ever

The `tls.Config` is built when the connection is bound for a task, from data
the runner just received. Changing a CA is an edit and a redeploy of the flow's
next run — there is nothing cached in a process to invalidate, because trust
was never process state. This falls out of the architecture rather than being a
feature we built.

### 4. One implementation, in the SDK

`sdk/tlsconf` owns the field names, the parsing, and the semantics, so every
connector gets identical behaviour instead of each inventing its own spelling.
The connector asks for a `*tls.Config` and gets one; the schema fragment is
merged into each action's config schema so the studio renders the same form
everywhere (ADR-0018).

Uniformity here is a security property, not tidiness: `insecure_tls` on one
connector and `skipVerify` on another means a review has to check both, and one
of them will be missed.

### 5. `insecure` becomes loud, narrow, and deployment-gated

Verification-off does not disappear — there are genuinely broken test systems —
but it stops being the path of least resistance:

- one spelling across all connectors (`tls.insecureSkipVerify`),
- refused entirely unless the deployment opts in
  (`SHIFT_ALLOW_INSECURE_TLS=1`), so a cloud tenant cannot reach for it,
- surfaced as a **design-time warning** through the ADR-0042 §7 review registry
  — "this connection does not verify the server it sends your data to" — on the
  deploy and publish responses, where somebody will read it.

That last one is why the review registry being extensible mattered: this is a
new check in a new file, not a change to the flows model.

### 6. Getting the certificate should not require `openssl`

The remaining friction is *obtaining* the PEM. A "Test connection" in the
studio dials the endpoint, and when verification fails it shows the chain the
server actually presented — subjects, issuers, SHA-256 fingerprints, expiry —
with two buttons: **trust this CA** (stores the issuer's PEM on the connection)
and **pin this certificate** (stores the fingerprint).

This is trust-on-first-use with the decision made explicitly by a named human,
recorded in the audit log, against a chain they can see. It is what people
approximate today with `openssl s_client | sed`, done properly.

### 7. Expiry is monitored, because trust silently expiring is an outage

The hub can parse `notAfter` out of a stored CA or client certificate without
touching payload. A connection whose material expires within 30 days is a
review notice; within 7 days, a warning on the connections list. "The
integration broke overnight and nobody knew the certificate was expiring" is a
class of incident this deletes for a couple of days of work.

## Consequences

**An admin never touches a runner host.** Adding trust is an authenticated,
audited API call against the hub, made by the person who understands the
integration. No shell, no keytool, no restart, no ticket.

**GitOps works.** Connections are documents; a CA rotation is a diff on a
document, reviewable and revertible like any other change. The Java model has
no equivalent, because a keystore is a binary blob edited in place.

**Runner statelessness is preserved.** A new runner pod inherits every custom
trust decision the moment it leases work, because trust travels with the task
rather than with the host.

**More material flows through the connection resolve path.** CA PEMs are small
and public; client keys are already secrets and already handled. No new
security surface, more bytes.

**We must keep parsing certificates carefully.** Accepting PEM from a user
means validating it is a certificate, bounding its size, and refusing anything
with a private key in a field labelled as public.

## Alternatives considered

**A hub-wide trust bundle.** One list of extra CAs pushed to every runner.
Simpler, and it recreates the keystore's worst property: global trust, where an
addition for one integration silently applies to all of them. Rejected on blast
radius.

**Mount CAs into the runner image.** Works for a single-tenant deployment that
builds its own images, and requires a rebuild-and-redeploy per certificate,
which is the JVM-restart problem with extra steps. Nothing stops a deployment
doing it — the system store is still consulted — but it is not the platform's
answer.

**Automatic trust-on-first-use with no human.** Removes all friction and all
security: an integration that trusts whatever answered the first time it dialled
is an integration that trusts whoever was in the middle that day. The chain is
shown and a person decides.

## Open questions

1. **Chain-building for pinned intermediates.** Pinning a leaf breaks on the
   endpoint's next renewal; pinning an intermediate is more stable but subtler
   to explain. The studio should probably default to pinning the issuer and say
   why.
2. **Whether `caPem` should accept a secret reference.** It is public material,
   so no — but a customer may consider the *existence* of an internal CA name
   sensitive. Cheap to allow; decide when someone asks.
3. **Per-connection CRL/OCSP.** Out of scope here. Short-lived endpoints are
   the customer's problem; we verify what we are given.
