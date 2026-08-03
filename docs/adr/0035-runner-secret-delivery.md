# ADR-0035: Runner secret delivery — sealed resolve, key exchange, and opt-in caching

Date: 2026-08-03

Status: **Designed**

## Context

Secrets are stored envelope-encrypted (ADR-0010): a per-secret DEK, the DEK
wrapped by a pluggable KEK, and `{"$secret":"name"}` references that resolve
**runner-side** so plaintext never enters the task queue, task reads, or logs.
That decision has held up. This ADR does not revisit it; it addresses three
things the delivery path got wrong or left undone.

### 1. The resolve response is plaintext

`POST /api/v1/secrets/resolve` returns credential values as plaintext JSON,
protected only by TLS. In a real deployment TLS terminates somewhere that is
not the runner: an ingress controller, a corporate proxy, a service mesh
sidecar, an APM agent that captures request bodies. At each of those the
credential exists in the clear, on a box nobody audited for it.

The wire was never the exposure. The **intermediary** is.

### 2. Only one of four execution paths resolves at all

Secret resolution lives in `runner/internal/leaseloop`. A grep of the runner
module for `SecretRefs` and `ResolveSecrets` returns exactly one call site — the
hub-queued task path.

The three **runner-direct** paths (ADR-0016 / ADR-0024) never resolve:

- `POST /hooks/{name}` — webhook triggers
- `POST /api/flows/execute` — async direct execution
- `POST /api/flows/run` — synchronous request-reply

A flow triggered by any of them hands its connector the literal
`{"$secret":"name"}` object where a string was expected. Webhooks are a
first-class trigger, so this is a hole rather than a corner: any webhook flow
that touches a credential is broken today.

### 3. The hub is on the critical path for starting any secret-using flow

Resolution is deliberately uncached — `leaseloop.go` states the reason: a
per-task fetch keeps revocation immediate and the runner stateless. The cost is
that a hub outage stops any *new* flow that needs a credential, including the
runner-direct paths that otherwise survive an outage entirely (webhook configs
are already synced and cached locally, so the trigger works — it just cannot get
its credential).

There is also a throughput and latency cost that only shows up on the paths the
product is differentiated on. For a queued batch task, one round trip against
work measured in seconds is noise. For **synchronous request-reply**
(`RunSync`, ADR-0024) it is a fixed per-invocation tax on a path whose entire
purpose is answering in the same HTTP call. For **high-frequency webhooks** it
is a QPS problem: one hub call per invocation, repeatedly, for a value that did
not change.

## Decision

### 1. Runners get a keypair at registration; resolve returns sealed values

The runner generates an **X25519** keypair at startup and sends the **public**
key with its existing `POST /api/v1/runners/register` call — the same request
that already mints its bearer secret, so no new handshake is introduced. The
hub stores the public key on the `runners` row.

`POST /api/v1/secrets/resolve` (and the ADR-0034 connection resolve, which has
the same shape) returns, per name, a **sealed box**: an ephemeral public key
plus AES-256-GCM ciphertext, with AAD binding the runner id, the secret name,
and the secret version. Key agreement is X25519 + HKDF.

All of this is **standard library** — `crypto/ecdh`, `crypto/hkdf`,
`crypto/aes` — so the hub takes no new dependency, which matters for a
control plane that is deliberately dependency-light.

The **private key never leaves runner memory** and is not persisted. A
restarted runner has a new key and therefore cannot open anything a previous
process cached. That is the correct posture for a component that is disposable
by design (ADR-0002), and it makes cross-restart cache poisoning impossible.

What this buys, stated precisely so it is not oversold internally:

- plaintext no longer appears in an HTTP body that any TLS-terminating
  intermediary can read;
- a stolen runner bearer secret, replayed from another host, yields ciphertext
  that host cannot open.

What it does **not** buy: the hub still unwraps the DEK and holds plaintext
transiently in order to re-seal it. "The hub never sees plaintext" would require
runners to hold the wrapping keys and secrets to be stored wrapped per runner,
which makes adding a runner a re-wrap of the entire secret store. That is a
different and much larger design, and it is explicitly **not** what this ADR
decides.

### 2. All four execution paths resolve

Resolution moves out of `leaseloop` into a shared runner-side resolver used by
the queued path and the three runner-direct paths alike. A document reaching
the engine with an unresolved `{"$secret":...}` reference is a **task failure
with a clear error**, not a value passed through — silently handing a connector
a reference object is how the current bug hides.

### 3. Caching is opt-in, and the TTL policy belongs to the secret

A runner **may** cache a sealed value for a bounded period. The default is
**0 — no caching**, preserving today's semantics exactly.

The TTL is set **hub-side, per secret**, and travels sealed inside the bundle.
Not per runner: a rotating integration API key may be perfectly fine cached for
five minutes while a break-glass database password should never be cached, and
that judgement belongs to whoever owns the credential.

**A runner may lower a TTL, never raise it.** Runner-side configuration is a
conservatism knob only. Any other rule lets a runner operator decide the
security policy of someone else's credential.

Cached values are held **sealed in memory**, opened at point of use, and never
written to disk. The existing redaction path (resolved values are already fed
to the service's redactor) is unchanged.

### 4. Revocation rides the lease poll, so caching does not cost revocation speed

The hub maintains a **revocation epoch** per account, bumped on any secret
delete, rotate, or KEK rotation. The epoch is returned on the lease long-poll —
a channel the runner already exercises continuously — and a runner seeing a new
epoch flushes its cache.

The consequence is the point of the design:

- **hub reachable** → revocation is effectively immediate, as it is today;
- **hub unreachable** → the TTL is the worst-case exposure window.

Caching therefore does not trade revocation speed away in normal operation. It
trades it only for the outage window — which is the window the cache exists to
survive.

## Consequences

- **A runner group becomes genuinely hub-independent for its cached window.**
  Webhook ingress, direct execution and synchronous run already survive a hub
  outage (ADR-0016 keeps them off the control plane); with an opt-in cache they
  survive it *including* credentials. This is the availability property the
  hub-and-spoke story implies and did not yet deliver.
- **The request-reply path gets materially faster**, because the per-invocation
  hub round trip disappears for cached secrets. High-frequency webhook flows
  stop generating one control-plane call per invocation.
- **A real bug is fixed.** Webhook and direct-execution flows that use
  credentials work for the first time.
- **The enterprise security answer improves** without changing the trust model:
  credentials are no longer readable at a TLS-terminating hop.
- **New state to reason about.** A cache is state on a component whose value is
  being stateless. It is bounded (memory only, no disk, dropped on restart,
  flushed on epoch change, default off), but it is state, and it must be visible
  in the runner's status output so an operator can see what is held and for how
  long.
- **The hub is still required to start a flow whose secrets are not cached**, and
  with the default TTL of 0 that is every flow. Nothing about the current
  availability posture changes unless an operator opts in per secret.
- Registration gains a field. Existing runners that send no public key continue
  to receive plaintext responses, so the rollout is not a flag day — but the
  plaintext path should be removed on a stated deprecation once runners are
  updated, since leaving it in place leaves the intermediary exposure available
  to any client that simply omits a key.

## Alternatives considered

**Route payload through the hub (hub as load balancer).** Rejected. It would
make the hub a data-plane component, which costs the streaming/memory thesis
(the hub would need its own governor and spill store — v0's failure shape), puts
the hub in scope for data-residency and compliance regimes it is currently
outside of, inverts the scaling story (runners scale unbounded, the hub is the
Postgres-coupled part), and removes the outage survival that ADR-0016's
two-plane split currently provides. If a single front door is wanted, the hub
can pick a runner and answer `307` — routing *decisions*, not bytes — or a thin
proxy can ship in the deploy bundle as its own process. Either is a separate
decision from this one.

**Runner holds KEK material; hub stores secrets wrapped per runner.** Achieves
"hub never sees plaintext", but adding or replacing a runner requires re-wrapping
every secret, and a runner compromise becomes a compromise of the wrapping key
rather than of one delivery. Deferred, not rejected — §1 is a prerequisite for
it either way.

**Cache the resolved document at webhook sync time.** Simplest fix for the
webhook bug, and wrong: it writes plaintext into the runner's synced config,
which is exactly what ADR-0010 exists to prevent.

## Open questions

1. **Cache scope key.** Per secret name, or per (name, version)? Version keying
   makes a rotation self-invalidating without waiting for the epoch, at the cost
   of the runner needing the version before it can look up.
2. **Whether connections (ADR-0034) share the TTL policy or get their own.** A
   connection document is metadata rather than a credential, so a longer default
   may be defensible — but it *contains* secret references, and a split policy
   is a second thing to reason about.
3. **Key rotation without re-registration.** A long-lived runner currently keeps
   one keypair for its process lifetime. A `POST /api/v1/runners/{id}/rekey` may
   be worth it for very long-lived runners; process lifetime may simply be short
   enough in practice.
4. **Whether the plaintext resolve path is removed or kept behind a flag** once
   sealed resolve ships. Leaving it available weakens §1 for anyone who omits a
   public key.
