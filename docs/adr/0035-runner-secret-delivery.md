# ADR-0035: Runner secret delivery — sealed resolve, key exchange, and opt-in caching

Date: 2026-08-03

Status: **Partially implemented.** §3 (one round trip for connections and every
secret they reference) shipped as `POST /api/v1/task-config/resolve` +
`runner/internal/bind` (#58). §1 (sealed resolve — X25519+HKDF over the
runner's registration key) and §2 (opt-in TTL cache, default 0, revocation
epoch on the lease poll) are designed, build deferred.

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

There is also a latency cost that only shows up on the paths the product is
differentiated on. For a queued batch task, one round trip against work
measured in seconds is noise. For **synchronous request-reply** (`RunSync`,
ADR-0024) it is a fixed per-invocation tax on a path whose entire purpose is
answering in the same HTTP call.

The *load* case is weaker than it first appears and should not be used to argue
for caching: `/secrets/resolve` takes a **name list**, and the runner sends
every reference for a task in one call. It is one round trip per invocation
regardless of how many secrets a flow uses, and one indexed lookup plus a
symmetric decrypt is not a throughput problem for the hub. What remains is
purely **latency, and only when the hub is geographically distant** — a hub one
WAN hop away adds that hop to every webhook, while a same-region hub costs
effectively nothing.

The same measurement exposed a second, unrelated instance of the hub sitting on
the critical path: `RunSync` reported its execution metadata to the hub
**inline, before writing the response**, so every synchronous call paid a full
hub round trip after the work was already finished. That is fixed
independently of anything here — the report belongs off the response path — but
it is the same disease, and it is why §3 is scoped to what caching actually
buys rather than to "make the hub optional".

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

Resolution moves out of `leaseloop` into a shared runner-side resolver
(`runner/internal/bind`) used by the queued path and the three
runner-direct paths alike. A document reaching the engine with an unresolved
`{"$secret":...}` reference is a **task failure with a clear error**, not a
value passed through — silently handing a connector a reference object is how
the bug hid for as long as it did.

This part is independent of §1 and §3 and has already landed; it needed no key
exchange and no cache.

### 3. Caching is opt-in, and the TTL policy belongs to the secret

A runner **may** cache a sealed value for a bounded period. The default is
**0 — no caching**, preserving today's semantics exactly.

Default off is deliberate, and it follows from the load analysis above. There
are two distinct things a TTL could buy, and they want different numbers:

| Want | TTL | What it is for |
|---|---|---|
| Latency | ~60s | Removes the per-invocation hub hop. Captures essentially the whole win; longer buys almost nothing. |
| Outage survival | 15–60 min | Survives a hub restart, rolling deploy or Postgres failover. Sixty seconds does not. |

Neither is worth turning on for a deployment whose hub is local — there the
per-task fetch is sub-millisecond and strictly more secure. So this is a
**deployment-shaped switch, not a product default**: turn it on when the hub is
remote, or when a runner group must ride out a hub outage.

One rationale to avoid, because it is wrong: a short TTL does **not**
meaningfully bound an attacker's access to a credential. Anyone with code
execution on the runner can trigger a flow or wait for the next fetch; a
credential in active use is obtainable regardless of cache policy. What the TTL
bounds is the **revocation** window (§4). The defences that matter are the ones
in §1 — memory only, never on disk, sealed at rest in memory, key dies with the
process.

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
- **The request-reply path gets materially faster against a remote hub**,
  because the per-invocation round trip disappears for cached secrets. Against
  a local hub the difference is negligible — which is exactly why the default
  is off.
- **A real bug is fixed** (§2, landed): webhook and direct-execution flows that
  use credentials work for the first time.
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

**A short cache as the fix for the webhook bug.** Not a fix at all: the webhook
path never *called* the resolver, so a cache in front of a call nobody makes
changes nothing. That fix is independent and has landed separately.

**What the incumbents do**, for calibration: they all cache, and most persist.
A Boomi Atom holds connection credentials encrypted on the runtime's own
filesystem; Mulesoft's secure property placeholders put encrypted values in the
config file with the key supplied at startup; Vault Agent keeps a TTL'd lease
cache. The per-execution fetch here is *stricter* than the norm, which is why
it pays an availability cost nobody else pays. The differentiator worth keeping
is that all of those write credentials to disk, where they persist across
restarts and into backups, images and snapshots — memory-only with a
process-scoped key is a claim they cannot make. (The vendor specifics are
second-hand; verify before they appear in customer-facing material.)

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
