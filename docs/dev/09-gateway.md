# 09 — The Inbound Gateway (`gateway/`)

The gateway is SHIFT's **third application** and the only publicly reachable
one (ADR-0038). It is **optional**: a deployment whose flows are all scheduled
or polled never runs it and carries zero inbound attack surface.

It exists because ADR-0016's original arrangement was backwards. Runners hosted
the public webhook surface — and the runner is the **highest-value target in
the system**, holding decrypted secrets in memory, live payload and connector
subprocesses, while the hub holds ciphertext and metadata. That model exposed
the valuable component to the internet once per runner, each needing DNS, a
certificate and a firewall rule.

## The one rule everything else follows

**Nothing in the DMZ ever initiates a connection inward.**

| Connection | Direction | Mechanism |
|---|---|---|
| Hub → gateway (configuration) | internal → DMZ | POST on change + reconcile |
| Runner → gateway (work) | internal → DMZ | long-poll |
| Gateway → anywhere internal | **none** | — |

A compromised gateway therefore has no path onward: it holds no credential
that opens one, because it has none to hold. A firewall review sees only
outbound-to-DMZ rules, which is the easy approval.

## Availability is whoever is listening

This is the part worth understanding before reading `internal/runners`.

Because runners long-poll the gateway, **the set of runners currently polling
IS the set of available runners.** There is no liveness table to keep fresh, no
capacity query on the request path, and no way to route to a dead backend — a
runner that died is a runner no longer holding a poll.

And because the runner's lease loop is already capacity-gated (ADR-0005), a
runner only polls when it can accept work. **Admission control and load
balancing fall out of the same fact** instead of being two mechanisms that can
drift apart. FIFO across parked runners spreads work without tracking load: a
busy runner is not polling, so the queue is self-balancing.

The subtle part is the unpark race. A request handed to a runner in the same
instant its poll times out must still reach it — losing it would leave the
caller waiting out the full delivery timeout for a runner that had already
gone. `Poll` re-checks its channel after unparking for exactly that window.

## The request path (`internal/ingress`)

```
route lookup → IP allowlist → required headers → bearer token
             → dispatch to a parked runner → stream the response back
```

Nothing is persisted. The request body streams to the runner and the response
streams back — no buffer, no spool, **no queue**. When no eligible runner is
available the answer is **503 with Retry-After**, never a wait: a gateway that
holds work is a gateway with durable state, and durable state in the DMZ is
what this component exists to avoid.

Three behaviours that are correctness, not polish:

- **A blocked caller gets the same 404 as an unknown path.** Distinguishable
  responses are free reconnaissance.
- **`X-Forwarded-For` is honoured ONLY from a configured trusted proxy.**
  Believing it from an arbitrary caller would let anyone claim any source
  address and walk through every IP allowlist — the allowlist would look
  enforced and be worthless. Honouring it from a real proxy is how the gateway
  runs behind an F5/ALB (ADR-0038 §6 `upstream-tls`).
- **The caller's `Authorization` header is consumed and not forwarded.** The
  gateway authenticates the caller; the runner has no business seeing the
  credential.

## Configuration is the hub's, entirely (`internal/config`)

Routes, allowlists, rate limits, TLS mode and certificate material all arrive
**pushed from the hub** and live only in memory. Configuration is swapped
atomically, so a request sees the whole old policy or the whole new one — never
a half-applied mix.

What may sit in a local file is **facts about this host**: listen addresses,
identity bundle path, log level. The moment a route or an allowlist lands
locally there are two sources of truth, and the failure mode is serving stale
policy instead of a clean 503.

An unconfigured gateway serves 503 and says so on health. That is the **correct
starting state**, not a fault — and the diagnostic that matters lives on the
hub (`last contact: never` against the gateway record), because a passive
component that is never dialled is otherwise silent.

## Boundaries are a build gate

The gateway is its own module and `depguard` denies `sdk`, `engine`, `hub`,
`runner` and `connectors`. It is stdlib-only, and that is a **security
property rather than a style preference**: this is the one component in a DMZ,
so what it can import decides what code — and eventually what credentials —
can end up there.

## High availability

Each gateway's poll registry is **its own, in memory**. A runner parked on
gateway 1 is invisible to gateway 2 — and that is deliberate, because the
alternative is shared state between DMZ hosts.

So **a runner polls every gateway eligible to send it work**, and the **hub
computes that list** (it already holds the gateway records and the runner
labels) and hands it over on the config sync that already exists. Adding a
gateway reconfigures nothing.

The cost is small: 100 runners across 5 gateways is 100 parked connections per
gateway (~8 KB of goroutine stack each) and roughly 3 poll requests/sec per
gateway at a 30-second window.

**Runners must address each gateway individually, never a load-balancer VIP.**
A multi-gateway deployment will have an LB in front for *inbound* traffic, but
a poll parked on gateway 3 is only usable by gateway 3 — connecting through the
VIP would strand most of the fleet behind whichever backend the balancer picked.

A runner parked on several gateways can be handed several tasks at once, since
capacity is checked when it decides to poll rather than when work lands. It
drops its remaining polls on accepting, and anything still in flight waits on
the existing ADR-0005 admission — the same path hub-leased tasks take.

## Identity: strip, then stamp

The gateway authenticates the caller so the runner does not have to, and it
passes on **who called** rather than **the credential**. That is what lets a
certificate-authenticated caller work without the runner touching any PKI.

A statically coded set of headers — not configurable, not per-route:

```
X-Shift-Principal   who was authenticated
X-Shift-Route       the route that matched
X-Shift-Request-Id  correlates gateway, runner and hub records
X-Shift-Client-Ip   the caller, trusted-proxy aware
```

**Every inbound header matching `x-shift-*` (case-insensitive) is stripped
before stamping, unconditionally.** This is the whole property: if a caller
could send `X-Shift-Principal: admin` and have it survive, the gateway would
be an authentication bypass *with an audit trail that lies about it* — worse
than no identity propagation at all.

## Not built yet

- mTLS control listener + identity bundle (ADR-0038 §6a): bootstrap inverts,
  since a gateway that cannot dial the hub cannot register with it.
- Hub side: gateway records, config push, certificate lifecycle. **Identity
  renewal must be pushed before expiry** — a lapsed certificate strands a
  gateway permanently, because requesting a new one would mean dialling.
- Runner side: poll the gateway, bind privately (the ADR-0016 reversal).
- Rate limiting (reuse ADR-0021's token bucket), HMAC provider signatures.
