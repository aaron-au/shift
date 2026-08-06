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

**Status reads live under the developer's own route.** `GET /orders/_status/{id}`
resolves to the `/orders` route and inherits its ENTIRE policy — token,
allowlist, rate limit, principal. Authorisation is therefore structural: a
caller with access to `/orders` has no path on which to try a `/payroll` id.
It is GET-only regardless of the route's own method, and config validation
refuses any route that would shadow another's `_status` segment. An anonymous
route's status URL carries a per-task capability token in the query; the
gateway does not log it.

**Async is the default (ADR-0042).** A flow terminating at `@response` keeps
the caller's exchange open and returns the flow's output; anything else is
answered `202 Accepted` with a task id as soon as the request is verified, and
executes with the caller gone. The gateway does not know which mode it is
serving and must not — it hands over work and streams back whatever comes. Only
the *timing* of the delivery changes, which is why the exchange lifetime
collapses from "flow duration" to "validate and accept" without the gateway
gaining a single code path.

That matters for capacity: the `legacy-200ms` row in `docs/bench-gateway.md`
(239 req/s, because each request held an exchange for 200 ms) is bounded by the
backend only while the flow is synchronous.

**Requests are verified before they are accepted (ADR-0042 §4).** A flow may
declare an input schema on its `@webhook` source; the runner checks the request
against it and answers `400` with the offending field named, rather than `202`
followed by a dead letter. Validation runs on the RUNNER, never here: a schema
evaluator is a parser fed attacker-shaped input, and this is the box in the DMZ.

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

### A note on header case

The strip is case-insensitive because it has to be, and forcing lowercase
internally would be wrong. RFC 9110 makes HTTP/1.1 field names
**case-insensitive** with no preferred spelling; RFC 9113/9114 make them
**MUST-be-lowercase on the wire** for HTTP/2 and HTTP/3. Go satisfies both
already — `http.Header` keys are canonical MIME form and the h2 transport
lowercases when it serialises — so a hand-lowercased map key would simply be
invisible to `Get`, which canonicalises what it looks up.

Everything therefore canonicalises **before** comparing, and stores under the
canonical key. One comparison covers `x-shift-principal`,
`X-SHIFT-PRINCIPAL` and every other spelling a caller might try.

## Placement: label selectors, not a group name

A route names the runners eligible to serve it by **label set**
(`{environment: production, workload: api}`), and a parked runner matches when
its own labels are a **superset**. A single group string cannot express "any
production API runner", which is the shape real fleets have.

The empty selector matches any runner — right for a single-group deployment,
and a trap in a mixed fleet, so the hub is expected to be explicit.

Matching is a linear scan of parked runners, deliberately. A selector matches
label *sets* rather than a name, so there is no key to bucket by, and parked
runners number in the hundreds: an index over every distinct label set would
cost more to maintain than the scan saves.

## The two listeners

| Listener | Faces | Carries |
|---|---|---|
| `-public` (`:8443`) | the internet | caller requests |
| `-control` (`127.0.0.1:8444`) | the internal network only | runner poll/deliver, hub config push |

**The control listener is the runner-impersonation surface.** A caller who
reaches `/poll` can park a fake runner: it is handed real inbound payloads, and
it can deliver forged responses to real callers. Interception and response
forgery, from one open port.

Two things guard it today:

1. **A shared secret** (`SHIFT_GATEWAY_CONTROL_TOKEN`, env not flag — a flag
   would put it in every process listing). Stored as SHA-256 only, compared in
   constant time, sent by the runner as `SHIFT_GATEWAY_TOKEN`.
2. **A fail-closed start-up rule.** gatewayd REFUSES TO START when the control
   listener is non-loopback and no secret is set. Refused rather than warned:
   a warning is something a deployment scrolls past, and the failure mode here
   is silent payload interception.

Both are **interim**. ADR-0038 §6a specifies mutual TLS with a per-gateway
identity bundle, which also authenticates the gateway TO the runner — this
direction only proves a runner is entitled to receive work, not that the work
it receives is genuine.

```
POST /api/v1/gw/poll              park until work arrives (long-poll)   [authed]
POST /api/v1/gw/deliver/{id}      hand the response back                [authed]
GET  /healthz                     configured?, config version, runners parked
```

One inbound request becomes **two** runner-side calls rather than one duplex
connection. That costs an extra round trip on the deliver — sub-millisecond on
a LAN — and buys plain HTTP semantics: no framing protocol, no half-open state,
and a poll abandoned by simply letting the request time out.

The poll response carries the work as a normal HTTP message: metadata in
`X-Shift-Flow` / `X-Shift-Request-Id` / `X-Shift-Method` / `X-Shift-Path`, the
caller's own headers re-emitted under `X-Shift-Fwd-`, and the caller's body as
the response body. The prefix exists because the two share a namespace —
`Content-Type` would otherwise mean two different things.

Coming back, `X-Shift-Status` carries the runner's intended status and the body
streams through. Response headers are an **allowlist**, not a strip-list: the
runner is inside the trust boundary, but its answer goes to the public
internet, and "everything minus what we remembered to remove" is the shape that
leaks an internal header the day someone adds one.

**Measured** (`docs/bench-gateway.md`): platform overhead **0.26 ms p50 /
0.47 ms p99**, and **26,852 req/s** on one gateway at 64 concurrent callers
with zero errors — 268× the 100 tps that already counts as a very large
integration deployment. Against a simulated 20 ms REST backend the platform
adds **0.46 ms** at load.

The benchmark models connectors as service-time *distributions* with jitter and
a 1–3% spike arm, because a zero-latency stub measures Go's scheduler rather
than this system.

## Not built yet

- **Gateway side of adoption** (ADR-0049): the state directory, the bootstrap
  endpoint that closes once adopted, and mutual TLS on the control listener.
  Until it lands the control listener carries a shared secret and `-config`
  loads a local file. The shared secret is one-directional — it does not let a
  runner verify the gateway, which mTLS will.
- **Per-runner gateway address list**, and the config *builder* — the hub side
  now has the records, the CA, the dial-out client and the reconcile loop
  (`hub/internal/{pki,gwpush,gwsync}`), but `ConfigFor` is unset, so a hub
  currently maintains gateway identities and pushes nothing. An adopted gateway
  with a live identity and no routes serves 503, which is the correct answer
  for a gateway with nothing to route.

  Bootstrap no longer inverts: the gateway publishes a public-key fingerprint,
  an administrator carries it to the hub, and the hub dials. Identity renewal is
  pushed ahead of expiry by `gwsync`, and the fingerprint is *retained* after
  adoption, so an identity that lapses anyway is recovered by falling back to
  the pinned key rather than stranding the gateway.
- **Caller identity inside the flow.** The gateway stamps the principal and the
  runner receives it, but nothing yet binds it into the flow document — that
  needs the flow-variable model ADR-0031 leaves open. Until then the principal
  is logged, not addressable from a step.
- Streaming the caller's body straight through: the runner currently reads it
  fully, because the `@webhook` source binds a byte slice.
- Rate limiting (reuse ADR-0021's token bucket), HMAC provider signatures.
- **Pass-through proxy routes** (ADR-0040, drafted): fronting an internal API
  with no runner and no flow.
- `accept: "fast"` (ADR-0042 §6): today an accept that cannot be recorded is a
  503, which is the durable-by-default answer. The faster mode trades a status
  URL that may briefly 404 for edge availability when the hub is unreachable,
  and nobody has asked for it.
