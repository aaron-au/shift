# ADR-0033: Licensing — a hub-held concurrency budget

Date: 2026-08-03

Status: **Designed (build deferred)**

## Context

SHIFT needs a commercial model that does not fight its own architecture.

The product's differentiation is **concurrency and footprint**: goroutine-per-task
execution (ADR-0005), a streaming engine that moves 1 GiB through a transform at
24 MiB peak RSS, and a hub/runner split that self-hosts cleanly. Against the
incumbents that matters twice over — a Boomi `Branch` shape runs its branches
*sequentially* because JVM threads are expensive, while SHIFT's `tee` runs them
concurrently because goroutines are nearly free (ADR-0029, ADR-0032 §4). The
migration report and the performance pitch are the same document.

A pricing model must therefore not punish the thing being sold. Two shapes are
common in this market and both do:

- **Per connector / per connection** (Boomi-shaped) — taxes breadth of
  integration, which is exactly what an iPaaS is for, and makes every new
  connector a negotiation.
- **Per task / per recipe execution** (Workato-shaped) — taxes volume, so a
  customer is penalized for the high-throughput workloads SHIFT is best at.

Both also require the vendor to meter something fine-grained and trusted, which
is hostile to self-hosting — and self-hosting is a deliberate differentiator, not
a concession.

Separately, the revenue centre is **support contracts**, not licence keys. That
changes what enforcement has to achieve: it must be *clear and auditable* so
honest customers comply and enterprises buy properly. It does not need to be
unbreakable, and building as though it did would produce DRM that costs more
than it protects.

## Decision

Licence a **total concurrency budget, expressed in cores, held by the hub**.

### 1. The unit is a core, and a core is `GOMAXPROCS`

A "core" is one unit of `runtime.GOMAXPROCS` on a runner. Not a CPU share, not a
container limit, not a host thread — the Go runtime's own parallelism setting,
which is precisely "how many OS threads may execute Go code at once".

This is the right unit for three reasons:

- It is **honest**: the customer genuinely receives that much compute. A 64-core
  host running a 4-core runner uses four cores' worth.
- It is **enforceable without instrumentation**: the runner sets it at startup
  and reports it. There is no per-record counter to trust or tamper with.
- It **does not touch the programming model**. Goroutines remain free and
  unlimited; only the number executing Go code simultaneously is bounded. "Use
  goroutines for nearly everything" and "billed per core" are fully compatible,
  which is the property that makes this model viable at all.

Deliberately **not** limited: I/O concurrency. Blocking syscalls do not occupy a
`GOMAXPROCS` slot, so a small budget still permits thousands of concurrent HTTP
calls, database queries and file transfers. A customer running very high
concurrency over low-transform workloads therefore pays very little — that is
accepted, and welcomed. It is the cheapest possible advertising for the
concurrency story, and it prices generously exactly where SHIFT is unremarkable
while pricing fairly where its engine is the reason to buy.

### 2. The budget lives on the hub, not the runner

```
  licence server  →  hub  →  runners
     (ours)          (customer's)   (customer's)
```

The hub holds one licence and one **total** core budget. Runners request an
allotment at registration; the hub grants it only while the sum of live
allotments stays within budget.

Consequences that make this the right placement:

- **One licence to administer**, not one per runner. Runners stay disposable
  (ADR-0002) — a runner that dies returns its allotment to the pool via the
  existing heartbeat/reap machinery, with no licence state to clean up.
- **The customer splits the budget however they like.** Sixteen cores can be one
  runner of 16, four of 4, or two on-prem and two in a VPC (composing naturally
  with runner groups, ADR-0030). Incumbents sell fixed runtime sizes; this is a
  feature, and it costs nothing to offer.
- **It rides plumbing that already exists.** Runner registration, hub-issued
  credentials, capacity-gated lease claims and heartbeats are all built
  (ADR-0009); the allotment is one more field on a flow that already happens.

### 3. Community mode is the absence of a licence

A hub with **no licence runs in community mode: 4 cores total, community
support**. Not four cores per runner — four across the whole hub, or the tier
would be defeated by registering more runners.

Paid tiers start at **4 cores minimum**, which sets the base price. Note what
this means: entry-paid and community are *technically identical*. The licence at
4 cores carries no extra capability — it is a **support entitlement**. That is
intentional and it is the Red Hat shape: the free tier is genuinely useful, the
first paid tier sells the relationship, and the technical ceiling only starts
mattering above it.

The licence therefore carries its tier and entitlement as signed metadata, not
just a core count.

### 4. Offline by default — the licence is a signed artifact, not a phone-home

On-prem and air-gapped deployment is a differentiator; a licence that requires
the hub to reach a licence server would break exactly the customers the model is
meant to attract.

So the licence is a **signed, time-limited artifact** the hub verifies locally.
`pkg/consign` already does this shape — Ed25519 over a canonical manifest, with
private keys never held server-side — and should be reused rather than
reinvented.

**Offline is the primitive; online is optional automation over it.** The
customer downloads their licence file from the licence server and applies it to
the hub — that path always works and is the only one an air-gapped deployment
needs. A hub that *can* reach the licence server may opt into fetching and
renewing the same artifact automatically, but this is strictly a convenience: it
produces an identical file through an identical verification path, so there is
no second code path to trust and no behavior that exists only online. Turning
the network off degrades operator convenience, never capability.

This ordering matters. Building online-first and bolting on an offline
"export" produces a second-class path that rots — the failure mode every
enterprise vendor with an air-gapped tier eventually ships. Building
offline-first and automating the fetch cannot produce that outcome.

Failure behavior is **fail-open, loudly**. An unreachable licence server, an
expired-but-recently-valid licence, or a clock skew must never stop a customer's
integrations from running: the hub continues on the last known-good licence,
surfaces the state in the dashboard and audit log, and degrades to community
limits only after a generous grace period. A licensing subsystem that can take
production down is a larger liability than the revenue it protects.

### 5. What is deliberately not defended

**Running several hubs to multiply the free tier.** It is detectable only by
phone-home identity, which contradicts §4, punishes air-gapped customers, and is
trivially defeated anyway. Past a point this is fighting the inevitable — and
the customers willing to operate N hubs to dodge a support contract were never
going to buy one.

The same reasoning covers patching the binary. Enforcement targets honest
customers and audit trails, not determined evasion.

## Reconciliation with ADR-0005

ADR-0005 forbids "arbitrary limits" — admission governed by real resource
signals, never fixed task-count caps. A licensed core budget must not be read as
overturning it.

The distinction: **the licence declares how large a machine the runner believes
it has; ADR-0005 governs everything inside that boundary.** Within its allotment
a runner still gets goroutine-per-task, admission by memory watermark, a
coordinator that never executes, and no task ever waiting on another except for
genuine resource scarcity. What the licence changes is the size of the machine,
which is the one thing ADR-0005 never claimed to control — it is the same
constraint as deploying on a smaller instance.

What remains forbidden, and is not introduced here: per-task-count caps, queue
gates between tasks, or a ceiling on how many runners may join.

## Consequences

- The runner gains a startup `GOMAXPROCS` set from its hub-granted allotment,
  and reports both granted and actual in its heartbeat so drift is visible.
- The hub gains licence verification, a core-budget allocator over the existing
  registration path, and refusal (with a clear error) when a grant would exceed
  budget.
- **Metering is already half-built**: `usage_events` records `exec_seconds` per
  task (M6d), so core-seconds is derivable without new plumbing if consumption
  pricing is ever wanted alongside the core budget.
- The hub remains **task control, not the billing platform** — it verifies a
  licence and reports usage; invoicing, plans and quotas belong to the separate
  account platform.
- Core-based pricing under-monetizes I/O-bound workloads by construction (§1).
  Accepted deliberately; revisit only with evidence that it is costing real
  revenue rather than winning real customers.

## Open questions

1. **Allotment request shape.** Does a runner request a specific core count
   (operator-configured, hub validates the sum), or does the hub divide the
   budget? Leaning to the former — it matches "the customer splits it however
   they like" and keeps the hub free of placement policy.
2. **Reclaim timing.** How long after a runner stops heartbeating is its
   allotment returned? Too eager and a network blip costs capacity; too lazy and
   a redeployment stalls. Probably the existing lease-reap interval.
3. **Grace period length** for an expired licence before community limits apply.
4. **Whether community mode is 4 cores or fewer.** Four is generous enough to
   run real workloads, which is the point — but it is also the entry paid tier,
   so the free tier gives away the base SKU's capability. Deliberate for now.
