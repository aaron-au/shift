# ADR-0030: Runner groups / placement (runner-location awareness)

Date: 2026-07-29

Status: **Designed (build deferred)**

## Context

One hub manages many runners — hundreds at the top end of the target
deployment. Today the placement model is uniform: any registered runner may
claim any task. The lease query (ADR-0009) is a `SELECT … FOR UPDATE SKIP
LOCKED` over the account's ready tasks with no notion of *where* a runner sits
or *what* it can do. That was correct for M3/M4 (prove durability and
at-least-once), but it does not survive contact with the real deployment shape.

A customer runs runners in more than one place. A concrete, load-bearing
example (Aaron, 2026-07-29): an **on-prem** fleet holds the Exchange /
PowerShell / Active Directory connectors and has line-of-sight to the corporate
network; a **cloud** fleet holds S3 / SaaS-API connectors and lives in the
customer's VPC. A flow that runs an AD lookup *must* execute on an on-prem
runner — a cloud runner cannot reach the domain controller, and (per ADR-0016)
the payload plane never traverses the hub to bridge them. Customers deploy
runners anywhere but deliberately do **not** load every runner with every
connector and every network route. Placement therefore has to be a first-class
property of a flow's deployment, not an emergent accident of which runner
happened to long-poll first.

Aaron frames it as "like environments but more targeted": the useful labels are
things like `onprem-prod`, `onprem-nonprod`, `cloud-prod`. A flow (more
precisely, a flow *deployment*) targets one such label; only runners carrying
that label execute its tasks.

This must ride the existing durable queue (ADR-0002), not introduce a second
routing mechanism. Runners stay stateless and disposable; the hub stays the
sole owner of task durability. Placement is pure control-plane metadata
(ADR-0016) — it is a predicate on which runner may lease a row, and nothing in
it touches payload.

**Orthogonality to connector capability policy (ADR-0015) — stated explicitly
because the two are easy to conflate.** Capability policy is *hub-wide* and
answers "which connector **names** are allowed to exist and deploy on this
hub?" — a cloud hub hides dangerous connectors, and a flow referencing a denied
connector is rejected at deploy time and is invisible in list/resolve. Runner
groups answer a different question: "given a flow that legitimately deployed,
which **runners** may its tasks be placed on?" A group *tends to correlate* with
a capability set — the on-prem group is exactly the fleet that carries the exec
connectors — but they are separate axes and are enforced at separate points
(capability policy at deploy/validate; groups at lease). They are not merged,
and neither derives from the other: a hub can allow the `exchange` connector
hub-wide (capability policy) while a given flow still pins its tasks to
`onprem-prod` (group). Do not collapse them into one field.

## Decision

Introduce **runner groups**: free-form, per-account labels (in the spirit of
Kubernetes node labels + a `nodeSelector`), self-declared by runners at
registration and carried as a placement selector on a flow's deployment. The
lease query gains a group predicate. No new transport, no new durability
mechanism — placement is one indexed column on `tasks` and one on the runner
registration row.

### 1. Group model

A runner belongs to **one or more** named groups, declared at process start via
a new env var alongside the existing spawn/registration contract:

```
SHIFT_RUNNER_GROUPS=onprem-prod,onprem
```

Comma-separated, trimmed, de-duplicated, lower-cased, validated against a
conservative label grammar (`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`, matching the
`onprem-prod` style). Empty / unset ⇒ the runner joins the single reserved
group **`default`** (see §2). Groups are **per-account** (ADR-0010): the label
namespace is scoped by `account_id`; `onprem-prod` for tenant A is unrelated to
`onprem-prod` for tenant B.

Membership is captured **server-side at registration** into the runner
registration row, not held only in the runner:

```sql
ALTER TABLE runners
  ADD COLUMN groups text[] NOT NULL DEFAULT '{default}';
-- GIN index for the ANY/overlap lookups the lease path performs
CREATE INDEX runners_groups_gin ON runners USING gin (groups);
```

Groups are **created implicitly by first use** — registering a runner that
declares `onprem-prod` is sufficient for the label to exist and be selectable;
there is no separate group-CRUD surface in v1. An explicit registry of allowed
group names (to catch typos like `onprem-prd`, to power a dropdown in the
studio, to gate which labels a tenant may use) is a **plausible later
addition**, deliberately deferred: it is a convenience/validation layer over the
same column, adds an admin surface and migration, and buys nothing for the core
placement semantics. When it lands it will validate deployment selectors and
registration declarations against a per-account allow-list; until then, a
selector that matches no runner simply never leases (§4), which is a safe,
observable failure rather than a silent one.

### 2. How a flow targets a group

Placement lives on the **flow deployment / version row**, **not** in the flow
document (`flowdoc`).

Rationale — and this is the deliberate split:
- The flow document is the executable data graph (source/ops/sink, step edges;
  ADR-0013). The hub validates it but treats it as opaque, payload-free
  content — it must not encode *where* it runs, only *what* it does. Location is
  an environment-binding concern, not part of the graph.
- Putting the selector on the deployment lets the **same flow document deploy to
  different groups per environment**: publish flow doc vX once, deploy it to
  `onprem-nonprod` for testing and `onprem-prod` for production without
  re-authoring or re-signing the doc. That is precisely the "like environments
  but more targeted" ergonomic Aaron asked for, and it falls out for free only
  if the selector is deployment-scoped.
- It keeps `flowdoc` validation (which is authoritative, ADR-0019) untouched:
  no new field to validate, no 422 surface change.

The selector is **v1-minimal: a single required group name**, defaulting to
`default` so every existing deployment keeps running unchanged (§3 migration):

```sql
ALTER TABLE flow_deployments
  ADD COLUMN target_group text NOT NULL DEFAULT 'default';
```

A richer selector — *match ANY of a list* (`target_groups text[]`, task leases
if the runner overlaps any) — is a natural, backward-compatible extension (a
single name is the one-element list). It is **not** in v1: one required name
covers every motivating case (`onprem-prod`, `cloud-prod`) and keeps the lease
predicate a scalar comparison. We record it here so the column can widen later
without a model change.

### 3. Task placement and the lease query

Every enqueue path stamps the target group onto the task from its deployment.
There are three enqueue paths and all three must stamp it:
- **API execute** (`RunSync` / async execute) — read `target_group` from the
  resolved deployment.
- **Scheduler tick** (ADR-0012, exactly-once) — the enqueue that the DB-owned
  tick performs stamps `target_group` from the scheduled deployment, inside the
  same transaction that advances the tick and writes the `sched:<id>:<tick>`
  idempotency key. No second clock, no second mechanism.
- **Webhook report** (ADR-0016) — the runner-side webhook trigger reports
  execution metadata to the hub; the resulting task is stamped from the flow's
  deployment. (Payload stays on the runner; only the placement label crosses.)

Schema:

```sql
ALTER TABLE tasks
  ADD COLUMN target_group text;             -- NULL = any runner (see migration)
CREATE INDEX tasks_target_group ON tasks (target_group);
```

**Migration / backward compatibility.** Existing `tasks` rows get
`target_group = NULL`, and the predicate treats **NULL as "any runner may
claim"**. This makes the change strictly backward compatible: pre-migration
tasks, and any future task deliberately left unpinned, remain claimable by the
whole fleet. New tasks enqueued from a deployment carry that deployment's
`target_group` (which defaults to `default`, §2). We do **not** backfill
historical rows to `default`; NULL is the "unrestricted" sentinel and stays
meaningful.

**Lease predicate.** The runner's eligible groups are derived **server-side**
from the authenticated runner's registration row (§5) — call the derived array
`:runner_groups`. The `SKIP LOCKED` claim query (ADR-0009) gains one conjunct:

```sql
-- inside the existing FOR UPDATE SKIP LOCKED lease claim, WHERE clause:
AND (t.target_group IS NULL OR t.target_group = ANY(:runner_groups))
```

`:runner_groups` is bound from `runners.groups` for the authenticated runner —
never from anything the client sent on the lease call. A task is claimable by a
runner iff it is unpinned (`target_group IS NULL`) or its pin is one of the
runner's server-known groups. Everything else about the lease is unchanged:
long-poll, capacity gating (a runner still only claims with resource headroom,
ADR-0008/0005), reap-at-claim, and zombie-result rejection (409 on a result from
an expired lease — the group predicate does not touch that path and must not be
used to weaken it).

### 4. Unschedulable tasks

If no runner in a task's target group is online (or all are at capacity), the
task simply stays `ready` in the queue — no lease ever matches its predicate.
This is the correct, durable behavior: the task is not lost, not errored, and
executes the moment an eligible runner appears. There is no dispatch-side
timeout inventing a failure.

The failure mode it introduces is *silent starvation*: a task pinned to
`onprem-prod` with zero on-prem runners connected waits forever with no obvious
signal. v1 therefore **must make this observable** (ADR-0020 telemetry):
- a gauge of ready tasks per `target_group` with **no currently-leasing runner
  eligible** for that group (a task pinned to a group that has zero live
  runners), surfaced on the polling dashboard;
- age-of-oldest-ready-task per group.

A per-task **max-wait / alert / dead-letter-on-starvation** policy is **out of
scope for v1** — noted as a follow-up. v1 gives visibility (you can *see* the
stuck group); it does not yet give an automated SLA action. That belongs with a
broader task-timeout/alerting story, not this ADR.

### 5. Security

A runner **must not** be able to claim tasks for a group it is not in.
Enforcement is entirely server-side and non-negotiable:
- The lease call carries **no client-supplied group list**. The hub derives
  `:runner_groups` from `runners.groups` for the authenticated runner (bearer
  secret → registration row; ADR-0009 SHA-256-stored secret). A compromised or
  misbehaving runner cannot widen its own eligibility by lying on `POST /lease`,
  because the field does not exist on that call.
- Group membership is **bound at registration**. The single-use registration
  token exchange (ADR-0009) is where `SHIFT_RUNNER_GROUPS` is captured and
  written to the row. Changing a runner's groups = **re-register** in v1
  (destroy/re-provision the runner with new env). An explicit admin "update
  runner groups" endpoint is a reasonable later addition but is **not** v1 — it
  adds a mutation path to a security-sensitive column and is unnecessary for the
  disposable-runner model (you rebuild runners, you don't hand-edit them).
- Groups are account-scoped (ADR-0010): the lease query already runs under
  `store.WithAccount(ctx)`, so `:runner_groups` and `tasks.target_group` are
  only ever compared within one tenant. A tenant cannot name another tenant's
  group.

## Doctrine held

- **Hub owns durability; runners are stateless (ADR-0002).** Placement is one
  predicate on the existing durable queue. No per-runner routing state, no
  shared filesystem, no second queue. A runner that dies loses nothing; its
  pinned tasks return to `ready` and the next eligible runner claims them.
- **Control API is HTTP/JSON long-poll; no WebSockets (ADR-0009).** Nothing
  here adds a socket or a push. The runner still long-polls `POST /lease`; the
  only change is a `WHERE` conjunct evaluated against server-side membership.
  The dashboard still polls for the starvation gauges.
- **Two planes; payload never touches the hub (ADR-0016).** `target_group` and
  `runners.groups` are metadata — labels, not data. This is exactly the control
  plane doing control-plane work: deciding *where*, never moving *what*.
- **Capability policy stays orthogonal (ADR-0015).** No merge, no derivation.
  Two axes, two enforcement points.
- **Scheduler exactly-once preserved (ADR-0012).** Stamping `target_group`
  happens inside the existing tick transaction; the idempotency key and tick
  advance are untouched. Placement does not add a second dedup surface.
- **Tenancy (ADR-0010).** Per-account label namespace; all comparisons under
  `WithAccount`.
- **Parameterized SQL only.** `:runner_groups` and `:target_group` bind as
  parameters (`text[]` / `text`); no string-built predicates.

## Consequences

- The lease claim gains a group conjunct and binds one extra parameter. With the
  GIN index on `runners.groups` and the btree on `tasks.target_group`, the
  `SKIP LOCKED` scan cost is unchanged in practice — the predicate prunes,
  it does not fan out.
- Deployments carry a placement decision. The studio's deploy step needs a
  target-group input (free-text or, once the optional registry lands, a
  dropdown), defaulting to `default`. Existing deployments and existing tasks
  keep running with no change (NULL / `default` semantics).
- Operators gain a real deployment lever: run the AD-touching flow on-prem, the
  S3 flow in cloud, the same doc in nonprod vs prod, all by choosing a label at
  deploy time — no connector re-signing, no flow-doc edits.
- New observability requirement: per-group ready/starvation gauges. Without
  them, a mistyped or unmet group pin is invisible until someone notices a flow
  "not running".
- A new operator footgun exists: pin a flow to a group with no live runners and
  it silently waits. v1 mitigates with visibility, not prevention; the optional
  group registry (later) would prevent the *typo* class, and a max-wait policy
  (later) would bound the *no-capacity* class.

## Open questions

- **Environment vs location as separate dimensions.** v1 is a single **flat
  namespace of labels** — `onprem-prod` is one opaque string, not `location=onprem
  ∧ env=prod`. Richer *typed* structure (a `location` dimension and an
  `environment` dimension, with the selector matching on each) is genuinely
  useful — it would let a flow say "onprem, any env" or "prod, any location" —
  but it is a model change (multiple columns / a small selector language) and is
  **not v1**. Flat labels cover the motivating cases and can be widened later.
- **Selector expressiveness.** v1 is one required name. `match-ANY-of-a-list`
  (§2) is the first likely extension; full label-selector expressions
  (`in`/`notin`, negation) are almost certainly over-build — revisit only with a
  real requirement.
- **Explicit group registry.** Worth it for typo-prevention, a studio dropdown,
  and per-tenant governance — but a convenience layer, deferred (§1).
- **Admin re-labelling of a live runner.** v1 says re-register. If long-lived,
  expensive-to-reprovision runners become common, an authenticated admin
  "update groups" endpoint may earn its place (§5).
- **Automated starvation action.** Max-wait, alerting, and dead-letter-on-no-
  eligible-runner (§4) are deferred to a broader task-timeout story.
- **Capacity-aware placement.** Groups decide *eligibility*, not *balancing*;
  within a group, claim order is still first-eligible-with-headroom (ADR-0008).
  Weighted/affinity placement inside a group is out of scope and probably
  unnecessary given resource-governed admission (ADR-0005).
