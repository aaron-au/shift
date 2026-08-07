# ADR-0048: A test tier, and shapes that only exist in test

Date: 2026-08-06

Status: **§1/§3 built; §2 withdrawn; §5 revised (2026-08-07) and in build;
§4 pending.** §1 (tier as a hub-asserted roster
attribute) and §3 (test-marked dispatch) are in `hub/internal/store` +
`hub/internal/api`; see `docs/dev/06-hub.md`. §2 was withdrawn before being built (see
below). One deviation is recorded there: test-marked
work may also run on a PRODUCTION runner, because forbidding it would break
run-now in every deployment without test capacity. Depends on ADR-0041 (hub-asserted
identity), ADR-0033 (core-based licensing), ADR-0014 (capture). Pairs with
ADR-0047, which relies on the test tier as the place a forced connector upgrade
gets exercised.

## Context

Two problems meet here.

The first is commercial. Testing an integration usually means running it
against something real, because the alternative — a second licensed
environment — costs money the customer would rather spend on production
capacity. So people test in production, and the platform gets blamed for the
outcome. Aaron, 2026-08-06:

> Seeing the numbers we're able to get from 4 cores - I'm thinking we offer
> (purchased_cores / 2) for testing. Deployable runners that are only able to
> run test mode (including the API requests etc…)

The capacity is idle anyway, and the objection it removes is the one that
causes the damage.

The second is a gap in what "test" can even mean here. ADR-0014 capture samples
each stage's output, which tells you what a flow *did* — but a test run still
drives real sources and, more alarmingly, still writes to real sinks. Sampling
an SFTP `put` does not stop the file landing on the customer's server. A test
mode that faithfully performs every side effect is not a test mode.

## Decision

### 1. The test tier is a roster attribute, asserted by the hub

A runner cannot declare itself a test runner, for exactly the reason ADR-0041
gave for placement labels: a runner proves an identity, and the hub says what
that identity means. Tier is recorded against the runner row, alongside its
labels, and is asserted on every dispatch decision.

The inverse — a runner self-asserting `tier: production` to escape metering, or
self-asserting `tier: test` to receive work it should not see — is not
reachable, because nothing the runner sends is consulted.

### 2. ~~Entitlement is `purchased_cores / 2`~~ — WITHDRAWN (2026-08-07)

The original decision derived a separate test allowance from the licensed core
budget (ADR-0033), rounded down, minimum one core. It is withdrawn before being
built. Aaron, 2026-08-07:

> I don't think we need purchased / 2 as a concept. If the developer can only
> use test mode they can run it on their existing runner(s) without issue —
> they just select the appropriate runner for their test environment(s) and
> test mode. Same result — identical to the / 2 process.

That is right, and the reasoning is worth keeping because it deletes machinery
rather than adding it. The thing a customer actually wants is *"run this
against my test environment, and do not let it touch production"*. Two
mechanisms that already exist deliver exactly that:

- **placement** (ADR-0041 labels) chooses WHICH runner — the one that reaches
  the test SAP instance, not the live one;
- **§1's tier** stops that runner being handed production work by accident.

A core allowance would have added a third thing, bought nothing the first two
do not already give, and dragged the whole licensing budget into a feature that
does not need it. It would also have had a bad failure shape: "you are out of
test cores" is a refusal on the one path whose entire purpose is letting
somebody try something safely.

The commercial claim in §4 survives unchanged — test executions are metered
separately and excluded from billing — and it no longer needs a quota to sit
on. What bounds test usage is §3 (no schedules, no webhooks, so nothing
unattended) plus fair use, which is where §4 already put it.

### 3. Test runners take test-marked work only

Work reaches a test runner only when it carries a test marker, and only two
things can set that marker:

- the studio's run-now, and
- an API execution call that flags itself explicitly.

Not schedules. Not webhook routes. Not the hub's durable queue. This is not an
abuse control — it is the definition. A scheduled flow running on test capacity
is a production flow that happens to be metered wrong, and a webhook route
pointed at test capacity is a production ingress with no support commitment.

### 4. No caps. Metering and fair use instead

There is no record limit and no wall-clock limit on a test execution. Aaron,
2026-08-06:

> For there to be abuse there would need to be a person opening, using testmode
> over and over. It would be arduous at best. Agree we should still log this so
> we can understand abuse and instead have a fair use policy (back to no
> arbitrary limits)

That is right on the mechanics and right on the doctrine — arbitrary numeric
caps are exactly what ADR-0005 rejects. The abuse path requires a human
repeatedly clicking run, which is self-limiting, and the tier is bounded by §3
anyway: without schedules or webhooks there is no unattended way to consume it.

So: test executions are metered separately, excluded from billing, and
**visible** — in the account's usage export and on the dashboard. Abuse becomes
observable, and a fair-use clause in the contract handles the rare case.
Unmeasured is the failure mode to avoid, not uncapped.

### 5. Test-only behaviour is an OPTION ON A STEP, not a substitute node

*Revised 2026-08-07, before being built. The original design — three node types
(`@inject`, `@probe`, `@mock`) whose behaviour depended on execution mode — is
superseded. The invariant it was written to protect survives unchanged and is
the reason for the revision.*

> **A test-only shape is additive in test and strictly inert in production. It
> may never remove, replace or alter a production step.**

That invariant is what keeps this from becoming the classic trap where you no
longer deploy what you tested. The deployed data path is a strict *subset* of
the tested one, never a variant of it.

**The original §5 violated it.** `@mock` was a node the author put *in place
of* the real sink — deleting the connector and its config from the document —
and `@inject` did the same to the source. A replacement node is precisely
"removes or replaces a production step". The published document stopped saying
where data went, which is why `@mock` needed a publish-blocking 422 to be safe
at all: the block was a patch over a model error, not a design decision.

#### The shape

Mocking is a **property of the real connector step**, not a node that stands in
for it:

| | Where it lives | In test | Deployed |
|---|---|---|---|
| **mock** | option on a connector **sink** step | records what would have been written | inert — the connector writes |
| **test input** | option on a connector **source** step | emits the configured records | inert — the connector reads |
| **`@probe`** | a node of its own | taps the stream and reports | compiles to nothing |

The connector, its configuration and its version pin stay in the document in
every case. Production is complete **by construction**, so there is nothing to
refuse at publish and the 422 is deleted.

`@probe` stays a node because it is the only one of the three that genuinely
*adds*: it taps a point where nothing was, and replaces no production step. The
distinction is the invariant, applied consistently.

#### What the canvas shows

An enabled mock renders a **shift-decision** — a decision node the platform
owns: *test → mock, otherwise → connector*. It is drawn like any other branch,
so what runs in each mode is visible rather than implied, but it is **not
authorable**: no editing the predicate, no adding arms, no third branch. It is
derived from the checkbox and disappears when the checkbox clears.

That is the whole reason it is a distinct kind of node rather than an ordinary
router. Which brings us to the constraint that makes this design safe.

#### `running_mode` is NOT developer-routable

A developer may know which environment a flow is running in. A developer may
**not** author `if mode == test then A else B`.

The moment that is expressible, somebody writes "test hits the sandbox
endpoint, production hits the real one" — and production is then taking a path
nobody has ever executed. That is the same trap as deploying something other
than what you tested, wearing a costume. Making it inexpressible removes the
failure; a review check warning about it would only describe it.

This also means the **variable-gate** ADR-0031 leaves open is not needed here,
and this decision does not unblock it. One less feature to build.

#### Why this is a feature, not plumbing

Aaron, 2026-08-07, on the incumbent behaviour it fixes:

> Developer remains in control but we give a codified, repeatable canvas that
> allows for ultimate flexibility without having to make a new flow version to
> remove mocked data (a common problem I have with Boomi now).

That is the practical win and it falls straight out of the invariant. Because a
mock is inert in production, **it never has to be removed before shipping**.
The flow that runs in production is the same document the developer tested,
with the diversion simply not taken — no stripping step, no "remember to undo
the mock" version, no divergence between the tested artefact and the deployed
one.

And the control stays with the developer: unchecking the mock makes a test run
drive the real connector, for when hitting the real system is the point of the
test.

### 6. Where this leaves connector upgrades

ADR-0047 §4 forces a flow forward to `n-0.1` on republish, and §9 stages bulk
upgrades through a test run. Both assume this tier exists. Without it, "test
before you upgrade" is advice; with it, it is a step in the flow.

## Consequences

- The runner roster grows a tier column; dispatch and the gateway's placement
  selector both consult it. `runner/internal/leaseloop` gains nothing — the
  test marker travels with the work, and a test runner that receives unmarked
  work rejects it.
- Usage export grows a test dimension. Billing ignores it; the dashboard does
  not.
- `flowdoc` gains one node type (`@probe`) and two step OPTIONS (mock on a
  sink, test input on a source). All are inert in `Plan` lowering for a
  deployed flow, which keeps the engine unchanged. There is no publish-time
  validation, because the revised §5 leaves nothing incomplete to publish.
- Issue #60 (capture silently dropped on v3 DAG flows) has to be fixed before
  `@probe` is worth anything — a probe on a branch that never reports is worse
  than no probe.
- **Open:** whether a test execution may use production connections at all, or
  must resolve to a designated test connection set (ADR-0034 makes either
  expressible). `@mock` covers the sink case by construction, but a test run
  against a production *source* still authenticates as production. Leaning
  toward allowing it — reading is not the dangerous half — but it should be a
  visible property of the run, not an assumption.
