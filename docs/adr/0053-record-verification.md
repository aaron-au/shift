# ADR-0053: Record verification — a router whose predicate is a schema

Date: 2026-08-09

Status: **Designed; build not started.** Extends ADR-0029 (the v3 DAG) by adding
one node type and no new execution concept. Extends ADR-0042 §4, which
established synchronous schema verification for *webhook input*, to data a
connector **fetched**. Consumes `engine/schema` (the ADR-0042 validator) as-is.
Supersedes nothing. Closes register rows TC-025 and TC-026
(`docs/assurance/test-conformance.md`).

## Context

SHIFT is an iPaaS: effectively 100% of the bytes it handles are user-driven —
authored by a customer, or fetched by a connector from a system neither the
customer nor SHIFT controls. Two requirements follow, and they are different
requirements that a single mechanism has to serve:

1. **Survive it.** Hostile or malformed data must not break the platform. This
   is largely done — the TC-019…TC-024 sweep bounded every input unit, capped
   decompression, and closed an FTP injection and two zip-slips.
2. **Recover from it, and say something true about it.** A single bad record
   should not have to destroy a run, and the customer should be able to see that
   the data they received was valid — or exactly how it was not.

The second is not a testing gap; the behaviour does not exist. ADR-0031's error
model is **step-level**: a record that fails a step fails the step, which fails
the flow, which dead-letters. There is no "set this one aside and keep going",
and no per-record account of what was rejected and why.

### What we are deliberately not building

The obvious shape — a *quarantine* facility that captures bad records — was
designed and rejected. Every version of it grows the same features: a place to
put records, a retention policy, an access model, an eviction story, a way to
replay. That is a queue with a store attached, owned by the platform, holding
customer payload.

That is the Kafka lesson (CLAUDE.md, "Lessons already paid for") arriving by a
different road, and it breaks the hub-and-spoke doctrine at the first step: the
hub never touches payload, and a runner is disposable at any moment, so neither
is a place data can live. It would also make SHIFT a data processor for records
it was only ever supposed to move.

## Decision

### 1. Verification is a router whose predicate is a schema

`router` already exists (ADR-0029): it partitions records across arms by a match
function, and the arms provably conserve records (asserted by the generative
topology test, TC-005). A verify node is that node with a schema-based
predicate:

```
source ──▶ @verify ──valid──▶ map ──▶ sink
                └──invalid──▶ (any sink the author chose)
```

This is the whole design, and its value is what it does **not** add:

- **No new execution concept.** No per-record disposition model, no side
  channel, no second error path. Fan-out already partitions; this is a
  partition whose predicate happens to be `engine/schema`.
- **No platform storage.** The invalid arm is an ordinary edge to an ordinary
  sink. Where rejects go, how long they live, who can read them, and how they
  are encrypted are all answered by a destination the developer already owns and
  already governs. SHIFT stays a pipe.
- **Opt-in is structural.** No `@verify` node means today's behaviour, byte for
  byte. Nothing deployed changes on upgrade, and there is no flow-level flag
  whose default anyone has to argue about.
- **It is visible.** The node is on the canvas and so is the edge to the reject
  destination. "Where does bad data go" is answered by looking at the flow,
  which is a better answer than any amount of documentation.

An author who genuinely wants to discard rejects wires the existing `@discard`
sink (ADR-0024) explicitly. That reads as a decision. Silently dropping does
not, and is not offered.

**A `@verify` node with no invalid arm is a validation error.** "Recover from
bad data" with nowhere to put it is just "lose bad data quietly", and quietly is
the property being removed. The author must say where rejects go, even if the
answer is `@discard`.

### 2. Two placements, because they answer different questions

- **At the connector boundary** — does what the source *returned* match what it
  promised? A failure here means the supplier broke their contract. Declared
  per-action, alongside the config schema a connector already carries
  (ADR-0018).
- **Mid-flow, as a step** — does the data we *built* match what we intended?
  A failure here means our mapping, join or merge is wrong.

Both use the same validator and the same node semantics. They are kept distinct
because conflating them destroys the diagnosis: "the API changed" and "our
transform is broken" produce identical symptoms downstream and require opposite
responses.

### 3. The schema is author-owned and pinned in the flow

The schema lives in the flow document. Connector-declared schemas **seed the
editor** — "start from what this connector says it returns" — and are never the
authority.

This is not a convenience preference. **A schema supplied by the source cannot
detect that source changing.** If the expected shape were fetched from the
supplier, a supplier altering their API would alter the expectation with it, the
flow would keep passing verification, and the data would silently change shape —
the exact event verification exists to catch. The pin *is* the baseline.

Two further properties fall out:

- **Point-in-time is the feature.** Re-deriving the schema each run would
  re-baseline continuously and detect nothing.
- **A schema change is a flow version change.** Flows are versioned and only
  published versions fire (ADR-0012, ADR-0047), so the expected shape moves only
  when a human publishes — reviewable, auditable, revertible. No third party can
  change what your flow considers valid.

When a source legitimately changes, the author re-seeds, sees a diff against the
pinned schema, and decides. That is a deliberate act, which is the point.

### 4. Structural and semantic rules are the same mechanism

- **Structural** — types, required fields, nesting. Discoverable from a
  connector, a database, or an API description. Mostly boilerplate.
- **Semantic** — `amount > 0`, `currency` in a known set, "this field is
  required when that one is present". Never discoverable from any source,
  because it encodes the *business's* rules rather than the API's — and it is
  usually where the real defects are.

JSON Schema expresses both, and the subset `engine/schema` already implements
covers `minimum`/`maximum`, `enum`, `required`, `dependentRequired` and the
type/shape assertions. So this is one mechanism; the distinction is only about
where the content comes from, and it matters for the studio's authoring flow,
not for the engine.

### 5. Compile once at plan build; never fetch at run time

Schema discovery is a **design-time** action: one call, in the studio, when the
author asks to seed from a connector. At execution there is no fetch.

The compiled validator is built with the plan, exactly as ADR-0004's path
contract requires of `record.ParsePath` ("compile once at pipeline build, never
per record"). Per-record cost is evaluation only, and the TC-006 allocation
budgets apply to it like any other hot-path operator.

**A malformed or uncompilable schema fails at plan time, not per record.** This
is load-bearing: a schema that fails to compile and is treated as "no record
matches" would route 100% of traffic to the reject arm and look exactly like a
supplier outage. `engine/schema` already separates `Compile` from `Validate`, so
the split exists; it must be honoured at plan build.

### 6. The report is metadata, and only metadata

Verification produces per-step counts: records in, records passed, records
rejected, and rejection counts **by reason** — field path plus the rule that
failed (`required`, `type`, `enum`, …).

Field names and rule identifiers are metadata; **field values are payload and
never appear**. So the report rides the existing per-step execution report to
the hub (ADR-0039) without touching the two-plane split (ADR-0016). This is the
"assurance" half of the requirement, and it costs no payload storage anywhere.

An author who needs to see actual failing *values* has that already: test-mode
capture (ADR-0014) is bounded, secret-redacted, runner-only and ephemeral, which
is the correct set of properties for looking at payload and precisely why it
should not be reinvented here.

## Consequences

- A single bad record no longer has to destroy a run — but only in flows whose
  author asked for that, and only to a destination they chose.
- The platform gains no store, no queue, no retention policy and no new place
  customer data can rest.
- Rejects land somewhere with **different access control from the main
  destination**, and rejected records are disproportionately likely to hold the
  malformed PII that caused the failure. The reject sink therefore takes the
  same secret-ref and connection discipline as any other sink (ADR-0010,
  ADR-0034), and the studio must show the destination rather than defaulting it.
- ADR-0050 (structured queries + schema discovery) stops being a prerequisite
  and becomes a UX improvement to something that already works.
- Verification is a router, so it inherits the router's semantics exactly —
  including the record-conservation property TC-005 asserts. It also inherits
  the router's defects until they are fixed (TC-027/TC-028).

## Alternatives considered

- **A quarantine store owned by the platform** — rejected above: it is a queue
  with a retention policy, holding payload, in a system whose hub must never see
  payload and whose runners are disposable.
- **Per-record error dispositions on every operator** (keep/drop/quarantine as a
  return value) — rejected: it changes the engine's batch contract, touches
  every operator, and makes "what happened to my record" a property of each
  step's implementation rather than of the flow's shape. The router already
  expresses branching, and expressing it once is worth more than expressing it
  everywhere.
- **Validating inside the `starlark` step** (ADR-0052) — possible today, and
  authors will do it. Rejected as the *platform* answer: it is invisible to the
  canvas, produces no structured reason codes, is off unless
  `SHIFT_ALLOW_CODE_STEPS=1`, and makes every flow's validation a bespoke script.
- **Flow-level "skip bad records" switch** — rejected: it changes the behaviour
  of every deployed flow on upgrade, and it cannot say where the skipped records
  went.

## Resolved (2026-08-10)

The three questions this ADR opened are now answered.

### Where the connector-boundary check runs: in the RUNNER

The question was originally posed as "`sdk.SourceAction` or the runner", which
obscured the real geometry. Both options sit on the runner host: connectors are
gRPC subprocesses the runner launches (ADR-0001), so the choice was between the
connector's own process and the runner's.

**It cannot be the hub.** Verification reads payload, per record, and the hub
never touches payload (ADR-0016). That is not a preference to trade off.

The split that does apply is ownership, and it already follows the control/data
plane line:

- **The hub owns what "valid" means.** The schema lives in the flow document —
  authored, versioned, and published there, seeded at design time from
  connector-declared schemas (ADR-0018). Nothing about that changes.
- **The runner owns checking it.** One implementation, applied to every
  connector including third-party ones that never adopt anything.

Putting it in the SDK would be marginally cheaper and would make the guarantee
conditional on each connector author implementing it. TC-020 is the standing
lesson against that shape: a property that holds only where someone opted in is
not a platform property.

### A rejection reason carries the field's KIND, for type failures only

`type` failures report the kind found ("expected integer, got string"), because
there the kind IS the diagnosis and a type name identifies no data.

`enum`, `const` and `pattern` failures report the path and the rule alone. The
kind is already implied by those rules, and the genuinely useful detail is the
value — which this design deliberately does not carry. An author who needs
values has test-mode capture (ADR-0014): bounded, redacted, runner-only, and
already the right set of properties for looking at payload.

### The report is counts, so there is no sampling threshold

The open question asked where the report should switch from enumerating
rejections to counting them. It dissolves: the report is counts keyed by
**(field path, failed rule)**, so its size is bounded by the number of distinct
rules in the schema — fixed at plan build — and NOT by the number of records. A
million rejections produce the same report as ten.

**One bound is still required.** A schema using `additionalProperties` or
`patternProperties` derives field paths from the DATA, so the distinct-path set
is unbounded. Those report the first N distinct paths plus an overflow count.
That is the only place a cap is needed, and it is a property of the schema
rather than of the traffic.

## Open questions

None. This ADR is ready to build.
