# ADR-0052: The `starlark` step — in-process, fuel-metered, and off by default

Date: 2026-08-09

Status: **Accepted; building now.** Implements ADR-0017 tier 1 and resolves the
runtime choice that ADR-0017 explicitly deferred to build time. Amends
ADR-0001's transform-runtime clause by the mechanism ADR-0017 anticipated:
user transforms run in-process under `go.starlark.net`, **not** under wazero,
until a deployment needs mutual-untrust isolation. Extends ADR-0015 (capability
policy) with a deployment-level gate.

## Context

The mapper (ADR-0027's prerequisite, shipped) restructures records well:
rename, nest, constant, concat, default, coerce. What it cannot do is
**compute**. There is no arithmetic, so `qty × price` is not expressible. There
are no string functions. There is no per-field conditional. There is no way to
touch a list — the mapper builds nested *maps* only, so a repeating group
cannot be exploded into records or collapsed into a scalar, which is precisely
what an EDI interchange (one record per segment) or a nested JSON order needs.

Every real integration hits this within about a day.

### Why not JSONata

JSONata is the obvious candidate: it is the de-facto JSON transformation
language and arrives with an existing user base. It was rejected on a
structural ground, not a preference.

Every Go implementation of JSONata evaluates over `interface{}` by reflection.
Adopting one means materialising every record into `map[string]interface{}` to
evaluate an expression and converting back — the exact pattern ADR-0004 forbids
and the one the engine exists to avoid. It would place the incumbents'
architecture inside our hot path, on the single axis the product differentiates
on. A 1 GiB transform that holds 22 MiB today would not.

That reasoning generalises into the rule this ADR is really about:

> **A transform substrate must evaluate over `record.Value` natively. Anything
> that requires `interface{}` is disqualified, however good its ergonomics.**

JSONata's *syntax* remains available to us later — a documented subset could be
compiled to a plan over `record.Value` — and that option is deliberately left
open. What is rejected is JSONata as a *runtime*.

## Decision

### 1. Starlark, in-process, per record

ADR-0017 named WASM-under-wazero as the faithful reading of ADR-0001, with
`go.starlark.net` in-process as the fallback "if the WASM path proves
disproportionate to the value", and required the choice to be revisited at
build time. It is revisited here, and lands **in-process**, for reasons that
are about the threat model rather than about effort:

- **The runner is already the trust boundary.** A runner executes connector
  subprocesses, holds resolved secrets, and has network position inside the
  customer's perimeter. Wazero would isolate a Starlark bug from the runner
  process; it would not isolate the *script author* from anything, because the
  script author already controls the flow that runs there.
- **Starlark is not a general language.** No filesystem, no network, no
  `load()`, no ambient clock or randomness, and no unbounded loops once fuel is
  set. The residual risk that wazero addresses is a memory-safety bug in the
  interpreter itself — real, but small against a Go interpreter with no unsafe
  parsing of untrusted binary formats.
- **Per-record cost.** A wasm boundary crossing per record, plus marshalling
  across it, is the one thing that would make this feature too slow to use on
  the hot path — and a transform nobody can afford to run is not a feature.

ADR-0017's own design makes this reversible: **the step type names the
language, not the sandbox.** `{"type": "starlark"}` is identical either way, so
moving to wazero later changes no flow document. Multi-tenant runners with
mutually untrusting tenants are the trigger to revisit, and that is recorded
here rather than left implicit.

The unit is **one record**, via a `transform(rec)` entry point. Per-batch would
be faster and would expose batch lifetime to script authors — the one engine
contract that cannot survive contact with arbitrary code.

Returning `None` **drops** the record, so a script is also a filter.

### 2. It lives in the runner, not the engine

`engine` is stdlib-only and a leaf. The evaluator therefore lives in
`runner/internal/starlarkop` and attaches through the public
`stream.Pipeline.Apply`, so the engine never learns that Starlark exists and
needs no dependency exception. This is not bookkeeping: it keeps the property
that the engine can be reasoned about, benchmarked and fuzzed with no
third-party code in it.

### 3. Values adapt, they do not convert

A `record.Value` is exposed to Starlark through wrapper types implementing
`starlark.Mapping` / `Indexable`, reading the underlying value in place. No
document is materialised, and a script that touches two fields of a fifty-field
record pays for two fields.

Exact kinds keep their exactness inside the script. `KindDecimal` becomes a
Starlark value supporting `+`, `-`, `*` and comparison **exactly**, via
`record.ExactSum` and `record.Compare` — so `qty * price` on money is right, in
a language where it would otherwise silently become a float. Division is
**refused**: there is no exact quotient in general, and the honest answer is an
explicit rounding call rather than a silent one.

### 4. Bounded by fuel, memory and output size

- **Fuel, not wall-clock.** `SetMaxExecutionSteps` is deterministic, so a
  script always fits its budget or never does, and no result depends on machine
  load.
- **Output is bounded** on field count and nesting depth. Fuel bounds the
  *work* a script does; this is a separate budget bounding what it can hand
  back.
- **Evaluation is isolated on its own goroutine**, with `recover` and a
  wall-clock deadline. This contains an interpreter panic — which would
  otherwise take the runner process down and lose every in-flight task rather
  than the one at fault — and lets the caller abandon a script and fail the
  task instead of blocking a worker.

**And there is no per-script memory limit. This is a real, measured gap, not
an oversight to be discovered later.**

`go.starlark.net` exposes `SetMaxExecutionSteps` and `Cancel` and nothing for
allocation. Fuel counts STEPS, and one step can allocate a great deal:
`{"x": "a" * 200000000}` completed inside a **1000-step** budget having
allocated **190 MiB** (measured against this implementation before the deadline
existed). Neither the goroutine nor the deadline changes that — Go has no
per-goroutine heap cap, and an abandoned goroutine keeps its allocations until
it returns. Isolation buys containment and recovery, not a ceiling, and must
never be described as if it did.

What actually bounds memory today, in order of what does the work:

1. The **streaming model itself** — memory is a function of batch size, not
   payload size, so a script sees one record at a time and its output is
   bounded above.
2. The **output bounds** above, which stop a script handing back an unbounded
   structure.
3. A process-level ceiling (`GOMEMLIMIT`), which turns the failure mode from
   OOM-kill into GC pressure. Recommended for any deployment enabling code
   steps.
4. The **deadline**, which stops a thrashing script holding a worker.

A genuine per-script ceiling needs memory isolation the host language cannot
provide — which is precisely the trigger recorded in §1 for moving to the wasm
runtime. That is now a concrete reason with a number attached rather than a
hypothetical, and it is the strongest argument on the wasm side of that choice.

### 5. Determinism is enforced, because at-least-once demands it

No ambient clock, no randomness, no iteration-order surprises. This is a
correctness requirement, not tidiness: tasks are at-least-once (ADR-0002), so a
transform that returns different output on a retry produces a different result
for the same idempotency key. Test-mode sampling (ADR-0014) also has to
reproduce.

### 6. No I/O, and therefore no supply chain

`load()` is disabled and the predeclared environment is an explicit, reviewed
set of builtins. Tier 1 consequently has **no supply chain at all** — nothing
to import, pin, vendor, audit or revoke. That is a substantial security
property and the main reason tier 1 is worth building before ADR-0017's Python
tier, which has the opposite characteristic.

### 7. Secrets are out of scope, deliberately

A step's config may contain values resolved from `{"$secret":...}` refs
(ADR-0010). A script **cannot read its own config**. Without that rule, any
flow author would gain an exfiltration path for every secret used by the node
they attached a script to, and the secret would leave through the payload plane
where nothing is watching for it.

If a script ever needs a credential, it will be passed explicitly as a named
input, reviewed as its own decision.

### 8. Errors must not carry payload

A script error naturally quotes the value that caused it. That text would
otherwise travel to the hub in an execution report, turning a debugging
convenience into a payload leak — the failure mode already tracked as the
payload-in-error-strings backlog item, and a code step is the fastest way to
make it worse.

So: the hub receives the step id and an error class. The message and any value
context go to the **sampler** (ADR-0014) — runner-only, bounded, redacted, and
already the sanctioned channel for looking at data.

### 9. Off by default, fail closed

The sandbox bounds the blast radius. It does not answer *who is allowed*.

Today the hub is open-access (JIT default-admin, two coarse roles; real RBAC is
issue #16, deferred and blocked on the central identity platform). Shipping a
code step without a gate means **anyone who can author a flow can execute code
on a runner** — sandboxed, but still consuming CPU and memory and running
inside the network perimeter.

So a deployment must opt in explicitly (`SHIFT_ALLOW_CODE_STEPS=1`). A runner
without it **refuses the step type**, naming the setting. Cloud and
multi-tenant deployments leave it off; a self-hosted single-tenant runner turns
it on and accepts a boundary it already owns.

This is a stopgap and is recorded as one. RBAC is the real answer, and when
issue #16 lands, authority over code steps should move there.

## Consequences

**Good.** The gap that blocks real integrations closes: arithmetic, string
work, conditionals and list handling all become expressible, and exactly for
money. The escape hatch exists for everything the declarative layer will never
cover, so the mapper does not have to grow into a programming language by
accretion. No supply chain, and no new dependency in the engine.

**Costs, honestly.** A scripting language in the middle of the product is a
permanent support surface: people will write slow scripts, and "why is my flow
slow" becomes a question with a new answer. In-process means an interpreter bug
reaches the runner process rather than a wasm cage — accepted above, but it is
a real difference and the trigger to revisit is written down. Fuel limits will
occasionally stop a legitimate script, and the number will need tuning against
real flows rather than taste.

**Deliberately not now.** ADR-0017's Python tier (which brings the supply chain
back, and needs signing). Division on decimals without an explicit rounding
mode. Scripts reading config or secrets. A shared script library across flows —
that wants versioning and review, and inline scripts do not, so it is a
separate decision.
