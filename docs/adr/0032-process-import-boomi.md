# ADR-0032: Process import from external iPaaS platforms (Boomi first)

Date: 2026-07-29
Status: **Designed (build deferred)**

## Context

SHIFT's fastest route to displacing an incumbent iPaaS is not to ask a customer to
re-author every integration by hand — it is to **read their existing designs and
translate them into native SHIFT flows**. The project owner has named this the
absolute ideal goal: an **"Import from X"** function that connects to an existing
platform (Boomi, Workato, MuleSoft, …), downloads its process/component definitions,
and produces working SHIFT flows. It is a rip-and-replace **migration and adoption
lever** — the shortest path off a competitor.

**Boomi is v1.** It has full platform APIs (AtomSphere / Platform API), a well-defined
XML component model, and we have extensive real Boomi designs and live access to test
against. Boomi is "simple to ingest but difficult to get accurate": the wire format is
tractable, but faithful semantic translation is the hard part.

This sits on top of decisions already locked:

- **Flow model v3 (ADR-0029):** SHIFT flows are a validated DAG — one source → ops →
  sink, plus **tee** (concurrent fan-out, one goroutine per branch), **router**
  (per-record data-predicate fan-out), **merge** (concat / keyed-join fan-in), and
  outcome edges `onSuccess` / `onComplete` / `onFailure`. These v3 constructs are the
  **target** the Boomi shapes translate onto.
- **Flow error model (ADR-0031):** `onFailure` handlers (try/catch, dead-letter); the
  reserved built-in terminal **`@stop`** (deliberate early end *as success*);
  `@discard` / `@response` terminals; `@webhook` source.
- **Transforms:** `filter` / `project` / `coerce` / `flatten` / `aggregate` today; a
  declarative **mapper** (target-field ← source-expression) is planned. Custom-code
  steps `starlark` / `python` / `subflow` are *reserved but unbuilt* (ADR-0017).
- **Streaming doctrine:** SHIFT streams records with no whole-payload buffering. Boomi
  is a **document/batch** model — a process operates over a *set* of documents, often
  fully materialized. This is a genuine **semantic divergence** the translator must
  handle: map to streaming where the semantics preserve, flag where they do not.
- **Secrets (ADR-0010):** envelope-encrypted; `{"$secret":"name"}` refs resolved
  runner-side; plaintext never in the queue, logs, or payload.
- **Two planes (ADR-0016):** control (hub↔runner, metadata only) vs data (payload never
  touches the hub).
- **Importer/discovery thesis (ADR-0025/0027):** SHIFT already frames discovery
  **adapters** (openapi / odata / wsdl / sql-schema) and reusable **auth profiles**
  (OAuth2 / api-key / basic / bearer). The Boomi importer is a *new kind of adapter* — a
  **process-import adapter** — reusing auth profiles (Boomi API auth = token / basic).

## Decision

Build an **"Import from X" process-import adapter** subsystem, Boomi first, that fetches
foreign process definitions, translates them into native SHIFT flowdocs, and emits a
**draft flow plus a migration report**. The guiding rule throughout: **translate onto
SHIFT-native constructs, never distort SHIFT to mimic the source; where a faithful
mapping is impossible, flag it honestly rather than fake it.**

### 1. Where the importer runs — runner-side

The importer is a **runner-side import adapter**, not a hub feature.

- Boomi API credentials and every API fetch stay **off the hub**, honoring the ADR-0016
  two-plane split: the hub never holds source-platform creds and never parses foreign
  payloads. The Boomi Component XML *is* payload-shaped third-party data; parsing it on
  the hub would drag the control plane into the data plane.
- The runner authenticates (auth profile, ADR-0027), enumerates, exports, parses, and
  **translates runner-side**, then delivers a **draft flowdoc + migration report** to
  the hub as a *new draft flow version* — pure metadata (a validated `pkg/flowdoc`
  document the hub already knows how to store).
- Rejected alternative — **hub-side importer**: it would centralize Boomi creds on the
  control plane, force the hub to parse arbitrary vendor XML (attack surface + a payload
  concern the hub is doctrinally forbidden from touching), and couple import throughput
  to the HA hub. Runner-side keeps creds and parsing at the edge and lets import scale
  horizontally like any other runner work.

### 2. The pipeline

```
connect (auth profile)
  → enumerate   (Boomi Platform API: ComponentMetadata + ComponentQuery)
  → export      (fetch each process's Component XML)
  → parse       (build Boomi's shape graph from the XML)
  → translate   (shape-by-shape → SHIFT flowdoc constructs)
  → validate    (pkg/flowdoc — the same authoritative validation as any flow)
  → emit        (draft flow version + migration report, to the hub as metadata)
```

The output is explicitly a **DRAFT the author reviews and completes in the studio** —
never an auto-published black box. Import gets the customer 70–90% of the way; the
developer reviews divergences, re-enters secrets, and publishes. Trust comes from the
report, not from a promise of perfection.

### 3. Shape mapping (Boomi → SHIFT)

| Boomi shape | SHIFT construct | Notes |
|---|---|---|
| Start (connector) | connector **source** | trigger/listener Start → `@webhook` source (ADR-0031) |
| Start (no data) | `@webhook` or scheduled source | mode inferred from Start config |
| Connector — get / query / listen | connector **source** | `direction` follows the verb (ADR-0024) |
| Connector — send / create / update | connector **sink** | side-effecting; must honor idempotency key |
| Decision | **router** (2-way) | predicate → true/false edges |
| Route | **router** (n-way) | value-based fan-out |
| Branch | **tee** *or* chain | **see §4 — sequential vs concurrent** |
| Try/Catch | `onFailure` handler | maps to the ADR-0031 error edge |
| Stop | **`@stop`** | deliberate early end as success (ADR-0031) |
| Return Documents | **`@response`** | synchronous reply terminal (ADR-0024) |
| Map | transform ops + **mapper** | field maps → mapper; functions → ops or flag |
| Message / Set Properties (static) | `project` / const transform | literal / templated field set |
| Document / Dynamic Process Properties | flow **variables** | **DEFERRED feature → flag** |
| Data Process (Groovy / JavaScript) | **UNSUPPORTED** → flag | future `starlark` / `python` (ADR-0017) |
| Business Rules | **UNSUPPORTED** → flag | manual re-author; future custom-code step |
| Process Call / subprocess | `subflow` (reserved) → flag | needs the unbuilt subflow step |
| Cleanse | ops where expressible, else flag | validation rules → `filter` / `coerce` |
| Split / Combine Documents | `flatten` / `aggregate` where possible | batch-shape ops; else flag |
| Find Changes (CDC) | ops or flag | CDC semantics rarely map 1:1 — usually flag |
| Notify | connector sink or flag | maps if an equivalent connector exists |

Unmapped or partially-mapped shapes never fail the whole import — they land in the flow
as a `needs-manual` / `unsupported` marker and are itemized in the report.

### 4. The critical divergence — Boomi Branch is SEQUENTIAL, SHIFT tee is CONCURRENT

Boomi's **Branch** shape runs its branches **in order**: branch 1 runs to completion
before branch 2 begins. SHIFT's **tee** runs branches **concurrently** — one goroutine
per branch (ADR-0029). A naive Branch→tee mapping silently changes execution semantics,
which is exactly the failure this ADR exists to prevent.

The translator must **detect** the Branch shape and choose:

- If the branches have observable **ordering dependence** (later branch reads state a
  prior branch wrote, or the customer relies on sequencing), map to a **sequential
  chain** that preserves order.
- Otherwise map to a **tee**, and **emit an explicit divergence warning** in the report:
  "Boomi Branch mapped to concurrent tee; original ran branches sequentially — verify no
  cross-branch ordering dependency."

Never silently pick concurrency for a sequential source. This is the archetype of
**feature compatibility without breaking our ideals**: translate faithfully onto the
native construct, flag the semantic gap, and let the developer decide.

### 5. The migration report

The report is the deliverable that makes an "accurate but honest" import trustworthy.
Per process it contains:

- **Coverage summary** — % of shapes mapped, counts by status.
- **Per-shape status** ∈ `{ mapped, mapped-with-divergence, needs-manual, unsupported }`
  with a reason for anything not cleanly `mapped`.
- **Secrets requiring re-entry** — every Boomi encrypted value / password turned into a
  `{"$secret":"name"}` placeholder (see Doctrine), listed so the author knows exactly
  what to re-enter before publishing.
- **Unmapped connector types** — Boomi connectors with no SHIFT equivalent yet.
- **Doctrine divergences** — sequential-Branch→concurrent-tee, document-model→streaming,
  and any other semantic gap, each with a plain-language explanation.

A green coverage number is meaningless without this itemization; the report is a
first-class output, versioned alongside the draft flow.

### 6. Accuracy strategy

Start with a **covered subset** of the most common shapes (§3) and **report coverage %**
honestly. Iterate against the owner's real Boomi designs using **golden-file / round-trip
tests**: given process XML → expected flowdoc + expected report, asserted deterministically.
Expanding shape coverage is **incremental**; at every stage the report tells the exact
truth about what was and was not translated. "Difficult to get accurate" is managed by
never overclaiming — an unsupported shape is a labeled marker, not a silent omission.

### 7. Extensibility

The importer is a pluggable **process-import adapter** interface:

```
enumerate(ctx, auth) → []ProcessRef
export(ctx, ref)     → ComponentGraph      // vendor-native shape graph
translate(graph)     → (flowdoc.Document, Report)
```

Boomi is the first implementation; Workato and MuleSoft follow as additional adapters.
All adapters **reuse the ADR-0027 auth profiles** for connect, and all emit the same
`(draft flowdoc, migration report)` pair — so the studio review/publish flow and the
report format are adapter-agnostic. Each new adapter is *enumerate + export + a
shape-translator*; the runner-side hosting, validation, and hub-draft delivery are shared.

## Doctrine held

- **Hub never sees source creds and never parses foreign definitions.** The adapter runs
  runner-side; the hub receives only a validated draft flowdoc + report as metadata
  (ADR-0016).
- **Secrets are never imported in plaintext.** Boomi stores passwords/encrypted values
  encrypted and cannot export them in the clear; the translator emits
  `{"$secret":"name"}` placeholders (ADR-0010) that require re-entry — plaintext never
  enters SHIFT's queue, logs, or payload.
- **Translate to SHIFT-native, never distort SHIFT.** Boomi shapes lower onto v3 DAG
  constructs (ADR-0029) and the ADR-0031 error/terminal model; we do not add
  Boomi-shaped machinery to SHIFT to flatter the source.
- **Draft, not auto-publish.** Import produces a reviewable draft; a human completes and
  publishes it. No black-box auto-migration.
- **Streaming preserved or divergence flagged.** Where Boomi's document/batch semantics
  map cleanly to streaming, they do; where they cannot, the report says so rather than
  silently materializing whole payloads.

## Consequences

- A customer can move a real Boomi estate onto SHIFT in hours of review instead of weeks
  of re-authoring — the strongest possible adoption lever, and a concrete demonstration
  of the v3 flow model's expressive range.
- The migration report becomes a standing artifact type (stored, versioned, surfaced in
  the studio) — reused by every future adapter.
- Coverage is a moving target: early imports will carry many `needs-manual` /
  `unsupported` markers. That is acceptable *because it is honest*; the report never lies
  about coverage.
- Several mappings depend on **unbuilt** SHIFT features — flow **variables** (Process
  Properties), the **mapper** (Boomi Map), and `subflow` / `starlark` / `python`
  (custom-code, ADR-0017). Those shapes flag as `needs-manual` until the features land;
  import coverage ratchets up as they do.
- New runner surface: outbound calls to third-party platform APIs, governed by auth
  profiles and the runner's existing network guards.

## Open questions

1. **Document-model → streaming fidelity.** How faithfully can Boomi's fully-materialized
   document semantics (aggregating operations, whole-set Maps) be re-expressed as
   streaming ops before we must flag rather than translate?
2. **Map / profile system depth.** How much of Boomi's Map + profile machinery do we
   auto-translate into the declarative mapper vs. leave as a `needs-manual` mapper stub
   for the author?
3. **Connector configs vs. shapes.** Do we import Boomi connector *configuration*
   (endpoints, paths, operation settings) as pre-filled SHIFT connector config, or only
   the shape/topology and leave config to the author?
4. **Subprocess graphs.** Boomi Process Call / subprocess needs the reserved `subflow`
   step — do we import the subprocess as a separate draft flow and wire a `subflow`
   reference, and how do we present that dependency graph?
5. **Versioning / re-import.** Is a re-import an idempotent update of the existing draft
   (diff/merge against prior mappings) or always a fresh draft? How do we track which
   SHIFT flow originated from which Boomi component version?
