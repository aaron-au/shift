# ADR-0043: A strict YAML 1.2 subset — YAML as a validation tool, not a parsing liability

Date: 2026-08-05

Status: **Designed.** Extends ADR-0042 §4d/§4e, which established that the
schema validator sits behind the parser (so any format the engine can parse
into records is validatable) and deferred YAML. This is that decision.

## Context

ADR-0042 refused YAML as a payload format on three grounds: nobody posts YAML to
an integration endpoint, it has no useful streaming form, and YAML parsers carry
a bad security record. The first two hold. **The third was overstated and is
corrected here**, because it changes what is worth building: modern parsers
already bound alias expansion (post-billion-laughs) and reject duplicate keys in
strict mode. The residual problem is not memory, it is **meaning**.

Aaron's proposal (2026-08-05) inverts the framing: rather than treat YAML as a
liability to be kept out, **make strictness the product**. A customer whose
pipeline emits YAML configuration can put it through the gateway and have it
verified — against a schema, and against a defined dialect — *before* it reaches
the application that will otherwise interpret it loosely.

That is worth doing because the loose interpretation is where the real damage
happens:

```yaml
country: no          # 1.1: boolean false.  1.2 core: the string "no".
version: 1.10        # float 1.1, not the string "1.10"
port: 08             # 1.1: invalid octal.  1.2 core: the string "08"
time: 1:30           # 1.1: 90 (sexagesimal). 1.2 core: the string "1:30"
```

Every line above is a real outage shape, and **no parser reports an error for
any of them**. Two conformant implementations disagree about what the document
says, silently. A validation tool that does not pin the dialect is validating a
document whose meaning it cannot state.

The second half of the proposal — running CI/CD pipelines, e.g. executing a
GitHub Action — is deliberately **not** in this ADR. Executing marketplace
actions is arbitrary third-party code on the runner that holds decrypted secrets
and connector subprocesses, which inverts ADR-0017's confinement model. The
aligned subset of that idea (CI as a trigger; a workflow *importer* in the
ADR-0032 mould; governed command execution per ADR-0028) stands on its own and
does not need a YAML runtime to exist.

## Decision

### 1. The dialect is YAML 1.2 core schema, and nothing else

`shift-yaml` (in `engine/format/yamlf`) accepts a **strict subset of YAML 1.2**.
The version is not inferred, negotiated, or best-guessed. A document is 1.2 or
it is rejected.

This is the whole point of the feature. Under the **1.2 core schema** the
ambiguities above simply do not exist: `no`/`yes`/`on`/`off` are strings, only
`true`/`false` are booleans, sexagesimal is gone, and a leading zero does not
mean octal (`0o` does). Pinning the dialect is what lets us say what a document
means rather than what some parser thought it meant.

| Construct | Verdict | Why |
|---|---|---|
| Block mappings, block sequences, comments | **accept** | the reason to use YAML at all |
| Flow collections (`{a: 1}`, `[1, 2]`) | **accept** | JSON is a subset of YAML 1.2; rejecting these would reject JSON |
| Plain, single- and double-quoted scalars | **accept** | |
| Block scalars (`\|`, `>`, with chomping) | **accept** | how real configuration carries embedded text |
| 1.2 core scalar resolution | **accept, and mandatory** | the entire point — see above |
| `%YAML 1.1` directive, or 1.1 typing | **reject** | two meanings for one document is the defect being fixed |
| Tags (`!!str`, `!Custom`, `!!python/…`) | **reject** | harmless in Go, arbitrary deserialization elsewhere; a validation tool must not bless a document that is unsafe *downstream* |
| Merge keys (`<<:`) | **reject** | not in 1.2 core — a 1.1 optional type. Known incompatibility, see §6 |
| Non-scalar mapping keys (`? [a, b]`) | **reject** | no record model has them, and no schema can describe them |
| Duplicate keys | **reject** | ambiguity with no correct resolution; last-wins is a convention, not a rule |
| Anchors/aliases (`&x` / `*x`) | **accept, budgeted** | genuinely used (GitLab CI, Compose); bounded per §2 |

Rejection is always an **error naming the construct and its line**, never a
silent downgrade. "This validated" must mean "this is unambiguous", or the
feature is worse than useless — it would launder an ambiguous document as
approved.

### 2. Bounds are part of the dialect, not tuning

Enforced by the parser, not by a caller who remembers to:

| Bound | Default | Failure mode it closes |
|---|---:|---|
| Document size | route `max_body_bytes` (ADR-0042 §5) | memory |
| Nesting depth | 64 | stack exhaustion, and no real config is deeper |
| Total nodes after alias expansion | 250,000 | billion-laughs, expressed as a budget rather than a ban |
| Alias expansion factor | 10× the un-expanded node count | the same, from the other direction |
| Anchors per document | 1,000 | table growth |

A document exceeding any bound is refused with the bound named. These are not
configurable per route: a limit a caller can raise is a limit an attacker can
raise.

### 3. Multi-document YAML is a record stream

`---` separated documents map exactly onto the engine's record model: **one
document, one record**. That is not a convenience — it means Kubernetes-style
manifests (the dominant multi-document shape) get `scope: records` from ADR-0042
for free: the first document is validated before the 202 and the rest stream.

Within a single document there is still no streaming, and there cannot be —
nesting means nothing is known until the document closes. ADR-0042's `scope:
body` bound applies unchanged.

### 4. Parse to `record.Value`, never to `map[string]any`

The parser emits into a `record.Batch` through `record.Builder`, exactly as
`ndjson` and `csvf` do. The validator from ADR-0042 then applies **unchanged** —
this is the payoff of putting the validator behind the parser rather than behind
a JSON decoder.

Consequence worth stating: adding YAML costs one parser, not a second validation
engine, and every ADR-0042 property (compiled once at plan build, closed keyword
set, conformance-tested, no per-request tree walk) carries over untouched.

### 5. Errors carry the YAML source position, not the converted position

A validation failure reports **line and column in the submitted YAML**, plus the
JSON Pointer path:

```json
{ "error": { "status": 400, "code": "input_invalid", "message": "…",
  "details": [
    { "path": "/spec/replicas", "line": 12, "column": 14,
      "message": "expected integer, got string \"three\"" }
  ] } }
```

This is a real design constraint, not a nicety: source positions must survive
the conversion into records, so the node-to-position map is built during parse
and retained for the validation pass. A config-validation tool that answers
"something at `/spec/replicas` is wrong" without saying *where in the file*
loses most of its value to the person holding the file.

### 6. Known incompatibilities, stated up front

Real YAML this will refuse:

- **GitLab CI templates using merge keys.** `<<: *defaults` is common and is
  1.1. It is rejected. If demand is real, the answer is a merge-key *expansion*
  at parse time with the result validated — deliberate, bounded, and opt-in per
  route — not a quiet acceptance of 1.1 semantics.
- **Anything with custom tags**, including some Home Assistant, Ansible and
  CloudFormation dialects (`!Ref`, `!GetAtt`). Those are not YAML documents so
  much as YAML-shaped DSLs, and validating them means understanding the DSL.
- **Documents relying on 1.1 typing**, which is the point rather than a
  regression.

Better to publish this list than to have customers discover it one document at
a time.

### 7. The same parser serves flow authoring

ADR-0042 §4e proposed YAML flow documents at the hub/CLI boundary. That uses
**this** parser, with one addition: the result is canonicalised to JSON before
storage, signing and serving. `pkg/flowdoc` remains the single authority on
document validity, the runner never gains a YAML dependency on the payload path,
and there is exactly one YAML implementation in the codebase to review.

### 8. A pinned schema catalog, not a maintained one

The obvious next question after "validate my configuration" is "against what?",
and the obvious wrong answer is for us to maintain schemas for Kubernetes,
GitHub Actions, Compose and the rest. That is a treadmill with no end, and one
we would lose.

**SchemaStore.org already publishes ~1,000 of them**, maintained by people whose
job that is. Combined with ADR-0042 §4c-ii's publish-time pinning:

1. The catalog ships as **pins**: name, source URL, digest, fetched-at.
2. The hub fetches at **publish time**, inlines the schema into the stored flow
   version, and records the digest.
3. Nothing is fetched at request time — the runner stays offline-capable and
   there is no SSRF surface on the payload path.
4. An upstream change cannot alter a published contract; it appears as a new
   catalog version an operator chooses to adopt.

We maintain a list of pins and the machinery to refresh them. We do not maintain
schemas.

## Consequences

**The accepted subset becomes a compatibility contract.** The moment a customer
validates a document against us, "we accept this" is a promise. The subset may
be *widened* in a minor version; narrowing it requires the same treatment as any
breaking API change (ADR-0023). This is the ongoing cost Aaron identified, and
it is real — but it attaches to a small, written grammar rather than to a
catalog of other people's schemas.

**We now own a written dialect specification.** `docs/dev/` gets the accept and
reject tables above as normative reference, and the parser's tests are written
against it. A subset defined only by its implementation is not a subset, it is
an accident.

**Validation becomes a use case, not just a guard.** "Put your configuration
through the gateway before it reaches your application" is a coherent thing to
demo, and it costs a parser rather than a product line — because the validator,
the async accept path, the 400-with-details envelope and the gateway route
already exist. That is the argument for doing it; positioning it *as* a product
against Conftest/OPA/Kyverno is a different decision and is not proposed here.

**One more parser to keep correct.** Mitigated the same way as everything else
in `engine/format`: differential testing against the **YAML test suite**
(`yaml/yaml-test-suite`), restricted to the accepted subset, with every reject
case asserted to produce a named error rather than a silent pass.

## Doctrine held

- No `map[string]any` on the hot path: the parser builds records (ADR-0004).
- The gateway does not parse payload semantics: YAML parsing is a runner
  concern, like schema validation (ADR-0042 §4a, ADR-0038 §3).
- Nothing is fetched at request time: schemas are pinned at publish (ADR-0011's
  model, applied to schemas).
- Fail closed: an unrecognised construct is an error, never an ignored
  annotation — the same rule ADR-0042 applies to unknown schema keywords.

## Alternatives considered

**Support YAML as most tools do (whatever the vendored parser accepts).** This
is the status quo everywhere and is precisely the defect: the tool's dialect is
an accident of its dependency, and it cannot state what a document means. It
also inherits 1.1 typing wherever the parser does.

**Write a full YAML 1.2 parser.** The specification is large and its edge cases
are legendary; a complete implementation is a project, not a feature. The subset
is what makes this tractable *and* what makes it valuable.

**Reject anchors entirely.** Simpler and safer, but it refuses GitLab CI and
Compose files wholesale — a large share of the configuration people actually
want validated. Budgeted expansion keeps the safety property without that.

**JSON only, as ADR-0042 concluded.** Still defensible for payload, but it
forfeits both the authoring win and the configuration-gateway use case for a
parser we would end up writing for flow authoring regardless.

## Open questions

1. **Merge keys** (§6). Opt-in expansion is designed but not built; needs a real
   customer document before the ergonomics are decided.
2. **Emitting YAML.** Nothing here writes it. A `@response` returning YAML, or
   the studio exporting a flow as YAML, needs a canonical emitter — a smaller
   problem than parsing, but not free.
3. **Catalog curation.** Which pins ship by default, who refreshes them, and
   whether a stale pin warns or fails. The mechanism is decided; the policy is
   not.
4. **CI/CD adjacency.** A GitHub Actions *importer* (workflow YAML → flow, with
   an honest report of what did not map) fits ADR-0032's model and this parser.
   Executing actions does not, and is refused above.
