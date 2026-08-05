# ADR-0042: Async by default at the edge — 202 + status URL, with synchronous input verification

Date: 2026-08-05

Status: **Designed.** Amends ADR-0038 (the gateway's request/response
lifecycle) and extends ADR-0024 (`@response` and synchronous run). Nothing in
either is retracted: `@response` remains the synchronous path, and it becomes
the explicit opt-in rather than the accident of how the exchange happens to be
implemented.

## Context

The gateway holds the caller's connection open for the whole flow. A request
arrives on the public listener, a parked runner picks it up, the flow runs, and
the runner delivers a response that the gateway streams back to the original
caller (ADR-0038 §4). That is a synchronous request-reply model, and it is the
*only* model the edge currently offers.

For a request-reply API that is exactly right. For everything else — which is
most integration work — it is wrong in three separate ways.

**It makes flow duration a public-facing property.** A flow that pulls 40,000
rows out of an ERP and pushes them to a warehouse takes minutes. Held as an
open HTTP exchange, that is a caller timeout, a load-balancer idle timeout, and
a retry storm when the caller gives up and re-posts work that is already
running.

**It makes the gateway stateful for the wrong duration.** Every in-flight
request occupies a registry entry, a delivery timeout, and a goroutine on both
the gateway and the runner, for as long as the *slowest backend in the flow*
takes to answer. The benchmark shows this plainly (`docs/bench-gateway.md`):
against the `legacy-200ms` backend the same gateway that serves 26,852 req/s
against an instant backend serves 239. The gateway is not the bottleneck there —
it is simply being asked to hold hands for 200 ms at a time. Aaron's framing
(2026-08-04): *this would mean less locking on the gateway and align with our
standard.*

**It buries the one failure the caller could have fixed.** A malformed payload
today is accepted with a 200, flows into the engine, fails somewhere in the
middle, and lands in a dead-letter queue that somebody reads tomorrow. The only
party who could have corrected it is the caller, and the only moment they still
had the data in hand was the moment they sent it.

The platform's own doctrine already leans async: the hub queue is at-least-once
with idempotency keys (ADR-0002), triggers are decoupled from execution, and
the scheduler has always been fire-and-record. The edge is the one place that
still pretends an integration is a function call.

## Decision

### 1. Async is the default; `@response` is the synchronous opt-in

A flow whose happy path terminates at **`@response`** is synchronous: the
caller's exchange stays open and receives the flow's output, exactly as
ADR-0024 specifies today. **Every other flow is asynchronous**: the runner
answers `202 Accepted` with a status URL as soon as the request is accepted,
then executes with the caller already gone.

The choice is therefore made by *placing a node on the canvas*, not by setting
a flag. There is no `mode: sync|async` field anywhere in this ADR, and that is
deliberate. A separate switch would be a second source of truth about a
question the graph already answers: a flow ending at `@discard` has nothing to
say to the caller, and a flow ending at `@response` exists precisely to say
something. Making the terminal node the declaration keeps them from ever
disagreeing.

**The gateway does not know which mode it is serving, and must not.** It hands
work to a runner and streams back whatever comes; only the *timing* of the
delivery changes. This is why the change costs the gateway no new code path and
no new state — the exchange lifetime simply collapses from "flow duration" to
"time to validate and accept", which is milliseconds.

### 2. The 202 body

```http
HTTP/1.1 202 Accepted
Location: https://api.example.com/_shift/tasks/tsk_01JZ8Q3F2N7K
Content-Type: application/json
```
```json
{
  "task": "tsk_01JZ8Q3F2N7K",
  "flow": "orders-ingest",
  "status": "accepted",
  "status_url": "https://api.example.com/_shift/tasks/tsk_01JZ8Q3F2N7K",
  "accepted_at": "2026-08-05T04:11:07Z"
}
```

`Location` carries the same URL as `status_url`. Both are present because both
are load-bearing for different callers: `Location` is what a generic HTTP
client library will follow or surface, and an explicit body field is what a
human reading the response in a terminal will find. Neither is a redirect — a
202 with a `Location` is defined as "here is where the status lives", which is
what this is.

### 3. The status endpoint is a built-in gateway route

`GET /_shift/tasks/{id}` is served by every gateway without being configured,
under a reserved path prefix (`/_shift/`) that route configuration may not
claim. It is dispatched exactly like any other request: to an eligible parked
runner, which asks the hub (`GET /api/v1/tasks/{id}`, the runner realm) and
returns the answer.

This preserves the direction property that ADR-0038 exists to hold. **The
gateway still never dials inward.** It does not learn the hub's address, hold a
database connection, or cache task state; it asks a runner, and runner→hub is
already an allowed direction.

The response carries **metadata only** — never payload, in keeping with
ADR-0038 §3:

```json
{
  "task": "tsk_01JZ8Q3F2N7K",
  "flow": "orders-ingest",
  "state": "running",
  "accepted_at": "2026-08-05T04:11:07Z",
  "started_at": "2026-08-05T04:11:07Z",
  "finished_at": null,
  "records_in": 41233,
  "records_out": 41233,
  "attempt": 1,
  "error": null
}
```

On failure, `error` carries the canonical error shape from ADR-0031 (`step`,
`code`, `message`) — the step id and class of failure, with no record content.
A caller learning "step `post-to-warehouse` failed with `connector_timeout`" is
being told something operationally useful and nothing confidential.

**Authorization.** The task id is unguessable, and that is not sufficient on
its own. The gateway authenticates the status request against **any credential
configured on this gateway** and stamps the resulting `X-Shift-Principal`; the
hub records the accepting principal on the execution row and returns **404** —
not 403 — when the caller's principal does not match. A 403 would confirm that
someone else's task exists under that id, which is exactly the fact being
protected.

Matching against any configured credential (rather than the specific route's)
is a deliberate simplification: a deployment with twenty routes and twenty
tokens should not need a twenty-first for status reads. The principal check
does the real authorization work; the token match only establishes that the
caller is a known party at all.

### 4. Input verification: the schema is what makes 202 mean something

An unverified 202 is a promise the platform has not checked it can keep. So the
flow's entry step may declare an **input schema**, and when it does, the runner
validates the request *before* accepting it:

```json
{
  "id": "in",
  "type": "source",
  "connector": "@webhook",
  "input": {
    "scope": "body",
    "schema": { "$ref": "..." }
  }
}
```

- **Valid** → `202` with the status URL, as above.
- **Invalid** → `400` with the ADR-0023 error envelope, `code:
  "input_invalid"`, and a `details` array of per-field failures
  (`{"path": "/lines/2/qty", "message": "expected integer, got string"}`).
  Field-level, because "your JSON is wrong" is not a fixable error message.
- **Too large to verify** → `413`, see `scope` below.

This is the change with the largest practical effect on the product. It moves
the most common class of integration failure — a caller sending the wrong shape
— from an asynchronous dead-letter discovered hours later to a **synchronous
400 delivered while the caller is still holding the data and still in the code
path that produced it**. It also makes the 202 honest: it now means "this is
well-formed and will run", not merely "the bytes arrived".

#### 4a. Validation runs on the runner, never on the gateway

The gateway is stdlib-only by depguard rule, and that is a security property
rather than a style preference (ADR-0038 §3): it is the one box that sits in a
DMZ. A JSON Schema evaluator is a non-trivial parser applied to attacker-shaped
input, with well-known denial-of-service shapes (pathological `pattern`
regexes, deeply nested or recursive `$ref`). Putting one on the public edge
would hand an unauthenticated caller a CPU amplification primitive on the most
exposed component in the system.

It also would not work: the gateway does not hold flow documents, by design
(§6 of ADR-0038 — the hub pushes routes, not flows). The runner already has the
document, already compiles it into a plan, and is already the only component
permitted to look at payload.

#### 4b. `scope` is where the streaming doctrine bites

You cannot synchronously verify what you have not read, and reading a 1 GB
request in order to verify it is exactly the whole-payload buffering this
platform exists to avoid. So `scope` names which of the two honest options the
flow wants:

| `scope` | Meaning | Verification |
|---|---|---|
| `body` (default) | the request is one document — an API-shaped call | the **entire** body is validated before the 202; a body over `max_validate_bytes` (default 1 MiB) is refused with `413` |
| `records` | the request is a stream — NDJSON or a JSON array | the **first record** is validated before the 202; the rest stream, and a later bad record takes the flow's error path (ADR-0031) |

`records` scope is explicitly a weaker guarantee, and the ADR says so rather
than pretending otherwise. It catches the overwhelmingly common real failure —
a caller with the wrong field names or the wrong types, which is wrong from
record one — without buying that at the cost of buffering the stream. What it
cannot catch is record 40,000 being malformed while record 1 was fine, and that
case remains a dead-letter, because there is no version of this system where it
could be anything else.

#### 4c. A validator, not a JSON Schema library

The runner validates with a **compiled subset validator** in `engine/schema`,
operating on `record.Value` and compiled once at plan build — never a
per-request tree walk over `map[string]interface{}`, which would violate the
engine contract in the one place payload is hottest.

Supported keywords are a deliberate, closed set: `type`, `required`,
`properties`, `items`, `enum`, `const`, `minimum`/`maximum`,
`minLength`/`maxLength`, `pattern`, `minItems`/`maxItems`, `additionalProperties`
(boolean form), and `format` for a fixed list (`date-time`, `date`, `email`,
`uuid`).

**Unknown keywords are rejected at flow validation time, not ignored.** JSON
Schema's own rule is that an unrecognised keyword is an annotation and passes
silently; here that rule is a hole. `{"requred": ["id"]}` would validate
nothing at all and report success forever, which is worse than having no schema
— it is a schema that lies. Rejecting the document at authoring time turns a
silent runtime hole into a 422 in the studio, where somebody is looking.

The same schema is reusable in three other places the platform already wants
it: the studio can generate a sample request and a form, the hub can emit an
OpenAPI description of a gateway route, and the importer (ADR-0032) has
somewhere to put the input contract it recovers from a Boomi process. None of
that is in scope here; it is why the subset is worth defining properly rather
than reaching for a dependency.

### 5. The runner must consume the body before it answers

Delivering a response **ends the exchange**. The gateway releases the request
the moment the runner delivers, so anything the runner has not read from the
request body by then is gone.

Async therefore has a mechanical ordering requirement that synchronous
execution never had: **read the full request, then answer 202, then execute.**
For `scope: body` that is already implied. For `scope: records` it means the
stream must be drained into the runner — memory up to the watermark, spilling
past it via `spill.Store` (ADR-0003's machinery, already built) — before the
caller is released.

This bounds async request size by what the accepting runner can hold or spill,
and it is the reason `max_body_bytes` on the route stays mandatory-with-a-default
rather than becoming optional. A deployment wanting genuinely unbounded async
ingest wants a staged upload, which is ADR-0036/0037 territory and out of scope
here.

### 6. Accepting durably

The status URL must resolve the instant the caller receives it, so the
execution row is written to the hub **before** the 202 is returned. That costs
one hub round trip on the accept path (sub-millisecond in-region, and the
accept path no longer contains a flow run, so it is affordable), and it buys
two things: a status URL that never 404s, and a task that shows as `failed`
rather than vanishing if the accepting runner dies one instruction later.

A route may set `accept: "fast"` to return the 202 before that write lands. It
is offered because a deployment that values edge availability over bookkeeping
should be able to keep serving when the hub is briefly unreachable, and it is
not the default because the honest description of it is "the status URL may
404 briefly, and an accepted task may be lost without record if the runner dies
in the window".

**Async here means "the response is decoupled from execution". It does not mean
"queued at the hub".** The accepting runner executes the flow itself. Routing
the payload through the hub queue would buy retries and survive runner loss —
and would put payload on the control plane, which is the one line the entire
two-plane architecture exists to hold. The hub gets metadata; the payload never
leaves the runner.

## Consequences

**The gateway's exchange lifetime collapses.** From "as long as the flow" to
"as long as validation", for every async route. The `legacy-200ms` row in
`docs/bench-gateway.md` — 239 req/s, because each request held an exchange for
200 ms — becomes bounded by the accept path instead of by the backend. The
delivery timeout, the registry entry and the parked-runner accounting all keep
existing, but they stop being the constraint on edge throughput for anything
except genuine request-reply.

**Callers must poll, and polling is a worse API than a callback.** This ADR
ships polling only. A completion callback (the platform POSTing to a
caller-supplied URL when the task finishes) is the obvious follow-up and is
deliberately deferred: it makes the platform an outbound HTTP client to
caller-controlled addresses, which is an SSRF surface needing the same
treatment the `http` connector's guard already has, and it deserves its own
decision rather than a paragraph here.

**Two failure classes become distinguishable at the edge**, where before they
were one 200. A 400 means the caller is wrong and retrying unchanged will fail
identically. A 202 followed by a failed status means the platform or a backend
is wrong and retrying may well succeed. That distinction is most of what an
integration's consumers ever needed from it.

**Idempotency gets easier and more necessary at once.** Necessary, because a
caller that times out mid-accept and re-posts now genuinely does not know
whether the first attempt was accepted. Easier, because the 202 carries a task
id, so `Idempotency-Key` on the route can return the *original* 202 verbatim
for a repeat key rather than accepting twice — the existing hub-side
idempotency machinery (ADR-0002) already stores what it needs.

**Verification is not validation of business rules.** A schema catches shape.
It does not catch "this customer id does not exist", and the temptation to grow
the subset until it does should be resisted — that is the flow's job, and it is
the reason the flow still runs after the 202.

## Doctrine held

- Payload never touches the hub: the hub receives the execution record and the
  status, never the request body (ADR-0038 §3, ADR-0016).
- Nothing in the DMZ initiates inward: the status endpoint is answered by a
  runner that the gateway was already talking to (ADR-0038 §4).
- The gateway holds no policy and no flow documents: schemas live with flows,
  on the runner (ADR-0038 §6).
- No whole-payload buffering on the hot path: `scope: records` exists precisely
  so that verification does not require it (ADR-0003, ADR-0004).
- Errors are machine-readable and stable: `input_invalid` + `details` follow the
  ADR-0023 envelope rather than inventing a shape.

## Alternatives considered

**A `mode: sync|async` flag on the route.** Rejected: the route is deployment
configuration and the terminal node is flow authorship, and this is a question
about the flow. A route flag also lets the two disagree — a route marked `sync`
pointing at a flow with no `@response` has no defined answer, and the only
sensible resolutions are "ignore the flag" or "fail the deploy", which is an
argument that the flag should not exist.

**Validate at the gateway.** Rejected in §4a: it puts a parser for
attacker-shaped input on the DMZ box, and the gateway does not hold the flow
documents that carry the schemas.

**Queue async work at the hub for durability.** Rejected in §6: it is the
payload-on-the-control-plane line.

**Return 202 with no status URL at all** (fire and forget, check the dashboard).
Rejected: it makes every consumer of the platform dependent on a human with hub
access to answer "did my thing run", which is the failure mode that makes
integration platforms feel opaque to the teams that depend on them.

## Open questions

1. **Completion callbacks** — deferred above; needs the SSRF treatment before
   it can be designed.
2. **Status retention.** A task id is only resolvable while the execution row
   exists. Retention is currently a hub concern with no policy; a 404 from
   expiry is indistinguishable from a 404 from a wrong principal, which is
   correct for security and unhelpful for debugging.
3. **Sync flows that exceed the delivery timeout.** Today they fail at the
   gateway. With async as the default, the right answer may be to degrade to a
   202 rather than fail — but that changes the response *shape* mid-flight,
   which no client expects. Probably it should stay a failure, and the fix
   should be authoring the flow as async.
4. **Schema evolution.** A published route's schema is part of its public
   contract; tightening one breaks callers. The hub knows both versions at
   publish time and could refuse a narrowing change to a published route, which
   is a compatibility policy (ADR-0023) rather than an edge concern.
