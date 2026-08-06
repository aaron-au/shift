# ADR-0046: Operational logging — one contract, stdout, structured

Date: 2026-08-06

Status: **Accepted, building.** Fills a gap rather than superseding anything:
logging has never had a decision, and it shows.

## Context

Aaron, 2026-08-06, after an audit of what the three binaries actually emit:

> looks like there's a considerable gap in logging to stdout — formatting and
> content … we really should be showing logs there (particularly around the
> functioning of the app like loading runner, updating mTLS etc.)

The measured state before this ADR:

| app | destination | format | level control |
|---|---|---|---|
| `gatewayd` | stderr | slog JSON, no stdlib calls | `-debug` |
| `hubd` | stderr | slog JSON **mixed with** 30 plain-text `log.Print` lines | `SHIFT_HUB_LOG_LEVEL` |
| `runnerd` | stderr | **38 plain-text lines, no slog at all** | none |
| connectors, sdk, engine, pkg | — | none (libraries — correct) |

Three real problems, in order of how much they hurt:

1. **The runner — the component that does the work — has no structured logging
   at all.** No levels, so nothing can be turned down; no fields, so nothing can
   be filtered; no way to correlate a line with a task, flow, or request. The
   gateway and runner already exchange a request id, and it is ungreppable in
   any structured sense because one end writes prose.
2. **The hub is half-migrated**, so a pipeline configured for JSON hits
   unparseable lines. Worse than either format consistently.
3. **Nothing agrees on field names**, because nothing ever said what they are.

And a content gap underneath the format gap: the lifecycle events an operator
actually needs — this runner registered, its certificate renewed, the connector
pool launched a subprocess, the drain started — are either absent or phrased as
sentences.

## Decision

### 1. stdout, always

Logs go to **stdout**. Not stderr, not files.

The operator decides what happens next — a pipe, a file, a collector — which is
the whole point: a process that writes its own log files has taken that
decision away and acquired state we spent an architecture avoiding.

stderr keeps exactly one job: a fatal message on the way out, when the logger
may itself be the thing that failed.

### 2. One contract, and it is the OUTPUT, not a shared package

`pkg/shiftlog` gives the hub and the runner an identical logger in one line.

**The gateway does not import it.** Its `go.mod` currently has *zero*
dependencies — not even our own `pkg` — and that is a security property of the
one box that may sit in a DMZ, auditable by reading a four-line file. "The
gateway imports nothing of ours" is a bright line; "the gateway imports only
harmless things of ours" is a judgement call that erodes on a bad afternoon.

So the gateway keeps its own ~20 lines of setup, and a **conformance test
asserts all three binaries emit the same field names**. The contract is the
schema. Duplicating a little code to keep a dependency graph provable is a good
trade; sharing code to avoid duplication and losing the property is not.

### 3. The schema

Every record carries `time`, `level`, `msg`, plus:

| field | on | meaning |
|---|---|---|
| `component` | every record | `hub` \| `runner` \| `gateway` |
| `version` | every record | build version, so a mixed-version fleet is legible |
| `event` | lifecycle records | stable dotted name, e.g. `runner.registered` |
| `error` | failures | the error string, never a payload |

Context keys, spelled ONE way platform-wide — `runner`, `flow`, `task`,
`request`, `gateway`, `connector`, `account`, `duration_ms`. The existing
gateway↔runner `request` id keeps its name, which is what makes a request
traceable across two binaries once both are structured.

`event` is what a dashboard keys on and `msg` is what a human reads. Alerting
on prose is how a log message becomes an API nobody documented.

### 4. Format follows the terminal

JSON when stdout is not a terminal (containers, pipelines); human-readable text
when it is (`make up`, a local `runnerd`). `SHIFT_LOG_FORMAT=json|text`
overrides.

Auto-detection is not cleverness for its own sake: the alternative is a default
that is wrong for one of the two audiences, and every developer pasting
`| jq -r` into their notes forever.

### 5. The stdlib bridge, so nothing is lost on day one

`log.SetOutput` is pointed at a bridge that re-emits into slog. That does two
things at once: the ~68 existing `log.Print` call sites become structured
immediately rather than after a 68-file rewrite, and **any third-party library
that logs through the global logger** stops writing unstructured text into an
otherwise-JSON stream.

Call sites are then converted deliberately, starting with the lifecycle events
below. The bridge is not a permanent excuse — it is what makes the migration
safe to do incrementally.

### 6. The events that must exist

An operator's first question is almost always "did the thing start, and is it
talking to the other thing". These are logged at INFO, once, with fields:

**Runner** — `runner.started` (version, listen, budget), `runner.registered`
(runner id, hub, auth mode `mtls|bearer`), `runner.cert.renewed` (not_after),
`runner.cert.renew_failed` (WARN, ERROR inside the last hour before expiry),
`runner.gateway.connected` / `.lost`, `task.leased`, `task.completed`
(duration, records in/out), `task.failed` (step, code), `runner.draining`,
`runner.stopped`.

**Hub** — `hub.started` (listen, TLS, runner auth mode), `hub.migrated`
(version), `runner.registered` (from the hub's side, with the issued serial),
`runner.cert.issued`, `scheduler.fired`, `task.dispatched`, `hub.stopped`.

**Gateway** — `gateway.started`, `gateway.config.applied` (routes, generation),
`gateway.runner.parked` / `.left`, `request.dispatched`, `request.rejected`
(reason), `gateway.stopped`.

Certificate expiry is deliberately in that list twice. "The fleet went quiet
overnight" (ADR-0044) is only avoidable if the renewal that stopped working was
visible while it was still only a warning.

### 6a. Enforced, not agreed

Everything above is a convention until something checks it, and a convention
nothing checks decays in about three months. `pkg/shiftlog`'s vocabulary test
parses every non-test file in `hub/`, `runner/` and `gateway/` — source, not
imports, so it covers the gateway too — and fails the build on:

- a log call with **no `event` key**, at any level;
- a key using a **near-miss spelling** (`task_id`, `dur_ms`, `err`, `id`) where
  the platform has a canonical one. A near-miss is worse than a new key: two
  spellings of one idea split every query that uses it, invisibly;
- a key **named** like a credential or payload (`token`, `secret`, `password`,
  `payload`, …), unless it is on the short exempt list of names that
  *identify* rather than *carry* (`cert_serial`, `token_sha256`);
- `fmt.Print*` anywhere in the long-running packages, which would put
  unstructured bytes into the stream an operator is piping somewhere;
- the stdlib `log.Fatal*` in a binary, because through the bridge it emits at
  INFO — the one record you most need to find would look like every other one.
  `shiftlog.Fatalf` is ERROR plus a stderr copy.

One consequence worth stating: **log calls must list their keys literally.** A
`log.Info(msg, args...)` splat is invisible to a static check, so the gateway's
start-up record was rewritten to a fixed field set. That is better logging
anyway — a query should not have to handle a field that is sometimes absent.

When this landed it found **thirty** unnamed call sites and one misleading key
(`control_shared_secret`, whose value was a bool). That is the drift rate over
a few months of building, on a codebase with no other logging problems.

### 7. What must never appear

Secret values, tokens, private keys, and payload records. This is not a
convention — a test greps a captured log stream for known credential values and
fails the build if one appears.

A credential may be *identified* (`cert_serial`, a token's last four) so a
support conversation is possible; it may never be reproduced.

## Consequences

**A `docker logs` that is worth reading.** The lifecycle of every component is
visible without attaching a debugger or enabling a flag nobody knew about.

**Filtering and alerting become possible** for a self-hosted operator with no
observability stack — `jq 'select(.event == "runner.cert.renew_failed")'` is
the whole implementation.

**The gateway keeps its zero-dependency go.mod**, at the cost of ~20 duplicated
lines and a test that keeps the two honest.

**Volume goes up.** Per-task INFO records on a busy runner are real bytes.
Levels exist, `task.leased` is DEBUG rather than INFO, and the per-request
gateway records stay DEBUG — the default is "what an operator needs", not
"everything that happened".

**This is not tracing.** No spans, no sampling, no propagation beyond the
request id that already exists. Metrics (ADR-0020) stay where they are. Logs
answer "what happened to this one thing", and conflating the two is how you get
a logging bill and no answers.

## Alternatives considered

**Leave the runner on stdlib `log`.** It works, in the sense that characters
appear. Rejected: the component that executes every task is the one you most
need to filter, and prose cannot be filtered.

**A shared logging package imported by all three, gateway included.** The
obvious answer, and it costs the gateway's zero-dependency property. Rejected
on that alone — see §2.

**Log to stderr, as the two structured binaries already do.** Defensible, and
common in Go. Rejected because stdout is the convention operators expect for
application output, it is what every container runtime and log shipper assumes
by default, and keeping stderr for genuine failure output means the two streams
mean different things instead of both meaning "everything".

**OpenTelemetry logs.** A real answer for a fleet with a collector, and a heavy
dependency for a self-hosted single-node install. The schema here is
deliberately shaped so an OTel exporter can be added later without changing
what the code writes.
