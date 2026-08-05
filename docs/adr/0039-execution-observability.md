# ADR-0039: Execution observability — timings, logs, and where each one lives

Date: 2026-08-04

Status: **Partially implemented.** The per-step diagnostic fields (bytes,
batches, inclusive wall time) and the per-execution phase record are built.
Log levels, the log destination, the `task_steps` table and OTel span export
are designed here and not yet built.

Extends **ADR-0020** (observability): metrics shipped in M6a, tracing was
deferred. This turns tracing on and settles the question ADR-0020 left open —
what we *store* versus what we *emit*.

## Context

Aaron, 2026-08-04: do we have verbose enough logging for a customer to find
bottlenecks?

An audit says: we have the skeleton and not much else. Every execution records
per-step `records_in`, `records_out` and `seconds`, reported to the hub and
shown on the runner dashboard. `OpStats.Nanos` is deliberately *own work only*,
honest per the metrics doctrine. That answers "which step burned the most CPU"
and nothing else.

What it could not answer:

- **How much data moved.** Records only. Ten records of 1 MB and ten of 100
  bytes are indistinguishable, and for throughput work bytes matter more.
- **Whether batching went wrong.** The engine measured `Batches` and the
  conversion to the task record dropped it. The data existed; nobody could see
  it.
- **Whether a step was slow or starved.** `Nanos` is exclusive, so a step
  blocked on a slow downstream sink or a full fan-out queue reports as nearly
  free. In a pull pipeline *waiting* is frequently the bottleneck.
- **Where the wall time went outside the pipeline.** A task held on resource
  capacity (ADR-0005) is invisible in every measurement, so a runner at its
  memory ceiling is indistinguishable from a slow integration.
- **Anything over time.** Metrics are runner-level gauges; there is no per-flow
  or per-step series, so "p95 of step X on flow Y last week" — the actual
  bottleneck-hunting workflow — is unanswerable.
- **Anything shippable.** `hubd` uses `slog` with a JSON handler; the runner is
  a handful of `log.Printf` calls, with no correlation id.

And one thing that was assumed to work and does not: **test-mode payload
capture is silently dropped on v3 DAG flows** (issue #60). `executeMulti`
discards the sampler, so `?capture=1` returns nothing for exactly the flows
where per-branch inspection matters most — and an empty capture reads as "no
records flowed", not "unsupported here".

## Decision

### 1. A timing record is not a log, and the distinction sets the defaults

The thing a customer wants first — "where did the time go" — is a **fixed-width
record**, not text. A handful of integers per execution, not a variable-length
stream. That difference decides everything about volume:

| Level | Content | Per execution | Default |
|---|---|---|---|
| `timings` | phase durations + per-step counters | ~100 B fixed | **ON** |
| `step` | one structured line per step | ~500 B | opt-in |
| `debug` | per-batch and connector detail | unbounded | opt-in / test mode |
| `off` | nothing | 0 | available |

The arithmetic is what makes "on by default" honest rather than a hope. At the
measured trigger-path ceiling (~9,600 tps on 4 cores), a per-execution *text*
line is ~415 GB/day; the fixed-width tier is ~1 MB/s. Only the second can be
on for everyone.

Worth calibrating, because it recurs: **4,300–9,600 tps is headroom, not
expected load.** For integration work 100 tps sustained is already enormous
(Aaron, 2026-08-04). Storage decisions should be sized for the realistic case
and merely survive the ceiling.

### 2. What is stored, and in what shape

Two granularities, and the shape of each is what keeps it cheap:

- **Per-execution phases → columns on the task row.** `admission_ms`,
  `bind_ms`, `run_ms`, `total_ms`. One row per task, already written, no new
  storage.
- **Per-step counters → a `task_steps` table**, one row per step per
  execution: `(task_id, step_id, connector, records_in, records_out, bytes,
  batches, own_ns, wall_ns)`.

The table is deliberate, and it reverses an earlier draft that put per-step
data in a JSONB blob to avoid row multiplication. Rows buy the query that
matters — *"p95 of the sftp source across every flow last week"*, *"which step
regressed after Tuesday's deploy"* — and unnesting JSONB across millions of
rows does not do that job.

**This is normalisation, not log management**, and the line is schema versus
free text. A fixed-schema child table of `tasks` is architecturally identical
to `task_attempts`, which already exists and which nobody would call a log
store. What crosses into log-management territory is arbitrary text, full-text
indexing, a query language, and per-stream retention policy. None of that is
here.

Cost, at 4 steps per execution: 3.5M rows/day at 10 tps (~5 GB/week), ~50
GB/week at 100 tps with time partitioning. Comfortable for every realistic
deployment, and above that the emit path (§3) carries the load while retention
shortens.

### 3. Emit *and* store, split by what the volume is bounded by

The dividing line is not sensitivity, it is **cardinality**:

| | Bounded by | Where it lives |
|---|---|---|
| Phase + per-step summary | task count | **stored** (hub) |
| Spans, per-batch events | batch/record count | **emitted** (OTel), never stored by us |
| Logs | run count × verbosity | **stdout JSON**, shipped by the customer |

Store-only would make the hub a time-series database — the shape rejected for
the payload archive (ADR-0037), for the same reason. Emit-only would leave a
small self-hosted deployment with *nothing*, and the studio unable to answer
"why was this slow" without a backend the customer does not run.

**We do not run or bundle Tempo, Loki or an equivalent.** We speak OTel so a
customer already running one gets full spans, cross-execution analysis,
alerting and long retention — none of which we build, operate or support — and
we store the summary so a customer running nothing still gets the answer.
Bundling a TSDB would grow the "just runs" footprint against the container-first
doctrine to supply something most customers either already have or do not want.

### 4. Trace correlation across four components

ADR-0038 makes a request cross **gateway → hub → runner → connector**. A single
trace id spanning all four is now the highest-value piece of ADR-0020's
deferred tracing half, because no single component's view explains a slow
request any more.

The id is minted at ingress (gateway, or the runner for a direct execution),
carried as control metadata on the lease and the connector RPC, and stamped on
every log line and stored row. Metadata only — ADR-0016 is untouched.

### 5. Logs: levels, destination, and whose fault a leak is

Logs go to **stdout as JSON**, which the customer's existing shipper collects.
That is the container-native answer and it means we never own retention,
search or access control.

**Optional per-flow log capture** stores an execution's log output as a
bounded artifact — attached to the execution, downloadable, retained on a
timer. Explicitly *not* searchable: `execution → logs → download` is the whole
feature. Bounded artifact, no index, no query language.

It is **opt-in per flow**, for the §1 arithmetic reason rather than a
philosophical one, with a cap per execution and a retention default of 7 days
under a hub-wide operator ceiling (the ADR-0037 §5d shape).

**Destination is configurable**, defaulting to the hub's own store. An earlier
draft argued Postgres-versus-object-store as a custody question; Aaron
corrected that — a self-hosted hub's Postgres is equally the customer's. The
real distinction is coupling and fit: Postgres growth lands on hub backup,
restore and failover time, and a transactional store is a poor home for
append-only blobs. So: hub store by default (zero configuration, small
volumes), pointing at an ADR-0037-style destination for anything larger.

On payload leaking into logs, the responsibility split is explicit rather than
a refusal to build the feature:

- **Our lines are ours.** Payload-free by contract and secret-redacted through
  the existing redactor (ADR-0010).
- **Customer code is the customer's.** When `starlark`/`python` steps land
  (ADR-0017), what they choose to log is their call. At some point it cannot be
  the platform's responsibility (Aaron, 2026-08-04).

### 6. Metric cardinality is a decision, not a default

Per-flow, per-step, per-outcome labels on a busy hub is where a Prometheus
deployment falls over. **Per-flow series always; per-step opt-in.** Named here
so it is a choice rather than something discovered in production.

## Doctrine held

- **Honest metrics or none.** Inclusive (`wall`) and exclusive (`own`) time are
  reported as separate, differently-named fields rather than one ambiguous
  number. `Bytes` is exact at the source and documented as approximate
  downstream, because operators share the flowing batch's arena — reporting
  those as exclusive would be exactly the dishonest metric this project refuses
  to ship.
- **Hub never touches payload (ADR-0016).** Timings, counters and trace ids are
  metadata. Log capture is opt-in, bounded, and written to a destination the
  customer chooses.
- **Runners stay fast.** The default tier costs a struct copy on a call that
  already happens once per task, on the completion path, off the hot path
  entirely. Nothing per-record, and nothing per-batch, is ever on by default.
- **Container-first.** No bundled TSDB; stdout and OTel are the interfaces.

## Consequences

- A customer can answer "where did the time go" out of the box, including the
  two questions previously unanswerable: **was this step slow or starved**, and
  **was this task waiting for capacity**.
- The hub gains one child table and four columns. Bounded by task count, not by
  customer data.
- Anyone running Grafana/Tempo/Datadog gets full traces without us operating
  anything; anyone running nothing still gets the summary.
- Issue #60 becomes load-bearing rather than cosmetic: test-mode capture is the
  `debug` tier of this model, and it is silently broken on v3 flows.

## Open questions

- **Connector-side attribution.** A source step's time includes the gRPC round
  trip but not where it went — DNS, TCP, TLS, time-to-first-byte from the
  remote system. For an HTTP source that is the entire question ("is their API
  slow, or are we?"), and it needs an SDK contract addition.
- **Gateway hop timing.** ADR-0038 adds a hop whose latency is estimated and
  unmeasured. The phase record should carry a slot for it now rather than being
  retrofitted.
- **Retention alignment.** `task_steps` retention presumably follows task
  retention, but a customer may want counters long after payload and logs are
  gone — they are the cheapest and most useful thing to keep.
- **Sampling above the crossover.** Beyond ~100 tps sustained, storing every
  step row stops being free. Head-based sampling is the obvious answer and
  interacts badly with "find the one slow execution" — the exact request that
  motivates sampling in the first place.
