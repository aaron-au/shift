# ADR-0014: Test-mode per-step data capture

Date: 2026-07-20
Status: Accepted

## Context

M5's test mode lets a builder soft-deploy a draft flow, run it, and see what
happened at each step. The original sketch was **live** per-step logs
streamed mid-run to the studio canvas. In review the product owner cut that:
live is not needed — test mode can **read results from the runner on
completion** and overlay them on the canvas. That removes the single biggest
piece of net-new infrastructure (a runner→hub/studio streaming channel) and
keeps everything request/response.

What remains is the substrate: capturing per-step INPUT/OUTPUT data during a
run so it can be retrieved afterwards. This ADR covers that. The canvas
overlay that consumes it is studio work (M5d).

## Decision

### Capture the output of each stage, as a bounded sample

A step processes the whole stream, so "the data at a step" is unbounded —
capturing every record at every boundary would defeat streaming and blow
memory. Capture is therefore a **bounded sample** (default 20 records/step).
It records the **output** of every stage (the source and each operator);
the input to a step is the previous stage's output, so per-stage output
samples reconstruct the data on every canvas edge with half the copies. The
terminal sink is not sampled (its input is the last operator's output).

### An engine Sampler hook, not pipeline surgery

`engine/stream` gains a `Sampler` interface and `Pipeline.WithSampler`. Each
`measuredSource`/`opSource` calls `Sample(step, batch)` on its output when a
sampler is attached; `nil` (the default) is a single branch of zero cost.
This was chosen over the runner inserting extra capture operators into the
pipeline: extra ops would pollute the per-op telemetry rows and collide with
`RenameLastOp` (which stamps the step id used for `OpError` error routing).
The hook keeps capture invisible to telemetry and routing, and keeps the
step-id labelling (ADR-0013) intact. Samples are keyed by the same step id.

### Runner-side, redacted, ephemeral

- **Payload never reaches the hub** (doctrine). Capture is produced and held
  on the runner; the retrieval endpoint (`GET /api/tasks/{id}/capture`) is
  runner-local. The hub carries only metadata.
- **Redacted** (ADR-0010). Each sample is serialized to NDJSON and run
  through the same `newRedactor` (built from the task's resolved secret
  values) used on error text, at the serialized-text layer so every value —
  not just strings — is masked. Secret plaintext is never stored.
- **Ephemeral.** The sample lives on the in-memory task record and is evicted
  with the task from the runner's bounded ring. There is no durable store,
  no encryption at rest, no TTL — because there is nothing at rest.
- **Best-effort.** Capture never fails or meaningfully slows a task: sampling
  stops at the bound, serialization errors are dropped, and it runs only when
  explicitly enabled.

### Opt-in per task

`SubmitOpts.Capture` (+ `CaptureMax`) turns it on; the local execute API
exposes it as `?capture=1[&capture_max=N]`. Off by default. The lease path
leaves it off for now — hub-driven test runs (a draft executed with capture)
arrive with the studio in M5d.

## Consequences

- The whole live-streaming channel is avoided; test mode is plain
  request/response like the rest of the system.
- The runner dashboard shows captured samples inline in the task detail — a
  visible, testable slice of the test-mode experience before the studio
  exists.
- **Deferred to a later (enterprise) chunk:** durable payload storage —
  runners *optionally* persisting completed-task payloads — with a
  runner-local **encrypted** store (lift `kek`/envelope out of `hub/internal`
  to a shared package, add a runner KEK file), explicit **retention TTL**,
  **right-to-erasure**, **sampling** knobs for high volume, and push/pull to
  **Splunk/OpenTelemetry**. Enabling capture on a **production** task is
  sensitive (customer data) and will be **admin-gated + audited** when it
  lands; today capture is a test-mode/dev toggle only. None of this changes
  the model here — it adds a persistence + governance layer beneath it.

## Proof

`TestCaptureSamplerBoundsAndRedacts` (bound honored, `More` flagged, secret
masked); `TestCaptureEndToEnd` (a capture-enabled run through real connectors
collects per-stage samples, bounded, with the secret name redacted out of the
source sample). `engine/stream` tests unchanged (sampler nil in normal runs).
