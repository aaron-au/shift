# M5 design notes — studio, flow model, observability

> Pre-build design inputs (captured 2026-07-19, before M5). Not yet
> built. The flow model and studio are **hub** concerns (design-time is
> control-plane; runners only execute — headless is the end state).
> Milestone scope lives in `PLAN.md` M5; ADRs get written as the build
> starts. Nothing here required amending the M4b foundation.

## Flow / step model

- **"Step"** is the proposed unifying term for a DAG node. Today's model
  is linear v1: transform steps are `Op` (filter/project/coerce/flatten/
  aggregate), connectors are `Endpoint` (source/sink). M5's DAG makes
  every node a step.
- **Outcome-based edges** (green/red path): each step exits on
  **success** OR **failure** — two typed out-edges — with a single
  **complete** out-edge for steps that have no success/failure
  distinction. The builder wires each outcome to the next step
  (Boomi try/catch without the ceremony). Realizes PLAN M5
  "branch/merge, error handlers." Design the edge model into the DAG
  document from the start.
- **Versioning from the start** — already in the foundation
  (`flow_versions` keeps every save; publish promotes; rollback =
  publish older). Task-save versions are required.
  **Branching** (divergent parallel lines) deferred — linear
  save-versions + publish/rollback covers "know what ran, revert
  safely"; revisit only on concrete need.
- **Sub-tasks / sub-flows** — in M5 scope.
- The flow JSON stays **readily inspectable** — the document is the
  source of truth, not hidden behind a canvas.

## Test mode

- **Soft deploy + execute** already works at the primitive level: deploy
  creates a **draft**; executing a specific draft version via the API
  runs it without publishing (only the default/version-0 path requires a
  published version). Studio surfaces this as "test run."
- Per-step status + **live logs** during the run is the genuinely new
  build (today telemetry is per-op but arrives at completion). Needs a
  runner→hub/studio streaming channel; additive in M5/M6 (M6 scopes
  OpenTelemetry).

## Logging, data capture, and payload storage

- **Levels**: test mode defaults to verbose + data capture ON; a
  deployed task defaults to WARN/ERROR with data capture OFF. Both are
  set **in the task before deploy** (live in the flow doc / deploy
  config → auditable).
- **Data capture** = INPUT and OUTPUT payload of each step (before/after)
  for debugging.
- **Payload storage**: runners **optionally** persist completed-task
  payloads; the storage mechanism sets the lifetime (docker volume →
  persisted). Enterprise value-adds: pull captured data from the runner
  (Splunk), or push to **OpenTelemetry endpoints** (paid tier).

### Hard constraints (doctrine)

- **Payload never reaches the hub.** Data-capture logs contain payload
  bodies; the hub never touches payload data. So before/after payload
  logs stay **runner-side** — streamed live to studio in test mode,
  never durably persisted on the hub. The hub carries **metadata logs
  only** (levels, step outcomes, timings). Runners physically cannot
  leak payload to the hub — by design there is no channel.
- **Secret redaction**: resolved `{"$secret":…}` values must be redacted
  from any input/output capture (ADR-0010: secrets never in logs).
- **Encrypt stored payloads at rest** (reuse KEK/envelope, runner-local
  key), explicit **retention TTL**, support **right-to-erasure**.
- **Enabling capture on a production task is sensitive** (exposes
  customer data) → **audit-logged + role-gated (admin only)**.
- **Log streaming is lossy**: drop logs under pressure, never stall the
  data plane (distinct from the data-plane backpressure doctrine, which
  never drops DATA).

### Additional considerations

- **Data residency is a differentiator**: because the hub holds only
  metadata, payload residency = wherever the runner runs ("data never
  leaves your region/VPC; the control plane sees only metadata").
- **Per-hub/tenant connector capability policy**: cloud/shared hubs
  disable **and hide** dangerous connectors (filesystem/disk — "not even
  visible"); self-hosted allows them. The registry is already
  per-account; add the policy layer. One mechanism covers the full list
  of cloud-SaaS restrictions.
- **Egress control** on OTel/Splunk push: SSRF-guard posture like the
  http connector; the endpoint credential is a secret.
- **Sampling** for high-volume capture even when enabled (1-in-N).
- **Shared cloud runners**: tenant-isolate stored payloads, or dedicate
  runners per tenant.

## Custom code (write the ADR as the first M5 act)

Two tiers (decided in discussion; "custom code is the last option, not
the go-to — nerfed is fine"):

1. **Starlark-WASM inline transforms** — hot path, deterministic,
   fuel-metered, no filesystem/network, no packages.
2. **Full Python as an out-of-process step** — reuse the connector
   subprocess pattern (gRPC/UDS). Packages via **uv**, hash-pinned
   lockfile, **wheels-only** (no sdist `setup.py` execution), a single
   internal proxy index, resolve-at-design / install-at-deploy /
   frozen-at-runtime, shipped as signed bundles in the M4b registry.
