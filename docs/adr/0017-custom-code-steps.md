# ADR-0017: Custom-code steps — Starlark inline + Python out-of-process

Date: 2026-07-20
Status: Accepted (design); implementation deferred (M5b build)

## Context

Studio is low/no-code first; custom logic is the **escape hatch, not the
go-to** (product owner: "custom code is the last option — nerfed is fine").
ADR-0001 committed to sandboxed WASM (wazero) for user transforms rather than
embedded scripting engines. M5 needs the escape hatch defined so the flow
model reserves the right shape; this ADR fixes the design. The build is
deferred — it is the lowest-priority M5 capability and greenfield — but it is
fully specced here so M5 closes clean and the build is a known task.

Two distinct needs: a **fast inline transform** on the hot path, and a
**full-language** tier for genuinely complex logic. Hence two tiers.

## Decision

Both are **transform step types** in the v2 graph (ADR-0013); they lower on
the **runner** (the engine stays stdlib-only — the runtimes live runner-side;
the hub only validates the step shape, data-free). Outcome edges (onFailure)
handle their errors like any step. The capability policy (ADR-0015) can
disable either type per deployment (cloud hubs may forbid custom code).

The step type names the **language**, not the sandbox, so the sandbox impl
can change without touching flow documents.

### Tier 1 — `starlark` (inline, hot path)

Small transforms in Starlark (Python-like, deterministic, cheap). No
filesystem, no network, no unbounded loops: **fuel-metered** (execution-step
budget) + bounded memory, per-task. Deterministic (no ambient clock/random).
Invoked per-record or per-batch (decided at build).

Runtime, faithful to ADR-0001: a Starlark **guest compiled to WASM, run under
wazero** — true memory isolation + wazero's fuel metering, safe for
mutually-untrusted multi-tenant runners. `go.starlark.net` in-process is the
fallback if the WASM path proves disproportionate to the value (it is
deterministic and I/O-free, but a bug escapes only to the host process, not a
wasm sandbox); the flow document is identical either way. **This choice is
revisited at build time** and, if it lands as in-process, amends ADR-0001's
transform-runtime clause via a superseding note.

### Tier 2 — `python` (out-of-process)

Full Python for real code. Reuses the **connector subprocess pattern**
(gRPC/UDS, ADR-0007) — isolated process, no ambient network/filesystem beyond
what the sandbox grants, timeout-bounded. Packages are locked down:
- **uv**, hash-pinned lockfile, **wheels-only** (no sdist `setup.py`
  execution — arbitrary build code is the supply-chain hole);
- a **single internal proxy index** (no arbitrary PyPI);
- **resolve-at-design / install-at-deploy / frozen-at-runtime**;
- shipped as **signed bundles** through the M4b registry (`pkg/consign`,
  ADR-0011) — same signature + fail-closed verification as connectors.

### Data contract

Custom code sees records through the engine record model (or a marshaled
view across the wasm/subprocess boundary); it must honor batch lifetime
(copy to retain), stay within the memory watermark, and emit records the same
way operators do. No `map[string]interface{}` on the hot path. A step that
throws routes to its `onFailure` handler (ADR-0013); secret values are
redacted from any error text (ADR-0010).

## Consequences

- The flow model reserves `starlark`, `python`, `subflow` as step types
  (parsed, rejected "not yet supported" until built) — done in `pkg/flowdoc`
  now, so deploying such a doc fails cleanly rather than mis-executing.
- No engine dependency added yet; when built, the wasm/Starlark runtime and
  the Python subprocess host live in the runner module, not the engine.
- Custom code is governed by the same gates as everything else: capability
  policy, signing (Python bundles), fuel/timeout, outcome-edge error routing,
  secret redaction.

## Open questions (resolve at build)

- Starlark-in-wasm artifact source (TinyGo-compiled `go.starlark.net`, a Rust
  Starlark crate to `wasm32`, or in-process `go.starlark.net`).
- The uv proxy-index infrastructure (what serves the wheels; how the
  hash-pinned lockfile is produced at design time).
- Per-record vs per-batch invocation for the inline tier (throughput vs
  ergonomics).
- The marshaling format across the wasm/subprocess boundary (reuse the
  `engine/spill` binary Value codec vs NDJSON).
