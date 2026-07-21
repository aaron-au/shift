# ADR-0022: Testing coverage gate + benchmark visibility (M7)

Date: 2026-07-20
Status: Accepted; implemented (M7).

## Context

M0–M6 shipped the platform with tests from commit one and `-race` always on
(ADR-0006). But three things a buyer looks for to trust a release were missing:

1. **No coverage signal.** Coverage was never measured, gated, or reported. A
   package could silently rot to near-zero (the hub scheduler — exactly-once
   logic — was effectively untested at the package level) without any gate
   noticing.
2. **End-to-end coverage was hub-centric.** `hub/e2e` proved crash recovery,
   exactly-once scheduling, signed-artifact supply chain, and
   secrets-never-at-rest against real processes — but the public **webhook
   ingress** path (ADR-0016), the one place a payload could leak from runner to
   hub, had no full-stack proof.
3. **Benchmarks weren't visible or repeatable as a suite.** `shift-bench`
   enforced RSS ceilings (ADR-0003) but emitted only human text, took six
   flags, had no structured output, and no run-to-run stability or
   regression-vs-base visibility. Nobody could see the performance story
   without running it by hand.

This ADR sets the testing + benchmark policy. It adds tooling and tests only —
no product-behavior change.

## Decision

### 1. Coverage: measured, hard-gated per package, ratcheted

- `make cover` (`scripts/coverage.sh`) runs the race-enabled test pass for
  every module with `-coverpkg=./...` (so 0%-covered packages surface, and
  integrated coverage from cross-package tests counts), **merges** the
  count-mode profiles correctly (dedupe blocks, sum hits — naive concatenation
  double-counts), and aggregates per-package statement coverage.
- **`cover` is the test pass used by `check`.** `check` depends on `cover`, not
  `test`, so the race detector still runs on every gate and there is no double
  test run. `make test` remains as a standalone quick run.
- **Hard gate.** Every gated package must meet its floor in
  `coverage.thresholds` (per-package override, else `default`). The build fails
  on any breach.
- **Exclusions** (measured + reported, never gated): `cmd/*` (thin `main`
  wiring), `*/telemetry` (metric registration), generated `connectorpb`, and
  test helpers (`pgtest`, `oidctest`, `sdktest`, the `e2e` package itself).
  These carry no logic worth gating; gating them would invite coverage theatre.
- **Headline total is gated-only.** The top-line coverage number (and the
  README badge) is computed over the *gated* set — the same packages the gate
  enforces — not over every package. Folding the excluded `main`/generated/
  telemetry statements into the headline understates the coverage that is
  actually enforced (their statements are real but deliberately untested), so
  the excluded packages appear per-package in the table but not in the top-line
  figure. This matches the standard practice of omitting `main`/generated code
  from a coverage percentage; it is a reporting choice, not a change to what is
  gated.
- **Ratchet.** `make cover-bump` rewrites `coverage.thresholds` to
  achieved-minus-epsilon (2 pp of slack for nondeterministic tests). Floors
  only ever rise — an existing floor is never lowered. Run it after adding
  tests; commit the diff.

Rationale for the epsilon: e2e/timing-sensitive tests make coverage slightly
nondeterministic run to run; a zero-slack floor would flake CI. 2 pp is enough
headroom without letting real regressions through.

### 2. Visible results: CI artifacts + job summary + badge

- The `check` job appends `coverage/coverage.md` (per-package table + total) to
  the **GitHub job summary** and uploads `coverage.html`/`.json`/`.md` as an
  **artifact** (pinned-SHA `upload-artifact`, ADR-0006 supply-chain hygiene).
- A **coverage badge** on the README reads a committed `badges/coverage.json`
  (shields.io endpoint schema). An isolated `badge` job (main only,
  `contents: write` — the broad `check` job keeps `contents: read`) regenerates
  and commits it via `scripts/badge.py` when the number changes (`[skip ci]`).
- The `bench` job renders `docs/bench-M7/results.md` into the job summary,
  uploads the JSON, and on PRs runs `benchstat` on the **engine** micro-
  benchmarks (base vs PR — the perf-critical module, no Postgres needed).
  It stays **non-blocking** (`continue-on-error`); the only hard perf gates are
  the shift-bench RSS ceilings, which exit non-zero.

### 3. Benchmark suite: configurable + machine-readable

`shift-bench` gains `-json` (structured per-run metrics: throughput, wall,
peak RSS, B/rec, allocs/rec, spill), `-runs N` (repeat; median reported),
and `-warmup` (one untimed steady-state run). Gate status prints to stderr so
`-json` stdout stays pure JSON. `make bench-report` (`scripts/bench.sh`) runs
the scenario matrix and renders the visible table. RSS ceilings remain hard.

### 4. E2E: webhook ingress full-stack

`hub/e2e` gains a webhook scenario: a real runnerd registers a webhook, a
`POST /hooks/{name}` triggers an async execution, the runner reports it to the
hub as **metadata only**, and the test asserts the distinctive payload never
appears in anything the hub stored — the doctrine-critical "payload never
touches the hub" property (ADR-0016), now proven end-to-end.

### 5. Fuzzing: the untrusted-input surfaces

Native Go fuzz targets on the parsers/verifiers that eat untrusted bytes:
`flowdoc.Parse` (flow docs from the API/builder), `consign.Verify` (the
fail-closed signature gate), `ndjson.Reader` (the hand-rolled hot-path
tokenizer), and the `spill` binary Value codec (re-read from disk). The
property is **robustness** — never panic/hang/over-allocate; garbage must
error, and `Verify` must never false-accept. The **seed corpus runs in
`make test`** (regression on known cases); **`make fuzz`** (`FUZZTIME`
overridable) is the mutation-discovery pass, run short + blocking in CI
(a discovered crash is a real bug). Stronger invariants (e.g. a
Parse→marshal→re-Parse idempotency check) were considered but not adopted:
a round-trip variant produced a single unreproducible failure that ~17M
execs could not recreate, and a flaky fuzz target poisons CI — robustness is
the property that matters for untrusted input.

## Consequences

- `make check` runtime rises modestly (coverage instrumentation) but does not
  double (cover replaces test).
- New code must carry tests or it drags a package below its floor and fails the
  gate — coverage can only go up.
- Coverage floors are a living artifact; every milestone that adds packages
  runs `cover-bump`.
- The performance story is now visible on every push (job summary) and every
  PR (benchstat diff), not just on demand.

## Alternatives considered

- **Diff-coverage gate on new code only** — rejected: weaker signal, and the
  per-package hard floor + ratchet already forces new code to pull its weight.
- **Codecov/Coveralls** — rejected: an external service + token for what a
  self-contained script + committed badge JSON does with no third-party in the
  supply chain (consistent with the pinned-SHA discipline of ADR-0006).
- **benchstat as a hard regression gate** — deferred: micro-benchmark variance
  on shared CI runners is too noisy to block merges; RSS ceilings remain the
  hard perf gate, benchstat is informational. Revisit with dedicated runners.
