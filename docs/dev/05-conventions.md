# 05 — Conventions & Dev Workflow

## The gate (ADR-0006)

`make check` is THE gate — identical locally, in the pre-push hook, and in
CI. It runs: gofmt/goimports drift, `go vet`, golangci-lint (staticcheck,
errcheck, **gosec**, noctx, errorlint, rowserrcheck, bodyclose…),
`govulncheck` (reachability-based CVE scan), gitleaks, and `go test -race`
across every module. Rules:

- Findings are **fixed or suppressed inline** with
  `//nolint:<linter> // reason`. Blanket config exclusions need an ADR.
- `-race` is never optional. Benchmarks (`make bench`) are a separate,
  currently non-blocking CI job that *does* hard-enforce RSS bounds
  (`shift-bench -max-rss`) and transport parity (`-max-ratio`).
- When govulncheck flags a reachable stdlib CVE, bump `toolchain` in
  `go.work` (this has already happened once: 1.26.2 → 1.26.5).
- One-time per clone: `make setup` (enables the pre-push hook).

## Repo mechanics

- **Workspace:** every top-level component is its own Go module listed in
  `go.work`; cross-module deps use `replace ../x` so modules also build
  standalone. New module ⇒ add to `go.work` **and** `MODULES` in the
  Makefile (that's what the gate iterates).
- **Dependency direction** (enforced by review): `connectors → sdk →
  engine`; `engine` is stdlib-only, forever. Adding any dep to engine, or
  any new dep tree elsewhere, deserves an ADR sentence at minimum.
- **Generated code** (`sdk/connectorpb`) is committed; regenerate with
  `make proto` (protoc + protoc-gen-go/go-grpc required locally).
- **Binaries** build to `bin/` (gitignored). `make build` for the set.

## Decision & documentation discipline

- Architectural decisions → `docs/adr/NNNN-slug.md` (Status, Context,
  Decision, Consequences). Changing a locked decision = a **superseding**
  ADR that references the old one. v0 died partly from silent forks; we
  don't do those.
- Every milestone updates: `PLAN.md` (exit criteria → measured result),
  `docs/bench-M*.md` if performance claims changed, the relevant
  `docs/dev/*` page, and `CLAUDE.md` if contracts/layout changed.
- Deliberate scope cuts are written down where they'll be found (PLAN
  "Deferred", or a GitHub issue) — silent truncation reads as "done".

## Testing idioms used here

- Differential testing against a reference implementation where one exists
  (ndjson vs `encoding/json`), plus `go test -fuzz` seeds kept green.
- Spilled-vs-in-memory equivalence for anything with an overflow path.
- Error-path tests are first-class: auth rejection, version mismatch,
  corrupt/truncated input, injected action failures (see
  `sdk/sdktest/protocol_test.go`).
- Integration tests that spawn real subprocesses guard with
  `testing.Short()` and build what they need into `t.TempDir()`.
- `testing.AllocsPerRun` locks in zero-alloc claims (`record` tests).

## Style notes that keep recurring

- Wrap errors with `%w` and operator/action context; compare with
  `errors.Is` (errorlint enforces).
- Reused scratch: `fmt.Appendf(buf[:0], ...)`, builders, batches — the
  allocation discipline is cultural, not just in the engine.
- Panics are for programmer errors only (builder misuse); data errors are
  errors.
- Ports/paths/limits are flags or config with sane defaults — no
  hardcoded magic (another v0 lesson; see `_archive`).
