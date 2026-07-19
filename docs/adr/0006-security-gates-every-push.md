# ADR-0006: Security & reliability gates run on every push

**Status:** Accepted — 2026-07-19
**Decider:** Aaron

## Context
Security and reliability are up-front concerns, not afterthoughts — and Go's ecosystem ships mature, free tooling for both. The v0 prototype had zero static gates, which is how data races, swallowed errors, and plaintext-secret handling accumulated unnoticed.

## Decision
One gate, `make check`, runs identically in three places: locally on demand, automatically on **every `git push`** (pre-push hook via `core.hooksPath=.githooks`), and in GitHub Actions on every push/PR. It is the merge gate; a failure fails the push/build.

| Concern | Tool | How it runs |
|---|---|---|
| Known CVEs in dependencies (reachable-call analysis) | `govulncheck` | standalone, per module |
| Security anti-patterns (SQLi, weak crypto, temp files, exec) | `gosec` | as a golangci-lint linter |
| Correctness/reliability static analysis | `staticcheck`, `errcheck`, `govet` + curated linters | golangci-lint (`.golangci.yml`) |
| Committed secrets/credentials | `gitleaks` | standalone, repo-wide |
| Data races | `go test -race` | per module, always — never an opt-in mode |
| Formatting drift | `gofmt`/`goimports` | golangci-lint formatters |

Rules:
- The local and CI gates are the **same make target** — no "CI-only" checks that developers can't reproduce, no local checks CI doesn't enforce.
- Linter findings are fixed or explicitly suppressed inline with a justification comment (`//nolint:<linter> // reason`); blanket exclusions in config require an ADR-worthy reason.
- Dependency updates run govulncheck as part of the same gate; a new CVE blocks until patched or assessed.
- Tool versions are pinned in CI (and documented) so local/CI results agree.

## Consequences
- Pushes are slower by the gate's runtime — accepted; keep the gate fast enough that nobody is tempted to `--no-verify` (target < a couple of minutes; benchmarks are a separate non-blocking job).
- `.githooks/pre-push` requires each clone to run `git config core.hooksPath .githooks` once — documented in README and Makefile `setup` target.
