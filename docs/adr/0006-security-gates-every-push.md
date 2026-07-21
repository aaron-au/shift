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
| GitHub Actions workflow errors (YAML/expr/shellcheck) | `actionlint` | standalone, `.github/workflows/` |

Rules:
- The local and CI gates are the **same make target** — no "CI-only" checks that developers can't reproduce, no local checks CI doesn't enforce.
- Linter findings are fixed or explicitly suppressed inline with a justification comment (`//nolint:<linter> // reason`); blanket exclusions in config require an ADR-worthy reason.
- Dependency updates run govulncheck as part of the same gate; a new CVE blocks until patched or assessed.
- Tool versions are pinned in CI (and documented) so local/CI results agree.

## Consequences
- Pushes are slower by the gate's runtime — accepted; keep the gate fast enough that nobody is tempted to `--no-verify` (target < a couple of minutes; benchmarks are a separate non-blocking job).
- `.githooks/pre-push` requires each clone to run `git config core.hooksPath .githooks` once — documented in README and Makefile `setup` target.

## Amendment 2026-07-21: a release/scheduled supply-chain tier
The "one identical gate, no CI-only checks" rule (above) governs the
**correctness & code-security** gate — the things a developer must be able to
reproduce before pushing. Some genuinely valuable **supply-chain** checks
cannot live in pre-push at all: they need infrastructure a developer checkout
doesn't have (a built OCI image, a Docker daemon, protoc + pinned plugins) or
runtimes measured in minutes (deep SAST). Forcing them into `make check` would
either break loopback developers or blow the fast-gate budget; leaving them out
of `make check` while running them in CI would violate the rule above.

Resolution: a **separate, explicitly-scoped tier** — `.github/workflows/supply-chain.yml`
— run on release tags and `workflow_dispatch` (and optionally scheduled), NOT on
every push/PR. It is not part of `make check` and does not gate ordinary merges;
it is a distinct release-hardening surface. This is a conscious extension of
ADR-0006, not a loophole: the fast gate stays identical everywhere; the release
tier is additive and clearly labelled.

Candidate tier contents (each added only when it earns its keep — see the
2026-07-21 tooling review): Trivy image scan + digest-pinned base images,
hadolint Dockerfile lint, a `proto-check` generated-code drift gate (with pinned
protoc), CodeQL, SBOM generation, and artifact signing. Local/pre-push gains
`tidy-check` (module hygiene) and `-shuffle=on -count=1` on the test run, which
DO belong in the identical gate because every developer can run them.

## Amendment 2026-07-21: actionlint in the core gate
The gate validates Go but not the CI definition itself. A workflow file with an
unquoted `name:` value containing a colon parsed as invalid YAML; GitHub rejected
it as a `startup_failure` (0s, no logs) on every push while `make check` stayed
green — the exact failure the gate is meant to prevent, in the one file the gate
didn't cover. `actionlint` (single static binary; also runs shellcheck over `run:`
scripts) now runs in `make check` (`make actions`, fail-closed) and is pinned in
CI (`ACTIONLINT_VERSION`), so workflow errors fail locally and pre-push, not only
after the push lands. It belongs in the identical gate — every developer can run
it, no infrastructure needed.
