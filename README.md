# SHIFT

[![ci](https://github.com/aaron-au/shift/actions/workflows/ci.yml/badge.svg)](https://github.com/aaron-au/shift/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/aaron-au/shift/main/badges/coverage.json)](https://github.com/aaron-au/shift/actions/workflows/ci.yml)

A hub-and-spoke Integration Platform as a Service (iPaaS) in Go, built for
**performance**: streaming, memory-efficient, disk-light transformations —
where the incumbents buffer and hit the disk.

**Status: M0 (scaffold).** The platform is being rebuilt from a reviewed 2025
prototype (preserved in `_archive/`, reference-only).

## Shape

- **Hub** — HA control plane (stateless Go services over HA Postgres): identity
  (OIDC), design studio, versioned flows, signed connector registry, and the
  durable task queue. Deployable as cloud SaaS or local/on-prem — same images.
- **Runner** — stateless, disposable execution container. Leases tasks from a
  hub, streams record batches through connector subprocesses, holds nothing
  durable. Scale-out is unbounded by design.
- **Connectors** — standalone signed binaries spawned by the runner, speaking
  gRPC over unix sockets, streaming end-to-end.

Key documents: [PLAN.md](PLAN.md) (milestones), [docs/adr/](docs/adr/)
(locked decisions), [docs/REVIEW-2026-07.md](docs/REVIEW-2026-07.md)
(the prototype review that triggered the rebuild), [CLAUDE.md](CLAUDE.md)
(doctrine + agent guide).

## Development

Requires Go 1.26+, plus the gate tools: `golangci-lint`, `govulncheck`,
`gitleaks`.

```bash
make setup   # once per clone: enables the pre-push security gate
make build   # binaries into bin/
make test    # all modules, race detector always on
make check   # THE gate: fmt, vet, lint (incl. gosec/staticcheck),
             # govulncheck, gitleaks, race tests — runs on every push and in CI
```

Workspace layout: `go.work` spanning `engine/` (streaming core — no network
deps), `sdk/` (connector SDK), `runner/`, `hub/`, `pkg/` (shared primitives),
with `proto/` (gRPC contracts) and `deploy/` (compose profiles).
