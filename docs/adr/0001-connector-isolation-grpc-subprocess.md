# ADR-0001: Connectors run as gRPC subprocesses (WASM for transforms later)

**Status:** Accepted — 2026-07-19
**Decider:** Aaron

## Context
Connectors must never be compiled into the runner binary. The v0 prototype used Go native `plugin` `.so` files, which failed: exact toolchain/dependency/flag lock-in, Linux/macOS only, unloadable, and an unauthenticated-download RCE vector (see `docs/REVIEW-2026-07.md` §3.3). Requirements: crash isolation, independent release cadence, polyglot connector authorship, and compatibility with the streaming data-plane contract (ADR-0004).

## Decision
Connectors are **standalone binaries the runner spawns as child processes and communicates with over gRPC** (hashicorp/go-plugin handshake pattern, or an equivalent slim implementation), using unix domain sockets in-container.

- Streaming record batches cross the process boundary as gRPC streams — fits the engine contract natively.
- Crash containment: a panicking connector kills its process, not the runner; the runner restarts it with backoff.
- Polyglot: any language with gRPC can implement a connector; the official SDK is Go.
- Distribution: connectors ship as OCI artifacts / binaries **signed** by the hub's connector registry (checksum alone is insufficient — v0 lesson). Verified before spawn.
- Lifecycle: spawn on first use per flow deployment, health-checked, idle-reaped, hard-capped per runner.

**Deferred:** lightweight user-authored transforms/scripts run as sandboxed WASM (wazero) in a later milestone (M5+). WASM was rejected for connectors themselves today because WASI socket support is immature and connectors are network-heavy by definition.

## Consequences
- Runner images bundle (or lazily fetch) connector binaries; one extra process per active connector type.
- The connector SDK must expose a streaming interface, not buffer-in/buffer-out (v0 lesson: `Execute([]byte) ([]byte, error)` forces platform-wide buffering).
- Protocol versioning between runner and connector becomes an explicit compatibility surface (handshake includes protocol + schema versions).
