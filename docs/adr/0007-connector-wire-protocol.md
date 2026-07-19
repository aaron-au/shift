# ADR-0007: Connector wire protocol — gRPC over UDS, opaque batch frames, slim handshake

**Status:** Accepted — 2026-07-19
**Decider:** Claude (design), per ADR-0001's direction; Aaron approved M2 scope

## Context
ADR-0001 chose out-of-process connectors speaking gRPC ("hashicorp/go-plugin pattern, or an equivalent slim implementation") and ADR-0004 deferred the record-batch wire encoding to the M1/M2 boundary. Constraints: streaming end-to-end, no per-record serialization tax, engine module stays dependency-free, local-only transport for now (runner and connector share a container).

## Decisions

**1. Slim custom handshake, not hashicorp/go-plugin.** The runner spawns the
connector binary with two env vars: `SHIFT_CONNECTOR_SOCKET` (unix socket
path in a 0700 scratch dir) and `SHIFT_CONNECTOR_TOKEN` (per-process random
token). The connector serves gRPC on that socket; the host dials and calls
`Handshake`, which verifies the token and negotiates the protocol version.
go-plugin was rejected: it drags net/rpc heritage, connection muxing, and
its own lifecycle opinions for what is here ~300 lines of code we fully
control.

**2. Record batches cross as opaque binary frames.** The gRPC messages
carry `bytes` — a whole batch encoded with the engine's tag-based binary
codec (`engine/spill`, promoted to the shared wire encoding). Protobuf
never sees individual records: per-record proto marshalling would
reintroduce the reflection/allocation tax the engine exists to avoid, and
would force a schema where the record model is deliberately hierarchical
and schema-carrying (ADR-0004).

**3. Service shape mirrors the engine's Source/Sink contract.**
`Pull(action, config) → stream of frames` for sources;
`Push(stream of frames) → summary` for sinks; plus `Handshake`, `Health`,
`Shutdown`. Connector authors implement `sdk.SourceAction` /
`sdk.SinkAction` — the same pull/write semantics as `stream.Source`/`Sink`,
so in-process and out-of-process operators compose identically.

**4. Local security now, registry signing later.** The 0700 socket
directory plus required token on every RPC prevents same-host hijacking.
Binary signature verification belongs to the hub's connector registry (M4)
— noted, not implemented in M2. The HTTP connector ships with an SSRF guard
(link-local/metadata and loopback targets refused unless explicitly
allowed).

**5. Every RPC carries the token in metadata; streams are canceled via
context.** A connector crash surfaces as a stream error to the pipeline —
fail-fast, same as any operator error (retry/error-routing policy arrives
with the flow model, M5).

## Consequences
- `sdk` (and `connectors`) modules take on google.golang.org/grpc +
  protobuf deps; `engine` remains stdlib-only. Dependency direction:
  connectors → sdk → engine.
- Generated proto code is committed (no protoc needed to build); `make
  proto` regenerates from `proto/connector/v1/`.
- The parity benchmark (`sdk/cmd/shift-bench-remote`) quantifies UDS+frame
  overhead vs in-process pipelines — M2's exit criterion.
- TCP+mTLS transport for cross-host connectors is a straightforward later
  extension of the same protocol (the handshake already negotiates
  versions).
