# M2 Benchmark Results — Connector Transport Parity

Run 2026-07-19 on Apple M4 Max (darwin/arm64), Go 1.26.2. Reproduce with
`connectors/cmd/shift-bench-remote` (deterministic generator; identical
pipeline ops in both modes).

## Exit criterion (PLAN M2): quantified out-of-process overhead

Pipeline: gen source → filter → project → discard sink, 5M records
(~1 GiB-equivalent record volume). Remote mode runs source **and** sink in
a spawned connector subprocess over gRPC/UDS with binary batch frames
(ADR-0007) — records cross the process boundary twice.

| Mode | Wall | Throughput | Ratio |
|---|---|---|---|
| In-process source/sink | 3.11 s | 1.61M rec/s | 1.00× |
| Subprocess connector (spawn + handshake included) | 4.11 s | 1.22M rec/s | **1.32×** |

Interpretation: the full transport — frame encode → gRPC over unix socket
→ frame decode, both directions — costs ~32% wall time on a pipeline with
deliberately cheap ops. Real connectors do network I/O that dwarfs this;
the transport will not be the bottleneck. ~1.8M records/sec cross the
process boundary per core.

## What M2 shipped
- `proto/connector/v1` + generated `sdk/connectorpb` (gRPC over UDS,
  opaque batch frames using the engine binary codec — ADR-0007).
- `sdk`: author-side `Serve` (token auth on every RPC, graceful shutdown),
  `SourceAction`/`SinkAction` mirroring the engine stream contract.
- `sdk/host`: `Launch` (0700 socket dir, random token, handshake-as-
  readiness-probe, fail-fast on child exit, kill-after-grace close) and
  stream.Source/Sink adapters — remote connectors compose into pipelines
  identically to in-process operators.
- `sdk/sdktest`: in-process wire-protocol harness for connector tests.
- `connectors`: `gen` (test/bench) and `http` (streaming GET source that
  parses response bodies incrementally; NDJSON POST sink; SSRF guard
  refusing loopback/link-local unless `allow_local`).
- Tests: protocol suite (auth rejection, version negotiation, error
  propagation both directions, engine-pipeline composition), connector
  units, and real-subprocess integration (spawn → pull 25k → push 5k →
  graceful shutdown; fail-fast on dead binary).

## Deferred (tracked)
- Binary signature verification → M4 (hub connector registry owns signing;
  ADR-0007 §4).
- Idle-reap / restart-with-backoff pooling → M3 (runner lifecycle).
- TCP+mTLS transport for cross-host connectors → when a use case lands.
