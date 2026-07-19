# proto/

gRPC/protobuf contracts land here from M2 (ADR-0001):

- `connector/` — the runner↔connector protocol: handshake/version negotiation,
  record-batch framing, streaming source/sink services.
- `hub/` — the hub control API used by runners: registration, task
  lease/heartbeat/complete, telemetry.

Wire encoding of record batches is decided at the M1/M2 boundary and recorded
as an ADR (see ADR-0004 consequences).
