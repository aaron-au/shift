# 03 — The Connector Protocol (`sdk/`, `proto/`, `connectors/`)

Connectors are standalone binaries, never compiled into the runner
(ADR-0001; v0's Go-plugin `.so` approach is the cautionary tale). The
protocol is deliberately small — read `proto/connector/v1/connector.proto`
alongside this doc; ADR-0007 records the reasoning.

## The spawn contract

The host (runner) does, in order:

1. Creates a fresh **0700** temp dir; picks `connector.sock` inside it.
2. Generates a 32-byte random hex **token**.
3. Spawns the binary with env vars `SHIFT_CONNECTOR_SOCKET` and
   `SHIFT_CONNECTOR_TOKEN` (everything else inherited; stderr passes
   through for connector logs).
4. Dials the socket and retries `Handshake` every 50 ms until it answers
   (handshake **is** the readiness probe), the child exits (fail fast — a
   waiter goroutine watches `cmd.Wait`), or the timeout (10 s default)
   passes.

The connector side is all inside `sdk.Serve(connector)`: bind the socket,
serve gRPC, honor SIGTERM/SIGINT and the `Shutdown` RPC via
`GracefulStop` (in-flight streams drain).

**Auth:** every RPC — including Handshake — must carry the token in
metadata key `shift-token`; the server compares constant-time and returns
`Unauthenticated` otherwise. This is same-host hijack protection; binary
*authenticity* (signing) is the M4 registry's job.

**Version negotiation:** the host offers its protocol versions ascending;
the connector picks or rejects with `FailedPrecondition`. Bump
`sdk.ProtocolVersion` only with an ADR.

## How data crosses: frames

Batches are encoded with the engine binary codec (`engine/spill`) into one
`Frame{payload bytes, records int64}` per batch — protobuf never sees
individual records, so there is no per-record marshal tax and no forced
schema. Frame size therefore tracks batch size (~1 MiB target); gRPC
message limits are set to 64 MiB as a runaway guard. Measured cost of the
whole transport: **1.32×** in-process wall time with subprocess source and
sink (`docs/bench-M2.md`).

- **Source** (`Pull`): server-streaming. Request carries `action` +
  `config` (opaque JSON). Stream ends cleanly at EOF; action errors arrive
  as gRPC status errors (`InvalidArgument` for bad config, `Internal` for
  runtime failures).
- **Sink** (`Push`): client-streaming. First message is `PushOpen{action,
  config}`, then frames; the summary confirms total records. Note the gRPC
  quirk handled in `host.SinkStream.sendErr`: a failed `Send` reports
  `io.EOF` and the real error is parked on `CloseAndRecv`.

- **Describe** (`Describe`): unary. Returns the connector's identity plus,
  per action, its `direction` (`source`/`sink`) and an optional
  `config_schema` (JSON Schema, draft-07 subset). It is **not on the
  execution path** — the runner never calls it; publisher tooling does, to
  extract the signed *descriptor* (see below). Config stays opaque `bytes`
  on `Pull`/`Push`; `Describe` only publishes its *shape*.

## Config schemas & the descriptor (ADR-0018)

So the studio builder can render a typed config form per action, a
connector optionally declares a JSON Schema per action and the shape is
carried, **signed**, to the hub:

1. **Author** sets `Connector.Schemas[action] = []byte(jsonSchema)`.
   Secret-typed string fields carry `"x-shift-secret": true` so the builder
   offers a secret picker (`{"$secret":"name"}`). Optional — an action with
   no schema falls back to a raw-JSON editor.
2. **Descriptor.** `sdk.BuildDescriptor` + `sdk.CanonicalDescriptor` render
   a deterministic JSON blob (actions sorted by `direction,action`) — the
   *descriptor*. Two ways to extract it, both identical bytes:
   `<binary> describe` (a non-serving CLI mode of `sdk.Serve`, no gRPC), or
   `host.ExtractDescriptor` (spawns + calls the `Describe` RPC).
3. **Signing.** The publisher hashes the descriptor and folds the digest
   into `consign.Manifest.DescriptorDigest`; `Message()` then renders the
   **v2** signed form (`shift-connector-artifact-v2` + `descriptor-sha256`
   line). A manifest with no descriptor renders the byte-identical **v1**
   form, so pre-descriptor artifacts and signatures stay valid.
4. **Hub.** Upload carries the descriptor as base64 `X-Shift-Descriptor`;
   the hub re-hashes it, verifies the v2 signature fail-closed, and stores
   it verbatim (`connector_versions.descriptor`). `resolve`/`list` serve it
   back as base64 (byte-exact) so the runner re-verifies v2 and the builder
   renders forms **with no runner online**. The hub never parses it.

The descriptor is opaque signed bytes end to end — treated exactly like the
artifact digest, never re-marshaled (re-marshaling would change the bytes
and break the digest).

**Discovery metadata (M6e).** A connector may set `Meta *sdk.ConnectorMeta`
(description, category, icon, tags) — optional marketplace metadata that rides
*in* the descriptor, so it is signed and tamper-evident, and the hub still
never parses it (the studio decodes it client-side for the Marketplace browse
cards). `CanonicalDescriptor` sorts tags so re-hash is independent of declared
order; an absent `Meta` (nil) keeps the descriptor byte-identical to a
metadata-free one (v1/parity preserved).

**Publishing.** `shift-consign publish -key … -name … -version … -hub … \
-publisher-key … [-describe | -descriptor <file>] <artifact>` signs (v2 when a
descriptor is bound) and uploads in one step (`-describe` runs `<artifact>
describe` to extract the descriptor; the host must match the artifact's
os/arch). `shift-consign sign` still just prints the digest + signature.

## Writing a connector

Implement one or both interfaces (they mirror the engine's stream
contract, including the batch-lifetime rule):

```go
type SourceAction interface {
    Open(ctx context.Context, config []byte) error
    Next(ctx context.Context) (*record.Batch, error) // io.EOF when done
    Close() error
}
type SinkAction interface {
    Open(ctx context.Context, config []byte) error
    Write(ctx context.Context, b *record.Batch) error // must not retain b
    Close() error
}
```

Register factories (one fresh instance per stream) and serve:

```go
func main() {
    err := sdk.Serve(sdk.Connector{
        Name: "myconn", Version: "0.1.0",
        Sources: map[string]func() sdk.SourceAction{"read": newRead},
        Sinks:   map[string]func() sdk.SinkAction{"write": newWrite},
        Schemas: map[string][]byte{"read": []byte(readSchema)}, // optional (ADR-0018)
    })
    ...
}
```

Guidelines (enforced by review, informed by the existing connectors):
- **Stream, never buffer bodies.** The http connector parses response
  bodies incrementally by wrapping them in the engine's ndjson reader;
  its sink POSTs per batch (memory bounded by batch size).
- Build records with a reused `record.Batch` + builder; reuse scratch
  buffers (`fmt.Appendf` into a slice, not `Sprintf`).
- Config is a JSON document; validate in `Open` and fail there
  (surfaces as `InvalidArgument`).
- Network-facing connectors need SSRF posture: the http connector refuses
  loopback/link-local **post-DNS-resolution** unless `allow_local` is set.
- Secrets arrive inside config today; never log config. (Hub-managed
  secret references replace inline secrets in M4/M5.)

## Testing a connector

- **Unit-test actions directly** — they're plain structs (see
  `connectors/internal/httpconn/httpconn_test.go` with `httptest`).
- **Wire-test via `sdktest.Serve(t, connector)`** — runs your connector
  in-process over a real unix socket and returns an attached
  `*host.Process`; pull/push through it exactly as the runner will
  (see `sdk/sdktest/protocol_test.go` for the patterns, including error
  propagation assertions).
- The spawn path itself is covered by
  `connectors/launch_integration_test.go`; you don't need to re-test it.

## Host-side lifecycle (runner integration)

`host.Launch` → `*host.Process` → `.Source(action, config)` /
`.Sink(action, config)` adapters that satisfy `stream.Source`/`Sink` —
so a pipeline can't tell remote from local. `.Health(ctx)` probes
liveness; `.Close()` does Shutdown-RPC → 5 s grace → SIGKILL → reap +
socket-dir removal. Pooling/idle-reap/restart-backoff live in the
**runner's** connector pool (04-runner.md), not in `host`.
