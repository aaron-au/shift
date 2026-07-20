# ADR-0018: Connector config-schema discovery — signed manifest v2

Date: 2026-07-20
Status: Accepted (design); implementation in M5.5 (Studio Builder), phase A

## Context

The studio is becoming a visual **builder** (ADR-0019), not just a read-only
graph view. A builder must render a **typed config form** per connector action
— e.g. the `http` connector's `get` action wants a `url`, optional `headers`,
and a secret-typed `token`. Today none of that shape is discoverable:

- A connector action's config is opaque `[]byte` end to end (SDK
  `SourceAction.Open(ctx, config []byte)`, proto `bytes config`). The only
  machine-readable description of an action's config is the connector's own
  **unexported Go struct** inside the binary (e.g. `httpconn.commonConfig`),
  invisible to the SDK host, the runner, and the hub.
- `Handshake` reports action **names only** (`source_actions` /
  `sink_actions`); there is no `Describe`/`Schema` RPC.
- The signed manifest (`pkg/consign`) binds identity → digest only; the
  registry stores identity + signature + the opaque blob. Nothing carries a
  schema.

The builder UI lives on the **hub**. Per doctrine the hub is control-plane
only: it never spawns connector subprocesses and never touches payload data.
So the hub cannot introspect a connector at request time to learn its schema.
Two ways to bridge that gap were considered:

- **A — schema baked into the signed manifest at publish (chosen).** The
  publisher's CLI extracts each action's schema from the connector once, at
  publish time, and the schema travels as part of the signed, tamper-evident
  artifact. The hub stores and serves it; the builder works against the hub
  alone, with **no runner online**.
- B — hub proxies a live "describe" to a runner that spawns the verified
  connector. No signing change, but the builder is coupled to a live runner
  and a new control RPC, and every builder session pays a spawn.

A was chosen (product decision, 2026-07-20): enterprise-correct — the schema
is **trusted** (signed, fail-closed verified like the artifact itself),
tamper-evident, and the hub stays self-contained. The cost is a signed-message
format bump, taken deliberately below.

## Decision

Add an **action config schema** (JSON Schema, draft-07 subset) to the
connector chain, signed as part of the artifact and served by the hub for the
builder to render forms.

### Authoring — SDK

Connector authors declare, per action, an optional JSON Schema describing that
action's config document. It is optional (a connector with no schema still
publishes; the builder falls back to the free-form JSON editor for that
action). The schema is a plain `[]byte` of JSON Schema on the action
registration, kept next to the existing `Sources`/`Sinks` factory maps so the
author writes it where they write the action.

The schema is **descriptive, not the validator**: the connector's `Open` stays
the authority on its config (defense in depth). The schema drives the form and
an advisory client-side check only.

### Wire — proto

Add a `Describe` RPC to `Connector` (proto/connector/v1) returning, per action,
its name, direction (source/sink), and config schema bytes. This is what the
**publisher CLI** calls once to extract schemas. Regenerate `connectorpb`.
(`Handshake` is unchanged — it stays the readiness/identity probe; the runner
still needs no schema at execution time, config remains opaque on the hot
path.)

### Signing — consign v2

The signed message gains a **schema digest**. `consign.Manifest` grows a
`Schemas` field (canonical JSON: a sorted map of `action → schema-bytes`, or
its SHA-256), and `Manifest.Message()` bumps its scheme tag
`shift-connector-artifact-v1` → `-v2`, appending the schema digest line so the
schema is covered by the same Ed25519 signature as identity + artifact digest.
Verification stays fail-closed. v1 artifacts remain verifiable (schema-less)
during transition; new publishes are v2.

### Storage + serving — hub registry

`connector_versions` gains a `config_schema JSONB` column (migration). Upload
(`PUT /api/v1/connectors/{name}/versions/{version}`) verifies the v2 signature
over identity + artifact digest + schema digest before storing, fail-closed.
The schema is surfaced to the builder on the existing read paths — added to the
`GET /api/v1/connectors` list items and `GET .../resolve` — so the UI gets
`{name, version, actions:[{name, direction, config_schema}]}` with **no payload
plane and no runner involved**.

### Rendering — builder

The builder (ADR-0019) renders typed inputs from the schema: strings, numbers,
booleans, enums, nested objects, and a **`secret`-typed** field that inserts a
`{"$secret":"name"}` ref picked from the account's secret list (ADR-0010) — the
one place secrets enter a config, never as plaintext. Transform steps
(filter/project/coerce/flatten/aggregate) need no connector schema — their
shape is already known in `pkg/flowdoc` and gets hand-written forms.

## Consequences

- One-time signed-format bump (v1 → v2). The version tag in `Message()` was
  designed as the evolvability hook; this is its first use. Old signed
  artifacts stay verifiable (as schema-less) so the registry need not be
  re-signed in a flag day.
- Schema is trusted to the same standard as the binary: tamper-evident,
  fail-closed. A connector cannot lie about its config shape without breaking
  its signature.
- Hub gains schema **storage + serving** but no execution/data role — the
  control/data split (ADR-0016) holds; the hub still never spawns a connector.
- Config stays opaque on the runner hot path — `Open([]byte)` and proto
  `bytes config` are unchanged; no per-record schema cost.
- Authors get an incremental, optional surface: no schema ⇒ builder shows the
  free-form JSON editor (with the secret picker) for that action, so the
  builder ships before every connector is annotated.

## Related / future work (out of scope here)

- **Runner credential store.** Runner-pull secret resolution and persisted hub
  credentials exist (`SHIFT_HUB_CRED_FILE`, ADR-0010/M4b). A **persistent,
  encrypted runner-local credential store synced from the hub**, with the hub
  sourcing KEK/secret material from an external **key vault** (provider TBD),
  is a natural evolution of ADR-0010's pluggable KEK — tracked separately, not
  required by the builder.

## Open questions (resolve at build)

- JSON Schema dialect scope — how much of draft-07 the form renderer supports
  (start: object with scalar/enum/nested/secret properties; arrays later).
- Whether `Schemas` in the signed message carries full schema bytes or only
  their digest (digest keeps the signed message small; full bytes make the
  artifact self-describing offline). Leaning digest + schemas stored alongside.
- Migration policy for the existing (v1) demo/reference connectors — re-publish
  as v2 vs serve schema-less until re-published.
