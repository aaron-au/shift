# ADR-0025: Connector introspection — live Discover + dynamic operation catalogs

Date: 2026-07-28
Status: **Designed (build deferred)** — sequenced after the base connector fleet

## Context

ADR-0018 gave every connector a **static descriptor**: a per-binary, signed,
offline list of actions + config schemas, extracted once at publish and served
by the hub with no runner online and no payload plane. That is the right shape
for connectors whose operation set is **fixed and known at build time** — the
`http` connector's `get`/`post`, SFTP's `get`/`put`/`list`/`delete`. The verb
dropdown (ADR-0024) renders straight from it.

Real enterprise systems are not fixed at build time. Their operation set lives
**in the target system, behind the customer's credentials**:

- **ServiceNow** exposes hundreds of *tables* (`incident`, `change_request`,
  custom `u_*` tables) — each effectively an action with its own field set. No
  connector binary can enumerate them; they differ per instance.
- A **database** connector's real actions are "insert into `orders`", "query
  `customers`" — discoverable only by reading `information_schema` on the live
  DB, with the live column list as the config schema.
- A **SOAP** connector's operations are defined by a **WSDL** fetched from the
  endpoint; an **OpenAPI/Swagger** doc similarly defines an HTTP API's paths,
  params, and bodies. The "operations" are a document the connector must fetch
  and parse.

The static descriptor cannot carry any of this: it is per-binary and offline,
and the catalog is **per-connection, credential-gated, and dynamic**. But the
hub — where the builder lives — must never hold credentials or make outbound
calls (ADR-0016 control/data split; ADR-0010 runner-pull secrets). So the hub
cannot introspect the target itself, exactly as it cannot introspect a
connector's config schema itself (ADR-0018's whole premise).

What is missing is a **live, runner-side introspection** capability: connect to
the target with resolved credentials, enumerate its operations, and hand the
studio a **catalog** — as *metadata*, not payload — so the builder can offer
"insert into `orders`" as a first-class node without the author hand-typing
schemas.

## Decision

Add a **live Discover** capability to the connector contract: a connector may
connect to a live system using **resolved credentials** and return a **catalog**
of available operations and their config schemas. It runs **runner-side**; the
catalog is **metadata** relayed to the studio *through* the hub as an **opaque
blob**; and it is **distinct from the static descriptor** in every axis —
*live* vs offline, *per-connection* vs per-binary, *dynamic* vs signed-at-publish.

### Discover vs the static descriptor (ADR-0018) — keep these separate

| | Static descriptor (ADR-0018) | Live Discover (this ADR) |
|---|---|---|
| Source | the connector **binary** | the **target system**, via creds |
| When | once, at **publish** | per builder request, **live** |
| Scope | per-binary, all instances | per-**connection** (this instance) |
| Trust | **signed**, tamper-evident, fail-closed verify | fetched at runtime; **not** signed |
| Needs a runner? | **no** (hub serves it offline) | **yes** (runner spawns + calls out) |
| Needs creds? | no | **yes** (resolved runner-side) |
| Shape | `Descriptor{Actions:[ActionDescriptor]}` | **same shape**, dynamically built |

The descriptor advertises *whether* a connector supports Discover and *how to
drive it* (a discoverable action — e.g. the db connector's synthetic
`__discover` verb, or ServiceNow's "list tables"). Discover returns *what this
connection actually offers*. They compose: the static descriptor is the entry
point; the catalog is the live expansion.

### Authoring — SDK Discover RPC

Add an **optional** `Discover` capability to the connector contract, parallel to
`Describe` (ADR-0018) but live and credential-bearing:

- **Request** = `{action, config}` — the discovery action name plus its
  **resolved config** (the connection's endpoint + `{"$secret":...}` refs
  already resolved to plaintext by the runner, exactly as a task's config is —
  ADR-0010). This is the one place introspection receives credentials.
- **Response** = a **catalog** that **reuses the existing shapes**: a
  `Descriptor`/`ActionDescriptor` list (`sdk.ActionDescriptor{Action, Direction,
  ConfigSchema}`), where each discovered operation becomes an action with its
  own JSON-Schema `ConfigSchema` (e.g. a table's columns → the insert action's
  config fields). No new schema vocabulary — the builder already renders
  `ActionDescriptor` config forms (ADR-0018 §Rendering), so a discovered action
  renders identically to a static one, secret picker and all.

Connectors implement `Discover` when their operation set is dynamic; connectors
with a fixed set (the base fleet) simply don't, and the descriptor's
`supportsDiscover=false` tells the builder to skip it. Add a `Discover` RPC to
`proto/connector/v1` and regenerate `connectorpb`; the runner-side `sdk/host`
gains a `Discover(ctx, action, config)` adapter (like `Describe`/`Attach`).

### Triggering — hub endpoint dispatches to a runner (capacity-gated)

The builder cannot call a connector directly (no payload/exec plane on the hub).
So Discover rides the **same dispatch machinery as a task**, one-shot:

1. The studio calls a **hub** endpoint (design-time, human realm), e.g.
   `POST /api/v1/connectors/{name}/discover` with `{action, config}` where
   `config` still carries **inert `{"$secret":...}` refs** — the hub never sees
   plaintext (ADR-0010).
2. The hub **dispatches a discovery job to a runner** — a metadata-only control
   message, **capacity-gated** through the same admission as a leased task
   (ADR-0005/0008): a runner with no headroom does not take it. The hub queues
   *metadata*, never payload.
3. The runner **resolves the secret refs** (runner-pull, `POST
   /api/v1/secrets/resolve`, ADR-0010), spawns the **verified, signed**
   connector (ADR-0011 fail-closed), and calls `Discover`. The **outbound call
   to the target originates on the runner**, in the data plane — the hub makes
   no outbound call and holds no credential.
4. The runner returns the **catalog to the hub as an opaque blob** (metadata:
   action names + schemas, no target payload), which the hub relays to the
   studio. The hub **does not parse it** (mirrors ADR-0018: opaque descriptor
   bytes) — it is a transport, not a consumer.

This is ADR-0018's rejected "option B" (live describe via a runner) — correctly
rejected *there* because the static config schema is knowable offline and should
be signed. It is the **right** answer *here*: the catalog is inherently live,
credential-gated, and per-connection, so it *cannot* be signed at publish and
*must* touch a runner. The two mechanisms coexist; neither replaces the other.

### Optional materialize — persist a fetched catalog as a per-connection descriptor

Discover is live, so the builder needs a runner online each time — friction for
iterative design. **Optionally**, the studio may **materialize** a fetched
catalog: persist the returned `Descriptor` on the hub, **bound to a specific
connection/account** (not to the binary, not signed as an artifact — it is
*discovered* data, not *authored* trust), so the builder renders those actions
**offline afterward**, exactly like a static descriptor. Materialized catalogs
are:

- **Per-connection, per-account**, versioned, and **refreshable** (re-run
  Discover to pick up new tables / a changed WSDL) — with staleness surfaced in
  the UI, since the target can drift.
- **Untrusted** relative to signed artifacts: stored as tenant design data
  (`store.WithAccount`), never conflated with the signed `config_schema` column
  (ADR-0018) and never fed into signature verification. A materialized action
  still resolves, at run time, to the underlying connector's real action + the
  live config — the catalog only shaped the form.

Materialize is genuinely optional: the minimum viable feature is live-only
Discover; persistence is a builder-ergonomics layer on top.

### Security

- **Credentials resolved runner-side, never at the hub, never logged.** Discover
  reuses the ADR-0010 runner-pull path verbatim; the hub relays refs, the runner
  resolves them just-in-time. Secret values never enter the catalog, the hub,
  the dispatch metadata, or any log line — errors carry action/field **names,
  never values** (same rule as task secret resolution).
- **Catalog redaction.** The connector builds the catalog from target metadata
  (table names, columns, WSDL operations) — schema, not data rows. The runner
  applies the bounded, secret-redacted discipline of the `Sampler` (ADR-0014):
  the catalog is metadata only; no target **payload** (row values, response
  bodies beyond the schema doc) is ever included, and anything schema-shaped
  that could echo a credential is dropped.
- **Fail-closed, everywhere.** Connector still verified fail-closed before spawn
  (ADR-0011). A malformed/oversized catalog, a discovery timeout, or a
  capability the connector didn't declare ⇒ the request **fails**, no partial
  or coerced catalog. Capacity-gated dispatch means Discover cannot be used to
  stampede runners. Capability policy (ADR-0015) still applies — a connector
  denied on a hub cannot be discovered there.
- **Materialized catalogs are tenant-scoped and unsigned by construction**, kept
  out of the signed supply chain so a discovered blob can never be mistaken for
  a trusted artifact.

### Both cases are in scope

- **Technology connectors (generic introspection).** One `db` connector
  discovers *any* schema via `information_schema`; one `soap` connector parses
  *any* WSDL; the `http`/technology connector **imports from an
  OpenAPI/Swagger** doc — turning an API description into a set of actions. The
  connector is generic; the catalog is entirely instance-driven.
- **Specific connectors (ServiceNow).** A first-party ServiceNow connector lists
  the instance's **tables** and turns each into an action with its field set as
  the config schema — the classic "pick your object" iPaaS ergonomic, built on
  the exact same Discover RPC + catalog shape.

Both produce the same `Descriptor`/`ActionDescriptor` output and flow through the
same hub→runner→studio relay. There is no per-connector special-casing in the
hub or builder.

## Consequences

- The connector model now spans the full iPaaS range: **fixed** operation sets
  (static descriptor, offline, signed) *and* **dynamic** ones (live Discover,
  per-connection) — without weakening the hub's payload-free, credential-free
  posture. Discover is the first control-plane request that **requires** a
  runner and resolved credentials, and it is confined to the data plane exactly
  like a task.
- A new **optional** RPC (`Discover`) + proto/regeneration + a `sdk/host`
  adapter; a hub **dispatch** endpoint and a runner discovery job path (reusing
  admission, secret resolution, connector verification). No engine hot-path
  change — Discover is design-time, off the record path entirely.
- The static/dynamic distinction is **explicit and load-bearing**: ADR-0018
  stays the trusted, offline baseline; this ADR is the live expansion. Keeping
  materialized catalogs out of the signed supply chain preserves ADR-0011's
  trust boundary.
- **Sequencing:** design-only, build deferred. It sits *after* the base
  connector fleet (which needs no Discover) and naturally *after* ADR-0024
  Phase 2 request-reply, since a discovered "query `customers`" action is most
  useful as a mid-flow call. Picked up when the first dynamic-catalog connector
  (db or ServiceNow) is scheduled.

## Open questions (resolve at build)

- **Catalog scale.** ServiceNow/large DBs expose thousands of tables — does
  Discover page/filter (discover *one* table on demand) rather than return the
  full catalog? Likely a parameterized Discover (`{action:"describe-table",
  table:"orders"}`) plus a cheap "list names" pass.
- **Materialize lifecycle.** TTL/refresh policy, staleness signalling, and
  whether a materialized action pins a catalog version or always re-resolves
  live at run time.
- **Schema fidelity.** How faithfully a WSDL/OpenAPI/DB type maps onto the
  draft-07 subset the builder renders (ADR-0018 open question), and the
  fallback when a discovered type is unrepresentable (free-form field).
- **Discovery auth vs task auth.** Whether a discovery job reuses a task's
  idempotency/lease semantics at all (it is read-only, side-effect-free,
  one-shot) or gets a lighter dispatch path.
