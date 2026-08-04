# ADR-0027: Application connector framework — declarative SaaS/enterprise connectors

Date: 2026-07-28
Status: **Designed (build deferred)** — sequenced after the Discover RPC
(ADR-0025 connector introspection) and the base connector fleet. One
prerequisite has shipped ahead of the framework: the **declarative mapper**
(restructure + const + concat + coerce, `engine/stream` + `pkg/flowdoc`, #42),
which is how an app connector's canonical shape is mapped without custom code.

## Context

To rival webMethods/Workato/Boomi, SHIFT needs **breadth**: dozens of
branded application connectors — Salesforce, ServiceNow, Dynamics 365 /
Dataverse, SAP, NetSuite, HubSpot, Jira, Microsoft Graph — each exposing
hundreds of entities and operations. Writing bespoke Go per vendor, per
entity, does not scale and contradicts the "developer- and AI-friendly,
low-per-connector-cost" thesis.

Key realisation (the thesis of this ADR): **almost every enterprise app is
REST / OData / SOAP / GraphQL underneath, backed by a machine-readable
contract and a live discovery endpoint.** Salesforce has `/describe` and a
REST/Bulk API; ServiceNow has the Table API + a dictionary; Dynamics /
Dataverse *is* OData v4 with `$metadata`; SAP exposes OData / RFC-BAPI /
IDoc; NetSuite has SuiteQL + async record APIs; MS Graph is OpenAPI;
Jira/HubSpot are REST + OpenAPI. The per-vendor *code* is small; the
reusable **core** — protocol clients, auth, pagination, cursors, discovery
— is the real engineering, and it is shared across the whole fleet.

This layers directly on decisions already made:

- **ADR-0024** — a connector is **one canvas node**; the author picks a
  **verb** from a dropdown and the source/sink role follows the action's
  declared `direction`. Discovered application actions must render exactly
  like the hand-written `http`/`sftp` actions do — no new UX.
- **ADR-0018** — each action carries a signed JSON-Schema **descriptor**;
  the hub stores and serves it so the builder renders config forms with no
  runner online and no payload plane.
- **ADR-0025** (connector introspection, its predecessor) — a `Discover`
  RPC turns a *live* system's metadata into a **catalog** of actions +
  their schemas, the dynamic sibling of ADR-0018's static descriptor. This
  ADR is the framework that *produces* those catalogs and *executes* the
  actions they name.

Two connector **archetypes** fall out, and the framework serves both:

- **Technology connectors** — generic protocol engines: `http`, `db`,
  `soap`, `odata`, `graphql`. A protocol client plus a **contract importer**
  (point it at an OpenAPI doc / WSDL / OData `$metadata` / SQL schema and it
  produces the catalog). No branding, no vendor knowledge.
- **Specific connectors** — a branded shell over a technology core:
  base-URL template + an **auth profile** + a **discovery adapter** +
  **entity → action rules**. "Salesforce" is `odata`/REST-core + SFDC OAuth
  + the `salesforce-describe` adapter + upsert/bulk rules. The branded part
  is *declarative*, a few hundred lines of manifest, not a new Go binary.

## Decision

Build a reusable **application connector framework** in the SDK/connector
layer with four pluggable pieces. **Everything below runs runner-side**; the
hub only ever stores and serves the *resulting* signed catalog + schemas
(ADR-0016 control/data split, ADR-0018 storage-only role). Shipped
application connectors are still **signed artifacts** (ADR-0011).

### 1. Discovery adapters (pluggable)

A `DiscoveryAdapter` turns a live system's metadata into an **ADR-0025
catalog** — the list of actions, each with its `direction`, verb, and
JSON-Schema config/field shape (ADR-0018 descriptor form). Adapters are
registered by kind; a manifest names which one a connector uses:

- **Technology adapters** (contract → catalog):
  - `openapi` — OpenAPI 3 / Swagger → one action per operation.
  - `odata` — OData `$metadata` → entity sets → query/get/create/update/
    delete/upsert actions with typed fields.
  - `graphql` — GraphQL introspection → queries as sources, mutations as
    request-reply (ADR-0024 Phase 2) / sinks.
  - `wsdl` — SOAP WSDL → one action per operation, XML field shapes.
  - `sql-schema` — JDBC/SQL `INFORMATION_SCHEMA` → tables → CRUD actions.
- **Vendor adapters** (proprietary metadata → catalog):
  - `salesforce-describe` — `/sobjects` + `/describe` → SObjects and fields,
    picklists as enums, upsert on external-id.
  - `servicenow-dictionary` — `sys_dictionary` / Table API metadata → tables
    and columns.
  - (Dataverse/SAP reuse `odata`; NetSuite pairs `sql-schema` (SuiteQL) with
    a record adapter; Graph/Jira/HubSpot reuse `openapi`.)

Discovery runs **on a runner** against live credentials (it reads tenant
metadata), producing a catalog the hub then stores + serves like a static
descriptor. The hub never runs discovery and never sees payload — only the
metadata catalog, treated with the same fail-closed trust as ADR-0018.

### 2. Auth profiles (pluggable)

An `AuthProfile` is a named, declarative credential-and-signing strategy,
**resolved runner-side** from `{"$secret":...}` refs (ADR-0010 runner-pull)
and applied to every request. Profiles never log credentials and never place
plaintext in the manifest, the catalog, the queue, or task reads:

- **OAuth2** — `authorization-code`, `client-credentials`, `password`,
  `jwt-bearer` (certificate-signed assertion, e.g. SFDC/Graph server-to-
  server); token cache + refresh runner-side.
- **api-key** — header or query, name configurable.
- **basic** — user + secret (ADR-0016 posture).
- **bearer** — static/opaque token.
- **aws-sigv4** — request signing for AWS-fronted APIs.
- **session-token** — login call → session id → per-request header
  (ServiceNow, SAP, NetSuite TBA sits here or under OAuth1a).

A profile is config: which secret refs it consumes, which endpoints it
calls, where the credential lands on the wire. The token cache, refresh,
retry-on-401 loop are framework code shared by all profiles. Secrets stay
transient in runner memory exactly as ADR-0010 mandates.

### 3. Declarative application manifests

A specific connector is authored as a **manifest**, not Go:

```
name: salesforce
base_url: "https://{instance}.my.salesforce.com"   # template, per-account
auth: { profile: oauth2, grant: jwt-bearer, secret: "$secret:sfdc" }
discovery: { adapter: salesforce-describe }
pagination: { style: cursor, next: "nextRecordsUrl" }
incremental: { cursor_field: SystemModstamp, param: updated-since }
rate_limit: { hint: "per-org daily API limit; backoff on 429 + Retry-After" }
bulk: { source: bulk-2.0, sink: bulk-2.0 }          # optional accelerators
```

The manifest binds a **technology core + auth profile + discovery adapter +
pagination/cursor/bulk hints**. Fields the framework understands: base-URL
template, auth profile, discovery adapter, pagination style, incremental /
updated-since cursor, rate-limit hints, optional bulk-API bindings. Adding a
new SaaS connector is *authoring a manifest* and, at most, a thin vendor
adapter — the differentiator versus per-vendor codebases.

### 4. Cross-cutting execution concerns (framework, not per-connector)

- **Pagination** — offset/limit, page-number, cursor/next-link, RFC-5988
  `Link` header. One declarative `style` drives a shared reader that keeps
  the ADR-0004 streaming contract (batches, bounded RSS) — no whole-response
  buffering, pages pulled on demand.
- **Incremental cursor / CDC** — an `updated-since` watermark (per the
  manifest's `cursor_field`) advanced per run and persisted as flow state,
  so a source emits only changed records; native CDC/event streams
  (SFDC CDC, ServiceNow, Dataverse change tracking) plug in behind the same
  cursor abstraction later.
- **Bulk APIs** — declarative bindings to async batch endpoints (Salesforce
  Bulk 2.0, NetSuite async, SAP batch) as an **accelerator**: the same verb
  transparently routes large jobs through the bulk path (submit → poll →
  stream results) while small jobs use the sync REST path. Poll loops are
  framework code.

### How it layers on ADR-0024 and ADR-0025

1. The runner runs the connector's **`Discover`** (ADR-0025) → the discovery
   adapter hits the live system → a **catalog** of actions with `direction`
   + field schemas (ADR-0018 form).
2. The hub **stores + serves** that catalog exactly as it serves static
   descriptors — no runner online for the *builder*, no payload plane.
3. The studio renders each discovered action as **one node + a verb in the
   dropdown** (ADR-0024): `create Account (sink)`, `query Account (source)`,
   `upsert Account (sink)` — indistinguishable from `http`'s hand-written
   verbs. Request-reply actions (mid-flow calls, e.g. GraphQL mutations,
   SObject lookups) land when ADR-0024 Phase 2 ships.
4. At execution the auth profile + pagination + cursor + bulk logic run
   **runner-side**, streaming (ADR-0004). The hub sees only the metadata
   execution report (ADR-0016).

### Worked example — Salesforce, CSV → upsert Account

1. Operator installs the signed `salesforce` connector (manifest + core;
   ADR-0011) and stores an `sfdc` OAuth secret (ADR-0010).
2. Runner runs `Discover`; the `salesforce-describe` adapter enumerates
   SObjects and, for **Account**, its fields (Name, Industry,
   `External_Id__c`, picklists as enums). The catalog is stored + served by
   the hub.
3. In the studio the author drops a **Salesforce node**, picks verb
   **`upsert Account`** (a sink, per `direction`). The builder renders
   Account's fields **from the served schema** (ADR-0018) — the field map
   target list is the real SObject shape, no runner online.
4. Source node = a CSV file (`engine/format` csvf); the author maps CSV
   columns → Account fields, choosing `External_Id__c` as the upsert key.
5. On run, the runner resolves the `sfdc` secret (ADR-0010), the OAuth
   `jwt-bearer` profile mints a token, and records stream from CSV →
   upsert. For a large file the `bulk-2.0` binding routes through Bulk API
   (submit → poll → results) transparently; a small file uses the REST
   upsert. Pagination/cursor apply on the *read* side of a query verb. The
   hub receives only the execution report.

### Flagship targets (weighting)

Build order is weighted to **Microsoft / enterprise / ITSM**, where SHIFT's
performance + self-hosted story is most differentiated:

1. **Dynamics 365 / Dataverse** — `odata` core, straight `$metadata`.
2. **SAP** (OData / OData4SAP; RFC/IDoc later) — `odata` core.
3. **ServiceNow** — Table API + `servicenow-dictionary` adapter.
4. **Salesforce** — REST/Bulk core + `salesforce-describe`.

Then NetSuite, MS Graph, Jira, HubSpot (all reusing `openapi`/`odata`/
`sql-schema` cores with thin manifests).

## Consequences

- **Fleet economics invert.** After the core lands, a new SaaS connector is
  a manifest (+ maybe a small vendor adapter), not a Go project — the
  breadth needed to rival incumbents becomes tractable and AI-authorable.
- **Doctrine held, not amended.** Discovery + auth + pagination + bulk all
  run **runner-side**; the hub stores/serves only the metadata catalog +
  schemas (ADR-0016, ADR-0018) and holds **no credentials and no payload**
  (ADR-0010). Shipped application connectors are **signed artifacts**
  (ADR-0011); discovered catalogs are trusted fail-closed like descriptors.
- **Streaming preserved.** Pagination and bulk are pull loops feeding the
  batch model (ADR-0004) — no whole-response buffering, bounded RSS.
- **UX unchanged.** Discovered actions reuse ADR-0024's one-node + verb
  model and ADR-0018's schema forms verbatim; there is no application-
  connector-specific studio surface to build.
- **Sequencing.** This is **designed, build deferred.** It depends on the
  `Discover` RPC (ADR-0025) and a base technology-connector fleet
  (`http` exists; `odata`/`db`/`soap`/`graphql` are prerequisites). Bulk,
  native CDC, and request-reply verbs (ADR-0024 Phase 2) are follow-on
  layers, not gates on the first specific connector.

## Open questions (resolve at build)

- Catalog **freshness / re-discovery** cadence — SObjects and OData
  entities drift; when does a stored catalog re-run `Discover`, and who
  triggers it (scheduled runner job vs on-demand)?
- Where discovered catalogs live in the registry vs static descriptors, and
  whether a discovered catalog is per-account (tenant metadata differs) —
  it almost certainly is, unlike a static descriptor.
- Manifest format + signing surface — is the manifest itself part of the
  signed artifact (it should be), and how vendor adapters that need code
  (not just config) are packaged and signed.
- Cursor / CDC **state ownership** — flow-state persistence for the
  `updated-since` watermark across runs (runner-local vs hub-metadata).
- OAuth **authorization-code** connectors need an interactive consent
  redirect at design time — which realm hosts that dance (hub studio) while
  the resulting refresh token still resolves runner-side only.
