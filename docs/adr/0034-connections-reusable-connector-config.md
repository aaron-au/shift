# ADR-0034: Connections — reusable connector configuration

Date: 2026-08-03

Status: **Designed**

## Context

Authoring is repetitive in a way that is entirely self-inflicted.

A node today carries one flat config document: connector, action, and every
setting the action needs (ADR-0024). For a flow that touches the same system
more than once — an SFTP `get`, then a `delete` of what it read; a Jira `query`
then a `create` — the author re-enters the host, port, username, key reference
and TLS settings on **every node**. The same values, typed again, with the same
opportunity to typo one of them and get a failure that looks like a network
problem.

The project owner raised this directly (2026-07-22): re-entering endpoint and
auth on every verb node is tedious, and a reusable connection object plus an
always-visible config summary on the node body was the requested shape.

It is also a differentiator gap. Every incumbent has this split — Boomi's
Connection versus Operation is the canonical form, and a Boomi export literally
ships `connections/` and `operations/` as separate component types. An importer
(ADR-0032) that has nowhere to put a Boomi Connection must either duplicate its
fields across every node it feeds or drop them. Neither is acceptable for a
migration that claims fidelity.

Existing machinery that this must reuse rather than duplicate:

- **Config schemas (ADR-0018)** — a connector declares a per-action JSON Schema
  which travels in a signed, opaque descriptor. The studio renders forms from it
  with no runner online. Secret-typed fields are marked `x-shift-secret`.
- **Secrets (ADR-0010)** — `{"$secret":"name"}` refs are resolved **runner-side**;
  plaintext never reaches the queue, the hub, or logs.
- **Account scoping** — `store.WithAccount(ctx)` on every hub read/write.

## Decision

Introduce a **Connection**: a named, account-scoped, reusable configuration
document that several nodes reference.

### 1. The connector declares the split — one connection schema per connector

`sdk.Connector` gains a **`ConnectionSchema []byte`**: a single JSON Schema
describing the connection-level config for that connector, alongside the
existing per-action `Schemas`.

Per **connector**, not per action, and not a per-field marker inside the action
schemas. Connection-level config is a property of the *system being talked to*,
not of the verb — a host is a host whether you are reading or deleting. A
per-field marker (`x-shift-connection: true`, mirroring `x-shift-secret`) was
rejected because it lets the same field be connection-level in one action and
operation-level in another, which is not a distinction any real connector wants
and is a bug the author would have to notice.

The connection schema rides in the **same signed descriptor** as the action
schemas (ADR-0018), so the split is tamper-evident and the studio can render a
connection form with no runner online. A connector that declares no connection
schema behaves exactly as today — every field stays on the node.

### 2. A Connection is hub-stored, account-scoped, and may reference secrets

```
  PUT    /api/v1/connections/{name}     { "connector": "sftp", "config": {...} }
  GET    /api/v1/connections[/{name}]
  DELETE /api/v1/connections/{name}
```

The stored document is validated against that connector's connection schema at
deploy time. It **may contain `{"$secret":"name"}` refs**, which continue to
resolve runner-side by the existing mechanism — the hub stores a reference, never
a credential, so this adds no new secret-handling path and no new place for
plaintext to leak.

Connections are metadata, so the hub owning them violates nothing: it already
stores flow documents that carry the same references.

### 3. A node references a connection instead of repeating it

```jsonc
{ "type": "source", "connector": "sftp", "action": "get",
  "connection": "prod-sftp",              // ← named connection
  "config": { "path": "/in/orders.csv" }  // ← operation config only
}
```

The runner computes the effective config as **connection config, then node
config**. Connection-level keys are **rejected on the node** rather than merged
or overridden: silent override is how an author ends up with one node quietly
pointing at a different host than its siblings, which is precisely the failure
this ADR exists to prevent. If a node genuinely needs different connection
settings, it needs a different connection.

`connection` is optional and additive. A node with inline config and no
connection reference compiles exactly as it does today — **every existing flow
document remains valid**, which is a hard requirement given flows are stored,
versioned and published.

### 4. Resolution stays runner-side

The hub validates and stores; the **runner** fetches the referenced connection
with the task, merges, and resolves any `{"$secret":...}` refs — the same order
as today, one step earlier. The payload plane is untouched and the hub still
never holds a credential.

### 5. The studio shows the connection on the node

The node body displays its connection name, which is the "always-visible config
summary" the owner asked for: an author scanning a canvas sees *which system*
each node talks to without opening it. Choosing a connection filters the config
form down to operation fields only, so the form gets materially shorter — the
visible half of the welcome experience.

## Consequences

- **Authoring cost drops with flow size.** A five-node SFTP flow goes from five
  copies of the connection settings to one. This is the difference the owner
  named as the connector welcome experience.
- **Credential rotation becomes one edit.** Today a rotated key means editing
  every node that used it, across every flow — and missing one produces a
  partial outage that is hard to attribute. This is a correctness win, not just
  ergonomics.
- **The Boomi importer gains its target.** A Boomi Connection maps onto a SHIFT
  Connection, and its Operations onto nodes referencing it, instead of being
  duplicated or dropped (ADR-0032).
- **It is the prerequisite for auth profiles (ADR-0027)** — a reusable auth
  profile is a connection whose schema is an auth shape, so that work becomes an
  extension rather than a parallel mechanism.
- New surface to secure: connection CRUD is admin-realm, account-scoped, and
  audited like secrets. Connections are **not** payload and carry no plaintext.
- A connector adding a connection schema **moves fields off the node**, which
  changes what its action schemas should contain. Existing signed artifacts are
  unaffected (no connection schema = today's behavior), but a connector author
  making the change must publish a new version and existing flows keep working
  against the old one.

## Open questions

1. **Deletion of a referenced connection.** Refuse while any published flow
   references it (like a foreign key), or allow and fail at run time with a
   clear error? Leaning refuse — a deploy-time failure beats a 3 a.m. one.
2. **Connection-level defaults vs overrides.** §3 forbids node-level override of
   connection keys. If a real case appears (one node needing a longer timeout
   against the same host), the cleanest answer is probably an explicit
   per-node `overrides` block rather than relaxing the rule.
3. **Whether connections are versioned** like flows. Editing a connection changes
   behavior for every flow that references it, with no draft/publish step —
   which is convenient for rotation and dangerous for endpoints.
