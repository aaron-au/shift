# ADR-0024: Connector operation model — unified verbs, roles, and request-reply

Status: **accepted (Phase 1)** + **designed (Phase 2, build deferred)** — 2026-07-22

## Context

A connector should present as **one node** on the studio canvas: pick the
connector, pick a **verb** from a list, fill config, done — the classic iPaaS
"operation" ergonomic (Aaron, 2026-07-22). The streaming engine (ADR-0013),
however, places connectors only at pipeline **ends**: a **source** produces the
stream, a **sink** consumes it; there is no mid-flow "call a connector" step.
That mismatch produced friction: the builder made authors choose a *source
node* vs *sink node* before picking a connector/verb, and side-effecting SFTP
verbs (delete/mkdir/…) were first modelled as per-record sinks with no path
field on the node.

Key realisation: **source/sink is a property of the action, not a kind of
connector.** The signed descriptor already carries `direction` per action
(ADR-0018 `ActionDescriptor.Direction`). It is fundamental *inside* the engine
(a pull pipeline needs a producer and a consumer) but should be **invisible to
the flow author**.

Most real connector operations come in three shapes:
- **produce** (source) — GET, list: originate a stream.
- **consume** (sink) — put, fire-and-forget POST: terminate a stream.
- **request-reply / call** — POST/PUT/PATCH/DELETE/QUERY returning a response,
  API enrichment, SFTP `stat`, DB lookup: consume a record **and** emit a
  result, **mid-flow**. The engine does not support this yet.

## Decision

### Phase 1 — shipped 2026-07-22 (no engine change)

1. **Unified builder node.** The canvas shows one connector node with a **verb
   dropdown listing every action across both directions**, labelled with the
   role it resolves to (`get (source)`, `put (sink)`). Selecting a verb sets the
   node's source/sink role from the descriptor `direction`. Authors never pick a
   node "type". (`ui.html`; ADR-0019 builder.)
2. **Config-driven op verbs.** Side-effecting verbs take their target(s) from
   **config** (path on the node), not from records. They are modelled as
   **sources** that perform the op and emit a single status record, so a
   one-verb flow is runnable standalone. `put` stays the one sink (it consumes
   pipeline output to write a file). (SFTP connector v0.2.0.)
3. **Built-in `@discard` sink.** A flow's happy path must end at a sink
   (`graph.go`). `@discard` is a reserved, connector-free, side-effect-free
   terminal (reads the stream, drops it, counts records) — the studio
   auto-appends it when a flow ends on a source. Role-locked (sink only), needs
   no action, exempt from capability policy + signing (like `@webhook`, a
   source). (`pkg/flowdoc`, runner `discardSink`.)

### Phase 2 — designed, build deferred

4. **Request-reply action shape.** A new connector action kind (or connector-
   backed Op) that consumes each record, performs a call, and emits the response
   **downstream** — the missing mid-flow shape. Unlocks full-method HTTP with
   responses, API enrichment, SFTP `stat/exists`, DB lookup. Touches the engine
   (a mid-pipeline connector op) + the SDK contract (a third action interface or
   a request-reply variant of Write). ADR-level; scope TBD when picked up.
5. **`@response` terminal.** An explicit sink that returns the flow's output to
   the **caller** — the ingress requestor for a sync direct execution
   (ADR-0016 data plane, runner-side; **payload never touches the hub**) or the
   parent for a subflow. Distinct from `@discard` (drop). Aaron prefers this be
   explicit rather than implicit "return by default". Lands with the sync
   direct-execution response path.
6. **Verb / HTTP-status naming "where it makes sense".** HTTP exposes
   GET/POST/PUT/PATCH/DELETE/QUERY; other connectors keep their natural verbs
   (SFTP: get/put/list/delete/mkdir/rmdir/rename). Connector results carry a
   uniform HTTP-status-like code (ties to issue #14). Descriptive/naming
   convention, enforced as connectors adopt it.

## Consequences

- Phase 1 gives the single-node "pick a verb + path, just works" UX today for
  produce/consume verbs, with `@discard` making a lone op-node valid.
- Phase 2 is where the model becomes a full iPaaS operation set; it is the
  single biggest engine/SDK addition on the connector roadmap. Until then,
  request-reply is approximated by separate source/sink nodes.
- `@discard` carves a narrow exception to "built-in connectors cannot be sinks"
  (ADR-0013 era) — justified: it is side-effect-free and connector-free.

## Studio visual model (tracked, not in this ADR's build)

The node **rendering** Aaron specced — colourised header per component type
(connectors / transforms / set-value / error-handler), a type icon,
colour-blind-friendly + uniform, a body config summary (verb, path/URL with
`$var`s, output type), and three snap points (incoming / success / failure) —
is builder visual-polish (see the studio visual series). It layers on top of
this operation model; it does not change it.
