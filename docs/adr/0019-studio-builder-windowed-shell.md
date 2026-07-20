# ADR-0019: Studio builder — canvas authoring in a windowed UI shell

Date: 2026-07-20
Status: Accepted (design); implementation in M5.5 (Studio Builder)

## Context

M5d-3 shipped a **read-only** flow graph view: `flowdoc.GraphView()` → hub
`GET /api/v1/flows/{name}/graph`, rendered as inline SVG in a modal on the
embedded hub dashboard. Full drag-drop authoring was explicitly deferred out of
M5. This ADR fixes the design for that deferred builder, taken up as M5.5
before M6.

The write path already exists end to end and needs little: `PUT
/api/v1/flows/{name}` (`deployFlow` — parse + validate + secret-ref +
capability-policy checks, creates a draft version, returns 422 with errors),
`POST .../versions/{version}/publish`, `POST .../execute`, and `flow_versions`
JSONB storage with a draft→published workflow. The gap is the **UI**, plus the
connector config-schema discovery covered by ADR-0018.

Two design forks were decided with the product owner (2026-07-20):

- **Editing model: canvas drag-and-drop** (not a form-list). Draggable nodes
  on the SVG canvas, drag-to-connect outcome edges, a side panel for per-step
  config — the Boomi/Workato/Make feel.
- **Frontend tooling: stay vanilla, no build step.** Keep the "single
  dependency-free page" doctrine — no npm, no bundler, no framework, no
  supply-chain surface. The builder is hand-written vanilla JS, `go:embed`'d.

And a UI direction: an **"operating-system-lite" shell** — a left dock of
tools, each opening its own draggable application window, so a developer can
have the builder canvas and the task-runner list open side by side. Lite and
familiar (a common web pattern), not a heavy desktop-metaphor simulation.

## Decision

### Windowed shell

The hub dashboard becomes a lightweight **windowing shell**:

- A left **dock** lists tools; clicking one opens (or focuses) that tool's
  **application window**. One window per tool (singleton), not arbitrary
  multi-instance — keeps state simple.
- Windows are **draggable, focusable (z-order), minimisable, and closable**;
  resizable where it helps (canvas, task list). No maximise-to-fullscreen
  desktop metaphor beyond that — deliberately lite.
- Tools map to the existing dashboard sections, each now a window: **Builder**
  (flows + canvas), **Tasks**, **Runners**, **Connectors**, **Secrets**,
  **Schedules**, **Executions**. The existing polling/data calls are unchanged;
  they are relocated into their windows.
- Window layout (which are open, positions, z-order) is **client-only** UI
  state — persisted in browser storage, never sent to the hub. It is not flow
  data and carries no security weight.

### Canvas builder

The read-only SVG renderer (`renderGraph`) is promoted to an **interactive
canvas**:

- **Node interactions:** select, drag-move, add (from a palette of step
  types), delete. Node positions are authored layout, see below.
- **Edge interactions:** drag from a node's outcome port to another node to
  create an `onSuccess`/`onComplete`/`onFailure` edge; delete edges. Edge
  colours match the read-only view (success/complete green, failure red).
- **Side panel:** per selected step — id, type, and (for connector steps)
  connector + action pickers driving a **schema-rendered config form**
  (ADR-0018), with the secret picker for `{"$secret":...}` refs. Transform
  steps get hand-written forms from the known `pkg/flowdoc` shapes.
- **Serialize → deploy:** the canvas serializes to a v2 `flowdoc.Document`
  (`steps` + outcome edges) and `PUT`s it to `deployFlow`. Server-side 422
  validation errors are surfaced **inline on the offending node/field** —
  `pkg/flowdoc` `Validate()`/`buildPlan()` stays the single source of truth;
  the builder does not re-implement validation, only presents it.
- **Draft/publish/test loop:** version list, publish/rollback, run-now (all
  existing endpoints), and — when a runner is online — an optional test-run
  overlay of per-step captured samples (ADR-0014 `Sampler`; the deferred
  studio→runner read).

### Node layout persisted in the flow document

The canvas needs saved node positions. `flowdoc.Document` gains an **optional,
presentational `layout`** field (a map of step id → `{x, y}`), **ignored by
`Validate()`, `buildPlan()`, `Plan()`, and the engine** — purely authoring
metadata so a reopened flow lays out as the author left it. It rides in the
same JSONB document (no new storage). Unknown/absent layout ⇒ the existing
auto-layout math positions nodes.

### Still vanilla, still embedded

No build step. The UI may be split across a few `go:embed`'d static files
(`.js`/`.css`) for maintainability as it grows, but stays dependency-free
hand-written JS/CSS — no npm, bundler, or framework. The page is still served
unauthenticated while every data call carries the bearer/OIDC session
(unchanged from today).

## Consequences

- The dashboard graduates from a single scrolling page to a windowed shell;
  this is a UX reorganisation, not a backend change — data endpoints and auth
  are untouched.
- `flowdoc.Document` grows one optional presentational field (`layout`). It
  must remain non-semantic: no validation, no engine effect, tolerated-if-
  unknown, so old runners/hubs ignore it safely.
- The builder is a **thin client over existing endpoints** plus ADR-0018's
  schema serving — `pkg/flowdoc` validation stays authoritative; the UI never
  forks it.
- Hand-written vanilla canvas + window manager is real JS effort (hit-testing,
  drag math, edge routing, z-order) but keeps zero toolchain/supply-chain cost,
  consistent with doctrine. If this later proves unsustainable, introducing a
  bundler is a superseding ADR, not a silent fork.
- The builder works against the hub alone (schema from ADR-0018 path A); a
  runner is needed only for run-now and the test-run overlay.

## Open questions (resolve at build)

- Edge-routing/auto-layout quality for larger graphs (orthogonal routing vs the
  current simple rows) — how far to push before it warrants a helper.
- Whether the window shell persists layout per-user server-side later (today:
  browser-only) — only if users ask to roam it across devices.
- Test-run overlay UX — inline on nodes vs a dedicated panel — deferred with
  the studio→runner cross-service read it depends on.
