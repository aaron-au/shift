# ADR-0015: Per-deployment connector capability policy

Date: 2026-07-20
Status: Accepted

## Context

Shared/cloud hubs must stop tenant flows from reaching the host: a
filesystem or process connector on a multi-tenant runner is a data-exfil
and escape risk. The product requirement is stronger than "disabled" — such
connectors must be **invisible** on a cloud hub ("not even visible"), while
self-hosted hubs keep full freedom. The registry is already per-account
(ADR-0011); this adds the policy layer.

## Decision

A **per-deployment** connector capability policy (`hub/internal/connpolicy`)
decides, hub-wide, which connectors flows may use, list, and resolve:

- `Policy{allow, deny}`: a non-empty **allowlist** permits only its members;
  the **denylist** is always subtracted (deny wins). Empty/empty — the
  self-hosted default, and a nil policy — allows everything at zero cost.
- Config is hub flags/env: `SHIFT_HUB_CONNECTOR_ALLOW` /
  `SHIFT_HUB_CONNECTOR_DENY` (comma-separated). Cloud deployments set them.

Enforced at three points:
- **Deploy** (`PUT /flows/{name}`): a flow referencing any disallowed
  connector is rejected 422. `flowdoc.Document.Connectors()` (data-free,
  in `pkg/flowdoc`) lists the names; the hub checks them — the hub keeps
  touching only metadata.
- **List** (`GET /connectors`): disallowed connectors are filtered out —
  invisible.
- **Resolve / download** (runner-pull): disallowed names return 404, as if
  absent. Defense in depth (deploy already blocks), and it keeps a
  policy-changed-after-deploy flow failing cleanly.

**Scope choices, deliberately minimal:**
- **Per-deployment, not per-tenant.** "Cloud hubs hide X" is a property of
  the deployment. Per-account policy is a later refinement on the same
  mechanism.
- **Name-based, not capability-metadata-based.** The registry keys on
  connector names; a name allow/deny list is enforceable today with no
  connector-manifest or signing change. Having connectors *declare* their
  capabilities (filesystem/network/process) in the signed manifest, so the
  policy can reason about kinds rather than names, is a larger change
  deferred until the name list stops being enough.

## Consequences

- Cloud hubs restrict with two env vars; self-hosted hubs are unaffected
  (nil policy, zero overhead).
- Reinforces the data-residency story (ADR-0010/M5c): the control plane not
  only holds no payload, it can forbid the connectors that would let a
  shared runner touch the host.
- When capability-metadata lands, `Policy` gains a capability dimension; the
  three enforcement points stay put.

## Proof

`connpolicy` unit matrix (allow/deny/default/deny-wins/nil-safe);
`flowdoc.TestConnectors`; api `TestConnectorPolicy` (deploy with a denied
connector → 422; resolve of a denied connector → 404).
