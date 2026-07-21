# ADR-0023: Hub API error envelope + versioning policy

Date: 2026-07-21
Status: Accepted; implemented (issue #6).

## Context

The hub control API (ADR-0009, HTTP/JSON) returned errors as
`{"error": "<human message>"}` and had no written versioning or
compatibility policy. For an enterprise iPaaS whose API is consumed by the
studio, runners, and third-party tooling, two things were missing: a stable
**machine-readable** error signal (clients were forced to string-match the
human message), and a documented **compatibility contract** for `/api/v1`.

A first idea — deriving a string code 1:1 from the HTTP status — was rejected:
if the code always mirrors the status, the status already *is* that code and
the string adds nothing. A code only earns its place when it is **finer-grained
than the status** (the many-to-one cases).

## Decision

### Error envelope

All hub error responses use:

```json
{ "error": { "status": 422, "code": "flow_not_published", "message": "…" } }
```

- **`status`** — the HTTP status, the primary coarse category (also on the
  status line; echoed in the body for convenience).
- **`code`** — an OPTIONAL, stable, machine-readable discriminator, present
  **only where several distinct conditions share one status** so a client can
  branch without matching `message`. Absent when the status alone is
  unambiguous. Codes are assigned at the handful of call sites that need them,
  driven off the store's sentinel errors — e.g. `flow_not_published` (409),
  `flow_invalid` (422), `idempotency_key_reserved` (422),
  `registration_token_invalid` (401), `lease_lost` (409).
- **`message`** — human-readable; not stable, may be reworded/localized. Never
  branch on it.

Central helpers: `writeErr(w, status, err)` (status + message) and
`writeErrCode(w, status, code, err)` (adds the finer code). Adding the code was
a one-site-per-condition change, not a blanket per-handler rewrite.

The connector plane will adopt the same status taxonomy over gRPC (issue #14,
its own ADR) so failure categories are uniform end-to-end
(client ← hub ← runner ← connector).

### Versioning + compatibility

- The API is versioned in the path: `/api/v1`. The **major** version changes
  only on a breaking change.
- **Within a major**, changes are additive and backward-compatible: new
  optional request fields, new response fields, new endpoints, new `error.code`
  values. Clients MUST ignore unknown response fields and tolerate new codes
  (fall back to `status`).
- **Breaking changes** (removing/renaming a field, changing a type, changing an
  error's status, tightening validation that rejects previously-accepted input)
  require a new major (`/api/v2`) served alongside `/api/v1` for a deprecation
  window.
- **Deprecation**: a deprecated endpoint/field is documented and, where
  practical, signalled with a `Deprecation` response header before removal; it
  is not removed within the same major.
- **Pre-1.0 caveat**: SHIFT is pre-production. Until the first tagged release,
  `/api/v1` may still change shape (this ADR itself is such a change — the error
  envelope). After the first release the guarantees above are binding.

## Consequences

- Clients get a stable, branchable error signal without depending on prose.
- The studio UI and the runner's `hubclient` read the envelope
  (`error.message`, and `error.code` where they branch); both were updated in
  lockstep with this change.
- The versioning rules give third-party integrators a contract to build
  against once we ship.

## Alternatives considered

- **Status-derived string code** — rejected (redundant with the status; see
  Context).
- **A hand-curated code for every endpoint** — rejected: most statuses are
  unambiguous per endpoint; codes only where they add information keeps the set
  small and meaningful.
- **RFC 7807 `application/problem+json`** — a reasonable future option, but the
  minimal envelope covers our needs without committing to the full media type;
  revisit if external integrators ask for it.
