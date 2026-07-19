# ADR-0011: Connector registry with Ed25519 artifact signing

Date: 2026-07-19
Status: Accepted

## Context

Until M4b, runners executed whatever `shift-connector-*` binaries an
operator placed in a directory — fine for dev, not a distribution story.
ADR-0001 promised signed artifacts; `sdk/host` carried a G204 suppression
citing "registry-signed from M4". The registry closes that loop.

## Decision

1. **Signatures bind identity to content** (`pkg/consign`, stdlib
   Ed25519). A signature covers a canonical manifest —
   `name/version/os/arch + SHA-256 digest` under a versioned format tag —
   never raw bytes alone, so artifact A cannot be republished under
   B's identity. Publisher **private keys never exist server-side**: a
   hub/DB compromise cannot forge artifacts. `shift-consign`
   (keygen/sign/verify) runs on the publisher's build machine.
2. **Hub stores blobs in Postgres** (`connector_blobs`, content-addressed
   by digest, deduped) rather than object storage: Postgres is already
   the only stateful service ("just runs" holds), hub replicas stay
   diskless, and 10–50 MB binaries at publish frequency are trivial.
   Upload/download crosses hub RAM, capped (`http.MaxBytesReader`,
   128 MiB default). If it ever matters, the swap is two store methods —
   no abstraction built now. "latest" = newest publish (registry-latest,
   not semver — a republish can downgrade, publisher-controlled).
3. **Upload verifies before storing** (403, nothing written, on unknown/
   revoked key or bad signature). Keys are per-account (`publisher_keys`)
   with revocation; resolve excludes yanked versions and revoked keys.
4. **Runners fail closed** (`runner/internal/connstore`):
   manifest → trusted-key check → `consign.Verify` → fetch through a
   SHA-256 tee → digest compare → `chmod 0500` + atomic rename into a
   content-addressed cache. The cached file is **re-hashed on every
   use**; a tampered cache is discarded and refetched. Nothing executes
   unverified — proven by the `TestSignedArtifactPath` e2e (DB blob
   corrupted → task fails "digest mismatch", no executable residue).
5. **Trust root:** the hub's trusted-key list, fetched over the runner's
   authenticated TLS channel (the runner already trusts the hub for the
   tasks it executes — no new trust edge; the signature still protects
   against storage/transport tampering and future cache/CDN hops).
   `SHIFT_TRUSTED_KEYS` pins the set exclusively for hub-independent
   trust. `SHIFT_REQUIRE_SIGNED=1` disables the operator-Dir fallback
   entirely (the compose bundle runs this way; local dev keeps its Dir
   workflow).

## Consequences

- The hub module now imports `pkg/consign` (payload-free control-plane
  crypto) — CLAUDE.md's import doctrine amended accordingly.
- Flow documents don't yet pin connector versions; runners resolve
  "latest" at fetch. Version pinning is an M5 flowdoc change.
