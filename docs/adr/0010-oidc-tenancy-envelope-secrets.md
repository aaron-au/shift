# ADR-0010: OIDC admin realm, tenancy enforcement, envelope-encrypted secrets

Date: 2026-07-19
Status: Accepted

## Context

M4a shipped the two-realm split (ADR-0009) with a placeholder admin
token. M4b replaces the placeholder with real human identity, activates
the tenancy columns that have existed since schema v1, and gives flows a
way to use credentials without ever storing or shipping them in the
clear. Doctrine constraints: secrets never echoed into payloads, results,
or logs; the hub never touches payload data; no IdP lock-in.

## Decision

1. **Generic OIDC, no IdP choice.** The hub implements the protocol —
   issuer discovery + JWKS signature validation of bearer JWTs (audience
   = the hub's client id) via `coreos/go-oidc/v3`. Config is three
   values (`SHIFT_HUB_OIDC_ISSUER/CLIENT_ID/CLIENT_SECRET`); Dex, Auth0,
   Entra, Keycloak, Okta all plug in. The dev/e2e/bundle IdP is Dex,
   provisioned entirely by config file. Cloud IdPs are provisioned by
   their Terraform providers — nothing manual (standing rule).
   Hand-rolled JWT validation was rejected: alg-confusion/kid-injection
   is exactly the bug class vetted libraries exist for, and the
   govulncheck gate covers the dependency.
2. **OIDC is the human realm only.** Runner auth is unchanged
   (single-use registration token → hashed bearer secret); the engine
   has no network auth surface at all. The dashboard gets a hub-mediated
   authorization-code flow (`/auth/login|callback|logout`); the session
   cookie IS the verified ID token — stateless, HA-safe, expires with
   the token. API clients present the JWT as a bearer directly.
3. **JIT user provisioning on `(issuer, subject)`,** never email alone;
   `email_verified` respected. One `role` column (`admin`/`viewer`,
   viewers read-only) instead of v0's RBAC tables — enough to replace
   the token, migrates cleanly later.
4. **Break-glass token stays as an option** (`SHIFT_HUB_ADMIN_TOKEN`,
   audited as `admin:break-glass`, loud boot warning alongside OIDC).
   Bootstrap automation and bring-up use it; production unsets it once
   OIDC login works.
5. **Tenancy via context.** `store.WithAccount(ctx)` is set by every
   realm's middleware; store queries read it. This fixed real
   cross-tenant gaps (Claim leased any account's tasks; task/runner
   reads were unfiltered). Guarded by a two-account isolation test.
6. **Secrets = envelope encryption, runner-pull resolution.**
   - Per-secret DEK (AES-256-GCM, AAD binds ciphertext to the secret's
     name — row-swap fails authentication); DEK wrapped by a KEK behind
     a tiny `kek.Provider` interface. First provider: local 32-byte
     0600 key file. KMS later implements the same interface; rotation =
     new active key, `POST /api/v1/keys/rotate` re-wraps DEKs only.
   - Flow documents carry inert references `{"$secret":"name"}`
     (strict one-key object form — no collision with real strings).
     The hub validates refs at deploy (422 naming missing ones) but
     never injects values: dispatch-time injection would persist
     plaintext in `tasks.document`, lease payloads, and task reads.
   - The **runner** resolves refs per task via the runner-realm
     `POST /api/v1/secrets/resolve` just before execution. Plaintext
     exists transiently in hub and runner memory only; every access is
     an audit row; errors carry names, never values. Proven by the
     `TestSecretsNeverAtRest` e2e (sentinel reaches the destination
     header and appears nowhere else — DB, APIs, logs).
   - Sealing-to-runner (hub can't decrypt) was rejected for M4b: any
     runner may lease any task, so it demands per-runner keys and fleet
     re-encryption on rotation — real feature, later.

## Consequences

- All admin endpoints accept OIDC identity; multi-tenant *signup* is
  still future work (new users join the default account), but the
  enforcement layer is account-correct now.
- The hub decrypts secrets (control-plane config, not payload data);
  a KMS provider later narrows its exposure without schema changes.
- go-jose/x-oauth2 join the dependency tree — accepted over hand-rolled
  crypto; the ADR-0006 gate is the control.
