# ADR-0026: Pluggable secret-store / KEK providers — KMS-KEK first

Date: 2026-07-28
Status: Accepted (KMS-KEK provider); vault-as-store designed, build deferred

## Context

ADR-0010 shipped envelope-encrypted secrets: each value is sealed under
its own fresh DEK (AES-256-GCM, AAD = the account-scoped secret name),
and the DEK is wrapped by a `kek.Provider`. Only one provider exists —
`kek.NewLocalFiles`, a 32-byte 0600 key file the hub reads at boot. That
file **is** the root of trust: whoever holds it can unwrap every DEK and
open every tenant's secrets. ADR-0010 explicitly anticipated this ("KMS
later implements the same interface") and the interface was built tiny on
purpose so a cloud KMS could drop in "without any schema or service
change".

Two forces make it time to cash that in:

1. **We must not hold a root key in production.** A local KEK file on the
   hub host is exactly the material an operator, a backup, a core dump,
   or a compromised node leaks. Enterprise buyers expect the root key to
   live in a managed HSM/KMS (Azure Key Vault, AWS KMS, GCP KMS) that the
   hub can *call* but never *possess*.
2. **We will not roll our own crypto** (CLAUDE.md doctrine; ADR-0006 gate
   is the control). Envelope AEAD over the value stays as-is — battle-
   tested `crypto/cipher` GCM. What changes is *who wraps the DEK*: a KMS
   whose keys never leave the boundary.

The doctrine constraints from ADR-0010 are unchanged and non-negotiable:
secrets never echoed into payloads, results, or logs; plaintext never at
rest in `tasks.document`, lease payloads, or task reads; the hub never
touches payload data; runner-pull resolution of `{"$secret":"name"}` just
before execution; no IdP/vendor lock-in baked into the wire model.

## Decision

Evolve the single `kek.Provider` into a **provider set** selected by
configuration, keeping the `{"$secret":"name"}` reference model and the
runner-pull resolution path byte-for-byte identical. Two provider
families, one shipping now and one designed for later.

### 1. Provider interface (unchanged shape, KMS-ready)

The existing interface is already the right one — a KMS provider needs
nothing added:

```go
type Provider interface {
    ActiveID() string
    Wrap(ctx context.Context, dek []byte) (wrapped []byte, kekID string, err error)
    Unwrap(ctx context.Context, kekID string, wrapped []byte) ([]byte, error)
}
```

- `Wrap` becomes a KMS **Encrypt** (or wrap-key) call against the active
  key version; the returned `wrapped` blob is the KMS ciphertext, and
  `kekID` is the fully-qualified key-version identifier (e.g.
  `azkv:https://vault.vault.azure.net/keys/shift-kek/<version>`,
  `awskms:arn:aws:kms:…:key/<uuid>`, `gcpkms:projects/…/cryptoKeyVersions/N`).
- `Unwrap` becomes a KMS **Decrypt** (or unwrap-key) call selected by the
  `kekID` recorded on the envelope. `ErrUnknownKEK` still means "an
  envelope names a key version this provider is not configured to reach".
- **The DEK is generated hub-side** (as today, `crypto/rand`), then sent
  to the KMS to be wrapped. We deliberately keep our own DEK generation
  rather than KMS GenerateDataKey so the interface and the on-disk
  envelope format are identical across all providers and the local-file
  dev path needs no KMS emulator. (A provider MAY internally use
  GenerateDataKey and still satisfy the interface; not required.)
- **AEAD over the value does not move.** The KMS only ever sees the
  32-byte DEK, never a secret value — bounded, uniform, cheap KMS traffic
  (one wrap per Put, one unwrap per resolve, amortised by DEK reuse only
  within a single Put). The secret ciphertext stays AEAD-encrypted at
  rest in Postgres exactly as ADR-0010 defined.

New providers live under `hub/internal/kek/` (e.g. `kms_azure.go`,
`kms_aws.go`, `kms_gcp.go`), each behind the vendor's Go SDK gated by a
build/depguard allowance for the hub module only (runners never wrap;
see §6). No SDK is imported unless its provider is selected — prefer
thin per-vendor packages so an unused cloud SDK stays out of the hub
binary's default build where feasible.

### 2. Configuration and selection

A single `SHIFT_HUB_KEK_PROVIDER` selector (default `local`) chooses the
family; provider-specific settings follow the established env-var style:

| Provider | Selector | Key config |
|---|---|---|
| Local file (dev/self-host) | `local` | `SHIFT_HUB_KEK_FILE`, `SHIFT_HUB_KEK_FILES_OLD` (today) |
| Azure Key Vault KMS-KEK | `azurekv` | `SHIFT_HUB_KEK_AZURE_KEY_URL` (key, not secret); creds via workload identity / `DefaultAzureCredential` |
| AWS KMS-KEK | `awskms` | `SHIFT_HUB_KEK_AWS_KEY_ARN`; creds via IRSA / instance role |
| GCP KMS-KEK | `gcpkms` | `SHIFT_HUB_KEK_GCP_KEY_NAME`; creds via workload identity |

Cloud credentials are **never** a static secret in hub config — they come
from the platform's workload identity (managed identity / IRSA / GCP WI),
provisioned by the same Terraform that stands up the hub (standing rule).
The hub is granted only wrap/unwrap on the one KEK, not key management.
As with the KEK file today, an absent/misconfigured provider simply means
the secrets store is disabled (endpoints absent), never a silent
plaintext path.

### 3. Key rotation

Rotation is unchanged in mechanism — it was designed for this:

- **KEK rotation** stays `POST /api/v1/keys/rotate` → `Service.RotateKEK`,
  which re-wraps every DEK not already under `ActiveID()`. Under KMS this
  is unwrap-old + wrap-new per envelope; **the value ciphertext never
  moves**. New key versions are created in the KMS out-of-band (KMS-native
  rotation or Terraform); the hub's active `kekID` picks up the new
  version and old versions remain reachable for `Unwrap` until re-wrap
  completes, then may be disabled.
- **DEK rotation** stays implicit: re-`Put` a secret and it seals under a
  fresh DEK (version bump), exactly as today.
- Retiring a KMS key version before its envelopes are re-wrapped yields
  `ErrUnknownKEK` on resolve — fail closed, never a fallback.

### 4. Multi-tenancy / per-account key scoping

Envelopes are already account-scoped (`store.WithAccount`, ADR-0010 §5),
and the AAD binds ciphertext to the account-scoped name. Two KMS scoping
modes, both expressible without schema change because `kekID` is a free
identifier per envelope:

- **Shared KEK (default):** one KMS KEK wraps all tenants' DEKs. Tenant
  isolation rests on the store's account filter + per-secret DEK + AAD.
  Simplest, cheapest, correct for single-tenant and most cloud
  deployments.
- **Per-account KEK (opt-in):** the provider maps `account_id → KMS key`
  (naming convention or a small config map), so `Wrap` selects the
  tenant's key and `kekID` records it. A tenant's key can be independently
  rotated, disabled, or (with a customer-managed key, BYOK) revoked —
  cryptographically shredding just that tenant. This is the hook for
  "customer holds the key" without touching the envelope format.

### 5. Failure modes — fail closed, always

- **KMS unreachable / throttled / permission denied on `Unwrap`:**
  `Resolve` returns an error naming the affected secret(s) and **the task
  fails** (or the runner retries per normal at-least-once semantics).
  There is **never** a plaintext fallback, a cached-DEK-on-disk fallback,
  or a "degrade to local file" path. Availability of secrets is bounded
  by availability of the KMS — an accepted, deliberate coupling.
- **KMS unreachable on `Wrap` (Put):** the Put fails; no secret is stored
  half-sealed.
- **`ErrUnknownKEK`:** envelope names a key version the provider can't
  reach (retired too early / wrong provider configured) — hard error,
  fix config or restore the key version.
- **Bounded in-memory caching:** a provider MAY cache *unwrapped DEKs* in
  hub memory for a short TTL to cut KMS calls under load, but DEKs are
  never persisted and the cache is cleared on shutdown. No plaintext
  secret value is ever cached beyond the single resolve call.

### 6. Runner-pull and never-in-logs contract preserved

Nothing about the runner changes. Runners still call the runner-realm
`POST /api/v1/secrets/resolve` per task, just before execution; the
**hub** performs the KMS `Unwrap` inside `Service.Resolve` and returns
plaintext transiently over the authenticated control channel. The KEK
(local or KMS) is a **hub-side** concern — runners never wrap, never
unwrap, never import a KMS SDK, and depguard keeps `kek`/KMS SDKs out of
the runner module. Every resolve is still an audit row; errors still
carry names, never values (`MissingError`, "authentication failed"
without content). `TestSecretsNeverAtRest` remains the proof and gains a
KMS-provider variant (fake/emulated KMS) so the sentinel is verified to
appear nowhere — DB, APIs, logs, or KMS request/response captures.

### 7. Future / second provider — vault-as-store (designed, deferred)

A stronger posture where **the hub holds no ciphertext at all**: the
value lives *in* an external vault (e.g. Azure Key Vault **secrets**, AWS
Secrets Manager, HashiCorp Vault KV), and the hub stores only a
reference/URI in place of the envelope.

- **Model:** `Put` writes the value to the tenant's vault and stores a
  `store.SecretRef{vault, path, version}` row; `{"$secret":"name"}` is
  unchanged in flow docs. At resolve time the value is fetched from the
  vault — ideally **pulled by the runner directly** (data-plane-shaped),
  so the value never transits the hub, tightening the "hub never touches
  secret data" line even further than today. Where direct runner→vault
  networking isn't available, the hub proxies the fetch (still
  transient, still audited).
- **Provider interface:** this is a different seam — a `SecretStore`
  (Put/Resolve/Delete over refs) rather than a `kek.Provider` (Wrap/
  Unwrap over DEKs). Selection is the same `SHIFT_HUB_SECRET_BACKEND`
  style switch; the two families are mutually exclusive per deployment.
- **Tradeoffs (why it's second, not first):**
  - Requires a **per-tenant vault mapping** and per-tenant access
    policies — more provisioning surface, and BYO-vault is now a hard
    onboarding dependency.
  - The hub loses the single-store rotation/rewrap primitive; rotation
    and lifecycle move into the vault's model (per-vendor divergence).
  - Rich secrets features (versioning, TTL, revocation, native audit)
    come for free; but list/consistency/latency now depend on an external
    system per read.
  - No ciphertext at rest in Postgres is a genuine security win (nothing
    to steal from our DB), paid for in operational coupling.

  It is recorded here so the `SecretStore` seam is designed alongside the
  KEK seam; KMS-KEK ships first because it is a **drop-in behind the
  existing interface with zero schema change** and immediately removes the
  root-key-on-hub problem.

## Consequences

- **Supersedes/evolves ADR-0010's KEK decision.** ADR-0010's envelope
  design, `{"$secret":…}` refs, runner-pull resolution, tenancy, and
  never-at-rest proof all stand; only "first provider = local file, KMS
  later" is now realised — local file is demoted to the dev/self-host
  provider and KMS-KEK is the production default.
- The hub **no longer holds a usable root key** under a KMS provider: it
  can call wrap/unwrap but cannot exfiltrate key material. Compromise of
  the hub host no longer trivially decrypts the secret store (attacker
  must also hold live KMS access, which is revocable).
- New dependency: exactly one cloud KMS SDK per selected provider, gated
  to the hub module by depguard; the ADR-0006 vuln/lint gates are the
  control. No new crypto is authored.
- Secret availability is now coupled to KMS availability — accepted; the
  fail-closed rule forbids trading it for a plaintext or cached-key
  fallback.
- Per-account KEK scoping (incl. customer BYOK / crypto-shred) becomes
  possible without schema change, unlocking a real enterprise ask.
- The `SecretStore` (vault-as-store) seam is designed but unbuilt; a
  future ADR/PR implements it when a customer requires the hub to hold no
  ciphertext.
