-- Schema v2 (M4b): OIDC users and secrets with envelope encryption.
-- Connector registry and schedules land in their own migrations
-- (0003–0005); billing remains reserved.

-- OIDC users, JIT-provisioned on the first verified token. (issuer,
-- subject) is the stable identity; email is informational and only
-- trusted when the IdP marks it verified. Full RBAC is deliberately
-- collapsed to a single role column — viewer/admin is all the admin
-- realm needs until multi-tenant signup exists.
CREATE TABLE users (
    id            UUID PRIMARY KEY,
    account_id    UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    issuer        TEXT NOT NULL,
    subject       TEXT NOT NULL,
    email         TEXT NOT NULL DEFAULT '',
    display_name  TEXT NOT NULL DEFAULT '',
    role          TEXT NOT NULL DEFAULT 'admin' CHECK (role IN ('admin','viewer')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at TIMESTAMPTZ,
    UNIQUE (issuer, subject)
);
CREATE INDEX users_account ON users (account_id);

-- Envelope-encrypted secrets: the value is sealed by a per-secret DEK
-- (AES-256-GCM, 12-byte nonce prefixed to the blob), and the DEK is
-- wrapped by the KEK named in kek_id. KEK rotation re-wraps DEKs only;
-- ciphertext never moves. Plaintext is never stored and never returned
-- by admin reads — only the runner resolve path decrypts.
CREATE TABLE secrets (
    id          UUID  PRIMARY KEY,
    account_id  UUID  NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name        TEXT  NOT NULL CHECK (name ~ '^[A-Za-z0-9_.-]{1,128}$'),
    ciphertext  BYTEA NOT NULL,   -- nonce || AES-256-GCM(DEK, value, aad = id:version)
    wrapped_dek BYTEA NOT NULL,   -- nonce || AES-256-GCM(KEK, DEK)
    kek_id      TEXT  NOT NULL,
    version     INT   NOT NULL DEFAULT 1,
    created_by  UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);
CREATE INDEX secrets_kek ON secrets (kek_id);
