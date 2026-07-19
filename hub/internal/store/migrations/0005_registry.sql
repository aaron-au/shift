-- Connector registry (M4b, ADR-0011): content-addressed artifact blobs,
-- detached Ed25519 signatures over the consign manifest, and trusted
-- publisher keys. Publisher PRIVATE keys never exist server-side — a
-- hub/DB compromise cannot forge artifacts. Blobs live in Postgres
-- (the only stateful service; hub replicas stay diskless); dedup is by
-- content digest.
CREATE TABLE publisher_keys (
    id         UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    public_key BYTEA NOT NULL,            -- 32-byte Ed25519
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    UNIQUE (account_id, name),
    UNIQUE (account_id, public_key)
);

CREATE TABLE connector_blobs (
    digest     BYTEA PRIMARY KEY,         -- SHA-256 of the artifact bytes
    size_bytes BIGINT NOT NULL,
    data       BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE connectors (
    id         UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,             -- runner connpool naming rules
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);

CREATE TABLE connector_versions (
    connector_id     UUID NOT NULL REFERENCES connectors(id) ON DELETE CASCADE,
    version          TEXT NOT NULL,
    os               TEXT NOT NULL,
    arch             TEXT NOT NULL,
    digest           BYTEA NOT NULL REFERENCES connector_blobs(digest),
    signature        BYTEA NOT NULL,      -- 64-byte Ed25519 detached signature
    publisher_key_id UUID NOT NULL REFERENCES publisher_keys(id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    yanked_at        TIMESTAMPTZ,
    PRIMARY KEY (connector_id, version, os, arch)
);
CREATE INDEX connector_versions_latest ON connector_versions (connector_id, created_at DESC);
