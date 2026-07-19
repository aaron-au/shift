-- Schema v1: the hub's durable state (ADR-0002). Evolved from the v0
-- prototype schema (docs/reference/schema-v0.sql) — tightened to what
-- M4a actually serves: accounts, runner identity, flow versions, the
-- task queue with leases, attempt history, and an audit log.
-- OIDC users/roles, secrets (envelope encryption), connector registry,
-- schedules, and billing land in M4b with their features.

CREATE TABLE accounts (
    id         UUID PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Single-tenant seed; multi-tenancy activates with OIDC in M4b.
INSERT INTO accounts (id, name)
VALUES ('00000000-0000-0000-0000-000000000001', 'default');

-- Single-use runner registration tokens (ADR-0002: token-based runner
-- registration). Only the SHA-256 of the token is stored.
CREATE TABLE runner_registration_tokens (
    id         UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    token_hash BYTEA NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Registered runners. secret_hash is the SHA-256 of the bearer secret
-- issued at registration; the plaintext is returned once and never stored.
CREATE TABLE runners (
    id            UUID PRIMARY KEY,
    account_id    UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    secret_hash   BYTEA NOT NULL UNIQUE,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ
);

CREATE INDEX runners_account ON runners (account_id);

CREATE TABLE flows (
    id             UUID PRIMARY KEY,
    account_id     UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    latest_version INT  NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);

CREATE TABLE flow_versions (
    flow_id    UUID NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
    version    INT  NOT NULL,
    document   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (flow_id, version)
);

-- The durable task queue (ADR-0002): claimed with FOR UPDATE SKIP LOCKED,
-- held by time-bounded leases, re-dispatched on expiry.
CREATE TABLE tasks (
    id               UUID PRIMARY KEY,
    account_id       UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    flow_id          UUID REFERENCES flows(id) ON DELETE SET NULL,
    flow_name        TEXT NOT NULL,
    flow_version     INT  NOT NULL,
    document         JSONB NOT NULL,
    idempotency_key  TEXT,
    state            TEXT NOT NULL DEFAULT 'queued'
                     CHECK (state IN ('queued','leased','completed','failed')),
    attempt          INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 3,
    leased_by        UUID REFERENCES runners(id) ON DELETE SET NULL,
    lease_expires_at TIMESTAMPTZ,
    enqueued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    error            TEXT,
    result           JSONB
);

CREATE UNIQUE INDEX tasks_idempotency
    ON tasks (account_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX tasks_claimable ON tasks (enqueued_at) WHERE state IN ('queued','leased');
CREATE INDEX tasks_recent ON tasks (enqueued_at DESC);

-- One row per lease of a task: who ran it and how it ended
-- (completed | failed | lease-expired).
CREATE TABLE task_attempts (
    task_id     UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt     INT  NOT NULL,
    runner_id   UUID REFERENCES runners(id) ON DELETE SET NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome     TEXT,
    error       TEXT,
    PRIMARY KEY (task_id, attempt)
);

CREATE TABLE audit_log (
    id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor  TEXT NOT NULL,
    action TEXT NOT NULL,
    entity TEXT NOT NULL DEFAULT '',
    detail JSONB
);

CREATE INDEX audit_log_at ON audit_log (at DESC);
