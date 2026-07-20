-- Webhook configuration authored on the hub (control plane) and synced to
-- runners (ADR-0016). The hub stores only metadata + the token HASH, never
-- the payload a hook carries. flow_name references a deployed flow by name;
-- the runner receives the published document at sync time.
CREATE TABLE webhooks (
    id         UUID PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    flow_name  TEXT NOT NULL,
    token_hash TEXT,  -- hex SHA-256 of the per-hook token; NULL = open hook
    enabled    BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);

CREATE INDEX webhooks_account ON webhooks (account_id);
