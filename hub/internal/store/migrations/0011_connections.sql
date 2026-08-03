-- Connections: reusable, account-scoped connector configuration (ADR-0034).
--
-- A node names a connection instead of repeating the host/credential settings
-- on every verb it uses against the same system. The stored document is
-- ordinary connector config and MAY contain {"$secret":"name"} references —
-- which resolve runner-side by the existing mechanism (ADR-0010), so this
-- table holds references, never credentials. Same posture as flow_versions,
-- which already stores documents carrying the same references.
--
-- Not versioned (unlike flows): editing a connection takes effect for every
-- flow referencing it with no publish step. That is deliberate for the case
-- this exists to serve — rotating a credential is one edit, not one edit per
-- node per flow — and is ADR-0034 open question 3 if it ever bites.
CREATE TABLE connections (
    id         UUID  PRIMARY KEY,
    account_id UUID  NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Mirrors flowdoc.ConnectionNamePattern: addressed in the control API and
    -- rendered on studio nodes, so it is an identifier, not prose.
    name       TEXT  NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
    -- The connector this connection configures. A node referencing it must
    -- declare the same connector; the API rejects a mismatch at deploy.
    connector  TEXT  NOT NULL CHECK (connector <> ''),
    config     JSONB NOT NULL,
    version    INT   NOT NULL DEFAULT 1,   -- bumped on replace; audit/debug only
    created_by UUID  REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);
