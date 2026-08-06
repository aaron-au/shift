-- Gateway records: the hub's side of adoption (ADR-0049).
--
-- A gateway is identified by the PUBLIC KEY an administrator carried out of
-- band, not by a credential the hub issued first. That inversion is the whole
-- point: nothing secret travels toward the DMZ before trust exists, and the
-- fingerprint an interceptor sees grants nothing (ADR-0038 §4).
CREATE TABLE gateways (
    id              UUID PRIMARY KEY,
    account_id      UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,

    -- Where the hub dials. The hub initiates every control connection; the
    -- gateway never dials inward.
    url             TEXT NOT NULL,

    -- Hex SHA-256 of the gateway's long-lived public key. This is the trust
    -- anchor for the FIRST connection and every later one: it is retained
    -- after adoption, deliberately, so a gateway whose short-lived identity
    -- lapsed can still be reached and re-issued rather than being stranded
    -- behind a certificate it cannot renew (ADR-0049 §6).
    fingerprint     TEXT NOT NULL,

    -- NULL until the hub has completed the pinned exchange. An unadopted
    -- record is an intent to adopt, and a gateway that is already adopted
    -- refuses a second attempt rather than being overwritten.
    adopted_at      TIMESTAMPTZ,

    -- The issued identity, kept for operations rather than authentication:
    -- the subject IS the gateway id, so nothing here is consulted to
    -- authenticate. It answers "is renewal actually working".
    cert_serial     TEXT,
    cert_not_after  TIMESTAMPTZ,
    cert_issued_at  TIMESTAMPTZ,

    -- The configuration generation this gateway last acknowledged. Drift is
    -- visible from the hub, which is where the administrator is; the gateway
    -- persists no configuration of its own (ADR-0049 §3).
    config_version  BIGINT NOT NULL DEFAULT 0,
    pushed_version  BIGINT NOT NULL DEFAULT 0,
    last_push_at    TIMESTAMPTZ,
    last_push_error TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);

CREATE INDEX gateways_account ON gateways (account_id);

-- One key, one gateway. Two records sharing a fingerprint would make the
-- pinned dial ambiguous about who answered.
CREATE UNIQUE INDEX gateways_fingerprint ON gateways (fingerprint);
