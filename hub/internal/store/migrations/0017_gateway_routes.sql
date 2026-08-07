-- Gateway routes, runner labels, and per-gateway proxy trust (ADR-0038 §5/§6).
--
-- Until now a gateway's routes were hand-written JSON on the DMZ host, which is
-- the second source of truth ADR-0038 §6 exists to remove: the hub owns
-- configuration, and the gateway converges on it. This is the model the hub
-- builds that configuration from.
CREATE TABLE gateway_routes (
    id          UUID PRIMARY KEY,
    account_id  UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,

    -- NULL means every gateway in the account. A route pinned to one gateway is
    -- for the deployment that fronts different estates from different DMZs;
    -- the common case is one route set everywhere, so that is the default.
    gateway_id  UUID REFERENCES gateways(id) ON DELETE CASCADE,

    path        TEXT NOT NULL,
    method      TEXT NOT NULL DEFAULT '',   -- '' = any
    flow        TEXT NOT NULL,

    -- Which runners may serve this route, by LABEL SET (ADR-0038 §5).
    -- Empty matches any runner.
    selector    JSONB NOT NULL DEFAULT '{}',

    -- The caller's bearer credential, stored as a HASH only. The plaintext is
    -- returned once at creation and never again — the gateway compares a hash
    -- in constant time, so nothing here needs to be reversible.
    auth_token_sha256 TEXT NOT NULL DEFAULT '',
    -- WHO that credential belongs to: the principal stamped for the runner
    -- (ADR-0038 §4b). Travels with the verification material so "who" is a
    -- configured fact rather than something each auth method derives its own way.
    auth_principal    TEXT NOT NULL DEFAULT '',

    allow_cidrs      TEXT[] NOT NULL DEFAULT '{}',
    require_headers  JSONB  NOT NULL DEFAULT '{}',
    max_body_bytes   BIGINT NOT NULL DEFAULT 0,   -- 0 = the gateway's default

    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX gateway_routes_account ON gateway_routes (account_id);

-- One handler per method+path per gateway. The gateway refuses a configuration
-- with duplicates outright (config.Validate), so catching it here turns a
-- whole-config rejection into a single failed edit.
CREATE UNIQUE INDEX gateway_routes_unique
    ON gateway_routes (account_id, COALESCE(gateway_id, '00000000-0000-0000-0000-000000000000'::uuid), method, path);

-- Runner labels are ASSERTED BY THE HUB (ADR-0041 §3). They live here and never
-- on the runner, because a runner that could state what it IS could promote
-- itself into a trust tier by claiming one — placement would then be a
-- suggestion rather than a decision.
ALTER TABLE runners ADD COLUMN labels JSONB NOT NULL DEFAULT '{}';

-- Whose X-Forwarded-* headers this gateway believes. Per gateway rather than
-- per account because it describes where the box SITS, and two gateways in
-- different DMZs sit behind different things. Empty believes none, which is the
-- safe default: a spoofable forwarded header would defeat every per-route IP
-- allowlist above.
ALTER TABLE gateways ADD COLUMN trusted_proxies TEXT[] NOT NULL DEFAULT '{}';
