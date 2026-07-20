-- Direct (push) executions run entirely on a runner (webhook / direct API,
-- ADR-0016); the hub never sees their payload. The runner reports metadata
-- afterwards so the hub has fleet load + history. This is deliberately NOT
-- the durable task queue: these tasks were never queued or leased, they
-- arrive already terminal, and they carry no document or payload — only
-- counts and outcome.
CREATE TABLE direct_executions (
    id          UUID PRIMARY KEY,
    account_id  UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    runner_id   UUID REFERENCES runners(id) ON DELETE SET NULL,
    flow_name   TEXT NOT NULL,
    trigger     TEXT NOT NULL,  -- 'webhook' | 'api'
    state       TEXT NOT NULL CHECK (state IN ('completed','failed')),
    records_in  BIGINT NOT NULL DEFAULT 0,
    records_out BIGINT NOT NULL DEFAULT 0,
    error       TEXT,
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX direct_executions_account ON direct_executions (account_id, created_at DESC);
