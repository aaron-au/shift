-- Usage metering substrate (M6d). Append-only ledger: one row per terminal
-- execution — queued tasks (source 'task') and direct/push runs (source
-- 'webhook'|'api', ADR-0016). Metadata only (counts + seconds), never payload.
--
-- The hub is TASK CONTROL, not the central account/billing platform (that is a
-- separate, external system). This table is the metering substrate + export
-- point the external billing platform pulls from; account_id is a tenant key,
-- not an account of record. It is deliberately decoupled from `tasks`: the
-- operational task row (and its potentially large `result` JSONB) may be pruned,
-- but the usage record must survive, so metrics are promoted to typed columns
-- here at completion time rather than being re-derived from `tasks.result`.
CREATE TABLE usage_events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id   UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    at           TIMESTAMPTZ NOT NULL DEFAULT now(),   -- when the execution finished
    source       TEXT NOT NULL,                        -- task | webhook | api
    flow_name    TEXT NOT NULL DEFAULT '',
    outcome      TEXT NOT NULL,                         -- completed | failed
    records_in   BIGINT NOT NULL DEFAULT 0,
    records_out  BIGINT NOT NULL DEFAULT 0,
    exec_seconds DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- Aggregation + window queries scope by account, newest first.
CREATE INDEX usage_events_account_at ON usage_events (account_id, at DESC);
-- Cursor-based export pull (external billing ingest) walks by monotonic id.
CREATE INDEX usage_events_id ON usage_events (id);
