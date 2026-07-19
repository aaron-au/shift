-- Cron schedules on flows (M4b). Fired by the hub scheduler loop:
-- advisory-lock gated (823401), row-locked with FOR UPDATE SKIP LOCKED,
-- and deduped by the task idempotency key sched:<id>:<next_fire_at> —
-- the layered exactly-once mechanism (see docs/adr/0012). All cron
-- computation is UTC in M4b.
CREATE TABLE schedules (
    id            UUID PRIMARY KEY,
    account_id    UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    flow_id       UUID NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
    cron          TEXT NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT true,
    next_fire_at  TIMESTAMPTZ NOT NULL,
    last_fired_at TIMESTAMPTZ,
    last_task_id  UUID,
    last_error    TEXT,
    max_attempts  INT NOT NULL DEFAULT 3,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (flow_id)   -- one schedule per flow (M4b minimal)
);
CREATE INDEX schedules_due ON schedules (next_fire_at) WHERE enabled;
