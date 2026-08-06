-- Caller-facing status for asynchronous requests (ADR-0042 §3).
--
-- Deliberately NOT direct_executions. That table is operator-facing history
-- plus the metering rows the usage export reads (M6d); it is written once, when
-- an execution is already terminal, and it is kept. This one is caller-facing,
-- exists from the moment a request is ACCEPTED, and is pruned once the caller
-- has read it. Collapsing the two is how pruning quietly breaks billing data.
--
-- Metering therefore still happens on the direct_executions write at
-- completion, never here: counting usage at accept would bill work that has not
-- happened and may yet fail.
CREATE TABLE execution_status (
    -- Minted by the accepting RUNNER (UUIDv4), not by the hub, so it can be
    -- quoted in the 202 without waiting on an id the hub would have to invent.
    -- Minting is not owning: the row lives here, so ANY runner can serve a
    -- status read and the accepting runner may be long gone (ADR-0042 §3a).
    id           UUID PRIMARY KEY,
    account_id   UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    runner_id    UUID REFERENCES runners(id) ON DELETE SET NULL,

    flow_name    TEXT NOT NULL,
    -- route is the public path that accepted the request. The status read
    -- arrives under that same path (/orders/_status/{id}), so comparing them is
    -- cheap defence in depth: a valid id from a DIFFERENT route is refused even
    -- if the caller is authorised for both.
    route        TEXT NOT NULL DEFAULT '',
    -- principal is who the gateway said the caller was. It is the authorisation
    -- key for reads: a mismatch is 404, never 403 -- a distinguishable response
    -- confirms that someone else's task exists under that id.
    principal    TEXT NOT NULL DEFAULT '',
    -- token_sha256 is set only for ANONYMOUS routes, where every caller shares
    -- one principal and a principal comparison authorises nothing. The status
    -- URL then carries a per-task token and becomes a capability URL. Only the
    -- digest is stored; the hub never holds the token itself (ADR-0042 §3b).
    token_sha256 TEXT,

    state        TEXT NOT NULL CHECK (state IN ('accepted','running','completed','failed')),
    records_in   BIGINT NOT NULL DEFAULT 0,
    records_out  BIGINT NOT NULL DEFAULT 0,
    -- error carries the canonical error shape (ADR-0031): step id and class of
    -- failure. Never record content -- this is read by an internet caller.
    error_step   TEXT,
    error_code   TEXT,
    error        TEXT,

    accepted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    -- consumed_at is stamped by the first successful TERMINAL read. The sweeper
    -- prunes consumed rows after a grace period rather than on the read itself,
    -- because clients poll twice and deleting on first read makes the second
    -- look like a forgery (410 Gone during the window, ADR-0042 §3c).
    consumed_at  TIMESTAMPTZ,
    -- expires_at bounds an UNREAD row: a caller that never polls must not keep
    -- a row forever.
    expires_at   TIMESTAMPTZ NOT NULL
);

-- The read path is (account, id); the sweeper walks by expiry.
CREATE INDEX execution_status_expiry ON execution_status (expires_at);
CREATE INDEX execution_status_account ON execution_status (account_id, accepted_at DESC);
