-- Test executions are metered separately (ADR-0048 §4).
--
-- There is no record limit and no wall-clock limit on a test execution, and no
-- quota bounding test capacity (§2 was withdrawn). What replaces them is
-- measurement: test usage is metered, excluded from billing, and VISIBLE.
--
-- That is the whole control. The abuse path requires a person repeatedly
-- clicking run, which is self-limiting, and §3 already removes every unattended
-- route into test capacity — no schedules, no webhooks. So abuse becomes
-- observable rather than capped, and a fair-use clause handles the rare case.
-- Unmeasured is the failure mode to avoid; uncapped is fine.
--
-- The column exists on the EVENT rather than being derived at read time
-- because the export is a cursor-based pull the billing platform ingests row by
-- row (M6d). A row it cannot classify on its own is a row it has to ask about.
ALTER TABLE usage_events ADD COLUMN test BOOLEAN NOT NULL DEFAULT FALSE;

-- Every aggregate splits on it, and the export filters on it.
CREATE INDEX usage_events_test ON usage_events (account_id, test, at);
