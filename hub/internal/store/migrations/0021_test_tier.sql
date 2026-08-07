-- The test tier (ADR-0048 §1/§3).
--
-- Testing an integration usually means running it against something real,
-- because the alternative is a second licensed environment. So people test in
-- production and the platform gets blamed for the outcome. Test capacity
-- removes the objection that causes the damage.
--
-- TIER IS A ROSTER ATTRIBUTE, ASSERTED BY THE HUB — exactly as placement
-- labels are (ADR-0041 §3). A runner proves an identity; the hub says what
-- that identity means. The inverse is what makes it worth enforcing: a runner
-- that could self-assert `tier: production` would escape metering, and one
-- that could self-assert `tier: test` would receive work it should not see.
-- Nothing the runner sends is consulted, so neither is reachable.
ALTER TABLE runners ADD COLUMN tier TEXT NOT NULL DEFAULT 'production'
    CHECK (tier IN ('production', 'test'));

-- Whether a task is test-marked (ADR-0048 §3).
--
-- Only two things set it: the studio's run-now, and an API execution that
-- flags itself explicitly. NOT schedules, NOT webhook routes, NOT anything
-- that arrives unattended. That is not an abuse control, it is the definition:
-- a scheduled flow running on test capacity is a production flow metered
-- wrong, and a webhook route pointed at test capacity is a production ingress
-- with no support commitment.
ALTER TABLE tasks ADD COLUMN test BOOLEAN NOT NULL DEFAULT FALSE;

-- The claim query filters on it, and a test runner asks on every poll.
CREATE INDEX tasks_queued_test ON tasks (account_id, test, enqueued_at) WHERE state = 'queued';
