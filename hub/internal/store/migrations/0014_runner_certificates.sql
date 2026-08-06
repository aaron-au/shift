-- Runner identity by client certificate (ADR-0044).
--
-- The bearer secret does not disappear on upgrade: a deployment that
-- terminates TLS at a proxy keeps using it (ADR-0044 §4), and one mid-cutover
-- has runners of both kinds. What changes is that it stops being MANDATORY, so
-- a runner registered under mTLS never has a long-lived replayable credential
-- written down at all.
ALTER TABLE runners ALTER COLUMN secret_hash DROP NOT NULL;

-- The issued certificate's identity, kept for operations rather than for
-- authentication: the subject IS the runner id, so nothing here is consulted
-- to authenticate a request. It answers "which certificate is this runner
-- currently using, and when does it expire" — the questions asked when a
-- renewal path is suspected of not working.
ALTER TABLE runners ADD COLUMN cert_serial     TEXT;
ALTER TABLE runners ADD COLUMN cert_not_after  TIMESTAMPTZ;
ALTER TABLE runners ADD COLUMN cert_issued_at  TIMESTAMPTZ;
