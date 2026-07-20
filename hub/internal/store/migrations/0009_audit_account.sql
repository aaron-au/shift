-- Audit-log completion (M6b). audit_log was the one control-plane table not
-- tenant-scoped; give it an account_id so the audit view can be scoped like
-- every other list. Existing rows default to the seed account.
ALTER TABLE audit_log ADD COLUMN account_id UUID NOT NULL
    DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES accounts(id) ON DELETE CASCADE;
CREATE INDEX audit_log_account_at ON audit_log (account_id, id DESC);
