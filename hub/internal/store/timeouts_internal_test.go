package store

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestSetParamDefault pins the statement/lock-timeout resolution (issue #8):
// default applied when unset, env override, and an explicit DSN value winning.
func TestSetParamDefault(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://u:p@localhost:5432/db")
	if err != nil {
		t.Fatal(err)
	}
	setParamDefault(cfg, "statement_timeout", "", "30s")
	if got := cfg.ConnConfig.RuntimeParams["statement_timeout"]; got != "30s" {
		t.Errorf("default: statement_timeout = %q, want 30s", got)
	}
	setParamDefault(cfg, "lock_timeout", "9s", "5s") // env wins over def
	if got := cfg.ConnConfig.RuntimeParams["lock_timeout"]; got != "9s" {
		t.Errorf("env: lock_timeout = %q, want 9s", got)
	}

	// An explicit DSN value is never overridden.
	cfg2, err := pgxpool.ParseConfig("postgres://u:p@localhost:5432/db?statement_timeout=99s")
	if err != nil {
		t.Fatal(err)
	}
	setParamDefault(cfg2, "statement_timeout", "1s", "30s")
	if got := cfg2.ConnConfig.RuntimeParams["statement_timeout"]; got != "99s" {
		t.Errorf("dsn should win: statement_timeout = %q, want 99s", got)
	}
}
