package store

import (
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/hub/internal/pgtest"
)

// TestStatementTimeoutFires proves the issue-#8 statement_timeout is real: a
// query that runs longer than the configured timeout is cancelled by Postgres
// rather than hanging the connection.
func TestStatementTimeoutFires(t *testing.T) {
	t.Setenv("SHIFT_HUB_STMT_TIMEOUT", "500ms")
	ctx := context.Background()
	s, err := Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// pg_sleep(3s) far exceeds the 500ms statement_timeout → cancelled.
	var out int
	err = s.pool.QueryRow(ctx, "SELECT 1 FROM pg_sleep(3)").Scan(&out)
	if err == nil {
		t.Fatal("pg_sleep(3) completed; statement_timeout did not fire")
	}
	if !strings.Contains(err.Error(), "statement timeout") {
		t.Fatalf("err = %v, want a statement-timeout cancellation", err)
	}
}
