package scheduler

import (
	"testing"

	"github.com/aaron-au/shift/engine/leaktest"
)

// TC-001 (docs/assurance/test-conformance.md). The scheduler is a long-running
// tick loop holding a Postgres advisory lock (ADR-0012). A loop that outlives
// its context keeps the lock, and the next hub replica to try never gets it —
// exactly-once degrades to never.
func TestMain(m *testing.M) { leaktest.Main(m) }
