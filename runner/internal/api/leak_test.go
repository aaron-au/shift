package api

import (
	"testing"

	"github.com/aaron-au/shift/engine/leaktest"
)

// TC-001 (docs/assurance/test-conformance.md). Each accepted trigger runs its
// task on its own goroutine (ADR-0005); a request that returns while its task
// goroutine is stranded is invisible to a status-code assertion.
func TestMain(m *testing.M) { leaktest.Main(m) }
