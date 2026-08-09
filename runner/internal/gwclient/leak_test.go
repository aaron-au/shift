package gwclient

import (
	"testing"

	"github.com/aaron-au/shift/engine/leaktest"
)

// TC-001 (docs/assurance/test-conformance.md). The gateway client holds a
// long-poll goroutine per configured gateway (ADR-0038) for the process
// lifetime, so "it stops when told to" has no natural assertion point other
// than this one — a poll loop that ignores its stop signal looks identical to
// one that is simply still waiting.
func TestMain(m *testing.M) { leaktest.Main(m) }
