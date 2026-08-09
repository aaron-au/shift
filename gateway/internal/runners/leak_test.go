package runners

import (
	"testing"

	"github.com/aaron-au/shift/gateway/internal/leaktest"
)

// TC-001 (docs/assurance/test-conformance.md). The gateway holds a long-poll
// goroutine per waiting runner and one per in-flight request (ADR-0038); a
// runner that withdraws, or a request whose client hangs up, must take its
// goroutine with it. This is the DMZ component — an unbounded goroutine leak
// here is reachable from the public internet.
func TestMain(m *testing.M) { leaktest.Main(m) }
