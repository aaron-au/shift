package service

import (
	"testing"

	"github.com/aaron-au/shift/engine/leaktest"
)

// TC-001 (docs/assurance/test-conformance.md). This is where ADR-0005 and
// ADR-0029 actually meet: every task runs on its own goroutine(s), and a v3 DAG
// fans branches out at tee/router and joins them at merge. TestRouterArmToStop
// EndsTheFlowCleanly and friends assert the flow ended — not that the branches
// it started did.
func TestMain(m *testing.M) { leaktest.Main(m) }
