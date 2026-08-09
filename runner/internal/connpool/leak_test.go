package connpool

import (
	"testing"

	"github.com/aaron-au/shift/engine/leaktest"
)

// TC-001 (docs/assurance/test-conformance.md). The pool owns a reaper goroutine
// for its whole life and a connector subprocess per pooled entry; Close is the
// only thing that ends either. A Close that returned without stopping the
// reaper would pass every existing assertion in this package.
func TestMain(m *testing.M) { leaktest.Main(m) }
