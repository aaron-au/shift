package stream

import (
	"testing"

	"github.com/aaron-au/shift/engine/leaktest"
)

// TC-001 (docs/assurance/test-conformance.md). Pipe, Tee and the merge
// operators start goroutines; a branch that is never drained, a reader closed
// without its writer, or a cancelled context that nobody selects on all leave a
// goroutine parked forever. The flow-level assertions in this package prove the
// records come out right, which they still do while a goroutine is stranded.
func TestMain(m *testing.M) { leaktest.Main(m) }
