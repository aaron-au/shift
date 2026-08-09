package fixedw

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/batchtest"
)

// TC-009 (docs/assurance/test-conformance.md). The layout is config rather
// than data, so this reader has the least cross-batch state of the five — but
// column NAMES still become keys in every batch, and "the risky ones only"
// is how a harness stops being run at all. Cheap, and it pins the reader
// against a future optimisation that starts caching per-batch.
func TestColumnKeysDoNotPointIntoARetiredBatch(t *testing.T) {
	var sb strings.Builder
	for i := range 25 {
		fmt.Fprintf(&sb, "%06d%-10s  %08d%08d%1d\n", i, fmt.Sprintf("row-%d", i), 1250+i, 20260809, i%2)
	}
	in := sb.String()
	batchtest.AssertPoisonSafe(t, func() batchtest.Source {
		return NewReader(strings.NewReader(in), ReaderOptions{Columns: invoice(), BatchRecords: 4})
	})
}
