package csvf

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/batchtest"
)

// TC-009 (docs/assurance/test-conformance.md). csvf is the reader with the
// most to lose here: it reads a HEADER row once and uses those names as the
// keys of every record in every later batch. Had the header been kept as
// slices into the batch that carried it, each batch after the first would key
// its records off freed memory — and no existing assertion would show it,
// because that memory still holds the header until something reuses it.
func TestHeaderNamesDoNotPointIntoARetiredBatch(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,name,amount\n")
	for i := range 25 {
		sb.WriteString(strconv.Itoa(i) + ",row-" + strconv.Itoa(i) + ",1" + strconv.Itoa(i) + ".50\n")
	}
	in := sb.String()
	batchtest.AssertPoisonSafe(t, func() batchtest.Source {
		return NewReader(strings.NewReader(in), ReaderOptions{BatchRecords: 4})
	})
}
