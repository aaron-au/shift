package xmlf

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/batchtest"
)

// TC-009 (docs/assurance/test-conformance.md). The XML reader carries element
// names and an open-element stack across Next calls; any of it held as a slice
// into the batch being built would survive testing intact and corrupt in
// production, where the reuse pattern differs.
func TestElementStateDoesNotPointIntoARetiredBatch(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("<rows>")
	for i := range 25 {
		sb.WriteString(`<row id="` + strconv.Itoa(i) + `"><name>row-` + strconv.Itoa(i) +
			`</name><inner><deep>d` + strconv.Itoa(i) + `</deep></inner></row>`)
	}
	sb.WriteString("</rows>")
	in := sb.String()
	batchtest.AssertPoisonSafe(t, func() batchtest.Source {
		return NewReader(strings.NewReader(in), ReaderOptions{RecordElement: "row", BatchRecords: 4})
	})
}
