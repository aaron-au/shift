package edi

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/batchtest"
)

// TC-009 (docs/assurance/test-conformance.md). EDI discovers its delimiters
// from the interchange header — the one piece of state every later segment in
// every later batch depends on. Held as slices into the batch that carried
// ISA, they would decode the first batch correctly and garbage thereafter,
// invisibly, because the retired arena still reads as the header until reused.
func TestDiscoveredDelimitersDoNotPointIntoARetiredBatch(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *260809*1200*U*00401*000000001*0*P*>~")
	sb.WriteString("GS*PO*SENDER*RECEIVER*20260809*1200*1*X*004010~")
	for i := range 25 {
		sb.WriteString("BEG*00*SA*PO-" + strconv.Itoa(i) + "**20260809~")
	}
	sb.WriteString("GE*1*1~IEA*1*000000001~")
	in := sb.String()
	batchtest.AssertPoisonSafe(t, func() batchtest.Source {
		return NewReader(strings.NewReader(in), ReaderOptions{BatchRecords: 4})
	})
}
