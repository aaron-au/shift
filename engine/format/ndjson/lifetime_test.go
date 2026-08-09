package ndjson

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/batchtest"
)

// TC-009 (docs/assurance/test-conformance.md). The reader reuses one batch
// across every Next. Nothing in Go stops it from also reading its own previous
// output — a retained key, a cached value, a slice into the last arena — and a
// test that only checks the records would never notice, because the retired
// memory is still alive and still holds the right bytes.
func TestTheReaderNeverReadsItsOwnRetiredBatch(t *testing.T) {
	var sb strings.Builder
	for i := range 25 {
		sb.WriteString(`{"id":` + strconv.Itoa(i) +
			`,"name":"row-` + strconv.Itoa(i) +
			`","tags":["a","b"],"nested":{"k":"v` + strconv.Itoa(i) + `"}}` + "\n")
	}
	in := sb.String()
	batchtest.AssertPoisonSafe(t, func() batchtest.Source {
		return NewReader(strings.NewReader(in), ReaderOptions{BatchRecords: 4})
	})
}
