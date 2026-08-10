package fixedw

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
)

// TC-019 residual. Every other reader bounds a single unit: ndjson has
// MaxLineBytes, xmlf and ndjson have MaxDepth, edi has MaxSegmentBytes, csvf
// has MaxRecordBytes. fixedw was recorded as "bounded by its layout" and left
// unaudited — but readLine falls back to unbounded accumulation whenever a line
// exceeds the bufio buffer, and that path serves BOTH the SkipLines preamble
// and every record read.
//
// A fixed-width layout says how wide a record is, which is exactly why a line
// that never ends is a contradiction the reader should refuse rather than
// buffer: the bytes past the layout's width can never become fields.

func bytesAllocated() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.TotalAlloc
}

// endless yields 'x' with no newline — a source that trickles, which is the
// shape a hostile or broken upstream actually has.
//
// It stops at a ceiling rather than running truly forever, deliberately: an
// unbounded reader must FAIL this test, not hang the package. Measured before
// the bound, a genuinely endless source ran `go test` to its 180-second timeout.
type endless struct {
	n   int64
	cap int64
}

func (e *endless) Read(p []byte) (int, error) {
	if e.cap > 0 && e.n >= e.cap {
		return 0, io.ErrUnexpectedEOF
	}
	for i := range p {
		p[i] = 'x'
	}
	e.n += int64(len(p))
	return len(p), nil
}

// endlessCap is the ceiling: far past any legitimate line, small enough that a
// regression fails in seconds.
const endlessCap = 256 << 20

func TestALineThatNeverEndsIsRefusedRatherThanBuffered(t *testing.T) {
	// An explicit small bound, and a source far larger than it. The property is
	// that cost tracks the BOUND rather than the input: with 256 MiB on offer
	// and a 1 MiB limit, allocation must stay near the limit. (It cannot equal
	// it — append doubles as it grows — so the assertion allows a multiple of
	// the bound while staying far below the input.)
	const limit = 1 << 20
	src := &endless{cap: endlessCap}
	r := NewReader(src, ReaderOptions{
		Columns:      []Column{{Name: "a", Width: 10}},
		MaxLineBytes: limit,
	})

	before := bytesAllocated()
	_, err := r.Next(context.Background())
	after := bytesAllocated()
	grew := after - before

	t.Logf("offered %d MiB, consumed %d MiB, allocated %d MiB with a %d MiB bound, err = %v",
		endlessCap>>20, src.n>>20, grew>>20, limit>>20, err)

	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("an endless line was accepted (err = %v) after consuming %d MiB: "+
			"readLine accumulates without limit", err, src.n>>20)
	}
	if !strings.Contains(err.Error(), "max_line_bytes") {
		t.Fatalf("endless line refused as %q, not as a size problem", err)
	}
	if grew > 16*limit {
		t.Fatalf("allocated %d MiB against a %d MiB bound: cost tracks the input, not the limit",
			grew>>20, limit>>20)
	}
}

// TestSkipLinesCannotBeUsedToBuffer: the preamble skip uses the same readLine,
// so a file whose FIRST line never ends is the same bomb wearing a hat.
func TestSkipLinesCannotBeUsedToBuffer(t *testing.T) {
	src := &endless{cap: endlessCap}
	r := NewReader(src, ReaderOptions{
		SkipLines:    1,
		Columns:      []Column{{Name: "a", Width: 10}},
		MaxLineBytes: 1 << 20,
	})

	before := bytesAllocated()
	_, err := r.Next(context.Background())
	after := bytesAllocated()

	t.Logf("consumed %d MiB, allocated %d MiB, err = %v", src.n>>20, (after-before)>>20, err)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("an endless SkipLines preamble was accepted (err = %v) after %d MiB", err, src.n>>20)
	}
}

// TestAnOrdinaryLongLineIsStillRead guards the other side: the fallback path
// exists because a legitimate line can exceed the bufio buffer, and a bound
// that refused those would break real fixed-width extracts.
func TestAnOrdinaryLongLineIsStillRead(t *testing.T) {
	// 128 KiB of record — well past bufio's default buffer, well under the
	// bound. The layout is as wide as the line, because a fixed-width record
	// that does not match its layout is a different failure entirely.
	const width = 128 << 10
	wide := strings.Repeat("a", width)
	r := NewReader(strings.NewReader(wide+"\n"), ReaderOptions{
		Columns: []Column{{Name: "a", Width: width}},
	})
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("a %d-byte line was refused: %v", len(wide), err)
	}
	if b.Len() != 1 {
		t.Fatalf("read %d records, want 1", b.Len())
	}
}
