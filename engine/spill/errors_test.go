package spill

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

var errBoom = errors.New("boom")

// countingWriter accepts everything and counts the calls, so a test can learn
// how many distinct writes encoding one value performs.
type countingWriter struct{ writes int }

func (w *countingWriter) Write(p []byte) (int, error) { w.writes++; return len(p), nil }

// failAtWriter accepts `ok` writes and then fails every subsequent one.
type failAtWriter struct{ ok int }

func (w *failAtWriter) Write(p []byte) (int, error) {
	if w.ok <= 0 {
		return 0, errBoom
	}
	w.ok--
	return len(p), nil
}

// TestEncoderPropagatesWriteErrors: a scratch-file write failure must surface
// from Encode at whatever point it happens — a half-written value that reports
// success would corrupt the spill segment silently. The sample value exercises
// every tag (null/bool/int/float/string/bytes/list/map), and every write
// position in it is failed in turn.
func TestEncoderPropagatesWriteErrors(t *testing.T) {
	src := record.NewBatch()
	v := sampleValue(src)

	cw := &countingWriter{}
	if err := NewEncoder(cw).Encode(v); err != nil {
		t.Fatal(err)
	}
	if cw.writes < 10 {
		t.Fatalf("sample value encoded in only %d writes; too coarse to be a useful probe", cw.writes)
	}
	for i := range cw.writes {
		fw := &failAtWriter{ok: i}
		if err := NewEncoder(fw).Encode(v); !errors.Is(err, errBoom) {
			t.Fatalf("failing write #%d: Encode = %v, want errBoom", i+1, err)
		}
	}
}

// TestDecoderRejectsEveryTruncation: a segment cut short at any offset must
// come back as an error (ErrUnexpectedEOF — distinct from the clean io.EOF the
// decoder returns only at a value boundary), never a panic and never a
// half-built value reported as good.
func TestDecoderRejectsEveryTruncation(t *testing.T) {
	src := record.NewBatch()
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(sampleValue(src)); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	for n := 1; n < len(full); n++ {
		dec := NewDecoder(bufio.NewReader(bytes.NewReader(full[:n])), 0)
		err := dec.Decode(record.NewBatch().Builder())
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncated to %d/%d bytes: Decode = %v, want ErrUnexpectedEOF", n, len(full), err)
		}
	}
}

// TestDecoderRejectsCorruptTags: a byte flipped to an unknown tag inside a
// container must be reported, not silently skipped.
func TestDecoderRejectsCorruptTags(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(tagList)
	buf.WriteByte(2) // two elements
	buf.WriteByte(tagNull)
	buf.WriteByte(0xEE) // corrupt tag for the second element
	dec := NewDecoder(bufio.NewReader(&buf), 0)
	err := dec.Decode(record.NewBatch().Builder())
	if err == nil || !strings.Contains(err.Error(), "unknown tag") {
		t.Fatalf("Decode = %v, want unknown-tag error", err)
	}

	// Same inside a map value.
	buf.Reset()
	buf.WriteByte(tagMap)
	buf.WriteByte(1)
	buf.WriteByte(1) // key length
	buf.WriteByte('k')
	buf.WriteByte(0xEE)
	dec = NewDecoder(bufio.NewReader(&buf), 0)
	if err := dec.Decode(record.NewBatch().Builder()); err == nil ||
		!strings.Contains(err.Error(), "unknown tag") {
		t.Fatalf("Decode = %v, want unknown-tag error", err)
	}
}

// TestDecoderRejectsDeepNesting: corrupt input claiming unbounded nesting must
// hit the depth guard and error, not recurse until the stack dies.
func TestDecoderRejectsDeepNesting(t *testing.T) {
	var buf bytes.Buffer
	for range 70 {
		buf.WriteByte(tagList)
		buf.WriteByte(1) // one element, which is the next list
	}
	buf.WriteByte(tagNull)
	dec := NewDecoder(bufio.NewReader(&buf), 0)
	err := dec.Decode(record.NewBatch().Builder())
	if err == nil || !strings.Contains(err.Error(), "nesting too deep") {
		t.Fatalf("Decode = %v, want nesting-too-deep error", err)
	}
}

// TestDecoderRejectsOversizedMapKey: the blob guard applies to keys too.
func TestDecoderRejectsOversizedMapKey(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(tagMap)
	buf.WriteByte(1)
	buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}) // huge key length
	dec := NewDecoder(bufio.NewReader(&buf), 1024)
	err := dec.Decode(record.NewBatch().Builder())
	if err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("Decode = %v, want blob-limit error", err)
	}
}

func TestNewStoreDefaultsToTempDir(t *testing.T) {
	s, err := NewStore("") // empty dir => OS temp dir
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.StartSegment()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("hello"); err != nil {
		t.Fatal(err)
	}
	seg, err := s.FinishSegment()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(s.OpenSegment(seg))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("segment content = %q", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The scratch file is unlinked at creation, so Close is idempotent and
	// leaves nothing behind.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}

func TestNewStoreFailsOnUnusableDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir")
	s, err := NewStore(missing)
	if err == nil {
		_ = s.Close()
		t.Fatal("NewStore on a missing dir succeeded, want error")
	}
	if !strings.HasPrefix(err.Error(), "spill:") {
		t.Fatalf("err = %q, want spill: prefix", err)
	}
}

func TestFinishSegmentWithoutOpenSegment(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.FinishSegment(); err == nil ||
		!strings.Contains(err.Error(), "no open segment") {
		t.Fatalf("FinishSegment = %v, want no-open-segment error", err)
	}
}

// TestStoreSegmentsInventory: Segments reports every sealed extent, in order,
// with contiguous offsets — that inventory is how a spilling operator finds its
// state again.
func TestStoreSegmentsInventory(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if got := s.Segments(); len(got) != 0 {
		t.Fatalf("fresh store has %d segments, want 0", len(got))
	}
	var want []Segment
	for _, payload := range []string{"aaa", "bbbbb", ""} {
		w, err := s.StartSegment()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteString(payload); err != nil {
			t.Fatal(err)
		}
		seg, err := s.FinishSegment()
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, seg)
	}
	if !reflect.DeepEqual(s.Segments(), want) {
		t.Fatalf("Segments() = %v, want %v", s.Segments(), want)
	}
	var off int64
	for i, seg := range s.Segments() {
		if seg.ID != i || seg.Off != off {
			t.Fatalf("segment %d = %+v, want ID %d at offset %d", i, seg, i, off)
		}
		off += seg.Len
	}
	if s.BytesWritten() != off {
		t.Fatalf("BytesWritten = %d, want %d", s.BytesWritten(), off)
	}
}
