package spill

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

func sampleValue(b *record.Batch) record.Value {
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("id")
	bld.Int(-42)
	bld.KeyLiteral("name")
	bld.StringLiteral("ada")
	bld.KeyLiteral("f")
	bld.Float(2.75)
	bld.KeyLiteral("ok")
	bld.Bool(true)
	bld.KeyLiteral("nil")
	bld.Null()
	bld.KeyLiteral("blob")
	bld.Bytes([]byte{0, 1, 2, 255})
	bld.KeyLiteral("list")
	bld.BeginList()
	bld.Int(1)
	bld.StringLiteral("two")
	bld.BeginMap()
	bld.KeyLiteral("three")
	bld.Int(3)
	bld.EndMap()
	bld.EndList()
	bld.EndMap()
	return bld.Finish()
}

func valueEqualJSONish(t *testing.T, a, b record.Value) bool {
	t.Helper()
	if a.Kind() != b.Kind() || a.Len() != b.Len() {
		return false
	}
	switch a.Kind() {
	case record.KindList:
		for i := range a.Len() {
			if !valueEqualJSONish(t, a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case record.KindMap:
		for i := range a.Len() {
			if string(a.KeyAt(i)) != string(b.KeyAt(i)) {
				return false
			}
			if !valueEqualJSONish(t, a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	default:
		return a.EqualScalar(b)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	src := record.NewBatch()
	v := sampleValue(src)

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	for range 3 {
		if err := enc.Encode(v); err != nil {
			t.Fatal(err)
		}
	}

	dst := record.NewBatch()
	dec := NewDecoder(bufio.NewReader(&buf), 0)
	for range 3 {
		if err := dec.Decode(dst.Builder()); err != nil {
			t.Fatal(err)
		}
		got := dst.Builder().Finish()
		if !valueEqualJSONish(t, v, got) {
			t.Fatal("round trip mismatch")
		}
	}
	if err := dec.Decode(dst.Builder()); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF at boundary, got %v", err)
	}
}

func TestDecoderCorruption(t *testing.T) {
	// Truncated mid-value must be ErrUnexpectedEOF, not EOF.
	src := record.NewBatch()
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(sampleValue(src)); err != nil {
		t.Fatal(err)
	}
	trunc := buf.Bytes()[:buf.Len()-3]
	dec := NewDecoder(bufio.NewReader(bytes.NewReader(trunc)), 0)
	err := dec.Decode(record.NewBatch().Builder())
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}

	// Unknown tag.
	dec = NewDecoder(bufio.NewReader(strings.NewReader("\xF0")), 0)
	if err := dec.Decode(record.NewBatch().Builder()); err == nil {
		t.Fatal("want unknown tag error")
	}

	// Oversized blob guard.
	var big bytes.Buffer
	big.WriteByte(tagString)
	big.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}) // huge uvarint
	dec = NewDecoder(bufio.NewReader(&big), 1024)
	if err := dec.Decode(record.NewBatch().Builder()); err == nil {
		t.Fatal("want blob limit error")
	}
}

func TestStoreSegments(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var segs []Segment
	for i := range 3 {
		w, err := s.StartSegment()
		if err != nil {
			t.Fatal(err)
		}
		for range 100 {
			if _, err := w.WriteString(strings.Repeat(string(rune('a'+i)), 10)); err != nil {
				t.Fatal(err)
			}
		}
		seg, err := s.FinishSegment()
		if err != nil {
			t.Fatal(err)
		}
		if seg.Len != 1000 {
			t.Fatalf("segment len = %d", seg.Len)
		}
		segs = append(segs, seg)
	}
	if s.BytesWritten() != 3000 {
		t.Fatalf("bytes written = %d", s.BytesWritten())
	}
	// Read back out of order.
	for i := 2; i >= 0; i-- {
		data, err := io.ReadAll(s.OpenSegment(segs[i]))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 1000 || data[0] != byte('a'+i) || data[999] != byte('a'+i) {
			t.Fatalf("segment %d content wrong", i)
		}
	}
	// Double-open guard.
	if _, err := s.StartSegment(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartSegment(); err == nil {
		t.Fatal("expected already-open error")
	}
}
