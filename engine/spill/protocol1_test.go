package spill

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// TestProtocol1RefusesTheExactKindsWithAUsableMessage: this codec is the
// connector wire framing as well as the spill format, and a connector artifact
// published before ADR-0051 has a decoder that would meet an unknown tag. The
// host refuses first, and the error has to say what to do about it.
func TestProtocol1RefusesTheExactKindsWithAUsableMessage(t *testing.T) {
	newKinds := []record.Value{
		record.Decimal(1010, 2),
		record.TimestampAt(time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)),
		record.Date(0),
		record.TimeOfDay(0),
	}
	for _, v := range newKinds {
		var buf bytes.Buffer
		err := NewEncoderProtocol1(&buf).Encode(v)
		if err == nil {
			t.Errorf("protocol 1 encoder accepted a %v", v.Kind())
			continue
		}
		msg := err.Error()
		for _, want := range []string{v.Kind().String(), "protocol 1", "rebuild"} {
			if !strings.Contains(msg, want) {
				t.Errorf("%v error %q does not mention %q", v.Kind(), msg, want)
			}
		}
		if buf.Len() != 0 {
			t.Errorf("%v: refused but wrote %d bytes", v.Kind(), buf.Len())
		}
	}
}

// TestProtocol1StillCarriesEverythingItAlwaysDid — the restriction must be
// exactly the four new kinds, not a broader regression.
func TestProtocol1StillCarriesEverythingItAlwaysDid(t *testing.T) {
	src := record.NewBatch()
	bld := src.Builder()
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
	bld.EndList()
	bld.EndMap()
	v := bld.Finish()

	var strict, normal bytes.Buffer
	if err := NewEncoderProtocol1(&strict).Encode(v); err != nil {
		t.Fatalf("protocol 1 refused a value it has always carried: %v", err)
	}
	if err := NewEncoder(&normal).Encode(v); err != nil {
		t.Fatal(err)
	}
	// Byte-identical: the tags for the pre-existing kinds are untouched, which
	// is what makes appending safe in the first place.
	if !bytes.Equal(strict.Bytes(), normal.Bytes()) {
		t.Error("protocol 1 and current encodings differ for pre-ADR-0051 kinds")
	}
}

// TestANestedExactKindIsAlsoRefused: the guard has to hold inside containers,
// not only at the top level.
func TestANestedExactKindIsAlsoRefused(t *testing.T) {
	src := record.NewBatch()
	bld := src.Builder()
	bld.BeginMap()
	bld.KeyLiteral("amount")
	bld.Decimal(1010, 2)
	bld.EndMap()

	var buf bytes.Buffer
	if err := NewEncoderProtocol1(&buf).Encode(bld.Finish()); err == nil {
		t.Error("a decimal nested in a map was accepted over protocol 1")
	}
}
