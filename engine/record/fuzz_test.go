package record

import (
	"strings"
	"testing"
)

// FuzzParsePath compiles arbitrary path expressions. Paths reach here from
// flow documents and from connector config, so a malformed one must be an
// error at plan build — never a panic, and never a compiled path that then
// misbehaves per record.
func FuzzParsePath(f *testing.F) {
	f.Add("$")
	f.Add("$.a.b[0].c")
	f.Add("$.items[2][3]")
	f.Add(" $.a ") // ParsePath trims, so raw and parsed text differ

	f.Add("$.")                              // empty field name
	f.Add("$..a")                            // and again, mid-path
	f.Add("$[")                              // unterminated index
	f.Add("$[-1]")                           // negative index
	f.Add("$[99999999999999999999999]")      // index past int64
	f.Add("$[0")                             //
	f.Add("a.b")                             // no root
	f.Add("$\x00")                           // NUL after the root
	f.Add("$.\xff\xfe")                      // invalid UTF-8 field name
	f.Add("$" + strings.Repeat(".a", 2000))  // deep: steps must stay bounded by input
	f.Add("$" + strings.Repeat("[0]", 2000)) //
	f.Add("$." + strings.Repeat("a", 8000))  // one enormous field name
	f.Add("")

	f.Fuzz(func(t *testing.T, expr string) {
		if len(expr) > 16<<10 {
			return // bounded work per input
		}
		p, err := ParsePath(expr)
		if err != nil {
			return // rejecting is always a valid outcome
		}
		// Every step consumes at least one byte of the expression, so a parse
		// that produced more steps than bytes grew allocation out of nowhere.
		if len(p.steps) > len(expr) {
			t.Fatalf("path %q compiled to %d steps from %d bytes", expr, len(p.steps), len(expr))
		}
		if p.IsRoot() != (len(p.steps) == 0) {
			t.Fatalf("path %q: IsRoot=%v with %d steps", expr, p.IsRoot(), len(p.steps))
		}
		// LeafName is the default output name for a projection: it must name a
		// field, and only when the path actually ends in one.
		leaf := p.LeafName()
		endsInField := len(p.steps) > 0 && p.steps[len(p.steps)-1].index < 0
		if (leaf != "") != endsInField {
			t.Fatalf("path %q: LeafName=%q but endsInField=%v", expr, leaf, endsInField)
		}
		if endsInField && leaf != p.steps[len(p.steps)-1].key {
			t.Fatalf("path %q: LeafName=%q, last step key=%q", expr, leaf, p.steps[len(p.steps)-1].key)
		}
		if p.String() != expr {
			t.Fatalf("path %q renders as %q", expr, p.String())
		}
		MustParsePath(expr)        // must not panic once ParsePath has accepted
		_, _ = p.Get(sampleTree()) // evaluation of any compiled path is total
	})
}

// sampleTree is a small record with a map, a list, and a nested map, so an
// evaluated path can descend, index, and run off the end of each.
func sampleTree() Value {
	bld := NewBatch().Builder()
	bld.BeginMap()
	bld.KeyLiteral("a")
	bld.BeginList()
	bld.Int(1)
	bld.BeginMap()
	bld.KeyLiteral("b")
	bld.StringLiteral("x")
	bld.EndMap()
	bld.EndList()
	bld.EndMap()
	return bld.Finish()
}

// FuzzParseScalars feeds the exact-value parsers arbitrary bytes. These run on
// every typed cell of a CSV or fixed-width file — untrusted text straight from
// a partner — and the decimal one does its own overflow arithmetic rather than
// leaning on strconv, so a wrong answer is as much a failure here as a panic.
func FuzzParseScalars(f *testing.F) {
	f.Add([]byte("10.10"))
	f.Add([]byte("-0.000000000000000001"))
	f.Add([]byte("2026-08-01T12:00:00.123456789+10:00"))
	f.Add([]byte("2026-08-01"))
	f.Add([]byte("23:59:60.999999999"))

	f.Add([]byte("9223372036854775807"))  // MaxInt64 coefficient
	f.Add([]byte("-9223372036854775808")) // MinInt64: negation does not fit int64
	f.Add([]byte("9223372036854775808"))  // one past it
	f.Add([]byte(strings.Repeat("9", 400)))
	f.Add([]byte("1e127"))                 // scale at the int8 edge
	f.Add([]byte("1e-128"))                //
	f.Add([]byte("1e9223372036854775807")) // exponent arithmetic must not wrap into range
	f.Add([]byte("1.5e-9223372036854775808"))
	f.Add([]byte("1.2.3"))
	f.Add([]byte("+"))
	f.Add([]byte("."))
	f.Add([]byte("\x00\xff"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, cell []byte) {
		if len(cell) > 4<<10 {
			return // bounded work per input
		}
		_, _ = ParseTimestamp(cell)
		_, _ = ParseDate(cell)
		_, _ = ParseTimeOfDay(cell)

		v, err := ParseDecimal(cell)
		if err != nil {
			return
		}
		coef, scale := v.Decimal()
		if scale < 0 {
			// A negative scale renders as trailing zeros, and those digits can
			// legitimately exceed what the coefficient can hold on the way back
			// in ("1e-100"). Round-tripping is only claimed where the text is the
			// coefficient itself.
			return
		}
		back, err := ParseDecimal([]byte(v.DecimalText()))
		if err != nil {
			t.Fatalf("%q parsed to %d×10^-%d, whose text %q does not parse: %v", cell, coef, scale, v.DecimalText(), err)
		}
		if c, ok := Compare(v, back); !ok || c != 0 {
			t.Fatalf("%q round-tripped through %q to a different value (cmp=%d ok=%v)", cell, v.DecimalText(), c, ok)
		}
	})
}
