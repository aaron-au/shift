// Package schema is a compiled JSON Schema subset validator over record.Value
// (ADR-0042 §4c).
//
// Two properties distinguish it from importing a JSON Schema library, and both
// are the reason it exists:
//
//   - It evaluates record.Value directly. A library takes `any`, which means
//     materialising payload into map[string]interface{} on every request —
//     the exact thing the engine exists not to do (ADR-0004). Schemas compile
//     once at plan build; validation walks the record tree and allocates
//     nothing while a document is valid.
//   - It has a CLOSED keyword set and rejects anything outside it at compile
//     time. JSON Schema's own rule is that an unrecognised keyword is an
//     annotation and passes silently, which makes {"require": ["id"]} a schema
//     that validates nothing, forever, without complaint. That is worse than
//     having no schema: the 202 asserts a check that never ran.
//
// "Subset" describes which keywords are implemented, not how strictly they are
// enforced. For a supported keyword the semantics are the specification's, and
// that is verified against the official JSON-Schema-Test-Suite rather than
// asserted (see suite_test.go).
package schema

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// MaxViolations caps how many problems one validation reports. A document that
// is wrong in 40,000 places produces an error response nobody reads and a lot
// of allocation on a path that is about to return 400 anyway; the first few are
// what the caller acts on.
const MaxViolations = 50

// Violation is one failed assertion, addressed by JSON Pointer (RFC 6901).
type Violation struct {
	// Path is a JSON Pointer to the offending value ("" is the document root).
	Path string
	// Message says what was expected and what was found, in that order, so it
	// reads usefully when concatenated after the path.
	Message string
}

func (v Violation) String() string {
	if v.Path == "" {
		return v.Message
	}
	return v.Path + ": " + v.Message
}

// Schema is a compiled schema, safe for concurrent use.
type Schema struct {
	root *node
}

// Validate appends every violation of v to dst and returns it. A valid
// document appends nothing and allocates nothing.
//
// Passing a nil dst is fine; passing a reused slice avoids the allocation on
// the error path too.
func (s *Schema) Validate(v record.Value, dst []Violation) []Violation {
	e := evaluator{out: dst}
	e.eval(s.root, v)
	return e.out
}

// Valid reports whether v satisfies the schema, stopping at the first failure.
func (s *Schema) Valid(v record.Value) bool {
	e := evaluator{stopEarly: true}
	e.eval(s.root, v)
	return len(e.out) == 0
}

// --- evaluation ------------------------------------------------------------

type evaluator struct {
	out       []Violation
	stopEarly bool
	full      bool // MaxViolations reached

	// segs is the path to the value being evaluated, as a flat stack of
	// segments rather than a linked chain of frames.
	//
	// The chain is the obvious design and it costs 8 allocations on a small
	// VALID document: a frame holding a *frame parent, passed through mutually
	// recursive functions, defeats escape analysis and pushes every frame in
	// the walk onto the heap — for a JSON Pointer that is usually never built.
	// A flat array of value segments has no pointers to escape.
	//
	// The bound matches ndjson.DefaultMaxDepth, so a document the engine agreed
	// to parse can never be deeper than the path recorded for it.
	segs  [ndjson.DefaultMaxDepth]seg
	depth int
}

// seg is one JSON Pointer segment: either an object member (key) or an array
// position (index).
type seg struct {
	key   []byte // a view into the batch arena; never copied while walking
	index int
	isIdx bool
}

func (e *evaluator) push(key []byte) {
	if e.depth < len(e.segs) {
		e.segs[e.depth] = seg{key: key}
	}
	e.depth++
}

func (e *evaluator) pushIndex(i int) {
	if e.depth < len(e.segs) {
		e.segs[e.depth] = seg{index: i, isIdx: true}
	}
	e.depth++
}

func (e *evaluator) pop() { e.depth-- }

// pointer renders the RFC 6901 pointer for the current position, and is called
// ONLY when something has already failed.
func (e *evaluator) pointer() string {
	if e.depth == 0 {
		return ""
	}
	var dst []byte
	for i := 0; i < e.depth && i < len(e.segs); i++ {
		s := e.segs[i]
		dst = append(dst, '/')
		if s.isIdx {
			dst = strconv.AppendInt(dst, int64(s.index), 10)
			continue
		}
		if bytes.IndexByte(s.key, '~') < 0 && bytes.IndexByte(s.key, '/') < 0 {
			dst = append(dst, s.key...)
			continue
		}
		for _, c := range s.key {
			switch c {
			case '~':
				dst = append(dst, '~', '0')
			case '/':
				dst = append(dst, '~', '1')
			default:
				dst = append(dst, c)
			}
		}
	}
	return string(dst)
}

func (e *evaluator) fail(format string, args ...any) {
	if e.full {
		return
	}
	if len(e.out) >= MaxViolations {
		e.full = true
		return
	}
	e.out = append(e.out, Violation{Path: e.pointer(), Message: fmt.Sprintf(format, args...)})
}

func (e *evaluator) done() bool {
	return e.full || (e.stopEarly && len(e.out) > 0)
}

func (e *evaluator) eval(n *node, v record.Value) {
	if n == nil || e.done() {
		return
	}
	if n.refTarget != nil {
		e.eval(n.refTarget, v)
		if e.done() {
			return
		}
	}
	if n.types != 0 && !n.types.matches(v) {
		e.fail("expected %s, got %s", n.types, describe(v))
		// Every other assertion is type-specific, so continuing would produce
		// a second, confusing complaint about the same value.
		return
	}
	if n.konst != nil && !n.konst.equal(v) {
		e.fail("expected the constant %s, got %s", n.konst, describe(v))
	}
	if n.enum != nil && !n.enumHas(v) {
		e.fail("expected one of %s, got %s", n.enumList(), describe(v))
	}

	switch v.Kind() {
	case record.KindMap:
		e.evalObject(n, v)
	case record.KindList:
		e.evalArray(n, v)
	case record.KindString:
		e.evalString(n, v)
	case record.KindInt, record.KindFloat, record.KindDecimal:
		e.evalNumber(n, v)
	case record.KindNull, record.KindBool, record.KindBytes,
		record.KindTimestamp, record.KindDate, record.KindTime:
		// No further assertions apply to these kinds. The temporal ones cannot
		// occur in a document this package validates — input arrives as JSON or
		// YAML and parses to strings — so asserting string keywords against
		// them would invent a lexical form the schema never described.
	}
}

func (e *evaluator) evalObject(n *node, v record.Value) {
	for _, req := range n.required {
		if _, ok := v.Field(req); !ok {
			// The conversion allocates, and only ever on this failure path.
			e.push([]byte(req))
			e.fail("required property is missing")
			e.pop()
			if e.done() {
				return
			}
		}
	}
	if len(n.props) == 0 && !n.hasAddl {
		return
	}
	for i := range v.Len() {
		// Go elides the copy when a []byte conversion indexes a map.
		sub, known := n.props[string(v.KeyAt(i))]
		switch {
		case known:
			e.push(v.KeyAt(i))
			e.eval(sub, v.Index(i))
			e.pop()
		case n.hasAddl && !n.addlAllowed:
			e.push(v.KeyAt(i))
			e.fail("property is not allowed here")
			e.pop()
		}
		if e.done() {
			return
		}
	}
}

func (e *evaluator) evalArray(n *node, v record.Value) {
	if n.minItems != nil && v.Len() < *n.minItems {
		e.fail("expected at least %d items, got %d", *n.minItems, v.Len())
	}
	if n.maxItems != nil && v.Len() > *n.maxItems {
		e.fail("expected at most %d items, got %d", *n.maxItems, v.Len())
	}
	if n.items == nil {
		return
	}
	for i := range v.Len() {
		e.pushIndex(i)
		e.eval(n.items, v.Index(i))
		e.pop()
		if e.done() {
			return
		}
	}
}

func (e *evaluator) evalString(n *node, v record.Value) {
	if n.minLength != nil || n.maxLength != nil {
		// JSON Schema counts CHARACTERS (code points), not bytes. Counting
		// bytes would reject a legitimate 20-character name written in Greek.
		count := utf8Count(v.Bytes())
		if n.minLength != nil && count < *n.minLength {
			e.fail("expected at least %d characters, got %d", *n.minLength, count)
		}
		if n.maxLength != nil && count > *n.maxLength {
			e.fail("expected at most %d characters, got %d", *n.maxLength, count)
		}
	}
	if n.pattern != nil && !n.pattern.Match(v.Bytes()) {
		e.fail("expected a value matching %q", n.pattern.String())
	}
	if n.format != formatNone && !n.format.valid(v.Bytes()) {
		e.fail("expected a valid %s", n.format)
	}
}

func (e *evaluator) evalNumber(n *node, v record.Value) {
	if n.minimum == nil && n.maximum == nil {
		return
	}
	x := v.Float() // widens KindInt
	if n.minimum != nil && x < *n.minimum {
		e.fail("expected a value >= %s, got %s", num(*n.minimum), describe(v))
	}
	if n.maximum != nil && x > *n.maximum {
		e.fail("expected a value <= %s, got %s", num(*n.maximum), describe(v))
	}
}

func (n *node) enumHas(v record.Value) bool {
	for i := range n.enum {
		if n.enum[i].equal(v) {
			return true
		}
	}
	return false
}

func (n *node) enumList() string {
	parts := make([]string, len(n.enum))
	for i := range n.enum {
		parts[i] = n.enum[i].String()
	}
	return strings.Join(parts, ", ")
}

// utf8Count counts code points without decoding: every byte that is not a
// UTF-8 continuation byte starts one.
func utf8Count(b []byte) int {
	n := 0
	for _, c := range b {
		if c&0xC0 != 0x80 {
			n++
		}
	}
	return n
}

// describe renders a value for an error message. Strings are quoted and
// truncated: an error that echoes an unbounded field back to the caller is a
// payload-in-error-string leak, and errors are logged.
func describe(v record.Value) string {
	switch v.Kind() {
	case record.KindNull:
		return "null"
	case record.KindBool:
		return strconv.FormatBool(v.Bool())
	case record.KindInt:
		return strconv.FormatInt(v.Int(), 10)
	case record.KindFloat:
		return num(v.Float())
	case record.KindDecimal, record.KindTimestamp, record.KindDate, record.KindTime:
		return v.Text()
	case record.KindString:
		s := v.String()
		if len(s) > 40 {
			return strconv.Quote(s[:40]) + "…"
		}
		return strconv.Quote(s)
	case record.KindBytes:
		return "binary data"
	case record.KindList:
		return "an array"
	case record.KindMap:
		return "an object"
	default:
		return "an unknown value"
	}
}

func num(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// --- compiled node ---------------------------------------------------------

type node struct {
	types typeSet

	// refTarget is a resolved $ref, evaluated IN ADDITION to this node's own
	// assertions (2020-12 semantics).
	refTarget *node

	// object
	required    []string
	props       map[string]*node
	hasAddl     bool
	addlAllowed bool

	// array
	items    *node
	minItems *int
	maxItems *int

	// string
	minLength *int
	maxLength *int
	pattern   *regexp.Regexp
	format    formatKind

	// any
	enum  []constVal
	konst *constVal

	// number
	minimum *float64
	maximum *float64
}

// typeSet is the set of JSON types a value may take. Zero means "any".
type typeSet uint8

const (
	typeNull typeSet = 1 << iota
	typeBool
	typeObject
	typeArray
	typeNumber
	typeInteger
	typeString
)

func (t typeSet) matches(v record.Value) bool {
	switch v.Kind() {
	case record.KindNull:
		return t&typeNull != 0
	case record.KindBool:
		return t&typeBool != 0
	case record.KindMap:
		return t&typeObject != 0
	case record.KindList:
		return t&typeArray != 0
	case record.KindString, record.KindBytes:
		return t&typeString != 0
	case record.KindInt:
		return t&(typeNumber|typeInteger) != 0
	case record.KindFloat:
		if t&typeNumber != 0 {
			return true
		}
		// A number with a zero fractional part IS an integer per the spec, so
		// 1.0 satisfies "integer". Rejecting it would fail every caller whose
		// JSON encoder writes trailing zeros.
		return t&typeInteger != 0 && v.Float() == math.Trunc(v.Float())
	case record.KindDecimal:
		if t&typeNumber != 0 {
			return true
		}
		// Same rule as the float case, decided exactly: 10.00 is an integer,
		// and asking float64 would be a needlessly lossy way to find out.
		return t&typeInteger != 0 && decimalIsIntegral(v)
	default:
		return false
	}
}

// decimalIsIntegral reports whether a decimal has no fractional part, without
// going through float64.
func decimalIsIntegral(v record.Value) bool {
	coef, scale := v.Decimal()
	if scale <= 0 {
		return true // a non-positive scale only ever multiplies
	}
	for ; scale > 0; scale-- {
		if coef%10 != 0 {
			return false
		}
		coef /= 10
	}
	return true
}

func (t typeSet) String() string {
	if t == 0 {
		return "any value"
	}
	names := []struct {
		bit  typeSet
		name string
	}{
		{typeNull, "null"}, {typeBool, "boolean"}, {typeObject, "an object"},
		{typeArray, "an array"}, {typeNumber, "a number"}, {typeInteger, "an integer"},
		{typeString, "a string"},
	}
	var got []string
	for _, n := range names {
		if t&n.bit != 0 {
			got = append(got, n.name)
		}
	}
	return strings.Join(got, " or ")
}

var errCompile = errors.New("schema")
