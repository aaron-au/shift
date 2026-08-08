// Package edi streams an EDI interchange into record batches, one record per
// SEGMENT, without buffering the interchange.
//
// # Scope: structure, not semantics
//
// This reads the two wire syntaxes — ANSI X12 and UN/EDIFACT — and stops
// there. It does not know what an 850 is, does not validate a transaction set
// against an implementation guide, and does not group segments into their
// envelope hierarchy. Those are enormous, version-and-trading-partner-specific
// domains, and building them into the reader would make every flow depend on a
// table of standards nobody can keep current.
//
// What it gives a flow instead is the segment stream, typed and addressable:
//
//	{"tag":"ISA","elements":[["00"],["          "],…]}
//	{"tag":"BEG","elements":[["00"],["SA"],["3639829"],[],["20260801"]]}
//
// Every element is a LIST of components, always — even when there is one, and
// even when it is empty. A field that is sometimes a string and sometimes a
// list is the shape that breaks every downstream mapping the first time a
// trading partner sends a composite, so the cost is paid once, here, in
// exchange for `$.elements[3][0]` meaning the same thing in every message.
//
// Grouping into interchange/group/transaction and mapping to a business
// document belong in the flow — a router on `$.tag` is enough to start — or in
// a later semantic layer built on top of this one.
//
// # Separators are read from the data, never configured
//
// Both syntaxes carry their own delimiters, which is what lets one reader
// handle files from partners who disagree about them:
//
//   - X12 declares them positionally in the fixed-width ISA header: the
//     element separator is the 4th byte, the component separator is ISA16, and
//     the byte after that terminates the segment.
//   - EDIFACT declares them in an optional UNA header, and otherwise uses the
//     defaults from ISO 9735.
//
// A configured separator would be a fourth source of truth that disagrees with
// the file whenever a partner changes theirs.
package edi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// Reader defaults.
const (
	// DefaultBatchRecords is the target segments per batch.
	DefaultBatchRecords = 1024
	// DefaultMaxSegmentBytes bounds one segment. An interchange with no
	// terminator in sight is either corrupt or hostile, and reading it to EOF
	// would buffer the whole file — the one thing this package exists not to
	// do.
	DefaultMaxSegmentBytes = 1 << 20
)

// Syntax identifies which EDI wire format an interchange uses.
type Syntax string

// The supported syntaxes.
const (
	X12     Syntax = "x12"
	EDIFACT Syntax = "edifact"
)

// separators are the delimiters in force for an interchange.
type separators struct {
	element   byte
	component byte
	segment   byte
	release   byte // EDIFACT escape; 0 when the syntax has none
	repeat    byte // X12 repetition separator (ISA11 in 5010); 0 when unused
}

// ReaderOptions configure a Reader. Zero fields take the defaults above.
type ReaderOptions struct {
	BatchRecords    int
	MaxSegmentBytes int
}

func (o *ReaderOptions) defaults() {
	if o.BatchRecords <= 0 {
		o.BatchRecords = DefaultBatchRecords
	}
	if o.MaxSegmentBytes <= 0 {
		o.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
}

// Reader streams EDI segments into record batches. It implements stream.Source:
// the batch returned by Next is valid only until the next Next or Close call.
type Reader struct {
	br      *bufio.Reader
	opts    ReaderOptions
	sep     separators
	syntax  Syntax
	batch   *record.Batch
	seg     []byte // reused segment scratch
	sniffed bool
	done    bool
}

// NewReader wraps r. The syntax and its separators are detected from the first
// bytes of the interchange on the first Next; it does not close r.
func NewReader(r io.Reader, opts ReaderOptions) *Reader {
	opts.defaults()
	return &Reader{
		br:    bufio.NewReaderSize(r, 64<<10),
		opts:  opts,
		batch: record.NewBatch(),
		seg:   make([]byte, 0, 256),
	}
}

// Syntax reports the detected syntax, valid after the first Next.
func (r *Reader) Syntax() Syntax { return r.syntax }

// Next returns the next batch of segments, or io.EOF when the interchange is
// exhausted.
func (r *Reader) Next(ctx context.Context) (*record.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.done {
		return nil, io.EOF
	}
	if !r.sniffed {
		if err := r.sniff(); err != nil {
			return nil, err
		}
	}
	r.batch.Reset()
	for r.batch.Len() < r.opts.BatchRecords {
		seg, err := r.readSegment()
		if errors.Is(err, io.EOF) {
			r.done = true
			break
		}
		if err != nil {
			return nil, err
		}
		if len(seg) == 0 {
			continue // trailing separator, or whitespace between segments
		}
		r.appendSegment(seg)
	}
	if r.batch.Len() == 0 {
		return nil, io.EOF
	}
	return r.batch, nil
}

// Close releases the batch.
func (r *Reader) Close() error {
	r.batch = nil
	r.done = true
	return nil
}

// sniff detects the syntax and its separators from the interchange header.
func (r *Reader) sniff() error {
	r.sniffed = true

	// Skip leading whitespace: files arrive with a BOM-ish prologue or a
	// stray newline more often than not.
	for {
		b, err := r.br.ReadByte()
		if err != nil {
			return fmt.Errorf("edi: reading the interchange header: %w", err)
		}
		if b != ' ' && b != '\r' && b != '\n' && b != '\t' {
			if err := r.br.UnreadByte(); err != nil {
				return err
			}
			break
		}
	}

	head, err := r.br.Peek(3)
	if err != nil {
		return fmt.Errorf("edi: reading the interchange header: %w", err)
	}
	switch string(head) {
	case "ISA":
		return r.sniffX12()
	case "UNA", "UNB":
		return r.sniffEDIFACT(string(head) == "UNA")
	default:
		return fmt.Errorf("edi: not an interchange: expected it to open with ISA (X12) or UNA/UNB (EDIFACT), got %q", head)
	}
}

// isaLen is the fixed width of an X12 ISA segment including its terminator.
// The segment is fixed-width BY DESIGN so that a reader can locate the
// delimiters before it knows any of them.
const isaLen = 106

func (r *Reader) sniffX12() error {
	head, err := r.br.Peek(isaLen)
	if err != nil {
		return fmt.Errorf("edi: the ISA header is truncated (need %d bytes): %w", isaLen, err)
	}
	r.syntax = X12
	r.sep = separators{
		element:   head[3],
		component: head[104],
		segment:   head[105],
		repeat:    head[82], // ISA11; 'U' in pre-5010 means "unused"
	}
	if r.sep.repeat == 'U' || r.sep.repeat == r.sep.element {
		r.sep.repeat = 0
	}
	if r.sep.element == 0 || r.sep.segment == 0 {
		return errors.New("edi: the ISA header declares an empty delimiter")
	}
	return nil
}

// EDIFACT defaults from ISO 9735, used when no UNA header is present.
const (
	edifactComponent = ':'
	edifactElement   = '+'
	edifactRelease   = '?'
	edifactSegment   = '\''
)

func (r *Reader) sniffEDIFACT(hasUNA bool) error {
	r.syntax = EDIFACT
	r.sep = separators{
		component: edifactComponent,
		element:   edifactElement,
		release:   edifactRelease,
		segment:   edifactSegment,
	}
	if !hasUNA {
		return nil
	}
	// UNA:+.? ' — six delimiter bytes after the tag, positionally defined.
	head, err := r.br.Peek(9)
	if err != nil {
		return fmt.Errorf("edi: the UNA header is truncated: %w", err)
	}
	r.sep.component = head[3]
	r.sep.element = head[4]
	// head[5] is the decimal notation and head[7] is reserved; neither
	// participates in tokenising.
	r.sep.release = head[6]
	r.sep.segment = head[8]
	if _, err := r.br.Discard(9); err != nil {
		return err
	}
	return nil
}

// readSegment reads up to and including the next segment terminator, returning
// the segment without it.
func (r *Reader) readSegment() ([]byte, error) {
	r.seg = r.seg[:0]
	for {
		b, err := r.br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(r.seg) > 0 {
				// A final segment with no terminator: accept it rather than
				// discard data that is otherwise complete.
				return r.trim(r.seg), nil
			}
			return nil, err
		}
		// An EDIFACT release character escapes the NEXT byte, including a
		// terminator. Consuming both here is what stops "?'" ending a segment.
		if r.sep.release != 0 && b == r.sep.release {
			nb, err := r.br.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("edi: the interchange ends with a dangling release character")
			}
			r.seg = append(r.seg, b, nb)
			continue
		}
		if b == r.sep.segment {
			return r.trim(r.seg), nil
		}
		r.seg = append(r.seg, b)
		if len(r.seg) > r.opts.MaxSegmentBytes {
			return nil, fmt.Errorf("edi: a segment exceeded %d bytes with no terminator; the file is corrupt or uses a different delimiter", r.opts.MaxSegmentBytes)
		}
	}
}

// trim removes the line breaks many partners insert after each segment for
// readability. They are not data, and leaving them would prefix every tag.
func (r *Reader) trim(b []byte) []byte {
	for len(b) > 0 && (b[0] == '\r' || b[0] == '\n') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == '\r' || b[len(b)-1] == '\n') {
		b = b[:len(b)-1]
	}
	return b
}

// appendSegment builds {tag, elements} for one segment into the batch.
func (r *Reader) appendSegment(seg []byte) {
	bld := r.batch.Builder()
	bld.BeginMap()

	elems := r.split(seg, r.sep.element)

	bld.KeyLiteral("tag")
	if len(elems) > 0 {
		bld.String(elems[0])
	} else {
		bld.StringLiteral("")
	}

	bld.KeyLiteral("elements")
	bld.BeginList()
	for _, e := range elems[min(1, len(elems)):] {
		// Always a list, even for a single component: see the package comment.
		bld.BeginList()
		for _, c := range r.split(e, r.sep.component) {
			bld.String(r.unescape(c))
		}
		bld.EndList()
	}
	bld.EndList()

	bld.EndMap()
	r.batch.Append(bld.Finish())
}

// split divides b on sep, honouring the release character. Returns a nil slice
// for empty input so an empty element yields zero components rather than one
// empty string.
func (r *Reader) split(b []byte, sep byte) [][]byte {
	if len(b) == 0 {
		return nil
	}
	out := make([][]byte, 0, 8)
	start := 0
	for i := 0; i < len(b); i++ {
		if r.sep.release != 0 && b[i] == r.sep.release {
			i++ // skip the escaped byte
			continue
		}
		if b[i] == sep {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	return append(out, b[start:])
}

// unescape removes EDIFACT release characters, so a value reaches the flow as
// the text it represents rather than as its wire encoding.
func (r *Reader) unescape(b []byte) []byte {
	if r.sep.release == 0 {
		return b
	}
	idx := -1
	for i := range b {
		if b[i] == r.sep.release {
			idx = i
			break
		}
	}
	if idx < 0 {
		return b
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] == r.sep.release && i+1 < len(b) {
			i++
			out = append(out, b[i])
			continue
		}
		out = append(out, b[i])
	}
	return out
}
