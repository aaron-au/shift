// Package ndjson streams newline-delimited JSON to and from record batches.
// The parser is hand-rolled to build values directly into batch arenas:
// unescaped strings are copied exactly once (input buffer → arena) and the
// steady state allocates only for float parsing.
package ndjson

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/aaron-au/shift/engine/record"
)

// Reader options.
const (
	// DefaultMaxLineBytes bounds a single NDJSON line (16 MiB).
	DefaultMaxLineBytes = 16 << 20
	// DefaultBatchRecords is the target records per batch.
	DefaultBatchRecords = 1024
	// DefaultBatchBytes is the target arena payload bytes per batch (1 MiB).
	DefaultBatchBytes = 1 << 20
	// DefaultMaxDepth bounds JSON nesting to keep adversarial input from
	// exhausting the stack.
	DefaultMaxDepth = 64
)

// ReaderOptions configure a Reader. Zero fields take the defaults above.
type ReaderOptions struct {
	MaxLineBytes int
	BatchRecords int
	BatchBytes   int64
	MaxDepth     int
}

func (o *ReaderOptions) defaults() {
	if o.MaxLineBytes <= 0 {
		o.MaxLineBytes = DefaultMaxLineBytes
	}
	if o.BatchRecords <= 0 {
		o.BatchRecords = DefaultBatchRecords
	}
	if o.BatchBytes <= 0 {
		o.BatchBytes = DefaultBatchBytes
	}
	if o.MaxDepth <= 0 {
		o.MaxDepth = DefaultMaxDepth
	}
}

// Reader streams NDJSON into record batches. It implements stream.Source:
// the batch returned by Next is valid only until the next Next or Close
// call (the reader reuses it).
type Reader struct {
	sc    *bufio.Scanner
	opts  ReaderOptions
	batch *record.Batch
	p     parser
	line  int64
	done  bool
}

// NewReader wraps r. The reader owns its batch; see Reader for the lifetime
// contract.
func NewReader(r io.Reader, opts ReaderOptions) *Reader {
	opts.defaults()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), opts.MaxLineBytes)
	return &Reader{sc: sc, opts: opts, batch: record.NewBatch()}
}

// Next returns the next batch of records, or io.EOF when the input is
// exhausted.
func (r *Reader) Next(ctx context.Context) (*record.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.done {
		return nil, io.EOF
	}
	r.batch.Reset()
	for r.batch.Len() < r.opts.BatchRecords && r.batch.ArenaBytes() < r.opts.BatchBytes {
		if !r.sc.Scan() {
			if err := r.sc.Err(); err != nil {
				return nil, fmt.Errorf("ndjson: line %d: %w", r.line+1, err)
			}
			r.done = true
			break
		}
		r.line++
		lineBytes := r.sc.Bytes()
		if isBlank(lineBytes) {
			continue
		}
		v, err := r.p.parseLine(lineBytes, r.batch.Builder(), r.opts.MaxDepth)
		if err != nil {
			return nil, fmt.Errorf("ndjson: line %d: %w", r.line, err)
		}
		r.batch.Append(v)
	}
	if r.batch.Len() == 0 {
		return nil, io.EOF
	}
	return r.batch, nil
}

// Close releases the reader. It does not close the underlying io.Reader.
func (r *Reader) Close() error {
	r.done = true
	return nil
}

func isBlank(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' && c != '\r' {
			return false
		}
	}
	return true
}

// parser is a within-line recursive-descent JSON parser. scratch is reused
// for escape decoding.
type parser struct {
	b       []byte
	i       int
	scratch []byte
}

var errUnexpectedEnd = errors.New("unexpected end of input")

func (p *parser) parseLine(b []byte, bld *record.Builder, maxDepth int) (record.Value, error) {
	p.b, p.i = b, 0
	if err := p.value(bld, maxDepth); err != nil {
		return record.Value{}, err
	}
	p.ws()
	if p.i != len(p.b) {
		return record.Value{}, fmt.Errorf("column %d: trailing data after JSON value", p.i+1)
	}
	return bld.Finish(), nil
}

func (p *parser) ws() {
	for p.i < len(p.b) {
		switch p.b[p.i] {
		case ' ', '\t', '\r', '\n':
			p.i++
		default:
			return
		}
	}
}

func (p *parser) value(bld *record.Builder, depth int) error {
	if depth <= 0 {
		return errors.New("maximum nesting depth exceeded")
	}
	p.ws()
	if p.i >= len(p.b) {
		return errUnexpectedEnd
	}
	switch c := p.b[p.i]; {
	case c == '{':
		return p.object(bld, depth)
	case c == '[':
		return p.array(bld, depth)
	case c == '"':
		s, err := p.string()
		if err != nil {
			return err
		}
		bld.String(s)
		return nil
	case c == 't':
		if err := p.literal("true"); err != nil {
			return err
		}
		bld.Bool(true)
		return nil
	case c == 'f':
		if err := p.literal("false"); err != nil {
			return err
		}
		bld.Bool(false)
		return nil
	case c == 'n':
		if err := p.literal("null"); err != nil {
			return err
		}
		bld.Null()
		return nil
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number(bld)
	default:
		return fmt.Errorf("column %d: unexpected character %q", p.i+1, c)
	}
}

func (p *parser) literal(lit string) error {
	if len(p.b)-p.i < len(lit) || string(p.b[p.i:p.i+len(lit)]) != lit {
		return fmt.Errorf("column %d: invalid literal", p.i+1)
	}
	p.i += len(lit)
	return nil
}

func (p *parser) object(bld *record.Builder, depth int) error {
	p.i++ // '{'
	bld.BeginMap()
	p.ws()
	if p.i < len(p.b) && p.b[p.i] == '}' {
		p.i++
		bld.EndMap()
		return nil
	}
	for {
		p.ws()
		if p.i >= len(p.b) || p.b[p.i] != '"' {
			return fmt.Errorf("column %d: expected object key", p.i+1)
		}
		k, err := p.string()
		if err != nil {
			return err
		}
		bld.Key(k)
		p.ws()
		if p.i >= len(p.b) || p.b[p.i] != ':' {
			return fmt.Errorf("column %d: expected ':'", p.i+1)
		}
		p.i++
		if err := p.value(bld, depth-1); err != nil {
			return err
		}
		p.ws()
		if p.i >= len(p.b) {
			return errUnexpectedEnd
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case '}':
			p.i++
			bld.EndMap()
			return nil
		default:
			return fmt.Errorf("column %d: expected ',' or '}'", p.i+1)
		}
	}
}

func (p *parser) array(bld *record.Builder, depth int) error {
	p.i++ // '['
	bld.BeginList()
	p.ws()
	if p.i < len(p.b) && p.b[p.i] == ']' {
		p.i++
		bld.EndList()
		return nil
	}
	for {
		if err := p.value(bld, depth-1); err != nil {
			return err
		}
		p.ws()
		if p.i >= len(p.b) {
			return errUnexpectedEnd
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case ']':
			p.i++
			bld.EndList()
			return nil
		default:
			return fmt.Errorf("column %d: expected ',' or ']'", p.i+1)
		}
	}
}

// string parses a JSON string and returns its decoded bytes. The returned
// slice is either a view into the input line (fast path, no escapes) or the
// parser's scratch buffer — callers must copy it before the next parser
// call (Builder.String/Key do).
func (p *parser) string() ([]byte, error) {
	p.i++ // '"'
	start := p.i
	// Fast path: scan for closing quote with no escapes or control chars.
	for p.i < len(p.b) {
		c := p.b[p.i]
		if c == '"' {
			s := p.b[start:p.i]
			p.i++
			return s, nil
		}
		if c == '\\' || c < 0x20 {
			break
		}
		p.i++
	}
	if p.i >= len(p.b) {
		return nil, errUnexpectedEnd
	}
	if p.b[p.i] < 0x20 {
		return nil, fmt.Errorf("column %d: raw control character in string", p.i+1)
	}
	// Slow path: decode escapes into scratch.
	p.scratch = append(p.scratch[:0], p.b[start:p.i]...)
	for p.i < len(p.b) {
		c := p.b[p.i]
		switch {
		case c == '"':
			p.i++
			return p.scratch, nil
		case c == '\\':
			p.i++
			if p.i >= len(p.b) {
				return nil, errUnexpectedEnd
			}
			esc := p.b[p.i]
			p.i++
			switch esc {
			case '"', '\\', '/':
				p.scratch = append(p.scratch, esc)
			case 'b':
				p.scratch = append(p.scratch, '\b')
			case 'f':
				p.scratch = append(p.scratch, '\f')
			case 'n':
				p.scratch = append(p.scratch, '\n')
			case 'r':
				p.scratch = append(p.scratch, '\r')
			case 't':
				p.scratch = append(p.scratch, '\t')
			case 'u':
				r, err := p.hex4()
				if err != nil {
					return nil, err
				}
				if utf16.IsSurrogate(r) {
					if p.i+1 < len(p.b) && p.b[p.i] == '\\' && p.b[p.i+1] == 'u' {
						p.i += 2
						r2, err := p.hex4()
						if err != nil {
							return nil, err
						}
						if dec := utf16.DecodeRune(r, r2); dec != utf8.RuneError {
							r = dec
						} else {
							r = utf8.RuneError
						}
					} else {
						r = utf8.RuneError
					}
				}
				p.scratch = utf8.AppendRune(p.scratch, r)
			default:
				return nil, fmt.Errorf("column %d: invalid escape \\%c", p.i, esc)
			}
		case c < 0x20:
			return nil, fmt.Errorf("column %d: raw control character in string", p.i+1)
		default:
			p.scratch = append(p.scratch, c)
			p.i++
		}
	}
	return nil, errUnexpectedEnd
}

func (p *parser) hex4() (rune, error) {
	if p.i+4 > len(p.b) {
		return 0, errUnexpectedEnd
	}
	var r rune
	for _, c := range p.b[p.i : p.i+4] {
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			r |= rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			r |= rune(c-'A') + 10
		default:
			return 0, fmt.Errorf("column %d: invalid \\u escape", p.i+1)
		}
	}
	p.i += 4
	return r, nil
}

func (p *parser) number(bld *record.Builder) error {
	start := p.i
	if p.b[p.i] == '-' {
		p.i++
	}
	isFloat := false
	for p.i < len(p.b) {
		c := p.b[p.i]
		if c >= '0' && c <= '9' {
			p.i++
			continue
		}
		if c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-' {
			isFloat = true
			p.i++
			continue
		}
		break
	}
	tok := p.b[start:p.i]
	if !validNumber(tok) {
		return fmt.Errorf("column %d: invalid number %q", start+1, tok)
	}
	if !isFloat {
		if n, ok := parseInt(tok); ok {
			bld.Int(n)
			return nil
		}
		// Overflows int64: fall through to float like encoding/json.
	}
	f, err := strconv.ParseFloat(string(tok), 64)
	if err != nil {
		return fmt.Errorf("column %d: invalid number %q", start+1, tok)
	}
	bld.Float(f)
	return nil
}

// validNumber enforces the JSON number grammar, which is stricter than
// strconv.ParseFloat (JSON forbids "1.", "01", ".5", "1e").
func validNumber(tok []byte) bool {
	i := 0
	if i < len(tok) && tok[i] == '-' {
		i++
	}
	// integer part: "0" or [1-9][0-9]*
	switch {
	case i < len(tok) && tok[i] == '0':
		i++
	case i < len(tok) && tok[i] >= '1' && tok[i] <= '9':
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			i++
		}
	default:
		return false
	}
	// fraction
	if i < len(tok) && tok[i] == '.' {
		i++
		if i >= len(tok) || tok[i] < '0' || tok[i] > '9' {
			return false
		}
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			i++
		}
	}
	// exponent
	if i < len(tok) && (tok[i] == 'e' || tok[i] == 'E') {
		i++
		if i < len(tok) && (tok[i] == '+' || tok[i] == '-') {
			i++
		}
		if i >= len(tok) || tok[i] < '0' || tok[i] > '9' {
			return false
		}
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			i++
		}
	}
	return i == len(tok)
}

// parseInt parses a JSON integer token without allocating. ok is false on
// overflow or malformed input.
func parseInt(tok []byte) (int64, bool) {
	neg := false
	i := 0
	if tok[0] == '-' {
		neg = true
		i = 1
		if len(tok) == 1 {
			return 0, false
		}
	}
	// Reject leading zeros per JSON grammar (e.g. "01"), allow "0".
	if tok[i] == '0' && i+1 < len(tok) {
		return 0, false
	}
	var n uint64
	for ; i < len(tok); i++ {
		c := tok[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if n > (1<<63-1)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	if neg {
		if n > 1<<63 {
			return 0, false
		}
		return -int64(n), true
	}
	if n > 1<<63-1 {
		return 0, false
	}
	return int64(n), true
}
