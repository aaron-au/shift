package xmlf

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/aaron-au/shift/engine/record"
)

// Writer options / defaults.
const (
	// DefaultRootElement wraps the record elements so the output is a single
	// well-formed document rather than a bare sequence of siblings.
	DefaultRootElement = "records"
	// DefaultRecordElement names each record's element.
	DefaultRecordElement = "record"
	// DefaultMaxWriteDepth bounds nesting on the way out. A record deep enough
	// to exhaust the stack here came from somewhere — a join, a badly shaped
	// transform — and failing beats recursing until the process dies.
	DefaultMaxWriteDepth = 64
)

// Writer streams record batches out as XML, without buffering the document.
//
// It is the exact inverse of Reader's mapping, so a document that goes in
// comes back out with the same shape (see the package comment):
//
//   - a field keyed "@name" becomes an ATTRIBUTE of the enclosing element
//   - a field keyed "#text" becomes the element's character data
//   - a list becomes the key REPEATED once per element, which is how the
//     reader collapses repeats in the first place
//   - any other field becomes a child element named for the key
//
// What it cannot round-trip is namespace prefixes: the reader strips them on
// the way in (<ns:Order> and <Order> map identically), so by the time a record
// reaches here the prefix is gone and inventing one would be a guess. Local
// names are written bare.
//
// Implements stream.Sink.
type Writer struct {
	w        *bufio.Writer
	opts     WriterOptions
	wroteAny bool
	err      error
}

// WriterOptions configure a Writer. Zero fields take the defaults above.
type WriterOptions struct {
	// RootElement wraps every record. Set it empty to write a bare record
	// sequence — a FRAGMENT, not a document, which is what an appending sink
	// wants and what a standards-conformant parser will reject on its own.
	RootElement string
	// RecordElement names each record's element.
	RecordElement string
	// Indent, when non-empty, pretty-prints with this string per level. Costs
	// bytes on the wire; the reader ignores inter-element whitespace either
	// way, so this is for humans reading a file, never for correctness.
	Indent string
	// MaxDepth bounds nesting per record.
	MaxDepth int
	// OmitDeclaration suppresses the <?xml …?> prolog. Implied when
	// RootElement is empty, since a fragment has no prolog.
	OmitDeclaration bool
}

func (o *WriterOptions) defaults() {
	if o.RecordElement == "" {
		o.RecordElement = DefaultRecordElement
	}
	if o.MaxDepth <= 0 {
		o.MaxDepth = DefaultMaxWriteDepth
	}
}

// NewWriter wraps w. It does not close the underlying io.Writer; Close flushes
// and closes the document element.
func NewWriter(w io.Writer, opts WriterOptions) *Writer {
	// An empty RootElement here reads as "unset", not "deliberately none": a
	// string cannot carry that distinction, and the safe reading is the one
	// that produces a well-formed document. NewFragmentWriter is how a caller
	// asks for the other thing.
	if opts.RootElement == "" {
		opts.RootElement = DefaultRootElement
	}
	opts.defaults()
	return &Writer{w: bufio.NewWriter(w), opts: opts}
}

// NewFragmentWriter writes record elements with no wrapping root and no
// prolog, for appending to an existing document or concatenating shards.
func NewFragmentWriter(w io.Writer, opts WriterOptions) *Writer {
	opts.RootElement = ""
	opts.OmitDeclaration = true
	opts.defaults()
	return &Writer{w: bufio.NewWriter(w), opts: opts}
}

// Write encodes each record in b as one record element.
func (w *Writer) Write(ctx context.Context, b *record.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	for _, rec := range b.Records() {
		if err := w.prologue(); err != nil {
			return w.fail(err)
		}
		if err := w.element(w.opts.RecordElement, rec, 1); err != nil {
			return w.fail(err)
		}
	}
	return nil
}

// prologue emits the declaration and opening root element, once.
func (w *Writer) prologue() error {
	if w.wroteAny {
		return nil
	}
	w.wroteAny = true
	if w.opts.RootElement != "" && !w.opts.OmitDeclaration {
		if _, err := w.w.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`); err != nil {
			return err
		}
		if err := w.newline(0); err != nil {
			return err
		}
	}
	if w.opts.RootElement == "" {
		return nil
	}
	if err := w.open(w.opts.RootElement); err != nil {
		return err
	}
	return nil
}

// element writes one named element for v, recursing into containers.
func (w *Writer) element(name string, v record.Value, depth int) error {
	if depth > w.opts.MaxDepth {
		return fmt.Errorf("xmlf: record nests deeper than %d elements", w.opts.MaxDepth)
	}
	// A list is the SAME element repeated, not a wrapper element containing
	// items: that is precisely the shape the reader collapses into a list, so
	// anything else would fail to round-trip.
	if v.Kind() == record.KindList {
		for i := range v.Len() {
			if err := w.element(name, v.Index(i), depth); err != nil {
				return err
			}
		}
		return nil
	}

	if err := w.newline(depth); err != nil {
		return err
	}
	if v.Kind() != record.KindMap {
		return w.leaf(name, v)
	}

	// Split the map: attributes ride on the open tag, #text is character data,
	// everything else is a child element.
	if _, err := w.w.WriteString("<" + name); err != nil {
		return err
	}
	for i := range v.Len() {
		key := string(v.KeyAt(i))
		if len(key) < 2 || key[0] != '@' {
			continue
		}
		val, err := w.scalar(v.Index(i))
		if err != nil {
			return fmt.Errorf("xmlf: attribute %q: %w", key, err)
		}
		if _, err := w.w.WriteString(" " + key[1:] + `="`); err != nil {
			return err
		}
		if err := w.escape(val, true); err != nil {
			return err
		}
		if _, err := w.w.WriteString(`"`); err != nil {
			return err
		}
	}
	if _, err := w.w.WriteString(">"); err != nil {
		return err
	}

	var children int
	for i := range v.Len() {
		key := string(v.KeyAt(i))
		switch {
		case len(key) >= 2 && key[0] == '@':
			continue // already on the open tag
		case key == "#text":
			text, err := w.scalar(v.Index(i))
			if err != nil {
				return fmt.Errorf("xmlf: #text: %w", err)
			}
			if err := w.escape(text, false); err != nil {
				return err
			}
		default:
			children++
			if err := w.element(key, v.Index(i), depth+1); err != nil {
				return err
			}
		}
	}
	if children > 0 {
		if err := w.newline(depth); err != nil {
			return err
		}
	}
	_, err := w.w.WriteString("</" + name + ">")
	return err
}

// leaf writes a scalar element. An empty string closes as <name></name>
// rather than <name/>, because the reader maps both to "" and the long form
// is what a diff of input against output will match.
func (w *Writer) leaf(name string, v record.Value) error {
	s, err := w.scalar(v)
	if err != nil {
		return fmt.Errorf("xmlf: element %q: %w", name, err)
	}
	if _, err := w.w.WriteString("<" + name + ">"); err != nil {
		return err
	}
	if err := w.escape(s, false); err != nil {
		return err
	}
	_, err = w.w.WriteString("</" + name + ">")
	return err
}

// scalar renders a non-container value as text.
func (w *Writer) scalar(v record.Value) (string, error) {
	switch v.Kind() {
	case record.KindNull:
		return "", nil
	case record.KindBool:
		return strconv.FormatBool(v.Bool()), nil
	case record.KindInt:
		return strconv.FormatInt(v.Int(), 10), nil
	case record.KindFloat:
		f := v.Float()
		if math.IsInf(f, 0) || math.IsNaN(f) {
			// XML has no notation for either, and writing "NaN" would read
			// back as the string "NaN" rather than a number.
			return "", fmt.Errorf("value %v has no XML representation", f)
		}
		return strconv.FormatFloat(f, 'g', -1, 64), nil
	case record.KindString, record.KindBytes:
		return v.String(), nil
	default:
		return "", fmt.Errorf("value is %v, want a scalar", v.Kind())
	}
}

// escape writes s with XML metacharacters replaced.
//
// Hand-rolled rather than xml.EscapeText because that also escapes newlines
// and tabs as character references, which turns readable text output into an
// unreadable wall — and because it lets the attribute and text cases differ,
// which the standard helper does not.
func (w *Writer) escape(s string, attr bool) error {
	last := 0
	for i := range len(s) {
		var rep string
		switch s[i] {
		case '&':
			rep = "&amp;"
		case '<':
			rep = "&lt;"
		case '>':
			// Not strictly required in text, but "]]>" is, and escaping every
			// '>' is cheaper than tracking the sequence.
			rep = "&gt;"
		case '"':
			if !attr {
				continue
			}
			rep = "&quot;"
		case '\r':
			// A literal CR is normalised away by every conformant parser, so
			// round-tripping it requires the reference.
			rep = "&#xD;"
		case '\n', '\t':
			if !attr {
				continue // readable in text; only attributes need these escaped
			}
			if s[i] == '\n' {
				rep = "&#xA;"
			} else {
				rep = "&#x9;"
			}
		default:
			continue
		}
		if _, err := w.w.WriteString(s[last:i]); err != nil {
			return err
		}
		if _, err := w.w.WriteString(rep); err != nil {
			return err
		}
		last = i + 1
	}
	_, err := w.w.WriteString(s[last:])
	return err
}

func (w *Writer) open(name string) error {
	_, err := w.w.WriteString("<" + name + ">")
	return err
}

// newline emits the line break and indent for a level, or nothing when not
// pretty-printing.
func (w *Writer) newline(depth int) error {
	if w.opts.Indent == "" {
		return nil
	}
	if _, err := w.w.WriteString("\n"); err != nil {
		return err
	}
	for range depth {
		if _, err := w.w.WriteString(w.opts.Indent); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return err
}

// Close closes the root element and flushes.
//
// A writer that saw NO records still emits an empty document rather than an
// empty file: zero records is a legitimate result, and a consumer that parses
// the output should get "no records" rather than a parse error.
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	if err := w.prologue(); err != nil {
		return w.fail(err)
	}
	if w.opts.RootElement != "" {
		if err := w.newline(0); err != nil {
			return w.fail(err)
		}
		if _, err := w.w.WriteString("</" + w.opts.RootElement + ">"); err != nil {
			return w.fail(err)
		}
	}
	if w.opts.Indent != "" {
		if _, err := w.w.WriteString("\n"); err != nil {
			return w.fail(err)
		}
	}
	return w.w.Flush()
}
