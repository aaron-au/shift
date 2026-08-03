// Package xmlf streams an XML document into record batches without buffering
// the whole document. Parsing rides encoding/xml.Decoder's token stream: at
// most one record's element subtree (plus the current batch) is resident at a
// time, matching the streaming, disk-light doctrine — no whole-payload
// map[string]interface{} ever materialises.
//
// # Element → record mapping (prefix-agnostic)
//
// Each record element's subtree becomes one hierarchical record.Value using the
// same convention everywhere XML is mapped in SHIFT:
//
//   - Namespace prefixes are stripped: an element/attribute is keyed by its
//     local name (<ns:Order> and <Order> map identically).
//   - Attributes are fields keyed "@" + local-name (<row id="1"> → {"@id":"1"}).
//     xmlns / xmlns:* declarations are dropped, not mapped.
//   - Child elements are fields keyed by local name. A name that appears once is
//     a single value; a name that repeats collapses to a list (in document
//     order) — <r><t>a</t><t>b</t></r> → {"t":["a","b"]}.
//   - Character data is placed under "#text" when the element also carries
//     attributes or child elements (mixed content). A pure leaf element — no
//     attributes, no child elements — maps directly to its text as a string
//     (<name>ada</name> → "ada"; an empty element → ""). Whitespace-only text
//     between child elements (pretty-printing/indentation) is ignored.
//
// All string, key, and text bytes are copied into the batch arena via
// record.Builder; nothing is retained across batches.
package xmlf

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// Reader options / defaults (batch sizing rationale matches ndjson & csvf).
const (
	// DefaultBatchRecords is the target records per batch.
	DefaultBatchRecords = 1024
	// DefaultBatchBytes is the target arena payload bytes per batch (1 MiB).
	DefaultBatchBytes = 1 << 20
	// DefaultMaxDepth bounds element nesting inside one record, so adversarial
	// input cannot exhaust the stack or balloon a single record.
	DefaultMaxDepth = 64
)

// ReaderOptions configure a Reader. Zero fields take the defaults above.
type ReaderOptions struct {
	// RecordElement is the local name of the repeated element that delimits one
	// record (e.g. "row" or "Order"); each such subtree becomes one record,
	// matched prefix-agnostically at any depth. When empty, each direct child
	// element of the document's root element is one record.
	RecordElement string
	BatchRecords  int
	BatchBytes    int64
	MaxDepth      int
}

func (o *ReaderOptions) defaults() {
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

// Reader streams XML records into record batches. It implements stream.Source:
// the batch returned by Next is valid only until the next Next or Close call
// (the reader reuses it).
type Reader struct {
	dec     *xml.Decoder
	opts    ReaderOptions
	batch   *record.Batch
	depth   int    // count of currently-open ancestor elements at the token cursor
	attrKey []byte // reused "@name" scratch; each use copies into the arena before the next
	done    bool
}

// NewReader wraps r. The reader owns its batch; see Reader for the lifetime
// contract. It does not close the underlying io.Reader.
func NewReader(r io.Reader, opts ReaderOptions) *Reader {
	opts.defaults()
	dec := xml.NewDecoder(r)
	dec.Strict = true
	return &Reader{dec: dec, opts: opts, batch: record.NewBatch()}
}

// Next returns the next batch of records, or io.EOF when the document is
// exhausted.
func (r *Reader) Next(ctx context.Context) (*record.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.done {
		return nil, io.EOF
	}
	r.batch.Reset()
	bld := r.batch.Builder()
	for r.batch.Len() < r.opts.BatchRecords && r.batch.ArenaBytes() < r.opts.BatchBytes {
		toks, err := r.readRecordTokens()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.done = true
				break
			}
			return nil, err
		}
		if err := r.buildElement(toks, bld, 1); err != nil {
			return nil, err
		}
		r.batch.Append(bld.Finish())
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

// readRecordTokens advances the decoder to the start of the next record and
// returns that element's full subtree as a copied token slice (StartElement …
// matching EndElement). It returns io.EOF when no further record exists.
func (r *Reader) readRecordTokens() ([]xml.Token, error) {
	for {
		tok, err := r.dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("xmlf: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			isRecord := false
			if r.opts.RecordElement != "" {
				isRecord = t.Name.Local == r.opts.RecordElement
			} else {
				// Default: direct children of the root element. r.depth is the
				// number of open ancestors, so root opens at depth 0 and its
				// direct children at depth 1.
				isRecord = r.depth == 1
			}
			if isRecord {
				// collectSubtree consumes the whole balanced subtree, so r.depth
				// is unchanged afterwards (the element opened and closed).
				return r.collectSubtree(t)
			}
			r.depth++
		case xml.EndElement:
			r.depth--
		}
	}
}

// collectSubtree reads and copies the tokens of the element started by start,
// up to and including its matching EndElement.
func (r *Reader) collectSubtree(start xml.StartElement) ([]xml.Token, error) {
	toks := []xml.Token{xml.CopyToken(start)}
	sub := 1 // open elements within the subtree
	for sub > 0 {
		tok, err := r.dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF // truncated record
			}
			return nil, fmt.Errorf("xmlf: %w", err)
		}
		switch tok.(type) {
		case xml.StartElement:
			sub++
			if sub > r.opts.MaxDepth {
				return nil, fmt.Errorf("xmlf: max depth %d exceeded", r.opts.MaxDepth)
			}
		case xml.EndElement:
			sub--
		}
		toks = append(toks, xml.CopyToken(tok))
	}
	return toks, nil
}

// span marks one immediate child element within a record's token slice:
// toks[lo] is its StartElement, toks[hi] the matching EndElement.
type span struct {
	name   string
	lo, hi int
}

// buildElement maps one element subtree (toks[0] is its StartElement, the last
// token its matching EndElement) into the open builder. depth is the element's
// nesting level within the record (1 for the record element itself).
func (r *Reader) buildElement(toks []xml.Token, bld *record.Builder, depth int) error {
	if depth > r.opts.MaxDepth {
		return fmt.Errorf("xmlf: max depth %d exceeded", r.opts.MaxDepth)
	}
	se := toks[0].(xml.StartElement)

	// Scan immediate children (spans) and accumulate top-level character data.
	var children []span
	var text []byte
	end := len(toks) - 1 // matching EndElement of this element
	for i := 1; i < end; {
		switch t := toks[i].(type) {
		case xml.StartElement:
			j := matchEnd(toks, i)
			children = append(children, span{name: t.Name.Local, lo: i, hi: j})
			i = j + 1
		case xml.CharData:
			text = append(text, t...)
			i++
		default: // Comment, ProcInst, Directive: ignored
			i++
		}
	}

	nAttr := countAttrs(se.Attr)
	trimmed := bytes.TrimSpace(text)

	// Pure leaf (no attributes, no child elements): map directly to its text.
	if nAttr == 0 && len(children) == 0 {
		bld.String(trimmed)
		return nil
	}

	bld.BeginMap()

	// Attributes → "@name" (xmlns declarations dropped).
	for _, a := range se.Attr {
		if isNamespaceDecl(a) {
			continue
		}
		r.attrKey = append(r.attrKey[:0], '@')
		r.attrKey = append(r.attrKey, a.Name.Local...)
		bld.Key(r.attrKey)
		bld.StringLiteral(a.Value)
	}

	// Child elements → local name; repeated names collapse to a list. Each
	// distinct name is emitted at its first occurrence, preserving order.
	for c := 0; c < len(children); c++ {
		name := children[c].name
		if !firstOccurrence(children, c) {
			continue // already emitted as part of its list
		}
		bld.KeyLiteral(name)
		if nameCount(children, c) == 1 {
			s := children[c]
			if err := r.buildElement(toks[s.lo:s.hi+1], bld, depth+1); err != nil {
				return err
			}
			continue
		}
		bld.BeginList()
		for k := c; k < len(children); k++ {
			if children[k].name != name {
				continue
			}
			s := children[k]
			if err := r.buildElement(toks[s.lo:s.hi+1], bld, depth+1); err != nil {
				return err
			}
		}
		bld.EndList()
	}

	// Mixed-content text.
	if len(trimmed) > 0 {
		bld.KeyLiteral("#text")
		bld.String(trimmed)
	}

	bld.EndMap()
	return nil
}

// matchEnd returns the index of the EndElement that closes the StartElement at
// toks[start].
func matchEnd(toks []xml.Token, start int) int {
	d := 0
	for j := start; j < len(toks); j++ {
		switch toks[j].(type) {
		case xml.StartElement:
			d++
		case xml.EndElement:
			d--
			if d == 0 {
				return j
			}
		}
	}
	return len(toks) - 1 // unreachable for balanced input
}

// firstOccurrence reports whether children[c] is the first with its name.
func firstOccurrence(children []span, c int) bool {
	for k := 0; k < c; k++ {
		if children[k].name == children[c].name {
			return false
		}
	}
	return true
}

// nameCount counts children sharing children[c]'s name from c onward.
func nameCount(children []span, c int) int {
	n := 0
	for k := c; k < len(children); k++ {
		if children[k].name == children[c].name {
			n++
		}
	}
	return n
}

// countAttrs counts attributes that are not namespace declarations.
func countAttrs(attrs []xml.Attr) int {
	n := 0
	for _, a := range attrs {
		if !isNamespaceDecl(a) {
			n++
		}
	}
	return n
}

// isNamespaceDecl reports whether a is an xmlns / xmlns:* declaration.
func isNamespaceDecl(a xml.Attr) bool {
	return a.Name.Local == "xmlns" || a.Name.Space == "xmlns"
}
