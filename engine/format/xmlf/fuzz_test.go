package xmlf

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// FuzzReader drives the XML reader with arbitrary bytes. XML arrives from
// callers and partners, and the two ways it hurts a streaming reader are depth
// (stack) and breadth (one record's subtree is the only thing held in memory —
// as long as the depth bound actually holds). The target therefore sets
// MaxDepth low and ASSERTS it on every record produced, rather than settling
// for "did not crash".
func FuzzReader(f *testing.F) {
	f.Add("row", []byte(`<rows><row id="1"><name>ada</name><tag>a</tag><tag>b</tag></row><row id="2"><name>bob</name></row></rows>`))
	f.Add("", []byte(`<?xml version="1.0"?><rows><r><a>1</a></r><r><a>2</a></r></rows>`))
	f.Add("row", []byte(`<ns:rows xmlns:ns="urn:x"><ns:row a="1">t</ns:row></ns:rows>`))

	f.Add("row", []byte(`<rows><row><a>`+strings.Repeat("<b>", 200))) // deep and truncated mid-tree
	f.Add("row", []byte(`<rows>`+strings.Repeat("<row><a>x</a></row>", 200)+`</rows>`))
	f.Add("row", []byte(`<rows><row>`+strings.Repeat(`<a>x</a>`, 2000)+`</row></rows>`)) // one huge record
	// The billion-laughs shape: entity expansion must not happen, in a reader
	// whose whole premise is that one subtree is all that is resident.
	f.Add("row", []byte(`<!DOCTYPE l [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;">]><rows><row>&b;</row></rows>`))
	f.Add("row", []byte(`<rows><row a="1" a="2">dup attrs</row></rows>`))
	f.Add("row", []byte("<rows><row>\x00\xff\xfe</row></rows>")) // NUL + invalid UTF-8 in char data
	f.Add("row", []byte(`<rows><row>unterminated`))
	f.Add("row", []byte(`</row>`)) // an end element with nothing open
	f.Add("row", []byte(``))
	f.Add(strings.Repeat("x", 300), []byte(`<rows><row/></rows>`)) // record element that never matches

	f.Fuzz(func(t *testing.T, recordElement string, data []byte) {
		if len(recordElement) > 1024 || len(data) > 32<<10 {
			return // bounded work per input
		}
		const maxDepth = 4
		r := NewReader(bytes.NewReader(data), ReaderOptions{
			RecordElement: recordElement,
			BatchRecords:  4,
			BatchBytes:    4 << 10,
			MaxDepth:      maxDepth,
		})
		ctx := context.Background()
		for range 10000 {
			b, err := r.Next(ctx)
			if err != nil {
				break
			}
			for _, rec := range b.Records() {
				if d := elementDepth(rec); d > maxDepth {
					t.Fatalf("record nests %d elements deep, MaxDepth is %d", d, maxDepth)
				}
			}
		}
		_ = r.Close()
	})
}

// elementDepth measures nesting the way the reader counts it: one level per
// ELEMENT. Two mappings are therefore transparent here. A list is the collapsed
// form of repeated siblings, not a level of its own — otherwise
// `<r><t>a</t><t>b</t></r>` would read as deeper than the document it came
// from. And "@attr"/"#text" fields are the element's own scalar data, which
// live in the same element, not a child of it.
func elementDepth(v record.Value) int {
	switch v.Kind() {
	case record.KindMap:
		deepest := 0
		for i := range v.Len() {
			if k := v.KeyAt(i); len(k) > 0 && (k[0] == '@' || k[0] == '#') {
				continue
			}
			if d := elementDepth(v.Index(i)); d > deepest {
				deepest = d
			}
		}
		return 1 + deepest
	case record.KindList:
		deepest := 0
		for i := range v.Len() {
			if d := elementDepth(v.Index(i)); d > deepest {
				deepest = d
			}
		}
		return deepest
	default:
		return 1
	}
}
