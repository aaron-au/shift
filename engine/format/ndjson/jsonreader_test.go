package ndjson

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// drainJSON reads every record from a JSONReader over input, returning each
// record's top-level fields as a map (values stringified for easy asserting).
// Records are snapshotted per batch before the next Next reuses the batch.
func drainJSON(t *testing.T, input string, opts ReaderOptions) []map[string]string {
	t.Helper()
	r := NewJSONReader(strings.NewReader(input), opts)
	var out []map[string]string
	for {
		b, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		for i := range b.Len() {
			rec := b.Record(i)
			m := map[string]string{}
			if rec.Kind() == record.KindMap {
				for j := range rec.Len() {
					m[string(rec.KeyAt(j))] = scalarStr(rec.Index(j))
				}
			} else {
				m["_scalar"] = scalarStr(rec)
			}
			out = append(out, m)
		}
	}
	return out
}

func TestJSONReaderArray(t *testing.T) {
	// A pretty-printed top-level array — the shape a REST API returns and the
	// line-based Reader cannot parse.
	in := `[
	  {"id": 1, "email": "a@x.io"},
	  {"id": 2, "email": "b@x.io"},
	  {"id": 3, "email": "c@x.io"}
	]`
	recs := drainJSON(t, in, ReaderOptions{})
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	if recs[0]["email"] != "a@x.io" || recs[2]["id"] != "3" {
		t.Fatalf("unexpected records: %v", recs)
	}
}

func TestJSONReaderSingleObject(t *testing.T) {
	recs := drainJSON(t, `{"id": 7, "name": "solo"}`, ReaderOptions{})
	if len(recs) != 1 || recs[0]["name"] != "solo" {
		t.Fatalf("single object: %v", recs)
	}
}

func TestJSONReaderConcatenatedValues(t *testing.T) {
	// A stream of whitespace/newline-separated values (NDJSON is a subset).
	recs := drainJSON(t, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n", ReaderOptions{})
	if len(recs) != 3 || recs[1]["id"] != "2" {
		t.Fatalf("concatenated: %v", recs)
	}
}

func TestJSONReaderEmptyAndBatching(t *testing.T) {
	if recs := drainJSON(t, "   \n  ", ReaderOptions{}); len(recs) != 0 {
		t.Fatalf("blank input yielded %d records", len(recs))
	}
	// An array larger than one batch must span batches and total correctly.
	var sb strings.Builder
	sb.WriteByte('[')
	const n = 250
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"id":`)
		sb.WriteString(itoa(i))
		sb.WriteByte('}')
	}
	sb.WriteByte(']')
	recs := drainJSON(t, sb.String(), ReaderOptions{BatchRecords: 64})
	if len(recs) != n {
		t.Fatalf("array total = %d, want %d", len(recs), n)
	}
	if recs[0]["id"] != "0" || recs[n-1]["id"] != "249" {
		t.Fatalf("boundary records wrong: first=%v last=%v", recs[0], recs[n-1])
	}
}

func TestJSONReaderMalformed(t *testing.T) {
	r := NewJSONReader(strings.NewReader(`[{"id":1}, {oops}]`), ReaderOptions{})
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("expected error on malformed element")
	}
}

func scalarStr(v record.Value) string {
	switch v.Kind() {
	case record.KindString:
		return v.String()
	case record.KindInt:
		return itoa(int(v.Int()))
	case record.KindBool:
		if v.Bool() {
			return "true"
		}
		return "false"
	default:
		return v.Kind().String()
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
