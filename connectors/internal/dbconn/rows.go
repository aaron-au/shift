package dbconn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// appendValue maps one scanned SQL value onto the record builder, into batch.
// The set of Go types is what database/sql yields when scanning into
// *interface{} (the pgx stdlib driver included): nil, bool, int64, float64,
// []byte, string, time.Time. json/jsonb columns arrive as []byte/string and are
// parsed into a nested record (see appendJSON) so downstream steps can navigate
// them.
func appendValue(ctx context.Context, batch *record.Batch, bld *record.Builder, v any, isJSON bool) {
	switch t := v.(type) {
	case nil:
		bld.Null()
	case bool:
		bld.Bool(t)
	case int64:
		bld.Int(t)
	case int32:
		bld.Int(int64(t))
	case int:
		bld.Int(int64(t))
	case float64:
		bld.Float(t)
	case float32:
		bld.Float(float64(t))
	case time.Time:
		bld.StringLiteral(t.UTC().Format(time.RFC3339Nano))
	case []byte:
		if isJSON {
			appendJSON(ctx, batch, bld, t)
			return
		}
		bld.String(t)
	case string:
		if isJSON {
			appendJSON(ctx, batch, bld, []byte(t))
			return
		}
		bld.StringLiteral(t)
	default:
		// numeric/decimal, uuid, and other driver types surface as their Go
		// String() form; keep the value rather than dropping it.
		bld.StringLiteral(fmt.Sprint(t))
	}
}

// appendJSON parses a json/jsonb cell into a nested record value. jsonb scans
// back as a compact single-line document, which the ndjson line reader parses
// as one value (object, array, or scalar). Anything the single-value parser
// won't take cleanly (e.g. multi-line pretty-printed json) falls back to the
// raw text as a string, so a cell's data is never lost.
func appendJSON(ctx context.Context, batch *record.Batch, bld *record.Builder, raw []byte) {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 {
		rd := ndjson.NewReader(bytes.NewReader(trimmed), ndjson.ReaderOptions{})
		if b, err := rd.Next(ctx); err == nil && b.Len() == 1 {
			// CopyValue moves the parsed value into batch's arena/slabs so it
			// belongs to the row's batch (bld.Value requires same-batch data).
			// Chunk allocations are stable, so copying mid-build is safe.
			bld.Value(record.CopyValue(batch, b.Record(0)))
			return
		}
	}
	bld.String(raw)
}

// recordKeys returns the field names of a map record in order.
func recordKeys(v record.Value) []string {
	n := v.Len()
	keys := make([]string, n)
	for i := range n {
		keys[i] = string(v.KeyAt(i))
	}
	return keys
}

// valueToArg converts a record value into a database/sql query argument. It is
// only ever passed as a positional parameter ($1,$2,...) — never concatenated
// into SQL. Lists and maps are encoded as JSON text (suitable for a json/jsonb
// column).
func valueToArg(v record.Value) any {
	switch v.Kind() {
	case record.KindNull:
		return nil
	case record.KindBool:
		return v.Bool()
	case record.KindInt:
		return v.Int()
	case record.KindFloat:
		return v.Float()
	case record.KindString:
		return v.String()
	case record.KindBytes:
		return v.Bytes()
	case record.KindList, record.KindMap:
		var buf bytes.Buffer
		encodeJSON(&buf, v)
		return buf.String()
	default:
		return nil
	}
}

// encodeJSON writes v as JSON into buf without going through
// map[string]interface{} (doctrine: no map[string]interface{} on the hot path).
func encodeJSON(buf *bytes.Buffer, v record.Value) {
	switch v.Kind() {
	case record.KindNull:
		buf.WriteString("null")
	case record.KindBool:
		if v.Bool() {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case record.KindInt:
		buf.WriteString(strconv.FormatInt(v.Int(), 10))
	case record.KindFloat:
		buf.WriteString(strconv.FormatFloat(v.Float(), 'g', -1, 64))
	case record.KindString, record.KindBytes:
		writeJSONString(buf, v.Bytes())
	case record.KindList:
		buf.WriteByte('[')
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				buf.WriteByte(',')
			}
			encodeJSON(buf, v.Index(i))
		}
		buf.WriteByte(']')
	case record.KindMap:
		buf.WriteByte('{')
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeJSONString(buf, v.KeyAt(i))
			buf.WriteByte(':')
			encodeJSON(buf, v.Index(i))
		}
		buf.WriteByte('}')
	}
}

// writeJSONString writes s as a properly escaped JSON string. json.Marshal on a
// string never errors, so the discard is safe.
func writeJSONString(buf *bytes.Buffer, s []byte) {
	b, _ := json.Marshal(string(s))
	buf.Write(b)
}

// identRe permits a single SQL identifier: a leading letter/underscore then
// letters, digits, underscores, or $. It deliberately excludes the double-quote
// and every other character, so quoteIdent's output cannot be broken out of.
var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// quoteIdent validates and double-quotes a (optionally schema-qualified) SQL
// identifier. Identifiers CANNOT be passed as bind parameters in any SQL
// dialect, so table/column names must be interpolated — this is the sole guard
// against injection through them: reject anything outside identRe, then quote.
func quoteIdent(name string) (string, error) {
	if name == "" {
		return "", errors.New("db: empty identifier")
	}
	parts := strings.Split(name, ".")
	out := make([]string, len(parts))
	for i, p := range parts {
		if !identRe.MatchString(p) {
			return "", fmt.Errorf("db: invalid identifier %q", name)
		}
		out[i] = `"` + p + `"`
	}
	return strings.Join(out, "."), nil
}
