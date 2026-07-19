package ndjson

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// validCorpus: every line must parse identically to encoding/json.
var validCorpus = []string{
	`null`, `true`, `false`,
	`0`, `-0`, `7`, `-7`, `9223372036854775807`, `-9223372036854775808`,
	`92233720368547758079999`, // int64 overflow -> float
	`1.5`, `-2.25`, `1e10`, `1E-10`, `1.5e+300`, `0.000001`, `1e21`, `1e-7`,
	`""`, `"plain"`, `"with \"quotes\" and \\ slash"`,
	`"\b\f\n\r\t"`, `"Aé中"`, `"😀"`, // surrogate pair
	`"unicode direct: héllo 中文 🚀"`,
	`{}`, `[]`, `[1,2,3]`, `[[[[1]]]]`,
	`{"a":1,"b":"x","c":null,"d":true}`,
	`{"nested":{"deep":{"list":[{"k":"v"}]}}}`,
	`  {"ws" :  [ 1 , 2 ]  }  `,
	`{"dup":1,"dup":2}`,
	`{"empty":{},"elist":[]}`,
	`[0.1,0.2,0.30000000000000004]`,
}

// invalidCorpus: every line must be rejected by both parsers.
var invalidCorpus = []string{
	``, `{`, `}`, `[1,`, `{"a":}`, `{"a"}`, `{a:1}`, `"unterminated`,
	`tru`, `nul`, `falsey`, `01`, `- 1`, `+1`, `1.`, `.5`, `1e`, `1e+`,
	`"bad \x escape"`, `"lone quote \`, `[1]extra`, `{"a":1}{"b":2}`,
	"\"raw \t tab\"", `-`,
}

func parseOne(t *testing.T, line string) (record.Value, *record.Batch, error) {
	t.Helper()
	r := NewReader(strings.NewReader(line+"\n"), ReaderOptions{})
	b, err := r.Next(context.Background())
	if err != nil {
		return record.Value{}, nil, err
	}
	if b.Len() != 1 {
		t.Fatalf("expected 1 record, got %d", b.Len())
	}
	return b.Record(0), b, nil
}

// assertMatchesStd compares a record.Value against encoding/json's reading
// of the same input (UseNumber to preserve int/float distinction).
func assertMatchesStd(t *testing.T, input string, v record.Value) {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(input))
	dec.UseNumber()
	var want any
	if err := dec.Decode(&want); err != nil {
		t.Fatalf("stdlib failed on %q: %v", input, err)
	}
	compareValue(t, input, v, want)
}

func compareValue(t *testing.T, ctx string, v record.Value, want any) {
	t.Helper()
	switch w := want.(type) {
	case nil:
		if !v.IsNull() {
			t.Errorf("%q: got %v, want null", ctx, v.Kind())
		}
	case bool:
		if v.Kind() != record.KindBool || v.Bool() != w {
			t.Errorf("%q: got %v/%v, want bool %v", ctx, v.Kind(), v.Bool(), w)
		}
	case json.Number:
		switch v.Kind() {
		case record.KindInt:
			i, err := strconv.ParseInt(w.String(), 10, 64)
			if err != nil || i != v.Int() {
				t.Errorf("%q: got int %d, want %s", ctx, v.Int(), w)
			}
		case record.KindFloat:
			f, err := strconv.ParseFloat(w.String(), 64)
			if err != nil || f != v.Float() {
				t.Errorf("%q: got float %v, want %s", ctx, v.Float(), w)
			}
		default:
			t.Errorf("%q: got %v, want number %s", ctx, v.Kind(), w)
		}
	case string:
		if v.Kind() != record.KindString || v.String() != w {
			t.Errorf("%q: got %v %q, want string %q", ctx, v.Kind(), v.String(), w)
		}
	case []any:
		if v.Kind() != record.KindList || v.Len() != len(w) {
			t.Errorf("%q: got %v len %d, want list len %d", ctx, v.Kind(), v.Len(), len(w))
			return
		}
		for i, el := range w {
			compareValue(t, ctx, v.Index(i), el)
		}
	case map[string]any:
		if v.Kind() != record.KindMap {
			t.Errorf("%q: got %v, want map", ctx, v.Kind())
			return
		}
		// Build last-wins view of our ordered fields (stdlib maps do
		// last-wins on duplicate keys).
		got := map[string]record.Value{}
		for i := range v.Len() {
			got[string(v.KeyAt(i))] = v.Index(i)
		}
		if len(got) != len(w) {
			t.Errorf("%q: got %d unique keys, want %d", ctx, len(got), len(w))
			return
		}
		for k, el := range w {
			gv, ok := got[k]
			if !ok {
				t.Errorf("%q: missing key %q", ctx, k)
				continue
			}
			compareValue(t, ctx, gv, el)
		}
	default:
		t.Fatalf("unhandled stdlib type %T", want)
	}
}

func TestDifferentialValid(t *testing.T) {
	for _, line := range validCorpus {
		v, _, err := parseOne(t, line)
		if err != nil {
			t.Errorf("parse %q: %v", line, err)
			continue
		}
		assertMatchesStd(t, line, v)
	}
}

func TestDifferentialInvalid(t *testing.T) {
	for _, line := range invalidCorpus {
		if strings.TrimSpace(line) == "" {
			continue // blank lines are skipped by NDJSON framing, not errors
		}
		if !json.Valid([]byte(line)) {
			if _, _, err := parseOne(t, line); err == nil {
				t.Errorf("parse %q: expected error, stdlib rejects it too", line)
			}
		}
	}
}

func TestRoundTrip(t *testing.T) {
	var input strings.Builder
	for _, line := range validCorpus {
		input.WriteString(line)
		input.WriteByte('\n')
	}
	r := NewReader(strings.NewReader(input.String()), ReaderOptions{})
	var out bytes.Buffer
	w := NewWriter(&out)
	ctx := context.Background()
	for {
		b, err := r.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Write(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Every output line must be valid JSON and semantically equal to the
	// input line as the stdlib reads both (numeric comparison: "1e10" and
	// "10000000000" are the same JSON number).
	outLines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(outLines) != len(validCorpus) {
		t.Fatalf("got %d output lines, want %d", len(outLines), len(validCorpus))
	}
	for i, outLine := range outLines {
		var wantV, gotV any
		if err := json.Unmarshal([]byte(validCorpus[i]), &wantV); err != nil {
			t.Fatalf("stdlib rejects input %q: %v", validCorpus[i], err)
		}
		if err := json.Unmarshal([]byte(outLine), &gotV); err != nil {
			t.Errorf("stdlib rejects our output %q: %v", outLine, err)
			continue
		}
		if !reflect.DeepEqual(wantV, gotV) {
			t.Errorf("round-trip drift:\n in: %s\nout: %s", validCorpus[i], outLine)
		}
	}
}

func TestBatchingAndEOF(t *testing.T) {
	var input strings.Builder
	const n = 2500
	for i := range n {
		fmt.Fprintf(&input, `{"i":%d}`+"\n", i)
		if i%100 == 0 {
			input.WriteString("\n  \n") // blank lines skipped
		}
	}
	r := NewReader(strings.NewReader(input.String()), ReaderOptions{BatchRecords: 1000})
	ctx := context.Background()
	total := 0
	batches := 0
	for {
		b, err := r.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if b.Len() > 1000 {
			t.Fatalf("batch exceeded limit: %d", b.Len())
		}
		// verify continuity
		for j := range b.Len() {
			v, _ := b.Record(j).Field("i")
			if v.Int() != int64(total+j) {
				t.Fatalf("record %d has i=%d", total+j, v.Int())
			}
		}
		total += b.Len()
		batches++
	}
	if total != n || batches != 3 {
		t.Fatalf("total=%d batches=%d, want %d/3", total, batches, n)
	}
	if _, err := r.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("post-EOF Next = %v, want EOF", err)
	}
}

func TestErrorsCarryLineNumbers(t *testing.T) {
	r := NewReader(strings.NewReader("{\"ok\":1}\n{bad}\n"), ReaderOptions{})
	_, err := r.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("err = %v, want line 2 mention", err)
	}
}

func TestDepthLimit(t *testing.T) {
	deep := strings.Repeat("[", 100) + strings.Repeat("]", 100)
	r := NewReader(strings.NewReader(deep+"\n"), ReaderOptions{MaxDepth: 64})
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("expected depth error")
	}
}

func TestFloatFormatBoundaries(t *testing.T) {
	batch := record.NewBatch()
	bld := batch.Builder()
	for _, f := range []float64{0, 1e-6, 9.9e-7, 1e20, 1e21, -1e21, 0.1, math.MaxFloat64} {
		bld.BeginList()
		bld.Float(f)
		bld.EndList()
		batch.Append(bld.Finish())
	}
	var out bytes.Buffer
	w := NewWriter(&out)
	if err := w.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var got []float64
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Errorf("stdlib cannot read our float output %q: %v", line, err)
		}
	}
}

func FuzzDifferential(f *testing.F) {
	for _, s := range validCorpus {
		f.Add(s)
	}
	for _, s := range invalidCorpus {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if strings.ContainsAny(line, "\n") || strings.TrimSpace(line) == "" {
			t.Skip()
		}
		if !utf8ValidNoRawControls(line) {
			t.Skip() // reader passes raw bytes through; stdlib replaces — documented divergence
		}
		r := NewReader(strings.NewReader(line+"\n"), ReaderOptions{})
		b, err := r.Next(context.Background())
		stdOK := json.Valid([]byte(line))
		if err != nil {
			if stdOK {
				t.Fatalf("we reject, stdlib accepts: %q (%v)", line, err)
			}
			return
		}
		if !stdOK {
			t.Fatalf("we accept, stdlib rejects: %q", line)
		}
		if b.Len() == 1 {
			assertMatchesStd(t, line, b.Record(0))
		}
	})
}

func utf8ValidNoRawControls(s string) bool {
	inStr := false
	esc := false
	for _, c := range []byte(s) {
		if inStr && !esc && c < 0x20 {
			return false
		}
		switch {
		case esc:
			esc = false
		case c == '\\' && inStr:
			esc = true
		case c == '"':
			inStr = !inStr
		}
	}
	return strings.ToValidUTF8(s, "") == s
}

func BenchmarkParse(b *testing.B) {
	line := `{"id":123456,"name":"customer-name-here","email":"someone@example.com","amount":1234.56,"active":true,"tags":["retail","priority"],"address":{"street":"1 Collins St","city":"Melbourne","postcode":"3000"}}`
	var input strings.Builder
	for range 1000 {
		input.WriteString(line)
		input.WriteByte('\n')
	}
	data := input.String()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	ctx := context.Background()
	for b.Loop() {
		r := NewReader(strings.NewReader(data), ReaderOptions{})
		for {
			if _, err := r.Next(ctx); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkParseStdlib(b *testing.B) {
	line := `{"id":123456,"name":"customer-name-here","email":"someone@example.com","amount":1234.56,"active":true,"tags":["retail","priority"],"address":{"street":"1 Collins St","city":"Melbourne","postcode":"3000"}}`
	var input strings.Builder
	for range 1000 {
		input.WriteString(line)
		input.WriteByte('\n')
	}
	data := input.String()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		dec := json.NewDecoder(strings.NewReader(data))
		for {
			var v map[string]any
			if err := dec.Decode(&v); err == io.EOF {
				break
			} else if err != nil {
				b.Fatal(err)
			}
		}
	}
}
