package ndjson

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/aaron-au/shift/engine/record"
)

var errBoom = errors.New("boom")

// errReader fails every read with errBoom (never io.EOF), so the reader must
// surface it rather than treat it as end of stream.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errBoom }

// failWriter fails every write, so a sink must propagate the failure instead
// of silently dropping records.
type failWriter struct{}

func (failWriter) Write(p []byte) (int, error) { return 0, errBoom }

// ---------- Reader ----------

func TestReaderRespectsCanceledContext(t *testing.T) {
	r := NewReader(strings.NewReader(`{"a":1}`+"\n"), ReaderOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next = %v, want context.Canceled", err)
	}
}

func TestReaderSurfacesUnderlyingReadError(t *testing.T) {
	r := NewReader(errReader{}, ReaderOptions{})
	_, err := r.Next(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Next = %v, want errBoom", err)
	}
	if !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("err = %q, want line context", err)
	}
}

func TestReaderRejectsOversizedLine(t *testing.T) {
	// One line far larger than the scanner may buffer: an error, never a
	// silent truncation of the record.
	input := strings.Repeat("x", 100<<10) + "\n"
	r := NewReader(strings.NewReader(input), ReaderOptions{MaxLineBytes: 1024})
	_, err := r.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("Next = %v, want token-too-long error", err)
	}
	if !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("err = %q, want line context", err)
	}
}

// TestParseErrorMessages pins the diagnostic each malformed line produces:
// the message names what was expected, and (via Reader) which line failed.
func TestParseErrorMessages(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{`fals`, "invalid literal"},
		{`nul`, "invalid literal"},
		{`{"a":1`, "unexpected end of input"},
		{`{"a":1 2}`, "expected ',' or '}'"},
		{`{"a" 1}`, "expected ':'"},
		{`{1:2}`, "expected object key"},
		{`{"\q":1}`, "invalid escape"},
		{`[1`, "unexpected end of input"},
		{`[1 2]`, "expected ',' or ']'"},
		{`"a\q"`, "invalid escape"},
		{`"a\`, "unexpected end of input"},
		{`"a\nbroken`, "unexpected end of input"},
		{`"esc \u00"`, "unexpected end of input"},
		{`"esc \u12`, "unexpected end of input"},
		{`"esc \u00Z1"`, "invalid \\u escape"},
		{"\"raw \x01 control\"", "raw control character in string"},
		{"\"esc \\n\x01\"", "raw control character in string"},
		{`{"a":1} trailing`, "trailing data after JSON value"},
		{`@`, "unexpected character"},
	}
	for _, tc := range cases {
		_, _, err := parseOne(t, tc.line)
		if err == nil {
			t.Errorf("%q: parsed, want error %q", tc.line, tc.want)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: err = %v, want mention of %q", tc.line, err, tc.want)
		}
	}
}

func TestUnicodeEscapeDecoding(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{`"\u0041"`, "A"},
		{`"\u00e9\u4e2d"`, "\u00e9\u4e2d"},
		{`"\ud83d\ude00"`, "\U0001F600"}, // valid surrogate pair
		{`"pre\u0041post"`, "preApost"},
		{`"\u0000"`, "\x00"}, // escaped NUL is legal JSON
	}
	for _, tc := range cases {
		v, _, err := parseOne(t, tc.line)
		if err != nil {
			t.Errorf("%q: %v", tc.line, err)
			continue
		}
		if v.String() != tc.want {
			t.Errorf("%q: got %q, want %q", tc.line, v.String(), tc.want)
		}
	}
}

// TestInvalidSurrogatesYieldValidUTF8 pins the safety property for malformed
// UTF-16 escapes: whatever the parser makes of them, the resulting string is
// always valid UTF-8 carrying the replacement rune — never invalid bytes that
// would poison every downstream sink.
func TestInvalidSurrogatesYieldValidUTF8(t *testing.T) {
	for _, line := range []string{
		`"\ud800"`,            // lone high surrogate at end of string
		`"\udc00"`,            // lone low surrogate
		`"\ud800\ud800"`,      // high surrogate followed by another high one
		`"\ud800x"`,           // high surrogate followed by a plain char
		`"lead \ud83dtail"`,   // unpaired lead mid-string
		`"\ud83d\ude00 ok"`,   // valid pair, trailing text
		`"\ud800\u0041after"`, // high surrogate + non-surrogate escape
	} {
		v, _, err := parseOne(t, line)
		if err != nil {
			t.Errorf("%q: %v", line, err)
			continue
		}
		got := v.String()
		if !utf8.ValidString(got) {
			t.Errorf("%q: produced invalid UTF-8 %q", line, got)
		}
		if strings.Contains(line, `\ud800`) || strings.Contains(line, `\udc00`) || strings.Contains(line, `\ud83dtail`) {
			if !strings.ContainsRune(got, utf8.RuneError) {
				t.Errorf("%q: got %q, want a replacement rune for the unpaired surrogate", line, got)
			}
		}
	}
}

// TestInvalidSurrogateInPair covers a bad second escape in a surrogate pair.
func TestInvalidSurrogateInPair(t *testing.T) {
	if _, _, err := parseOne(t, `"\ud83d\uZZZZ"`); err == nil ||
		!strings.Contains(err.Error(), "invalid \\u escape") {
		t.Fatalf("err = %v, want invalid \\u escape", err)
	}
}

// TestParseIntContract covers the no-alloc integer fast path directly: it must
// report ok=false (so the caller falls back to float) on overflow and refuse
// malformed tokens outright.
func TestParseIntContract(t *testing.T) {
	cases := []struct {
		tok  string
		want int64
		ok   bool
	}{
		{"0", 0, true},
		{"-0", 0, true},
		{"7", 7, true},
		{"9223372036854775807", math.MaxInt64, true},
		{"-9223372036854775808", math.MinInt64, true},
		{"9223372036854775808", 0, false},   // MaxInt64+1
		{"-9223372036854775809", 0, false},  // MinInt64-1
		{"99999999999999999999", 0, false},  // overflows mid-accumulation
		{"-99999999999999999999", 0, false}, // negative, same
		{"01", 0, false},                    // leading zero is not JSON
		{"-01", 0, false},
		{"-", 0, false},
		{"1a", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseInt([]byte(tc.tok))
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("parseInt(%q) = %d,%v, want %d,%v", tc.tok, got, ok, tc.want, tc.ok)
		}
	}
}

// TestIntegerOverflowFallsBackToFloat is the reader-level consequence of the
// above: an out-of-range integer literal still parses, as a float.
func TestIntegerOverflowFallsBackToFloat(t *testing.T) {
	v, _, err := parseOne(t, `9223372036854775808`)
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind() != record.KindFloat || v.Float() != 9223372036854775808.0 {
		t.Fatalf("got %v/%v, want float 9223372036854775808", v.Kind(), v.Float())
	}
}

// ---------- JSONReader ----------

func TestJSONReaderRespectsCanceledContext(t *testing.T) {
	r := NewJSONReader(strings.NewReader(`[{"a":1}]`), ReaderOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next = %v, want context.Canceled", err)
	}
}

func TestJSONReaderEmptyArrayIsEOF(t *testing.T) {
	for _, input := range []string{`[]`, "  [ \n ]  "} {
		r := NewJSONReader(strings.NewReader(input), ReaderOptions{})
		if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatalf("%q: Next = %v, want io.EOF", input, err)
		}
	}
}

func TestJSONReaderSurfacesUnderlyingReadError(t *testing.T) {
	r := NewJSONReader(errReader{}, ReaderOptions{})
	_, err := r.Next(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Next = %v, want errBoom", err)
	}
}

func TestJSONReaderEnforcesDepthLimit(t *testing.T) {
	r := NewJSONReader(strings.NewReader(`[[[[[1]]]]]`), ReaderOptions{MaxDepth: 3})
	_, err := r.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "maximum nesting depth") {
		t.Fatalf("Next = %v, want depth error", err)
	}
	if !strings.HasPrefix(err.Error(), "json:") {
		t.Fatalf("err = %q, want json: prefix", err)
	}
}

func TestJSONReaderCloseStopsIteration(t *testing.T) {
	r := NewJSONReader(strings.NewReader(`[{"a":1},{"a":2}]`), ReaderOptions{})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close = %v, want io.EOF", err)
	}
}

// ---------- Writer ----------

func TestWriterRespectsCanceledContext(t *testing.T) {
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.Int(1)
	batch.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Write(ctx, batch); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write = %v, want context.Canceled", err)
	}
	if out.Len() != 0 {
		t.Fatalf("wrote %q after cancel, want nothing", out.String())
	}
}

func TestWriterEncodesBytesAsBase64(t *testing.T) {
	raw := []byte{0x00, 0x01, 0x02, 0xFF, 'h', 'i'}
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("blob")
	bld.Bytes(raw)
	bld.EndMap()
	batch.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out)
	if err := w.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Same encoding encoding/json uses for []byte, and it round-trips.
	var got struct {
		Blob []byte `json:"blob"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("stdlib cannot read our output %q: %v", out.String(), err)
	}
	if !bytes.Equal(got.Blob, raw) {
		t.Fatalf("round trip = %v, want %v", got.Blob, raw)
	}
	want, err := json.Marshal(map[string][]byte{"blob": raw})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out.String()) != string(want) {
		t.Fatalf("got %s, want %s", strings.TrimSpace(out.String()), want)
	}
}

func TestWriterRejectsNaNAndInf(t *testing.T) {
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		batch := record.NewBatch()
		bld := batch.Builder()
		bld.Float(f)
		batch.Append(bld.Finish())

		var out bytes.Buffer
		w := NewWriter(&out)
		err := w.Write(context.Background(), batch)
		if err == nil || !strings.Contains(err.Error(), "unsupported float value") {
			t.Fatalf("Write(%v) = %v, want unsupported-float error", f, err)
		}
	}
}

// TestWriterReplacesInvalidUTF8 pins that a record carrying invalid UTF-8
// (bytes from a legacy source, say) still yields valid JSON — the stdlib must
// be able to read our output back, with the bad bytes replaced.
func TestWriterReplacesInvalidUTF8(t *testing.T) {
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.String([]byte{'a', 0xFF, 'b', 0xC3, 'c'}) // 0xFF and a truncated 2-byte seq
	batch.Append(bld.Finish())

	var out bytes.Buffer
	w := NewWriter(&out)
	if err := w.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(out.String())
	var got string
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("stdlib cannot read our output %q: %v", line, err)
	}
	if want := "a\uFFFDb\uFFFDc"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	// encoding/json decodes its own encoding of the same bytes identically.
	std, err := json.Marshal(string([]byte{'a', 0xFF, 'b', 0xC3, 'c'}))
	if err != nil {
		t.Fatal(err)
	}
	var stdGot string
	if err := json.Unmarshal(std, &stdGot); err != nil {
		t.Fatal(err)
	}
	if stdGot != got {
		t.Fatalf("stdlib decodes %q, we decode %q", stdGot, got)
	}
}

func TestWriterPropagatesWriteError(t *testing.T) {
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.String(bytes.Repeat([]byte("x"), 300<<10)) // exceeds the internal buffer
	batch.Append(bld.Finish())

	w := NewWriter(failWriter{})
	if err := w.Write(context.Background(), batch); !errors.Is(err, errBoom) {
		t.Fatalf("Write = %v, want errBoom", err)
	}
}

func TestWriterCloseReportsFlushError(t *testing.T) {
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.Int(1)
	batch.Append(bld.Finish())

	w := NewWriter(failWriter{})
	if err := w.Write(context.Background(), batch); err != nil {
		t.Fatalf("Write = %v, want the small record to buffer cleanly", err)
	}
	if err := w.Close(); !errors.Is(err, errBoom) {
		t.Fatalf("Close = %v, want errBoom from the flush", err)
	}
}
