package ndjson

import (
	"bytes"
	"context"
	"testing"
)

// FuzzReader drives the hand-rolled NDJSON tokenizer with arbitrary bytes.
// The property is robustness: a malformed/adversarial stream must surface as
// an error, never a panic, hang, or unbounded allocation. (The tokenizer is
// on the hot path and differential-tested vs encoding/json elsewhere; this
// guards the parser's failure modes.)
func FuzzReader(f *testing.F) {
	f.Add([]byte("{\"a\":1}\n{\"b\":[1,2,3]}\n"))
	f.Add([]byte("{\"nested\":{\"x\":true,\"y\":null}}\n"))
	f.Add([]byte("not json\n{\n\"unterminated"))
	f.Add([]byte("\n\n  \n"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data), ReaderOptions{})
		ctx := context.Background()
		// Bounded drain: a well-behaved reader reaches EOF; the cap is a
		// belt-and-suspenders guard against a pathological non-terminating case.
		for range 100000 {
			if _, err := r.Next(ctx); err != nil {
				break
			}
		}
		_ = r.Close()
	})
}
