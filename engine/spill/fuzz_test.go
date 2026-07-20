package spill

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// FuzzDecode feeds the binary Value codec arbitrary bytes. Spilled data is
// re-read from disk through this decoder, so a corrupt/truncated/adversarial
// buffer must return a decode error — never panic or allocate unboundedly.
func FuzzDecode(f *testing.F) {
	// A couple of well-formed seeds: round-trip real values through the encoder.
	for _, v := range []func(*record.Builder) record.Value{
		func(b *record.Builder) record.Value { b.Int(42); return b.Finish() },
		func(b *record.Builder) record.Value { b.StringLiteral("hello"); return b.Finish() },
		func(b *record.Builder) record.Value {
			b.BeginMap()
			b.KeyLiteral("k")
			b.Int(1)
			b.EndMap()
			return b.Finish()
		},
	} {
		var buf bytes.Buffer
		bld := record.NewBatch().Builder()
		if err := NewEncoder(&buf).Encode(v(bld)); err == nil {
			f.Add(buf.Bytes())
		}
	}
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewDecoder(bufio.NewReader(bytes.NewReader(data)), 1<<20)
		bld := record.NewBatch().Builder()
		_ = d.Decode(bld) // must not panic; corrupt/truncated input → error
	})
}
