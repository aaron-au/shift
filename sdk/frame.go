package sdk

import (
	"bytes"
	"fmt"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
)

// ProtocolVersion is the connector protocol this SDK build speaks.
const ProtocolVersion uint32 = 1

// frameEncoder serializes batches into reusable frame payloads (ADR-0007:
// batches cross the wire as opaque sequences of binary-codec values).
type frameEncoder struct {
	buf bytes.Buffer
	enc *spill.Encoder
}

func newFrameEncoder() *frameEncoder {
	f := &frameEncoder{}
	f.enc = spill.NewEncoder(&f.buf)
	return f
}

// encode returns the frame payload for b. The returned slice is valid until
// the next encode call.
func (f *frameEncoder) encode(b *record.Batch) ([]byte, error) {
	f.buf.Reset()
	for _, rec := range b.Records() {
		if err := f.enc.Encode(rec); err != nil {
			return nil, fmt.Errorf("sdk: encode frame: %w", err)
		}
	}
	return f.buf.Bytes(), nil
}

// decodeFrame appends the frame's records into batch (which the caller has
// Reset as appropriate).
func decodeFrame(payload []byte, batch *record.Batch) error {
	r := bytes.NewReader(payload)
	dec := spill.NewDecoder(r, 0)
	bld := batch.Builder()
	for r.Len() > 0 {
		if err := dec.Decode(bld); err != nil {
			return fmt.Errorf("sdk: decode frame: %w", err)
		}
		batch.Append(bld.Finish())
	}
	return nil
}
