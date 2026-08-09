package sdk

import (
	"bytes"
	"fmt"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
)

// ProtocolVersion is the connector protocol this SDK build speaks.
//
// Version 2 added the exact decimal and temporal value kinds to the frame
// codec (ADR-0051). The wire is otherwise unchanged, and the version is what
// lets a host know whether a given connector can receive those kinds: a
// connector built against an older SDK still reports 1, and hosts refuse to
// send it a decimal rather than letting its decoder fail on an unknown tag.
const ProtocolVersion uint32 = 2

// ProtocolVersionExactKinds is the first protocol version whose frame codec
// carries the ADR-0051 kinds.
const ProtocolVersionExactKinds uint32 = 2

// SupportedProtocolVersions are the versions a host offers at handshake, in
// ascending order. Version 1 stays on the list so that connector artifacts
// published before ADR-0051 keep working: a connector requires its own version
// to appear in the host's offer, so dropping 1 would retire every older signed
// build at once (ADR-0047 keeps them resolvable on purpose).
func SupportedProtocolVersions() []uint32 { return []uint32{1, ProtocolVersion} }

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
