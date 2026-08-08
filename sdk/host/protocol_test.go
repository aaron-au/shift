package host

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/sdk"
)

// TestASinkToAnOlderConnectorRefusesTheExactKinds: signed connector versions
// stay resolvable and runnable (ADR-0047), so a current runner can legitimately
// be pushing to a connector built before ADR-0051. It must be told that, in
// those words, rather than having the subprocess fail on an unknown tag.
func TestASinkToAnOlderConnectorRefusesTheExactKinds(t *testing.T) {
	p := &Process{info: Info{Name: "legacy", ProtocolVersion: 1}}
	s := p.Sink("put", nil)

	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("amount")
	bld.Decimal(1010, 2)
	bld.EndMap()
	b.Append(bld.Finish())

	// Write opens the Push stream first, so drive the encoder directly: this
	// asserts the encoder choice, which is the decision under test.
	err := encodeBatch(s, b)
	if err == nil {
		t.Fatal("pushing a decimal to a protocol-1 connector must fail")
	}
	if !strings.Contains(err.Error(), "protocol 1") {
		t.Errorf("error = %v, want it to name the protocol", err)
	}
}

func TestASinkToACurrentConnectorCarriesTheExactKinds(t *testing.T) {
	p := &Process{info: Info{Name: "current", ProtocolVersion: sdk.ProtocolVersion}}
	s := p.Sink("put", nil)

	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("amount")
	bld.Decimal(1010, 2)
	bld.EndMap()
	b.Append(bld.Finish())

	if err := encodeBatch(s, b); err != nil {
		t.Fatalf("a current connector must accept a decimal: %v", err)
	}
}

// encodeBatch runs the frame-encoding half of SinkStream.Write.
func encodeBatch(s *SinkStream, b *record.Batch) error {
	s.buf.Reset()
	for _, rec := range b.Records() {
		if err := s.enc.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}

// TestTheHostOffersBothProtocolVersions: dropping version 1 from the offer
// would retire every connector artifact published before ADR-0051 at once,
// because a connector requires its own version to appear in the host's offer.
func TestTheHostOffersBothProtocolVersions(t *testing.T) {
	got := sdk.SupportedProtocolVersions()
	if len(got) < 2 || got[0] != 1 {
		t.Fatalf("offered versions = %v, want version 1 first", got)
	}
	if got[len(got)-1] != sdk.ProtocolVersion {
		t.Errorf("offered versions = %v, want the current version last (ascending)", got)
	}
}

// TestAProtocol1SinkStillCarriesOrdinaryValues keeps the gate narrow.
func TestAProtocol1SinkStillCarriesOrdinaryValues(t *testing.T) {
	p := &Process{info: Info{Name: "legacy", ProtocolVersion: 1}}
	s := p.Sink("put", nil)

	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("id")
	bld.Int(7)
	bld.KeyLiteral("name")
	bld.StringLiteral("ada")
	bld.EndMap()
	b.Append(bld.Finish())

	if err := encodeBatch(s, b); err != nil {
		t.Fatalf("protocol 1 must still carry int and string: %v", err)
	}
	if bytes.Equal(s.buf.Bytes(), nil) {
		t.Error("nothing was encoded")
	}
}
