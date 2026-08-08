package fileformat

import (
	"bytes"
	"strings"
	"testing"
)

// The registry exists so a connector cannot advertise a format it does not
// handle. If the schema offers it, a reader must exist for it.
func TestEverySupportedFormatHasAReader(t *testing.T) {
	for _, f := range Supported {
		if _, err := NewReader(f, strings.NewReader(""), Options{}); err != nil {
			t.Errorf("%s is advertised but has no reader: %v", f, err)
		}
		if !strings.Contains(SchemaEnum(), `"`+f+`"`) {
			t.Errorf("%s is supported but missing from the config schema enum", f)
		}
	}
}

// An unknown format must ERROR rather than fall through to a default. The
// silent fall-through is the exact bug this package was extracted to kill: the
// file gets parsed as NDJSON and a config typo looks like corrupt data.
func TestAnUnknownFormatNeverFallsThroughToADefault(t *testing.T) {
	if _, err := NewReader("parquet", strings.NewReader(""), Options{}); err == nil {
		t.Error("an unknown format silently produced a reader")
	}
	if _, err := NewWriter("parquet", &bytes.Buffer{}, Options{}); err == nil {
		t.Error("an unknown format silently produced a writer")
	}
}

// EDI is readable but not writable, and that has to be an explicit refusal:
// emitting an interchange without correct control numbers and segment counts
// produces a file a trading partner rejects, which is worse than not offering
// it at all.
func TestEDIRefusesToWriteRatherThanWriteSomethingInvalid(t *testing.T) {
	_, err := NewWriter(EDI, &bytes.Buffer{}, Options{})
	if err == nil {
		t.Fatal("EDI produced a writer")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("the refusal does not explain itself: %v", err)
	}
}

// Validate normalises in place, so a caller that forgets to write the default
// back cannot pass "" to Open and silently get NDJSON.
func TestValidateNormalisesAndNamesTheConnector(t *testing.T) {
	empty := ""
	if err := Validate("fs", &empty); err != nil || empty != Default {
		t.Fatalf("empty normalised to %q (err=%v), want %q", empty, err, Default)
	}

	bad := "parquet"
	err := Validate("sftp", &bad)
	if err == nil {
		t.Fatal("an unsupported format was accepted")
	}
	if !strings.Contains(err.Error(), "sftp") || !strings.Contains(err.Error(), "parquet") {
		t.Errorf("the error names neither the connector nor the format: %v", err)
	}
	// It must also list what IS allowed; an operator should not have to guess.
	for _, f := range Supported {
		if !strings.Contains(err.Error(), f) {
			t.Errorf("the error does not mention the supported format %q: %v", f, err)
		}
	}
}
