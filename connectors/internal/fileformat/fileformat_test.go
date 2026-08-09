package fileformat

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// minimalOptions are the settings a format cannot be constructed without.
// Only fixedw has any: it is the one format that is not self-describing, so a
// layout is configuration rather than a default.
func minimalOptions(format string) Options {
	if format == FIXEDW {
		return Options{Columns: []Column{{Name: "a", Width: 1}}}
	}
	return Options{}
}

// The registry exists so a connector cannot advertise a format it does not
// handle. If the schema offers it, a reader must exist for it.
func TestEverySupportedFormatHasAReader(t *testing.T) {
	for _, f := range Supported {
		if _, err := NewReader(f, strings.NewReader(""), minimalOptions(f)); err != nil {
			t.Errorf("%s is advertised but has no reader: %v", f, err)
		}
		if !strings.Contains(SchemaEnum(), `"`+f+`"`) {
			t.Errorf("%s is supported but missing from the config schema enum", f)
		}
	}
}

// TestFixedWidthRefusesWithoutALayout: every other format has a sensible
// default, and this one cannot — there are no delimiters to fall back on, so
// an absent layout has to be a config error rather than a guess.
func TestFixedWidthRefusesWithoutALayout(t *testing.T) {
	format := FIXEDW
	err := Validate("fs", &format, nil)
	if err == nil {
		t.Fatal("fixedw was accepted with no columns")
	}
	if !strings.Contains(err.Error(), "column layout") {
		t.Errorf("the refusal does not explain itself: %v", err)
	}
	if _, err := NewReader(FIXEDW, strings.NewReader(""), Options{}); err == nil {
		t.Error("a fixedw reader was built with no layout")
	}
	if _, err := NewWriter(FIXEDW, &bytes.Buffer{}, Options{}); err == nil {
		t.Error("a fixedw writer was built with no layout")
	}
}

// TestAMisspelledColumnTypeIsRejected: falling through to "string" would read
// every byte of the column and parse none of them, which looks like data that
// arrived wrong rather than a layout that is wrong.
func TestAMisspelledColumnTypeIsRejected(t *testing.T) {
	format := FIXEDW
	err := Validate("fs", &format, []Column{{Name: "a", Width: 4, Type: "decmial"}})
	if err == nil {
		t.Fatal("a misspelled column type was accepted")
	}
	for _, want := range []string{"decmial", "decimal"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should quote %q and offer the valid names", err, want)
		}
	}
}

// TestALayoutIsValidatedAtConfigTimeNotOnTheFirstRow — the structural rules
// belong to the engine, but a flow author should hear about them when they
// save the node.
func TestALayoutIsValidatedAtConfigTimeNotOnTheFirstRow(t *testing.T) {
	format := FIXEDW
	if err := Validate("fs", &format, []Column{{Name: "a", Width: 0}}); err == nil {
		t.Error("a zero-width column was accepted")
	}
	if err := Validate("fs", &format, []Column{{Name: "a", Width: 1}, {Name: "a", Width: 1}}); err == nil {
		t.Error("a duplicated column name was accepted")
	}
	if err := Validate("fs", &format, []Column{{Name: "a", Width: 2, Pad: "xy"}}); err == nil {
		t.Error("a multi-character pad was accepted")
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
	if err := Validate("fs", &empty, nil); err != nil || empty != Default {
		t.Fatalf("empty normalised to %q (err=%v), want %q", empty, err, Default)
	}

	bad := "parquet"
	err := Validate("sftp", &bad, nil)
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

// TestEveryColumnEnumIsRejectedByName: falling through to a default on any of
// these would read the column with the wrong rule and produce data that looks
// like it arrived wrong rather than a layout that is wrong.
func TestEveryColumnEnumIsRejectedByName(t *testing.T) {
	cases := []struct {
		name string
		col  Column
		want string
	}{
		{"type", Column{Name: "a", Width: 2, Type: "nope"}, "unknown type"},
		{"align", Column{Name: "a", Width: 2, Align: "middle"}, "unknown align"},
		{"trim", Column{Name: "a", Width: 2, Trim: "some"}, "unknown trim"},
		{"pad", Column{Name: "a", Width: 2, Pad: "xy"}, "single character"},
		{"scale", Column{Name: "a", Width: 2, Type: "decimal", Scale: 999}, "out of range"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			format := FIXEDW
			err := Validate("fs", &format, []Column{c.col})
			if err == nil {
				t.Fatalf("%s was accepted", c.name)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not contain %q", err, c.want)
			}
			// The connector is named, so an operator knows which node to fix.
			if !strings.Contains(err.Error(), "fs") {
				t.Errorf("error %q does not name the connector", err)
			}
		})
	}
}

// A filler column has no name, so errors about it have to say something.
func TestAnErrorAboutAFillerColumnSaysFiller(t *testing.T) {
	format := FIXEDW
	err := Validate("s3", &format, []Column{{Width: 2, Type: "nope"}})
	if err == nil {
		t.Fatal("an invalid filler column was accepted")
	}
	if !strings.Contains(err.Error(), "filler") {
		t.Errorf("error %q should identify the unnamed column as filler", err)
	}
}

// TestColumnsPropIsValidSchemaOfferingEveryType: the studio renders the config
// form from this, so an enum that disagrees with ColumnTypes would offer a
// value the connector then rejects.
func TestColumnsPropIsValidSchemaOfferingEveryType(t *testing.T) {
	var schema map[string]any
	if err := json.Unmarshal([]byte(ColumnsProp()), &schema); err != nil {
		t.Fatalf("ColumnsProp is not valid JSON: %v", err)
	}
	items, ok := schema["items"].(map[string]any)
	if !ok {
		t.Fatal("schema has no items")
	}
	props, ok := items["properties"].(map[string]any)
	if !ok {
		t.Fatal("items have no properties")
	}
	typeProp, ok := props["type"].(map[string]any)
	if !ok {
		t.Fatal("no type property")
	}
	enum, ok := typeProp["enum"].([]any)
	if !ok {
		t.Fatal("type property has no enum")
	}
	if len(enum) != len(ColumnTypes) {
		t.Fatalf("schema offers %d types, ColumnTypes has %d", len(enum), len(ColumnTypes))
	}
	for i, want := range ColumnTypes {
		if enum[i] != want {
			t.Errorf("enum[%d] = %v, want %q", i, enum[i], want)
		}
		// Every offered name must actually convert.
		format := FIXEDW
		if err := Validate("fs", &format, []Column{{Name: "a", Width: 4, Type: want}}); err != nil {
			t.Errorf("schema offers type %q but it is rejected: %v", want, err)
		}
	}
	// Width is the one thing a column cannot omit.
	req, ok := items["required"].([]any)
	if !ok || len(req) != 1 || req[0] != "width" {
		t.Errorf("items required = %v, want [width]", req)
	}
}

// TestWriterRejectsAnUnwritableFormatByName covers the writer's switch for the
// formats it cannot serve.
func TestWriterRejectsAnUnwritableFormatByName(t *testing.T) {
	if _, err := NewWriter(FIXEDW, &bytes.Buffer{}, Options{Columns: []Column{{Name: "a", Width: 0}}}); err == nil {
		t.Error("an invalid fixedw layout produced a writer")
	}
	if _, err := NewReader(FIXEDW, strings.NewReader(""), Options{Columns: []Column{{Name: "a", Width: 0}}}); err == nil {
		t.Error("an invalid fixedw layout produced a reader")
	}
}

// TestEverySupportedFormatHasAWriterOrSaysWhyNot is the writer half of the
// registry's promise. A format may legitimately be read-only, but then it must
// REFUSE with a reason — silence, or a writer that emits something invalid, is
// what this package exists to prevent.
func TestEverySupportedFormatHasAWriterOrSaysWhyNot(t *testing.T) {
	readOnly := map[string]bool{EDI: true}
	for _, f := range Supported {
		w, err := NewWriter(f, &bytes.Buffer{}, minimalOptions(f))
		if readOnly[f] {
			if err == nil {
				t.Errorf("%s is read-only but produced a writer", f)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s is advertised but has no writer: %v", f, err)
			continue
		}
		if w == nil {
			t.Errorf("%s returned a nil writer and no error", f)
		}
	}
}
