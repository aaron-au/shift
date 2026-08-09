package fileformat

import (
	"fmt"
	"strings"

	"github.com/aaron-au/shift/engine/format/fixedw"
)

// Column is the JSON-config shape of one fixed-width column.
//
// It exists so a connector never imports `fixedw` directly: the enum spellings
// a flow author types live here, next to the format list they belong to, and
// there is one place that maps them onto the engine's types. A connector that
// offers the fixed-width format offers this field and nothing more.
type Column struct {
	// Name is the record field name. Empty means filler — the bytes are
	// skipped on read and written as padding.
	Name string `json:"name,omitempty"`
	// Width is the column's size in bytes. Required.
	Width int `json:"width"`
	// Type is one of the ColumnTypes names (default "string").
	Type string `json:"type,omitempty"`
	// Scale is the implied decimal places for decimal/zoned columns.
	Scale int `json:"scale,omitempty"`
	// Align is "left" or "right" (default: right for numbers, left otherwise).
	Align string `json:"align,omitempty"`
	// Pad is the single padding character (default " ").
	Pad string `json:"pad,omitempty"`
	// Trim is "default", "both", "left", "right" or "none".
	Trim string `json:"trim,omitempty"`
	// Layout is the Go time layout for temporal columns.
	Layout string `json:"layout,omitempty"`
	// Location is the IANA zone a temporal column is written in ("" = UTC).
	Location string `json:"location,omitempty"`
}

// ColumnTypes are the legal Column.Type names, in the order a config form
// should offer them.
var ColumnTypes = []string{
	"string", "int", "float", "bool", "decimal", "zoned", "timestamp", "date", "time",
}

var columnTypeByName = map[string]fixedw.ColumnType{
	"":          fixedw.TypeString,
	"string":    fixedw.TypeString,
	"int":       fixedw.TypeInt,
	"float":     fixedw.TypeFloat,
	"bool":      fixedw.TypeBool,
	"decimal":   fixedw.TypeDecimal,
	"zoned":     fixedw.TypeZoned,
	"timestamp": fixedw.TypeTimestamp,
	"date":      fixedw.TypeDate,
	"time":      fixedw.TypeTime,
}

var alignByName = map[string]fixedw.Align{
	"":      fixedw.AlignDefault,
	"left":  fixedw.AlignLeft,
	"right": fixedw.AlignRight,
}

var trimByName = map[string]fixedw.Trim{
	"":        fixedw.TrimDefault,
	"default": fixedw.TrimDefault,
	"both":    fixedw.TrimBoth,
	"left":    fixedw.TrimLeft,
	"right":   fixedw.TrimRight,
	"none":    fixedw.TrimNone,
}

// toEngine converts the config columns into an engine layout, rejecting
// unknown enum values by name rather than falling through to a default — a
// misspelled type that silently became "string" would read every byte of the
// column and parse none of them.
func toEngine(connector string, cols []Column) ([]fixedw.Column, error) {
	out := make([]fixedw.Column, len(cols))
	for i, c := range cols {
		t, ok := columnTypeByName[c.Type]
		if !ok {
			return nil, fmt.Errorf("%s: column %d (%s): unknown type %q (want %s)",
				connector, i, columnName(c), c.Type, joinQuoted(ColumnTypes))
		}
		a, ok := alignByName[c.Align]
		if !ok {
			return nil, fmt.Errorf("%s: column %d (%s): unknown align %q (want \"left\" or \"right\")",
				connector, i, columnName(c), c.Align)
		}
		tr, ok := trimByName[c.Trim]
		if !ok {
			return nil, fmt.Errorf("%s: column %d (%s): unknown trim %q (want default, both, left, right or none)",
				connector, i, columnName(c), c.Trim)
		}
		if len(c.Pad) > 1 {
			return nil, fmt.Errorf("%s: column %d (%s): pad %q must be a single character",
				connector, i, columnName(c), c.Pad)
		}
		if c.Scale < -128 || c.Scale > 127 {
			return nil, fmt.Errorf("%s: column %d (%s): scale %d is out of range",
				connector, i, columnName(c), c.Scale)
		}
		var pad byte
		if c.Pad != "" {
			pad = c.Pad[0]
		}
		out[i] = fixedw.Column{
			Name:     c.Name,
			Width:    c.Width,
			Type:     t,
			Scale:    int8(c.Scale), //nolint:gosec // range-checked immediately above
			Align:    a,
			Pad:      pad,
			Trim:     tr,
			Layout:   c.Layout,
			Location: c.Location,
		}
	}
	// The engine owns the structural rules (positive widths, unique names), so
	// resolving here surfaces them at config time rather than on the first row.
	if _, err := fixedw.Length(out); err != nil {
		return nil, fmt.Errorf("%s: %w", connector, err)
	}
	return out, nil
}

func columnName(c Column) string {
	if c.Name == "" {
		return "filler"
	}
	return c.Name
}

func joinQuoted(vals []string) string {
	quoted := make([]string, len(vals))
	for i, v := range vals {
		quoted[i] = `"` + v + `"`
	}
	return strings.Join(quoted, ", ")
}

// ColumnsProp renders the fixed-width layout field for a connector's config
// schema. Kept beside SchemaEnum for the same reason as RecordElementProp: a
// connector that offers the format must offer the one setting it cannot work
// without.
func ColumnsProp() string {
	return `{"type": "array", "title": "Fixed-width columns",
      "description": "Column layout, in order (fixedw only). Columns are contiguous; a column with no name is filler. Required when format is fixedw.",
      "items": {"type": "object", "required": ["width"], "properties": {
        "name": {"type": "string", "title": "Field name", "description": "Record field name; empty means filler (skipped on read, padded on write)"},
        "width": {"type": "integer", "title": "Width", "description": "Column size in bytes", "minimum": 1},
        "type": {"type": "string", "title": "Type", "enum": [` + joinQuoted(ColumnTypes) + `], "default": "string"},
        "scale": {"type": "integer", "title": "Implied decimals", "description": "Decimal places implied by the layout when the digits carry no point (decimal/zoned)"},
        "align": {"type": "string", "title": "Alignment", "enum": ["left", "right"], "description": "Defaults to right for numbers, left otherwise"},
        "pad": {"type": "string", "title": "Pad character", "description": "Single padding character (default space)", "maxLength": 1},
        "trim": {"type": "string", "title": "Trim", "enum": ["default", "both", "left", "right", "none"], "description": "Padding stripped on read; default picks by pad character so a zero-padded number keeps its trailing zeros"},
        "layout": {"type": "string", "title": "Time layout", "description": "Go time layout for temporal columns (default 20060102 / 150405 / 20060102150405)"},
        "location": {"type": "string", "title": "Time zone", "description": "IANA zone the column is written in; empty is UTC"}
      }}}`
}
