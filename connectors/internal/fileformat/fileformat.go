// Package fileformat is the one place that knows which record formats a
// file-shaped connector can read and write.
//
// It exists because that knowledge was previously copied into every such
// connector — fs, sftp, ftp, s3, azureblob each carried its own format enum,
// its own validation with its own error wording, its own reader switch and its
// own writer switch. Adding a format meant twenty edits across five packages,
// and the failure mode of a missed one is silent: the config schema advertises
// a format, the reader switch falls through to its default, and the file is
// parsed as NDJSON without complaint.
//
// So the format set lives here, and a connector delegates. Adding a format is
// one edit, and a connector that forgets to handle it fails loudly rather than
// guessing.
package fileformat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/edi"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/format/xmlf"
	"github.com/aaron-au/shift/engine/record"
)

// The supported formats.
const (
	NDJSON = "ndjson"
	CSV    = "csv"
	XML    = "xml"
	// EDI covers both wire syntaxes; which one a file uses is detected from
	// its own interchange header, never configured.
	EDI = "edi"

	// Default is what an unset format means. NDJSON, because it is the only
	// one of the three that is lossless for the hierarchical record model:
	// CSV flattens and XML stringifies.
	Default = NDJSON
)

// Supported lists every format, in the order a config form should offer them.
var Supported = []string{NDJSON, CSV, XML, EDI}

// SchemaEnum renders the format field for a connector's JSON config schema, so
// the studio's dropdown and this package's validation cannot disagree.
func SchemaEnum() string {
	quoted := make([]string, len(Supported))
	for i, f := range Supported {
		quoted[i] = `"` + f + `"`
	}
	return `{"type": "string", "title": "Format", "enum": [` +
		strings.Join(quoted, ", ") + `], "default": "` + Default + `"}`
}

// Reader emits batches until io.EOF. The batch is valid only until the next
// Next call (the reader reuses it) — the engine's batch-lifetime contract.
type Reader interface {
	Next(ctx context.Context) (*record.Batch, error)
}

// Writer consumes batches and finalises on Close.
type Writer interface {
	Write(ctx context.Context, b *record.Batch) error
	Close() error
}

// Validate normalises an unset format to the default and rejects an unknown
// one. connector names the connector for the error message ("sftp", "s3"), so
// an operator is told which node is misconfigured.
//
// It takes a pointer because normalising in place is the point: every caller
// would otherwise have to remember to write the default back, and one that
// forgot would pass "" to Open and silently get NDJSON.
func Validate(connector string, format *string) error {
	if *format == "" {
		*format = Default
		return nil
	}
	if !slices.Contains(Supported, *format) {
		return fmt.Errorf("%s: unsupported format %q (want %s)",
			connector, *format, strings.Join(Supported, ", "))
	}
	return nil
}

// Options carry the per-format settings a connector exposes. Zero values take
// each format's own defaults.
type Options struct {
	// Comma overrides the CSV delimiter.
	Comma rune
	// RecordElement is the XML element delimiting one record on read, and
	// naming one record on write.
	RecordElement string
	// RootElement wraps XML output.
	RootElement string
	// NoHeader suppresses the CSV header row on write and treats the first
	// row as data on read.
	NoHeader bool
}

// NewReader builds the reader for a validated format.
//
// An unknown format is an ERROR rather than a fall-through to NDJSON. That is
// the whole reason this package exists: a default case that quietly parsed
// something else is how a misconfigured format reaches production looking like
// corrupt data rather than a config mistake.
func NewReader(format string, r io.Reader, opts Options) (Reader, error) {
	switch format {
	case NDJSON:
		return ndjson.NewReader(r, ndjson.ReaderOptions{}), nil
	case CSV:
		return csvf.NewReader(r, csvf.ReaderOptions{Comma: opts.Comma, NoHeader: opts.NoHeader}), nil
	case XML:
		return xmlf.NewReader(r, xmlf.ReaderOptions{RecordElement: opts.RecordElement}), nil
	case EDI:
		return edi.NewReader(r, edi.ReaderOptions{}), nil
	default:
		return nil, fmt.Errorf("fileformat: no reader for format %q", format)
	}
}

// NewWriter builds the writer for a validated format.
func NewWriter(format string, w io.Writer, opts Options) (Writer, error) {
	switch format {
	case NDJSON:
		return ndjson.NewWriter(w), nil
	case CSV:
		return csvf.NewWriter(w, csvf.WriterOptions{Comma: opts.Comma}), nil
	case XML:
		return xmlf.NewWriter(w, xmlf.WriterOptions{
			RootElement:   opts.RootElement,
			RecordElement: opts.RecordElement,
		}), nil
	case EDI:
		// Read-only for now. Writing EDI means composing envelopes with
		// correct control numbers and segment counts — a semantic layer, not a
		// serialiser — so a node that offers it must fail here rather than
		// write something a trading partner will reject.
		return nil, errors.New("fileformat: EDI is read-only; writing an interchange needs envelope " +
			"construction (control numbers, segment counts) that this connector does not do")
	default:
		return nil, fmt.Errorf("fileformat: no writer for format %q", format)
	}
}

// RecordElementProp is the XML record-element field for a connector's config
// schema. Kept here beside SchemaEnum so the two stay consistent: a connector
// that offers the XML format must also offer the one setting XML needs.
const RecordElementProp = `{"type": "string", "title": "XML record element", ` +
	`"description": "Element delimiting one record (XML only); empty treats each child of the root as a record"}`
