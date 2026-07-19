package ndjson

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/aaron-au/shift/engine/record"
)

// Writer streams record batches out as NDJSON. It implements stream.Sink.
type Writer struct {
	w       *bufio.Writer
	scratch []byte
}

// NewWriter wraps w with a buffered NDJSON encoder.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 256<<10)}
}

// Write encodes every record in the batch as one JSON line.
func (w *Writer) Write(ctx context.Context, b *record.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, rec := range b.Records() {
		if err := w.value(rec); err != nil {
			return err
		}
		if err := w.w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes buffered output. It does not close the underlying writer.
func (w *Writer) Close() error { return w.w.Flush() }

func (w *Writer) value(v record.Value) error {
	switch v.Kind() {
	case record.KindNull:
		_, err := w.w.WriteString("null")
		return err
	case record.KindBool:
		s := "false"
		if v.Bool() {
			s = "true"
		}
		_, err := w.w.WriteString(s)
		return err
	case record.KindInt:
		w.scratch = strconv.AppendInt(w.scratch[:0], v.Int(), 10)
		_, err := w.w.Write(w.scratch)
		return err
	case record.KindFloat:
		return w.float(v.Float())
	case record.KindString:
		return w.str(v.Bytes())
	case record.KindBytes:
		return w.bytes(v.Bytes())
	case record.KindList:
		if err := w.w.WriteByte('['); err != nil {
			return err
		}
		for i := range v.Len() {
			if i > 0 {
				if err := w.w.WriteByte(','); err != nil {
					return err
				}
			}
			if err := w.value(v.Index(i)); err != nil {
				return err
			}
		}
		return w.w.WriteByte(']')
	case record.KindMap:
		if err := w.w.WriteByte('{'); err != nil {
			return err
		}
		for i := range v.Len() {
			if i > 0 {
				if err := w.w.WriteByte(','); err != nil {
					return err
				}
			}
			if err := w.str(v.KeyAt(i)); err != nil {
				return err
			}
			if err := w.w.WriteByte(':'); err != nil {
				return err
			}
			if err := w.value(v.Index(i)); err != nil {
				return err
			}
		}
		return w.w.WriteByte('}')
	default:
		return fmt.Errorf("ndjson: cannot encode kind %v", v.Kind())
	}
}

func (w *Writer) float(f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// JSON has no NaN/Inf; match encoding/json by refusing.
		return fmt.Errorf("ndjson: unsupported float value %v", f)
	}
	// Match encoding/json's format choice: 'e' for very large/small
	// exponents, plain decimal otherwise.
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	w.scratch = strconv.AppendFloat(w.scratch[:0], f, format, -1, 64)
	_, err := w.w.Write(w.scratch)
	return err
}

const hexDigits = "0123456789abcdef"

// str writes a JSON string with a no-escape fast path.
func (w *Writer) str(s []byte) error {
	if err := w.w.WriteByte('"'); err != nil {
		return err
	}
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c < utf8.RuneSelf {
			i++
			continue
		}
		if c >= utf8.RuneSelf {
			// Valid UTF-8 passes through untouched; invalid bytes become
			// the replacement rune, matching encoding/json.
			r, size := utf8.DecodeRune(s[i:])
			if r == utf8.RuneError && size == 1 {
				if _, err := w.w.Write(s[start:i]); err != nil {
					return err
				}
				if _, err := w.w.WriteString("�"); err != nil {
					return err
				}
				i++
				start = i
				continue
			}
			i += size
			continue
		}
		if _, err := w.w.Write(s[start:i]); err != nil {
			return err
		}
		switch c {
		case '"':
			if _, err := w.w.WriteString(`\"`); err != nil {
				return err
			}
		case '\\':
			if _, err := w.w.WriteString(`\\`); err != nil {
				return err
			}
		case '\n':
			if _, err := w.w.WriteString(`\n`); err != nil {
				return err
			}
		case '\r':
			if _, err := w.w.WriteString(`\r`); err != nil {
				return err
			}
		case '\t':
			if _, err := w.w.WriteString(`\t`); err != nil {
				return err
			}
		default: // other control characters
			if _, err := w.w.WriteString(`\u00`); err != nil {
				return err
			}
			if err := w.w.WriteByte(hexDigits[c>>4]); err != nil {
				return err
			}
			if err := w.w.WriteByte(hexDigits[c&0xF]); err != nil {
				return err
			}
		}
		i++
		start = i
	}
	if _, err := w.w.Write(s[start:]); err != nil {
		return err
	}
	return w.w.WriteByte('"')
}

// bytes encodes KindBytes as a base64 string, like encoding/json does for
// []byte.
func (w *Writer) bytes(b []byte) error {
	if err := w.w.WriteByte('"'); err != nil {
		return err
	}
	enc := base64.NewEncoder(base64.StdEncoding, w.w)
	if _, err := enc.Write(b); err != nil {
		return err
	}
	if err := enc.Close(); err != nil {
		return err
	}
	return w.w.WriteByte('"')
}
