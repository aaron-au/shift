package spill

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/aaron-au/shift/engine/record"
)

// Compact binary encoding for record values: one tag byte, then
// varint/fixed payloads. Used for spilled state and (later) the connector
// wire framing baseline.
const (
	tagNull byte = iota
	tagFalse
	tagTrue
	tagInt    // zigzag varint
	tagFloat  // 8-byte LE
	tagString // uvarint len + bytes
	tagBytes
	tagList // uvarint count + values
	tagMap  // uvarint count + (uvarint len + key, value)...
)

// Encoder writes values to a byte-oriented writer.
type Encoder struct {
	w       io.Writer
	scratch [binary.MaxVarintLen64]byte
}

// NewEncoder wraps w (callers supply buffering; Store segments are
// buffered).
func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w} }

func (e *Encoder) tag(t byte) error {
	e.scratch[0] = t
	_, err := e.w.Write(e.scratch[:1])
	return err
}

func (e *Encoder) uvarint(v uint64) error {
	n := binary.PutUvarint(e.scratch[:], v)
	_, err := e.w.Write(e.scratch[:n])
	return err
}

// Encode writes one value.
func (e *Encoder) Encode(v record.Value) error {
	switch v.Kind() {
	case record.KindNull:
		return e.tag(tagNull)
	case record.KindBool:
		if v.Bool() {
			return e.tag(tagTrue)
		}
		return e.tag(tagFalse)
	case record.KindInt:
		if err := e.tag(tagInt); err != nil {
			return err
		}
		u := uint64(v.Int()<<1) ^ uint64(v.Int()>>63) //nolint:gosec // zigzag encoding is a deliberate bit transform
		return e.uvarint(u)
	case record.KindFloat:
		if err := e.tag(tagFloat); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(e.scratch[:8], math.Float64bits(v.Float()))
		_, err := e.w.Write(e.scratch[:8])
		return err
	case record.KindString, record.KindBytes:
		t := tagString
		if v.Kind() == record.KindBytes {
			t = tagBytes
		}
		if err := e.tag(t); err != nil {
			return err
		}
		b := v.Bytes()
		if err := e.uvarint(uint64(len(b))); err != nil { //nolint:gosec // len is never negative
			return err
		}
		_, err := e.w.Write(b)
		return err
	case record.KindList:
		if err := e.tag(tagList); err != nil {
			return err
		}
		if err := e.uvarint(uint64(v.Len())); err != nil { //nolint:gosec // Len is never negative
			return err
		}
		for i := range v.Len() {
			if err := e.Encode(v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	case record.KindMap:
		if err := e.tag(tagMap); err != nil {
			return err
		}
		if err := e.uvarint(uint64(v.Len())); err != nil { //nolint:gosec // Len is never negative
			return err
		}
		for i := range v.Len() {
			k := v.KeyAt(i)
			if err := e.uvarint(uint64(len(k))); err != nil { //nolint:gosec // len is never negative
				return err
			}
			if _, err := e.w.Write(k); err != nil {
				return err
			}
			if err := e.Encode(v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("spill: cannot encode kind %v", v.Kind())
	}
}

// Decoder reads values into a batch via its builder.
type Decoder struct {
	r       io.ByteReader
	rr      io.Reader
	scratch []byte
	maxLen  uint64
}

// NewDecoder wraps r, which must implement io.ByteReader (wrap with bufio
// otherwise). maxValueBytes bounds any single string/bytes payload as a
// corruption guard; <=0 uses 64 MiB.
func NewDecoder(r interface {
	io.Reader
	io.ByteReader
}, maxValueBytes int64) *Decoder {
	if maxValueBytes <= 0 {
		maxValueBytes = 64 << 20
	}
	return &Decoder{r: r, rr: r, maxLen: uint64(maxValueBytes)}
}

// Decode reads one value, building it in bld. Callers then call
// bld.Finish() (or compose within a larger construction). Returns io.EOF
// cleanly only at a value boundary.
func (d *Decoder) Decode(bld *record.Builder) error {
	t, err := d.r.ReadByte()
	if err != nil {
		return err // io.EOF at boundary is the stream-end signal
	}
	return d.decodeTagged(t, bld, 0)
}

func (d *Decoder) decodeTagged(t byte, bld *record.Builder, depth int) error {
	if depth > 64 {
		return errors.New("spill: nesting too deep")
	}
	switch t {
	case tagNull:
		bld.Null()
	case tagFalse:
		bld.Bool(false)
	case tagTrue:
		bld.Bool(true)
	case tagInt:
		u, err := binary.ReadUvarint(d.r)
		if err != nil {
			return d.corrupt(err)
		}
		bld.Int(int64(u>>1) ^ -int64(u&1)) // unzigzag
	case tagFloat:
		if err := d.fill(8); err != nil {
			return err
		}
		bld.Float(math.Float64frombits(binary.LittleEndian.Uint64(d.scratch[:8])))
	case tagString, tagBytes:
		b, err := d.blob()
		if err != nil {
			return err
		}
		if t == tagString {
			bld.String(b)
		} else {
			bld.Bytes(b)
		}
	case tagList:
		n, err := binary.ReadUvarint(d.r)
		if err != nil {
			return d.corrupt(err)
		}
		bld.BeginList()
		for range n {
			tt, err := d.r.ReadByte()
			if err != nil {
				return d.corrupt(err)
			}
			if err := d.decodeTagged(tt, bld, depth+1); err != nil {
				return err
			}
		}
		bld.EndList()
	case tagMap:
		n, err := binary.ReadUvarint(d.r)
		if err != nil {
			return d.corrupt(err)
		}
		bld.BeginMap()
		for range n {
			k, err := d.blob()
			if err != nil {
				return err
			}
			bld.Key(k)
			tt, err := d.r.ReadByte()
			if err != nil {
				return d.corrupt(err)
			}
			if err := d.decodeTagged(tt, bld, depth+1); err != nil {
				return err
			}
		}
		bld.EndMap()
	default:
		return fmt.Errorf("spill: unknown tag %d", t)
	}
	return nil
}

func (d *Decoder) blob() ([]byte, error) {
	n, err := binary.ReadUvarint(d.r)
	if err != nil {
		return nil, d.corrupt(err)
	}
	if n > d.maxLen {
		return nil, fmt.Errorf("spill: blob of %d bytes exceeds limit", n)
	}
	if err := d.fill(int(n)); err != nil { //nolint:gosec // n <= maxLen, which fits int
		return nil, err
	}
	return d.scratch[:n], nil
}

func (d *Decoder) fill(n int) error {
	if cap(d.scratch) < n {
		d.scratch = make([]byte, n)
	}
	d.scratch = d.scratch[:n]
	if _, err := io.ReadFull(d.rr, d.scratch); err != nil {
		return d.corrupt(err)
	}
	return nil
}

// corrupt maps mid-value EOF to ErrUnexpectedEOF so stream-end is only ever
// signalled at value boundaries.
func (d *Decoder) corrupt(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
