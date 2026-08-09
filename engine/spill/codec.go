package spill

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// Compact binary encoding for record values: one tag byte, then
// varint/fixed payloads. Used for spilled state and (later) the connector
// wire framing baseline.
// Tags are append-only: existing numbers are never reused or renumbered.
// There is deliberately no version header, because the spill store is a single
// scratch file unlinked on creation, so no reader can ever meet a file written
// by a different build (ADR-0051 §6). If this encoding is ever promoted to the
// connector wire — where two processes would have to agree — a header stops
// being optional and whoever promotes it owns adding one.
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
	// Appended for ADR-0051. Each carries its aux byte where it has one, so a
	// decimal's scale and a timestamp's offset survive a spill.
	tagDecimal   // zigzag varint coefficient + 1-byte scale
	tagTimestamp // zigzag varint unix nanos + zigzag varint zone offset seconds
	tagDate      // zigzag varint days since the epoch
	tagTime      // zigzag varint nanoseconds since midnight
)

// Encoder writes values to a byte-oriented writer.
type Encoder struct {
	w       io.Writer
	scratch [binary.MaxVarintLen64]byte
	// protocol1 restricts output to the tags that existed before ADR-0051.
	protocol1 bool
}

// NewEncoder wraps w (callers supply buffering; Store segments are
// buffered).
func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w} }

// NewEncoderProtocol1 wraps w and emits only the tags that existed in
// connector protocol version 1.
//
// It exists because this codec is not only the spill format: it is also the
// connector wire framing (ADR-0007), and a runner can legitimately be talking
// to a connector artifact built before the ADR-0051 kinds existed — signed
// versions stay resolvable and runnable by design (ADR-0047). Sending such a
// connector a decimal would fail on an unknown tag deep inside the subprocess's
// decoder. Refusing it here fails just as closed, but says why and names the
// fix.
func NewEncoderProtocol1(w io.Writer) *Encoder { return &Encoder{w: w, protocol1: true} }

// errKindNeedsProtocol2 reports a value the peer's protocol cannot carry.
func errKindNeedsProtocol2(k record.Kind) error {
	return fmt.Errorf("spill: cannot send a %v value over connector protocol 1: "+
		"the connector was built before exact decimal and temporal kinds existed "+
		"(rebuild it against the current SDK, or coerce the field to a string first)", k)
}

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

// zigzag writes a signed integer as a zigzag varint, so values near zero stay
// short whichever side of zero they are on.
func (e *Encoder) zigzag(v int64) error {
	return e.uvarint(uint64(v<<1) ^ uint64(v>>63)) //nolint:gosec // zigzag encoding is a deliberate bit transform
}

// aux writes a value's one-byte auxiliary payload (a decimal's scale, a
// timestamp's zone offset).
func (e *Encoder) aux(v int8) error {
	e.scratch[0] = byte(v) //nolint:gosec // int8 -> byte is lossless for all 256 values; Decoder.aux reverses it
	_, err := e.w.Write(e.scratch[:1])
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
		return e.zigzag(v.Int())
	case record.KindDecimal:
		if e.protocol1 {
			return errKindNeedsProtocol2(v.Kind())
		}
		if err := e.tag(tagDecimal); err != nil {
			return err
		}
		coef, scale := v.Decimal()
		if err := e.zigzag(coef); err != nil {
			return err
		}
		return e.aux(scale)
	case record.KindTimestamp:
		if e.protocol1 {
			return errKindNeedsProtocol2(v.Kind())
		}
		if err := e.tag(tagTimestamp); err != nil {
			return err
		}
		if err := e.zigzag(v.UnixNano()); err != nil {
			return err
		}
		// Offset in seconds rather than in record's 15-minute units, so the
		// quantum is not a constant two packages have to agree on. The value
		// is already quantised, so re-quantising on decode is exact.
		return e.zigzag(int64(v.ZoneOffset() / time.Second))
	case record.KindDate:
		if e.protocol1 {
			return errKindNeedsProtocol2(v.Kind())
		}
		if err := e.tag(tagDate); err != nil {
			return err
		}
		return e.zigzag(v.DateDays())
	case record.KindTime:
		if e.protocol1 {
			return errKindNeedsProtocol2(v.Kind())
		}
		if err := e.tag(tagTime); err != nil {
			return err
		}
		return e.zigzag(v.DayNanos())
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
		n, err := d.zigzag()
		if err != nil {
			return err
		}
		bld.Int(n)
	case tagDecimal:
		coef, err := d.zigzag()
		if err != nil {
			return err
		}
		scale, err := d.aux()
		if err != nil {
			return err
		}
		bld.Decimal(coef, scale)
	case tagTimestamp:
		nanos, err := d.zigzag()
		if err != nil {
			return err
		}
		offSecs, err := d.zigzag()
		if err != nil {
			return err
		}
		bld.Timestamp(nanos, time.Duration(offSecs)*time.Second)
	case tagDate:
		days, err := d.zigzag()
		if err != nil {
			return err
		}
		bld.Date(days)
	case tagTime:
		nanos, err := d.zigzag()
		if err != nil {
			return err
		}
		bld.TimeOfDay(nanos)
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

// zigzag reads one zigzag varint.
func (d *Decoder) zigzag() (int64, error) {
	u, err := binary.ReadUvarint(d.r)
	if err != nil {
		return 0, d.corrupt(err)
	}
	return int64(u>>1) ^ -int64(u&1), nil //nolint:gosec // reverses Encoder.zigzag
}

// aux reads one auxiliary payload byte (a decimal's scale).
func (d *Decoder) aux() (int8, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, d.corrupt(err)
	}
	return int8(b), nil //nolint:gosec // reverses Encoder.aux
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
