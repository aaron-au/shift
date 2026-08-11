// Package decompress bounds the inflation of a compressed response body.
//
// It exists because three connectors independently acquired a decompression
// path none of them wrote. Go's http.Transport advertises `Accept-Encoding:
// gzip` on its own and transparently inflates the reply, so any connector that
// builds a plain *http.Transport owns an unbounded decompressor it cannot see
// in its own source — and whether it is exposed depends on whether the SDK
// underneath happens to set that header itself. Measured on identical transport
// configuration: the AWS SDK sets it (s3conn never inflates), the Azure SDK does
// not (azureblobconn streamed 7,780,738 records out of 1,822,535 wire bytes, to
// completion, with no error).
//
// A property that holds by accident of a dependency is not a property. Every
// connector here now disables the transport's own compression, asks for gzip
// deliberately where it wants it, and meters what it gets.
//
// # Why a ratio and not a byte cap
//
// Streaming an arbitrarily large object is the job — ADR-0003's exit criterion
// is a 1 GB stream at bounded RSS, and the azureblob measurement above cost 0
// MiB of retained heap precisely because the streaming architecture worked.
// Volume is therefore not the threat and a byte cap would refuse legitimate
// work. The threat is AMPLIFICATION: a few hundred KB of attacker upload buying
// gigabytes of our CPU, GC and downstream record processing. A ratio bounds
// exactly that, and charges a hostile source real bandwidth per byte of our
// work.
//
// A connector that buffers a whole response rather than streaming it wants an
// absolute cap instead, applied to the decompressed stream — see soapconn's
// max_response_bytes. The two are not interchangeable: the right bound follows
// the shape of the consumer.
package decompress

import (
	"compress/gzip"
	"fmt"
	"io"
	"strings"
)

const (
	// DefaultMaxRatio bounds inflated bytes / wire bytes.
	//
	// Why 100: gzip over JSON/NDJSON in the field runs about 5-15x, and 20-30x
	// on records with heavily repeated keys. 100x is several times the worst
	// legitimate ratio observed, while gzip's theoretical maximum is 1032x — so
	// a real bomb (measured at 294x, 343x and 1029x) cannot get near it.
	DefaultMaxRatio = 100

	// FloorBytes is the inflated size below which the ratio is not enforced at
	// all. Small responses have a poor ratio for boring reasons (gzip framing, a
	// 200-byte body in a 300-byte member) and a tiny request that inflates to a
	// few MiB harms nobody. It only ever ADDS permission, so it cannot cause a
	// false rejection of a large legitimate transfer.
	FloorBytes = 8 << 20
)

// AcceptEncoding is what a caller should advertise once it has disabled the
// transport's own compression. Asking deliberately keeps the bandwidth saving
// with well-behaved endpoints while leaving this package the only thing that
// undoes it.
const AcceptEncoding = "gzip"

// countingReader counts the bytes that pass through it — the compressed bytes
// actually delivered over the wire.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Reader is a bounded decompressing reader. Consult Tripped on ANY failure and
// on EOF: a truncated stream does not reach the caller as this package's error,
// because the record readers sit on a bufio.Scanner or json.Decoder that still
// holds a partial line when the limit trips and they report THAT. Measured, an
// operator saw `ndjson: line 179179: column 11: expected ':'` for a gzip bomb —
// a parse error for a size problem, sending them hunting a data bug that does
// not exist.
type Reader struct {
	r      io.Reader // decompressed stream
	wire   *countingReader
	closer io.Closer
	ratio  int64
	source string // for the error message: a URL, a blob name, an object key
	out    int64
	err    error // sticky, once tripped
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.r.Read(p)
	r.out += int64(n)
	if r.out > FloorBytes && r.out > r.wire.n*r.ratio {
		// Drop the bytes that crossed the line rather than returning them with
		// the error: everything already handed out is within the allowance, so
		// nothing downstream ever sees more inflated bytes than we permitted.
		r.err = fmt.Errorf("refusing compressed response from %s: %d bytes on the wire inflated past %d bytes (max_decompression_ratio %d)",
			r.source, r.wire.n, r.out, r.ratio)
		return 0, r.err
	}
	return n, err
}

// Close closes the underlying body, when the caller handed one over.
func (r *Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// Tripped reports the ratio failure, if this reader stopped the stream.
func (r *Reader) Tripped() error { return r.err }

// Ratio returns the configured ratio, or the default when n is unset.
func Ratio(n int) int64 {
	if n > 0 {
		return int64(n)
	}
	return DefaultMaxRatio
}

// Gzip wraps body — the raw, still-compressed bytes — in a bounded gzip reader.
//
// The wire counter runs AHEAD of the bytes that produced the current output
// (the gzip reader buffers), which makes the check strictly permissive: it can
// never reject a stream that a whole-stream ratio would have accepted.
func Gzip(body io.Reader, ratio int64, source string) (*Reader, error) {
	wire := &countingReader{r: body}
	zr, err := gzip.NewReader(wire)
	if err != nil {
		return nil, fmt.Errorf("gzip response from %s: %w", source, err)
	}
	rd := &Reader{r: zr, wire: wire, ratio: ratio, source: source}
	if c, ok := body.(io.Closer); ok {
		rd.closer = c
	}
	return rd, nil
}

// Body decodes body according to the Content-Encoding value enc, bounding any
// decompression by ratio. An encoding the caller did not ask for is refused
// rather than handed to the record parser as garbage — before this, such a
// response surfaced as a confusing syntax error at byte one (measured:
// `unexpected character '\x1f'`, which is gzip's magic number).
//
// The returned Reader is nil when enc names no compression, so a caller that
// needs Tripped must keep the value it got here rather than re-deriving it.
func Body(enc string, body io.Reader, ratio int64, source string) (io.Reader, *Reader, error) {
	switch e := strings.ToLower(strings.TrimSpace(enc)); e {
	case "", "identity":
		return body, nil, nil
	case "gzip", "x-gzip":
		rd, err := Gzip(body, ratio, source)
		if err != nil {
			return nil, nil, err
		}
		return rd, rd, nil
	default:
		return nil, nil, fmt.Errorf("response from %s uses unsupported Content-Encoding %q (only gzip was offered)", source, e)
	}
}
