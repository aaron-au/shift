package httpconn

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Compression-bomb defence (TC-020).
//
// Go's http.Transport transparently inflates a gzip response whenever it set
// Accept-Encoding itself, which meant this connector had a decompression path
// it never opted into and could not bound: measured, a 521,845-byte response
// inflated to 512 MiB and drove 2,561 MiB of heap, and a 2.9 MB response fed
// 985 MB (21M records) into the pipeline. The wire cost to the attacker was
// under 3 MB in both cases.
//
// The fix is to take the decision back: the transport's own compression is
// disabled, the source asks for gzip explicitly (so well-behaved endpoints keep
// the bandwidth saving), and the inflated stream is metered against the wire
// bytes that produced it.
const (
	// defaultMaxDecompressionRatio bounds inflated bytes / wire bytes.
	//
	// Why a RATIO and not an absolute cap: streaming an arbitrarily large
	// response is the point of this connector (ADR-0003's exit criterion is a
	// 1 GB stream), and volume alone is not the threat — 985 MB streamed at
	// 8 MiB of heap. The threat is AMPLIFICATION: a few KB of attacker upload
	// turning into gigabytes of our memory and CPU. A ratio bounds exactly
	// that, and costs a hostile source real bandwidth per byte of our work.
	//
	// Why 100: gzip over JSON/NDJSON in the field runs about 5-15x, and 20-30x
	// on records with heavily repeated keys. 100x is several times the worst
	// legitimate ratio observed, while gzip's theoretical maximum is 1032x —
	// so a real bomb (measured at 343x and 1029x) cannot get near it.
	// Configurable via max_decompression_ratio for the rare corpus that
	// genuinely compresses better.
	defaultMaxDecompressionRatio = 100

	// decompressionFloorBytes is the inflated size below which the ratio is not
	// enforced at all. Small responses have a poor ratio for boring reasons
	// (gzip framing, a 200-byte body in a 300-byte member) and a tiny request
	// that inflates to a few MiB harms nobody. It only ever ADDS permission,
	// so it cannot cause a false rejection of a large legitimate transfer.
	decompressionFloorBytes = 8 << 20
)

// countingReader counts the bytes that pass through it — here, the compressed
// bytes actually delivered over the wire.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// ratioLimitedReader fails the stream as soon as the decompressed output
// exceeds ratio times the compressed input consumed so far. The check is per
// Read, not at the end, so the bomb is stopped while it is inflating rather
// than after — the whole point is never to hold the inflated bytes.
//
// The wire counter runs AHEAD of the bytes that produced the current output
// (the gzip reader buffers), which makes the check strictly permissive: it can
// never reject a stream that a whole-stream ratio would have accepted.
type ratioLimitedReader struct {
	r     io.Reader // decompressed stream
	wire  *countingReader
	ratio int64
	url   string
	out   int64
	err   error // sticky, once tripped
}

func (r *ratioLimitedReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.r.Read(p)
	r.out += int64(n)
	if r.out > decompressionFloorBytes && r.out > r.wire.n*r.ratio {
		// Drop the bytes that crossed the line rather than returning them with
		// the error: everything already handed out is within the allowance, so
		// nothing downstream ever sees more inflated bytes than we permitted.
		r.err = fmt.Errorf("http: refusing compressed response from %s: %d bytes on the wire inflated past %d bytes (max_decompression_ratio %d)",
			r.url, r.wire.n, r.out, r.ratio)
		return 0, r.err
	}
	return n, err
}

// tripped reports the ratio failure, if this reader stopped the stream.
//
// It exists because a truncated stream does not reach the caller as our error:
// the record readers sit on a bufio.Scanner / json.Decoder that still holds a
// partial line when the limit trips, and they report THAT — measured, the
// operator saw `ndjson: line 179179: column 11: expected ':'` for a gzip bomb.
// A parse error for a size problem sends the reader hunting a data bug that
// does not exist. The source consults this on every failure AND on EOF, so a
// bomb can never end the stream cleanly either.
func (r *ratioLimitedReader) tripped() error { return r.err }

// maxDecompressionRatio returns the configured ratio, or the default.
func (c *commonConfig) maxDecompressionRatio() int64 {
	if c.MaxDecompressionRatio > 0 {
		return int64(c.MaxDecompressionRatio)
	}
	return defaultMaxDecompressionRatio
}

// acceptEncoding is what the source advertises. The transport no longer adds
// its own (client() sets DisableCompression), so this is the only encoding a
// well-behaved server may use, and decodeBody is the only thing that undoes it.
const acceptEncoding = "gzip"

// decodeBody returns the response body decoded according to Content-Encoding,
// with decompression bounded by the ratio. An encoding we did not ask for is
// refused rather than handed to the record parser as garbage — before this,
// such a response surfaced as a confusing JSON syntax error.
func (c *commonConfig) decodeBody(resp *http.Response) (io.Reader, error) {
	switch enc := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))); enc {
	case "", "identity":
		return resp.Body, nil
	case "gzip", "x-gzip":
		wire := &countingReader{r: resp.Body}
		zr, err := gzip.NewReader(wire)
		if err != nil {
			return nil, fmt.Errorf("http: gzip response from %s: %w", c.URL, err)
		}
		return &ratioLimitedReader{r: zr, wire: wire, ratio: c.maxDecompressionRatio(), url: c.URL}, nil
	default:
		return nil, fmt.Errorf("http: response from %s uses unsupported Content-Encoding %q (only gzip was offered)", c.URL, enc)
	}
}
