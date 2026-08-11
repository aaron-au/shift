package httpconn

import (
	"fmt"
	"io"
	"net/http"

	"github.com/aaron-au/shift/connectors/internal/decompress"
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
// disabled (client()), the source asks for gzip explicitly so well-behaved
// endpoints keep the bandwidth saving, and the inflated stream is metered
// against the wire bytes that produced it. The metering itself lives in
// connectors/internal/decompress, shared with s3conn and azureblobconn — the
// same transport shape gave azureblobconn the identical unbounded path, and one
// implementation is the only way three connectors do not drift.
const acceptEncoding = decompress.AcceptEncoding

// decodeBody returns the response body decoded according to Content-Encoding,
// with decompression bounded by the ratio, plus the bounded reader itself when
// one was interposed. The caller must consult that reader's Tripped on failure
// AND on EOF — see decompress.Reader for why.
func (c *commonConfig) decodeBody(resp *http.Response) (io.Reader, *decompress.Reader, error) {
	body, bounded, err := decompress.Body(
		resp.Header.Get("Content-Encoding"),
		resp.Body,
		decompress.Ratio(c.MaxDecompressionRatio),
		c.URL,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("http: %w", err)
	}
	return body, bounded, nil
}
