package ndjson

import (
	"errors"
	"fmt"
	"io"
)

// ErrValueTooLong is returned when one top-level JSON value consumes more
// source bytes than MaxLineBytes allows.
var ErrValueTooLong = errors.New("ndjson: value too long")

// valueBudget bounds the source bytes a SINGLE top-level value may consume.
//
// The line-based Reader has MaxLineBytes and a bufio.Scanner to enforce it. The
// JSONReader has neither: it hands the stream to encoding/json, which
// materialises one whole value into a json.RawMessage before anything else gets
// a say. A pretty-printed document has no lines to bound, so a single value was
// the unbounded unit — the shape TC-019 exists to close, and the same shape as
// the EDI segment (TC-003) and the CSV field (TC-019).
//
// Measured before this existed: 521,845 gzipped wire bytes expanded into a
// single JSON value produced 2,561 MiB of peak heap. Streaming does not help
// when the unit itself is the payload.
//
// Bounding at the io.Reader is the only placement that works. Checking the size
// after Decode returns is too late: the allocation has already happened, which
// is the thing being prevented.
type valueBudget struct {
	r     io.Reader
	limit int64
	used  int64
}

func (b *valueBudget) Read(p []byte) (int, error) {
	if b.used >= b.limit {
		return 0, fmt.Errorf("%w: one value consumed more than %d bytes; the document is "+
			"truncated, malformed, or hostile", ErrValueTooLong, b.limit)
	}
	if room := b.limit - b.used; int64(len(p)) > room {
		p = p[:room]
	}
	n, err := b.r.Read(p)
	b.used += int64(n)
	return n, err
}

// reset starts a fresh budget for the next value. Called on a successful decode
// so the bound is per value: an array of a million legitimate objects must
// stream, and only one oversized member is refused.
//
// The count includes bufio read-ahead, so the effective limit is the configured
// one plus at most one buffer fill — which is why the default is megabytes.
func (b *valueBudget) reset() { b.used = 0 }
