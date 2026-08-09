package csvf

import (
	"errors"
	"fmt"
	"io"
)

// ErrRecordTooLong is returned when one CSV row consumes more source bytes than
// MaxRecordBytes allows.
var ErrRecordTooLong = errors.New("csvf: record too long")

// recordBudget bounds the source bytes a SINGLE row may consume.
//
// Why this is needed at the io.Reader layer rather than after the parse:
// encoding/csv has no size limit of its own, and an opening quote that is never
// closed makes it read to EOF looking for the closing one. Checking the row
// after Read returns is too late — by then the whole field is already in
// memory, which is precisely the failure being prevented. The only place to
// stop it is on the way in.
//
// This is the same class of bug TC-003 found in the EDI reader, and the same
// shape as ndjson's MaxLineBytes: the streaming architecture bounds BATCHES, so
// the only bombs that get through are the ones where a single unit — one line,
// one field, one segment — is unbounded. In an iPaaS every byte is
// user-driven, fetched from a system the platform does not control, so "the
// source would not do that" is not an argument available to us.
//
// The count is reset by the Reader at each successful row boundary, so the
// budget is per row and not for the whole stream.
type recordBudget struct {
	r     io.Reader
	limit int64
	used  int64
}

func (b *recordBudget) Read(p []byte) (int, error) {
	if b.used >= b.limit {
		return 0, fmt.Errorf("%w: one row consumed more than %d bytes without ending; "+
			"the file is truncated mid-quote, uses a different delimiter, or is hostile",
			ErrRecordTooLong, b.limit)
	}
	// Never hand up more than the remaining budget, so the overshoot is bounded
	// by one read rather than by the caller's buffer size.
	if room := b.limit - b.used; int64(len(p)) > room {
		p = p[:room]
	}
	n, err := b.r.Read(p)
	b.used += int64(n)
	return n, err
}

// reset starts a fresh budget for the next row.
//
// It is deliberately called on a successful row and not on a batch boundary:
// the bound is "one row", and a batch of a thousand legitimate rows must not
// accumulate toward it. Note the count includes csv's read-ahead buffering, so
// the effective limit is the configured one plus at most one bufio fill — which
// is why the default is megabytes and not kilobytes.
func (b *recordBudget) reset() { b.used = 0 }
