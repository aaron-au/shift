package ndjson

import (
	"errors"

	"github.com/aaron-au/shift/engine/record"
)

// ParseValue parses data as exactly ONE complete JSON value, into b.
//
// It exists because neither reader answers the question "what does this whole
// request say":
//
//   - Reader is line-oriented, so a pretty-printed document is many records
//     and each is a parse error;
//   - JSONReader streams a top-level array as one record PER ELEMENT, which is
//     right for a payload stream and wrong for verification — a schema saying
//     {"type": "array", "minItems": 1} must see the array, not its first item.
//
// That distinction is exactly what ADR-0042's scope: body and scope: records
// select between, so both need a primitive of their own.
//
// Whitespace anywhere (including newlines) is skipped; trailing non-space data
// is an error rather than an ignored suffix, because a request with a second
// document stuck on the end is not a request anybody meant to send.
//
// The returned value is owned by b and is valid until b is reset.
func ParseValue(data []byte, b *record.Batch, opts ReaderOptions) (record.Value, error) {
	if b == nil {
		return record.Value{}, errors.New("ndjson: ParseValue needs a batch")
	}
	opts.defaults()
	var p parser
	return p.parseLine(data, b.Builder(), opts.MaxDepth)
}
