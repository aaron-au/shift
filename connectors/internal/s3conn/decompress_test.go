package s3conn

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

// bytesAllocated is CUMULATIVE allocation, not live heap. Live heap is the
// wrong instrument here: a bomb that inflates through a streaming
// reader may hold only a few MiB at any instant and still cost the runner every
// one of those bytes in CPU and GC pressure. TotalAlloc sees the work; HeapAlloc
// sees a snapshot and can miss the whole event.
func bytesAllocated() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.TotalAlloc
}

// gzipBomb returns a gzip member whose inflated size is inflated bytes of
// NDJSON, and the compressed bytes that produce it.
func gzipBomb(t *testing.T, inflated int) []byte {
	t.Helper()
	var out bytes.Buffer
	zw, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	// One repeated NDJSON line: valid records, so nothing downstream rejects it
	// for being malformed. The bomb has to be plausible data, or the test proves
	// only that the parser refuses garbage.
	line := []byte(`{"a":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` + "\n")
	for n := 0; n < inflated; n += len(line) {
		if _, err := zw.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// TestAGzipEncodedObjectCannotInflateWithoutBound.
//
// s3conn never calls a decompressor, which is exactly why this needs asserting:
// Go's http.Transport adds `Accept-Encoding: gzip` on its own and transparently
// inflates the response, so the connector owns a decompression path it never
// opted into and cannot see in its own source. The bytes are attacker-chosen —
// an object's Content-Encoding is set by whoever wrote it to the bucket, which
// in a shared or partner-fed bucket is not the flow's author.
//
// The bound is a RATIO rather than an absolute cap for the reason httpconn
// records: streaming an arbitrarily large object is this connector's job, so
// volume is not the threat. Amplification is.
func TestAGzipEncodedObjectCannotInflateWithoutBound(t *testing.T) {
	const inflated = 64 << 20 // 64 MiB from a few KB on the wire
	bomb := gzipBomb(t, inflated)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write(bomb)
	}))
	defer srv.Close()

	cfg := fmt.Appendf(nil,
		`{"bucket":"b","access_key_id":"AK","secret_access_key":"SK","key":"k","format":"ndjson",`+
			`"endpoint":%q,"path_style":true,"allow_local":true,"region":"us-east-1","timeout_seconds":30}`,
		srv.URL)

	src := &getSource{}
	ctx := context.Background()
	if err := src.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = src.Close() }()

	before := bytesAllocated()
	var records int
	var lastErr error
	for {
		b, err := src.Next(ctx)
		if err != nil {
			lastErr = err
			break
		}
		records += b.Len()
	}
	after := bytesAllocated()
	grew := after - before

	t.Logf("wire bytes %d, inflated target %d MiB, records read %d, allocated %d MiB, ended with %v",
		len(bomb), inflated>>20, records, grew>>20, lastErr)

	if errors.Is(lastErr, io.EOF) {
		t.Fatalf("a %d-byte object inflated to %d MiB and was consumed in full (%d records, %d MiB allocated): "+
			"the transport's transparent gzip is unbounded",
			len(bomb), inflated>>20, records, grew>>20)
	}
	if !strings.Contains(lastErr.Error(), "max_decompression_ratio") {
		t.Fatalf("bomb stopped with %v, which is not the ratio bound", lastErr)
	}
	if want := inflated / 4 / 64; records > want {
		t.Fatalf("read %d records before the bound tripped (want well under %d): the ratio is being applied after buffering, not as the stream inflates", records, want)
	}
}

// TestALegitimatelyCompressedObjectStillReads. Bounding decompression must not
// cost the ability to read a gzip-encoded object — which, before this change,
// this connector could not do at all: the compressed bytes went straight to the
// record parser and surfaced as `unexpected character '\x1f'`.
func TestALegitimatelyCompressedObjectStillReads(t *testing.T) {
	var out bytes.Buffer
	zw, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	const want = 20000
	for i := range want {
		if _, err := fmt.Fprintf(zw, `{"i":%d,"name":"customer number %d","status":"active"}`+"\n", i, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	body := out.Bytes()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	cfg := fmt.Appendf(nil,
		`{"bucket":"b","access_key_id":"AK","secret_access_key":"SK","key":"k","format":"ndjson",`+
			`"endpoint":%q,"path_style":true,"allow_local":true,"region":"us-east-1","timeout_seconds":30}`,
		srv.URL)

	src := &getSource{}
	ctx := context.Background()
	if err := src.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = src.Close() }()

	var got int
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("a legitimately compressed object was refused after %d records: %v", got, err)
		}
		got += b.Len()
	}
	if got != want {
		t.Fatalf("read %d records, want %d", got, want)
	}
}
