package azureblobconn

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

// bytesAllocated is CUMULATIVE allocation, not live heap — a bomb that inflates
// through a streaming reader may hold only a few MiB at any instant and still
// cost the runner every byte in CPU and GC pressure.
func bytesAllocated() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.TotalAlloc
}

func gzipBomb(t *testing.T, inflated int) []byte {
	t.Helper()
	var out bytes.Buffer
	zw, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
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

// TestAGzipEncodedBlobCannotInflateWithoutBound is the azureblob half of the
// TC-020 audit, and the reason it is a test rather than a code reading: this
// connector never calls a decompressor, so the only way it can inflate anything
// is a behaviour of the transport or the SDK underneath it. That makes the
// property a DEPENDENCY property — true today because of code we do not own —
// and the only honest way to hold it is to measure it and let an SDK bump break
// the test.
//
// A blob's Content-Encoding is chosen by whoever wrote it. In a partner-fed or
// shared container that is not the flow's author.
func TestAGzipEncodedBlobCannotInflateWithoutBound(t *testing.T) {
	const inflated = 512 << 20
	bomb := gzipBomb(t, inflated)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("x-ms-blob-type", "BlockBlob")
		_, _ = w.Write(bomb)
	}))
	defer srv.Close()

	cfg := fmt.Appendf(nil,
		`{"account":"acct","account_key":"dGVzdGtleQ==","container":"c","blob":"b","format":"ndjson",`+
			`"endpoint":%q,"allow_local":true}`, srv.URL)

	src := &getSource{}
	ctx := context.Background()
	if err := src.Open(ctx, cfg); err != nil {
		t.Skipf("azureblob get source would not open against a stub endpoint: %v", err)
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
		t.Fatalf("a %d-byte blob inflated to %d MiB and was consumed in full (%d records, %d MiB allocated): "+
			"transparent gzip is unbounded here",
			len(bomb), inflated>>20, records, grew>>20)
	}
	// Assert the REASON, not merely that something failed. Any bug that broke
	// the download would also end in a non-EOF error and would otherwise pass
	// this test while proving nothing about the bound.
	if !strings.Contains(lastErr.Error(), "max_decompression_ratio") {
		t.Fatalf("bomb stopped with %v, which is not the ratio bound — a size problem must not surface as some other failure", lastErr)
	}
	// The whole point is stopping WHILE it inflates. The floor allows 8 MiB, so
	// a bound applied only at the end would show the full 7.7M records.
	if want := inflated / 4 / 64; records > want {
		t.Fatalf("read %d records before the bound tripped (want well under %d): the ratio is being applied after buffering, not as the stream inflates", records, want)
	}
}

// TestALegitimatelyCompressedBlobStillReads is the other half, and the reason
// the bound is a ratio: a blob that compresses well is ordinary, and refusing
// it would break real integrations. This one compresses about 30x — above
// anything gzip achieves on mixed data, below the bound.
func TestALegitimatelyCompressedBlobStillReads(t *testing.T) {
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
		`{"account":"acct","account_key":"dGVzdGtleQ==","container":"c","blob":"b","format":"ndjson",`+
			`"endpoint":%q,"allow_local":true}`, srv.URL)

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
			t.Fatalf("a legitimately compressed blob was refused after %d records: %v", got, err)
		}
		got += b.Len()
	}
	if got != want {
		t.Fatalf("read %d records, want %d", got, want)
	}
}
