package decompress

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"strings"
	"testing"
)

// gzipOf returns a gzip member of n copies of unit, plus the inflated length.
func gzipOf(t *testing.T, unit []byte, n int) (wire []byte, inflated int) {
	t.Helper()
	var out bytes.Buffer
	zw, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	for range n {
		if _, err := zw.Write(unit); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), len(unit) * n
}

func TestAStreamWithinTheRatioIsDeliveredWhole(t *testing.T) {
	// Mixed content compresses ~4x — an ordinary payload, nowhere near the bound.
	var raw bytes.Buffer
	for i := range 20000 {
		raw.WriteString(strings.Repeat("x", i%17))
		raw.WriteString("-record-\n")
	}
	var out bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&out, gzip.BestCompression)
	_, _ = zw.Write(raw.Bytes())
	_ = zw.Close()

	rd, err := Gzip(bytes.NewReader(out.Bytes()), DefaultMaxRatio, "src")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("a %dx stream was refused at a %dx bound: %v", raw.Len()/out.Len(), DefaultMaxRatio, err)
	}
	if !bytes.Equal(got, raw.Bytes()) {
		t.Fatalf("read %d bytes, want %d — the bounded reader altered the stream", len(got), raw.Len())
	}
	if rd.Tripped() != nil {
		t.Fatalf("Tripped reports %v on a stream that completed", rd.Tripped())
	}
}

func TestABombIsStoppedWhileItIsStillInflating(t *testing.T) {
	wire, inflated := gzipOf(t, bytes.Repeat([]byte("a"), 1<<20), 512) // 512 MiB
	if ratio := inflated / len(wire); ratio <= DefaultMaxRatio {
		t.Fatalf("test bomb amplifies only %dx; it must exceed the %dx bound to prove anything", ratio, DefaultMaxRatio)
	}

	rd, err := Gzip(bytes.NewReader(wire), DefaultMaxRatio, "src")
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, rd)
	if err == nil {
		t.Fatalf("a %d-byte bomb inflating to %d MiB was read in full", len(wire), inflated>>20)
	}
	if !strings.Contains(err.Error(), "max_decompression_ratio") {
		t.Fatalf("bomb refused as %q, not as a ratio problem", err)
	}
	// The bound must act DURING inflation. The floor permits 8 MiB, and the
	// wire counter runs ahead of the output, so allow generous headroom — but
	// nothing close to the full 512 MiB.
	if n > 64<<20 {
		t.Fatalf("delivered %d MiB before stopping: the ratio is being applied after buffering", n>>20)
	}
	if rd.Tripped() == nil {
		t.Fatal("Tripped reports nothing after the stream was stopped by the bound")
	}
}

// TestTheErrorIsStickyOnceTripped. The record readers keep calling Read after a
// failure, and a reader that resumed delivering bytes after tripping would hand
// downstream exactly the inflated data the bound exists to withhold.
func TestTheErrorIsStickyOnceTripped(t *testing.T) {
	wire, _ := gzipOf(t, bytes.Repeat([]byte("a"), 1<<20), 512)
	rd, err := Gzip(bytes.NewReader(wire), DefaultMaxRatio, "src")
	if err != nil {
		t.Fatal(err)
	}
	_, first := io.Copy(io.Discard, rd)
	if first == nil {
		t.Fatal("bomb was not stopped")
	}
	buf := make([]byte, 4096)
	for range 3 {
		n, err := rd.Read(buf)
		if n != 0 || err == nil {
			t.Fatalf("Read after a tripped bound returned (%d, %v), want (0, the sticky error)", n, err)
		}
	}
}

// TestTheFloorOnlyEverAddsPermission. A small body with a poor ratio is a
// boring event (gzip framing on a 200-byte payload), and rejecting it would be
// a false positive on traffic that harms nobody.
func TestTheFloorOnlyEverAddsPermission(t *testing.T) {
	wire, _ := gzipOf(t, bytes.Repeat([]byte("a"), 1<<20), 4) // 4 MiB, under the floor
	rd, err := Gzip(bytes.NewReader(wire), 1, "src")          // ratio 1: everything exceeds it
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, rd)
	if err != nil {
		t.Fatalf("a %d MiB body under the %d MiB floor was refused at ratio 1: %v", n>>20, FloorBytes>>20, err)
	}
}

func TestAnEncodingWeDidNotAskForIsRefusedRatherThanParsed(t *testing.T) {
	body, bounded, err := Body("br", strings.NewReader("whatever"), DefaultMaxRatio, "src")
	if err == nil {
		t.Fatal("an unsupported Content-Encoding was accepted; the record parser would see compressed bytes as a syntax error")
	}
	if body != nil || bounded != nil {
		t.Fatal("a refused encoding still returned a readable body")
	}
	if !strings.Contains(err.Error(), "br") {
		t.Fatalf("error %q does not name the offending encoding", err)
	}
}

func TestAnIdentityBodyPassesThroughUnwrapped(t *testing.T) {
	for _, enc := range []string{"", "identity", "  IDENTITY  "} {
		body, bounded, err := Body(enc, strings.NewReader("hello"), DefaultMaxRatio, "src")
		if err != nil {
			t.Fatalf("enc %q: %v", enc, err)
		}
		if bounded != nil {
			t.Fatalf("enc %q: an uncompressed body was wrapped in a decompressor", enc)
		}
		got, _ := io.ReadAll(body)
		if string(got) != "hello" {
			t.Fatalf("enc %q: body = %q", enc, got)
		}
	}
}

func TestGzipIsRecognisedUnderItsAliasAndCasing(t *testing.T) {
	wire, _ := gzipOf(t, []byte("hello\n"), 1)
	for _, enc := range []string{"gzip", "GZIP", " x-gzip "} {
		_, bounded, err := Body(enc, bytes.NewReader(wire), DefaultMaxRatio, "src")
		if err != nil {
			t.Fatalf("enc %q: %v", enc, err)
		}
		if bounded == nil {
			t.Fatalf("enc %q was accepted but not metered — the bound would be silently absent", enc)
		}
	}
}

func TestACorruptGzipHeaderIsAnError(t *testing.T) {
	if _, _, err := Body("gzip", strings.NewReader("not gzip at all"), DefaultMaxRatio, "src"); err == nil {
		t.Fatal("a body that is not gzip was accepted as gzip")
	}
}

func TestRatioFallsBackToTheDefault(t *testing.T) {
	for _, n := range []int{0, -1} {
		if got := Ratio(n); got != DefaultMaxRatio {
			t.Fatalf("Ratio(%d) = %d, want the default %d", n, got, DefaultMaxRatio)
		}
	}
	if got := Ratio(7); got != 7 {
		t.Fatalf("Ratio(7) = %d, want 7", got)
	}
}

// TestCloseReachesTheUnderlyingBody: the connectors hand over an
// http.Response.Body and rely on this to release the connection.
func TestCloseReachesTheUnderlyingBody(t *testing.T) {
	wire, _ := gzipOf(t, []byte("hello\n"), 1)
	src := &closeSpy{Reader: bytes.NewReader(wire)}
	rd, err := Gzip(src, DefaultMaxRatio, "src")
	if err != nil {
		t.Fatal(err)
	}
	if err := rd.Close(); err != nil {
		t.Fatal(err)
	}
	if !src.closed {
		t.Fatal("Close did not reach the underlying body; the connection would leak")
	}
}

func TestCloseIsSafeWhenTheSourceIsNotACloser(t *testing.T) {
	wire, _ := gzipOf(t, []byte("hello\n"), 1)
	rd, err := Gzip(bytes.NewReader(wire), DefaultMaxRatio, "src")
	if err != nil {
		t.Fatal(err)
	}
	if err := rd.Close(); err != nil {
		t.Fatalf("Close on a non-closing source = %v, want nil", err)
	}
}

type closeSpy struct {
	io.Reader
	closed bool
}

func (c *closeSpy) Close() error { c.closed = true; return nil }

// TestReadSurfacesTheUnderlyingErrorUnchanged: a network failure mid-stream is
// not a ratio problem and must not be reported as one.
func TestReadSurfacesTheUnderlyingErrorUnchanged(t *testing.T) {
	wire, _ := gzipOf(t, bytes.Repeat([]byte("a"), 4096), 4)
	boom := errors.New("connection reset")
	rd, err := Gzip(io.MultiReader(bytes.NewReader(wire[:len(wire)/2]), &errReader{err: boom}), DefaultMaxRatio, "src")
	if err != nil {
		t.Fatal(err)
	}
	_, got := io.Copy(io.Discard, rd)
	if !errors.Is(got, boom) {
		t.Fatalf("read error = %v, want the underlying %v", got, boom)
	}
	if rd.Tripped() != nil {
		t.Fatalf("Tripped reports %v for a transport failure that had nothing to do with the bound", rd.Tripped())
	}
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }
