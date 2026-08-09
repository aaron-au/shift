package httpconn

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// Hostile-response limits (TC-020 decompression, TC-021 endless streams).
//
// The threat model is the whole point of the connector: every byte here comes
// from a system neither we nor the customer controls, so "the endpoint would
// not do that" is never an argument. Each test below drives a real httptest
// server that misbehaves in one specific way.

// gzipOf returns a gzip stream that inflates to unit repeated n times, plus the
// inflated size, so a test can state the amplification it is defending against.
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

// gzipServer serves a fixed gzip body with the given Content-Type.
func gzipServer(t *testing.T, contentType string, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func openGet(t *testing.T, url, extra string) *getSource {
	t.Helper()
	s := &getSource{}
	cfg := fmt.Sprintf(`{"url":%q,"allow_local":true%s}`, url, extra)
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// drain reads the source to exhaustion, returning how many records arrived and
// the terminating error (io.EOF on a clean end).
func drain(t *testing.T, s *getSource) (int64, error) {
	t.Helper()
	var n int64
	for {
		b, err := s.Next(context.Background())
		if err != nil {
			return n, err
		}
		n += int64(b.Len())
	}
}

// bytesAllocated is CUMULATIVE allocation, not live heap. That distinction is
// the test: a body inflated whole and then discarded leaves no live heap
// behind, so a HeapAlloc reading after the call cannot tell "we never buffered
// it" from "we buffered it and freed it" — and it is the buffering, transient
// or not, that OOMs a runner.
func bytesAllocated() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.TotalAlloc
}

// A gzip bomb is refused while it is still inflating, not after.
//
// Measured before the bound existed: 2.87 MB on the wire inflated to 985 MB and
// pushed 21 million records through the pipeline with no error at all, because
// http.Transport had set Accept-Encoding: gzip itself and was decompressing
// transparently — a path this connector never opted into and could not meter.
func TestAGzipBombIsRefusedWhileItIsStillInflating(t *testing.T) {
	unit := bytes.Repeat([]byte(`{"a":1,"b":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`+"\n"), 1024)
	wire, inflated := gzipOf(t, unit, 1024) // ~48 MiB inflated
	if ratio := inflated / len(wire); ratio < 200 {
		t.Fatalf("test bomb only amplifies %dx; it must exceed the %dx bound to prove anything", ratio, defaultMaxDecompressionRatio)
	}
	srv := gzipServer(t, "application/x-ndjson", wire)

	s := openGet(t, srv.URL, "")
	n, err := drain(t, s)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("a %dx compression bomb streamed to completion (%d records): the response body is being inflated without bound", inflated/len(wire), n)
	}
	if !strings.Contains(err.Error(), "max_decompression_ratio") {
		t.Fatalf("bomb rejected, but not as a compression problem: %v\n"+
			"the operator has to be told it is a size bound, or they go hunting a data bug", err)
	}
	// The bound is on inflated bytes, so it must bite long before the whole
	// 48 MiB is decoded. The floor allows ~8 MiB, i.e. ~180k of these records.
	if n > 1_000_000 {
		t.Fatalf("bomb stopped only after %d records; the bound is not being applied as the stream inflates", n)
	}
}

// The bound holds even when the response is one enormous JSON value, which the
// per-line bounds cannot see: the JSON reader hands a whole top-level value to
// json.RawMessage, so a single value IS the unbounded unit.
//
// Measured before the bound existed: 521,845 bytes on the wire produced 2,561
// MiB of heap. That is a runner OOM bought for half a megabyte of upload.
func TestASingleEnormousJSONValueCannotExhaustMemory(t *testing.T) {
	var out bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&out, gzip.BestCompression)
	_, _ = io.WriteString(zw, `{"x":"`)
	unit := bytes.Repeat([]byte("a"), 64<<10)
	const units = 1024 // 64 MiB of one string value
	for range units {
		_, _ = zw.Write(unit)
	}
	_, _ = io.WriteString(zw, `"}`)
	_ = zw.Close()
	srv := gzipServer(t, "application/json", out.Bytes())

	before := bytesAllocated()
	s := openGet(t, srv.URL, "")
	_, err := s.Next(context.Background())
	after := bytesAllocated()

	if err == nil {
		t.Fatalf("a 64 MiB JSON value delivered in %d wire bytes parsed successfully: "+
			"nothing bounds what one value may inflate to", out.Len())
	}
	if !strings.Contains(err.Error(), "max_decompression_ratio") {
		t.Fatalf("rejected, but not as a compression problem: %v", err)
	}
	// The bound permits ~8 MiB of inflation; buffer doubling on the way there
	// costs a few multiples of that cumulatively. 96 MiB sits well above that
	// and well below the ~200 MiB an unbounded 64 MiB value costs.
	if grew := after - before; grew > 96<<20 {
		t.Fatalf("allocated %d MiB handling a %d-byte response; the inflated value was buffered",
			grew>>20, out.Len())
	}
}

// A normally-compressed response still streams every record. This is the guard
// against the fix being worse than the bug: real endpoints gzip their NDJSON,
// and a bound that rejects an ordinary 20x corpus would break more flows than
// any bomb ever did.
func TestAWellCompressedResponseStillStreamsEveryRecord(t *testing.T) {
	var raw bytes.Buffer
	const n = 50_000
	for i := range n {
		_, _ = fmt.Fprintf(&raw, `{"i":%d,"name":"customer-%d","city":"Melbourne"}`+"\n", i, i)
	}
	var out bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&out, gzip.BestCompression)
	_, _ = zw.Write(raw.Bytes())
	_ = zw.Close()
	ratio := raw.Len() / out.Len()
	t.Logf("realistic NDJSON corpus: %d bytes -> %d bytes (%dx)", raw.Len(), out.Len(), ratio)
	if ratio >= defaultMaxDecompressionRatio {
		t.Fatalf("this corpus compresses %dx, at or above the %dx bound — the default is too tight for real data",
			ratio, defaultMaxDecompressionRatio)
	}
	srv := gzipServer(t, "application/x-ndjson", out.Bytes())

	s := openGet(t, srv.URL, "")
	got, err := drain(t, s)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("legitimate gzipped response failed: %v", err)
	}
	if got != n {
		t.Fatalf("streamed %d records, want %d", got, n)
	}
}

// A response encoded with something we never offered is refused, rather than
// fed to the record parser as binary garbage. Before the transport's own
// compression was disabled this surfaced as an unexplained JSON syntax error.
func TestAnEncodingWeNeverOfferedIsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Encoding", "br")
		_, _ = w.Write([]byte{0x1b, 0x00, 0x00, 0x00})
	}))
	defer srv.Close()

	s := openGet(t, srv.URL, "")
	_, err := s.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Content-Encoding") {
		t.Fatalf("err = %v, want a refusal naming the unsupported Content-Encoding", err)
	}
}

// A source that streams forever must not pin the runner slot forever
// (ADR-0005). The request timeout covers reading the body, not just the
// headers, so it is what terminates it.
func TestAnEndlessResponseIsStoppedByTheRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		fl, _ := w.(http.Flusher)
		for {
			if _, err := io.WriteString(w, `{"a":1}`+"\n"); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			default:
			}
		}
	}))
	defer func() { srv.CloseClientConnections(); srv.Close() }()

	s := openGet(t, srv.URL, `,"timeout_seconds":1`)
	assertStopsWithin(t, 15*time.Second, func() error {
		_, err := drain(t, s)
		return err
	})
}

// A source that trickles — headers, then a byte every 200ms, forever — is the
// same threat wearing a different hat: it consumes no bandwidth and produces no
// records, so nothing downstream ever notices. The same total timeout catches
// it; there is deliberately no separate idle bound, because a total bound
// already subsumes one and an idle bound would misfire on a slow legitimate
// export.
func TestATricklingResponseIsStoppedByTheRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		if fl != nil {
			fl.Flush()
		}
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			if _, err := io.WriteString(w, "1"); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer func() { srv.CloseClientConnections(); srv.Close() }()

	s := openGet(t, srv.URL, `,"timeout_seconds":1`)
	assertStopsWithin(t, 15*time.Second, func() error {
		_, err := drain(t, s)
		return err
	})
}

// The request timeout is finite whatever the config says. "No limit" must not
// be expressible: a limit you can switch off protects nobody, and zero is what
// an omitted field decodes to.
func TestTheRequestTimeoutIsFiniteForEveryConfig(t *testing.T) {
	for _, tc := range []struct{ name, json string }{
		{"omitted", `{"url":"https://x.test/"}`},
		{"zero", `{"url":"https://x.test/","timeout_seconds":0}`},
		{"negative", `{"url":"https://x.test/","timeout_seconds":-1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c commonConfig
			if err := parseConfig([]byte(tc.json), &c); err != nil {
				t.Fatal(err)
			}
			if err := c.validate(); err != nil {
				t.Fatal(err)
			}
			if c.TimeoutSeconds != 300 {
				t.Fatalf("timeout = %ds, want the 300s default", c.TimeoutSeconds)
			}
			if got := c.client().Timeout; got != 300*time.Second {
				t.Fatalf("client timeout = %v; a zero Timeout means the client waits forever, "+
					"and an endless response would hold the runner slot for the life of the process", got)
			}
		})
	}
}

// The POST destination is as untrusted as any source: it answers with a body
// too, and the sink drains that body for connection reuse. The drain is
// bounded, so an endless 200 response cannot hold the sink for the whole
// request timeout.
func TestASinkIsNotHeldByAnEndlessSuccessBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		chunk := bytes.Repeat([]byte("x"), 32<<10)
		for {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer func() { srv.CloseClientConnections(); srv.Close() }()

	sink := &postSink{}
	cfg := fmt.Sprintf(`{"url":%q,"allow_local":true,"timeout_seconds":20}`, srv.URL)
	if err := sink.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()

	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.Key([]byte("a"))
	bld.Int(1)
	bld.EndMap()
	b.Append(bld.Finish())

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- sink.Write(context.Background(), b) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if el := time.Since(start); el > 5*time.Second {
			t.Fatalf("write took %v draining an endless success body", el)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("sink is still draining an endless 200 response after 10s: " +
			"the post-success drain is unbounded, so a hostile destination pins the task " +
			"for the whole request timeout")
	}
}

// assertStopsWithin runs fn on its own goroutine and fails — rather than
// hanging the suite — if it does not return. A missing bound is exactly the bug
// that makes a test hang, so the assertion has to survive it.
func assertStopsWithin(t *testing.T, limit time.Duration, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	var running atomic.Bool
	running.Store(true)
	go func() {
		err := fn()
		running.Store(false)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("the stream ended cleanly (%v); it was supposed to be endless", err)
		}
		t.Logf("stopped with: %v", err)
	case <-time.After(limit):
		t.Fatalf("still reading after %v: nothing bounds an endless response, "+
			"so this task would hold its runner slot indefinitely (ADR-0005)", limit)
	}
}
