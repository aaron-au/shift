package soapconn

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Hostile-response limits (TC-020 decompression, TC-021 endless streams).
//
// soapconn buffers the whole response by design — a SOAP reply is one bounded
// document, and a fault arrives with an HTTP 500 so the body must be parsed to
// know what happened. That makes max_response_bytes the only thing standing
// between a hostile endpoint and the runner's memory, so these tests attack it
// from every direction the wire allows rather than trusting it.

const limitsEnvelope = `<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><Go/></s:Body></s:Envelope>`

func openCall(t *testing.T, endpoint, extra string) *callSource {
	t.Helper()
	s := &callSource{}
	cfg := fmt.Sprintf(`{"endpoint":%q,"envelope_template":%q,"allow_local":true%s}`, endpoint, limitsEnvelope, extra)
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// bytesAllocated is CUMULATIVE allocation, not live heap. That distinction is
// the test: a body read whole and then discarded leaves no live heap behind, so
// a HeapAlloc reading after the call cannot tell "we never buffered it" from
// "we buffered it and freed it" — and it is the buffering, transient or not,
// that OOMs a runner.
func bytesAllocated() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.TotalAlloc
}

// An oversized response is rejected as oversized — not truncated at the limit
// and then reported as a syntax error.
//
// Measured before this: a 32 MiB chunked body against the 16 MiB default came
// back as `xml decode: XML syntax error on line 1: unexpected EOF`. The cap
// held, but the diagnosis was a lie: it sends the operator looking for a
// malformed document at a server that sent a perfectly well-formed one. Worse,
// truncation that happens to leave well-formed XML is silent data loss.
func TestAnOversizedResponseIsRejectedAsOversizedNotAsMalformed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		fl, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><r><pad>`)
		pad := bytes.Repeat([]byte("p"), 64<<10)
		for range 64 { // 4 MiB of padding, past the 1 MiB limit configured below
			_, _ = w.Write(pad)
			if fl != nil {
				fl.Flush()
			}
		}
		_, _ = io.WriteString(w, `</pad><v>42</v></r></s:Body></s:Envelope>`)
	}))
	defer srv.Close()

	s := openCall(t, srv.URL, `,"max_response_bytes":1048576`)
	_, err := s.Next(context.Background())
	if err == nil {
		t.Fatal("a response four times the limit was accepted")
	}
	if !strings.Contains(err.Error(), "max_response_bytes") {
		t.Fatalf("oversized response reported as %q; it has to name the limit it hit, "+
			"or the truncation reads as a data problem at the far end", err)
	}
}

// A response that exactly fills the limit is still valid data. The check reads
// one byte past the limit to detect overflow, so this pins the off-by-one:
// nothing legitimate may be rejected for being exactly as large as allowed.
func TestAResponseExactlyAtTheLimitIsStillAccepted(t *testing.T) {
	const head = `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><r><pad>`
	const tail = `</pad><v>42</v></r></s:Body></s:Envelope>`
	const total = 64 << 10
	body := head + strings.Repeat("p", total-len(head)-len(tail)) + tail
	if len(body) != total {
		t.Fatalf("test body is %d bytes, want exactly %d", len(body), total)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	s := openCall(t, srv.URL, fmt.Sprintf(`,"max_response_bytes":%d`, total))
	b, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("a response of exactly max_response_bytes was rejected: %v", err)
	}
	if b.Len() != 1 {
		t.Fatalf("got %d records, want 1", b.Len())
	}
}

// A declared Content-Length is a claim by the attacker, not a measurement. It
// must never size a buffer: a 1 GiB claim on a 200-byte body has to cost 200
// bytes of memory and fail fast.
func TestADeclaredContentLengthNeverSizesTheBuffer(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_, _ = c.Read(make([]byte, 4096))
				// Claims 1 GiB, sends a few bytes, hangs up.
				_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: 1073741824\r\n\r\n<a/>")
			}()
		}
	}()

	before := bytesAllocated()
	s := openCall(t, "http://"+ln.Addr().String(), `,"timeout_seconds":5`)
	start := time.Now()
	_, err = s.Next(context.Background())
	after := bytesAllocated()

	if err == nil {
		t.Fatal("a truncated response was accepted as complete")
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("took %v to fail on a short body claiming 1 GiB", el)
	}
	if grew := after - before; grew > 32<<20 {
		t.Fatalf("allocated %d MiB for a 4-byte body: the declared Content-Length is being trusted to size a buffer", grew>>20)
	}
}

// A gzip bomb is bounded by max_response_bytes, because the limit is applied to
// the DECOMPRESSED stream. Measured: 1,043,738 wire bytes claiming to inflate
// to 1 GiB cost 41 MiB of heap and stopped in 99ms.
//
// This pins the ordering, which is the whole property: move the limit to the
// compressed side, or read the body before limiting it, and 1 MB of upload
// buys a gigabyte of runner memory.
func TestAGzipBombIsBoundedByTheResponseLimit(t *testing.T) {
	var out bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&out, gzip.BestCompression)
	_, _ = io.WriteString(zw, `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><r><pad>`)
	pad := bytes.Repeat([]byte("p"), 64<<10)
	for range 1024 { // 64 MiB inflated, 64x the limit configured below
		_, _ = zw.Write(pad)
	}
	_, _ = io.WriteString(zw, `</pad></r></s:Body></s:Envelope>`)
	_ = zw.Close()
	bomb := out.Bytes()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(bomb)
	}))
	defer srv.Close()

	before := bytesAllocated()
	s := openCall(t, srv.URL, `,"max_response_bytes":1048576`)
	_, err := s.Next(context.Background())
	after := bytesAllocated()

	if err == nil {
		t.Fatalf("a %d-byte response inflating to 64 MiB was accepted", len(bomb))
	}
	if !strings.Contains(err.Error(), "max_response_bytes") {
		t.Fatalf("bomb rejected as %q, not as a size problem", err)
	}
	if grew := after - before; grew > 32<<20 {
		t.Fatalf("allocated %d MiB reading a %d-byte compressed response: the limit is not being applied AS the stream inflates, "+
			"only after the whole thing has been buffered",
			grew>>20, len(bomb))
	}
}

// An endpoint that trickles forever must not pin the runner slot (ADR-0005).
// The call timeout covers reading the body, not just the headers.
func TestATricklingEndpointIsStoppedByTheCallTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
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
			if _, err := io.WriteString(w, "<a/>"); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer func() { srv.CloseClientConnections(); srv.Close() }()

	s := openCall(t, srv.URL, `,"timeout_seconds":1`)
	done := make(chan error, 1)
	go func() {
		_, err := s.Next(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a response that never ends returned successfully")
		}
		t.Logf("stopped with: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("still reading after 15s: nothing bounds an endless response body, " +
			"so this call would hold its runner slot indefinitely (ADR-0005)")
	}
}

// The call timeout and the response limit are finite whatever the config says.
// Neither "no timeout" nor "no size limit" is expressible, deliberately: a
// limit that can be switched off protects nobody, and zero is what an omitted
// field decodes to.
func TestTheCallBoundsAreFiniteForEveryConfig(t *testing.T) {
	for _, tc := range []struct{ name, json string }{
		{"omitted", `{"endpoint":"https://x.test/","envelope_template":"<a/>"}`},
		{"zero", `{"endpoint":"https://x.test/","envelope_template":"<a/>","timeout_seconds":0,"max_response_bytes":0}`},
		{"negative", `{"endpoint":"https://x.test/","envelope_template":"<a/>","timeout_seconds":-1,"max_response_bytes":-1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c config
			if err := parseConfig([]byte(tc.json), &c); err != nil {
				t.Fatal(err)
			}
			if c.TimeoutSeconds != 60 {
				t.Fatalf("timeout = %ds, want the 60s default", c.TimeoutSeconds)
			}
			if c.MaxResponseBytes != defaultMaxResponseBytes {
				t.Fatalf("max_response_bytes = %d, want the %d default", c.MaxResponseBytes, defaultMaxResponseBytes)
			}
			if got := c.client().Timeout; got != 60*time.Second {
				t.Fatalf("client timeout = %v; a zero Timeout means the client waits forever", got)
			}
		})
	}
}

// A deeply nested response is refused instead of taking the process down.
//
// Everything that walks the parsed tree is recursive, and Go's answer to
// runaway recursion is `fatal error: stack overflow` — not a panic, so no
// recover() anywhere can contain it and the connector process simply dies.
// Measured before the depth bound: 7,518 gzipped bytes did exactly that, from
// inside the 16 MiB response cap. Depth is the dimension a byte cap cannot see.
func TestADeeplyNestedResponseIsRefusedRatherThanKillingTheProcess(t *testing.T) {
	var body bytes.Buffer
	body.WriteString(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body>`)
	// 200k is deliberate: deep enough to be far past the bound, shallow enough
	// that a build WITHOUT the bound still survives the walk — so this fails as
	// a test failure rather than taking the whole binary down with it.
	const depth = 200_000
	for range depth {
		body.WriteString("<a>")
	}
	for range depth {
		body.WriteString("</a>")
	}
	body.WriteString(`</s:Body></s:Envelope>`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write(body.Bytes())
	}))
	defer srv.Close()

	s := openCall(t, srv.URL, "")
	_, err := s.Next(context.Background())
	if err == nil {
		t.Fatalf("a %d-deep document was accepted", depth)
	}
	if !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("rejected as %q, not as a nesting problem", err)
	}
}

// Ordinary nesting is untouched. The bound is only worth having if it never
// fires on a real envelope, so this pins a depth well past anything a WS-*
// stack produces.
func TestOrdinaryNestingIsUnaffectedByTheDepthBound(t *testing.T) {
	var body bytes.Buffer
	body.WriteString(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><r>`)
	const depth = 64
	for range depth {
		body.WriteString("<lvl>")
	}
	body.WriteString("deep")
	for range depth {
		body.WriteString("</lvl>")
	}
	body.WriteString(`</r></s:Body></s:Envelope>`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write(body.Bytes())
	}))
	defer srv.Close()

	s := openCall(t, srv.URL, "")
	b, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("a %d-deep envelope was rejected: %v", depth, err)
	}
	if b.Len() != 1 {
		t.Fatalf("got %d records, want 1", b.Len())
	}
}
