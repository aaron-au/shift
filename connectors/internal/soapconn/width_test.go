package soapconn

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TC-022. Depth is bounded (see limits_test.go) and total bytes are bounded by
// max_response_bytes. WIDTH is the third structural dimension and neither bound
// can see it: a response can stay small, stay shallow, and still cost orders of
// magnitude more memory than it occupies on the wire, because every element
// becomes a node with per-node overhead the wire never pays for.
//
// This is the same shape as the TC-020 finding — cheap for the sender,
// expensive for us — with structure doing the amplifying instead of gzip.
func TestAVeryWideResponseIsBoundedByMemoryNotJustByBytes(t *testing.T) {
	// The narrowest legal element is 8 bytes ("<a></a>" is 7 plus separation),
	// so a tiny body buys an enormous number of nodes.
	const elems = 400_000
	var body bytes.Buffer
	body.WriteString(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><r>`)
	for range elems {
		body.WriteString("<a/>")
	}
	body.WriteString(`</r></s:Body></s:Envelope>`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write(body.Bytes())
	}))
	defer srv.Close()

	s := openCall(t, srv.URL, "")
	before := bytesAllocated()
	_, err := s.Next(context.Background())
	after := bytesAllocated()
	grew := after - before

	t.Logf("wire bytes %d, elements %d, allocated %d MiB, err = %v",
		body.Len(), elems, grew>>20, err)

	if err == nil {
		t.Fatalf("a %d-byte response of %d elements was ACCEPTED, allocating %d MiB (%.0fx amplification). "+
			"Width is unbounded: neither max_response_bytes nor the depth cap can see it",
			body.Len(), elems, grew>>20, float64(grew)/float64(body.Len()))
	}
	if !strings.Contains(err.Error(), "max_response_elements") {
		t.Fatalf("wide response refused as %q, not as a width problem", err)
	}
	// Refusing only after the whole document has been walked would spend the
	// memory anyway. The bound counts as it reads, so the cost stays near the
	// permitted element count rather than the offered one.
	if grew>>20 > 200 {
		t.Fatalf("allocated %d MiB before refusing: the element bound is applied after parsing, not during", grew>>20)
	}
}

// TestAWideResponseWithinReasonIsStillAccepted guards the other side: bounding
// width must not refuse the ordinary case of a SOAP list with many entries,
// which is most of what these services return.
func TestAWideResponseWithinReasonIsStillAccepted(t *testing.T) {
	const elems = 5_000
	var body bytes.Buffer
	body.WriteString(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><r>`)
	for i := range elems {
		body.WriteString("<item><id>")
		body.WriteString(strings.Repeat("x", 8))
		body.WriteString("</id></item>")
		_ = i
	}
	body.WriteString(`</r></s:Body></s:Envelope>`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write(body.Bytes())
	}))
	defer srv.Close()

	s := openCall(t, srv.URL, "")
	if _, err := s.Next(context.Background()); err != nil {
		t.Fatalf("an ordinary %d-element SOAP list was refused: %v", elems, err)
	}
}
