package httpconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

func TestGetSourceStreamsNDJSON(t *testing.T) {
	const n = 5000
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i := range n {
			_, _ = fmt.Fprintf(w, `{"i":%d,"name":"rec-%d"}`+"\n", i, i)
		}
	}))
	defer srv.Close()

	s := &getSource{}
	cfg := fmt.Sprintf(`{"url":%q,"allow_local":true,"auth":{"type":"bearer","token":"tok123"}}`, srv.URL)
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var count int64
	ctx := context.Background()
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range b.Records() {
			v, _ := rec.Field("i")
			if v.Int() != count {
				t.Fatalf("record %d has i=%d", count, v.Int())
			}
			count++
		}
	}
	if count != n {
		t.Fatalf("streamed %d records, want %d", count, n)
	}
	if gotAuth.Load() != "Bearer tok123" {
		t.Fatalf("auth header = %q", gotAuth.Load())
	}
}

// TestGetSourceJSONArray covers the REST shape: a Content-Type of
// application/json carrying a (pretty-printed) top-level array. The source must
// select the JSON reader and stream each element as a record — the shape the
// line-based NDJSON reader cannot parse.
func TestGetSourceJSONArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = io.WriteString(w, "[\n  {\"id\": 1, \"email\": \"a@x.io\"},\n  {\"id\": 2, \"email\": \"b@x.io\"},\n  {\"id\": 3, \"email\": \"c@x.io\"}\n]")
	}))
	defer srv.Close()

	s := &getSource{}
	cfg := fmt.Sprintf(`{"url":%q,"allow_local":true}`, srv.URL)
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	var count int64
	ctx := context.Background()
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range b.Records() {
			id, _ := rec.Field("id")
			count++
			if id.Int() != count {
				t.Fatalf("record %d has id=%d", count, id.Int())
			}
		}
	}
	if count != 3 {
		t.Fatalf("streamed %d records from JSON array, want 3", count)
	}
}

func TestGetSourceSSRFGuard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{}`)
	}))
	defer srv.Close()

	s := &getSource{}
	cfg := fmt.Sprintf(`{"url":%q}`, srv.URL) // allow_local defaults false
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}
	_, err := s.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("err = %v, want loopback refusal", err)
	}
}

func TestGetSourceHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer srv.Close()

	s := &getSource{}
	if err := s.Open(context.Background(), fmt.Appendf(nil, `{"url":%q,"allow_local":true}`, srv.URL)); err != nil {
		t.Fatal(err)
	}
	_, err := s.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("err = %v, want status 502", err)
	}
}

func TestPostSink(t *testing.T) {
	var bodies []string
	var basicUser atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("content-type = %q", r.Header.Get("Content-Type"))
		}
		u, _, _ := r.BasicAuth()
		basicUser.Store(u)
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	s := &postSink{}
	cfg := fmt.Sprintf(`{"url":%q,"allow_local":true,"auth":{"type":"basic","user":"u1","pass":"p1"}}`, srv.URL)
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}

	batch := record.NewBatch()
	bld := batch.Builder()
	for i := range 3 {
		bld.BeginMap()
		bld.KeyLiteral("i")
		bld.Int(int64(i))
		bld.EndMap()
		batch.Append(bld.Finish())
	}
	ctx := context.Background()
	if err := s.Write(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 {
		t.Fatalf("got %d POSTs, want 2", len(bodies))
	}
	want := `{"i":0}` + "\n" + `{"i":1}` + "\n" + `{"i":2}` + "\n"
	if bodies[0] != want {
		t.Fatalf("body = %q, want %q", bodies[0], want)
	}
	if basicUser.Load() != "u1" {
		t.Fatalf("basic user = %q", basicUser.Load())
	}
}

func TestPostSinkErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusUnprocessableEntity)
	}))
	defer srv.Close()

	s := &postSink{}
	if err := s.Open(context.Background(), fmt.Appendf(nil, `{"url":%q,"allow_local":true}`, srv.URL)); err != nil {
		t.Fatal(err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.EndMap()
	batch.Append(bld.Finish())
	err := s.Write(context.Background(), batch)
	if err == nil || !strings.Contains(err.Error(), "status 422") {
		t.Fatalf("err = %v, want 422", err)
	}
}

func TestConfigValidation(t *testing.T) {
	s := &getSource{}
	if err := s.Open(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("want url-required error")
	}
	if err := s.Open(context.Background(), []byte(`{"url":"http://x","auth":{"type":"magic"}}`)); err == nil {
		t.Fatal("want unknown-auth error")
	}
}

// TestGetSourceSSRFPrivateRanges pins issue #5: the SSRF guard refuses
// RFC1918, CGNAT (and by extension ULA) targets when allow_local is false.
// The dialer Control hook rejects pre-connect on the literal IP, so no real
// connection is attempted.
func TestGetSourceSSRFPrivateRanges(t *testing.T) {
	for _, target := range []string{
		"http://10.0.0.1/", "http://192.168.1.1/", "http://172.16.0.1/", "http://100.64.0.1/",
	} {
		s := &getSource{}
		if err := s.Open(context.Background(), fmt.Appendf(nil, `{"url":%q}`, target)); err != nil {
			t.Fatalf("%s: open: %v", target, err)
		}
		_, err := s.Next(context.Background())
		if err == nil || !strings.Contains(err.Error(), "private/internal") {
			t.Fatalf("%s: err = %v, want private/internal refusal", target, err)
		}
	}
}
