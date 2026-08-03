package httpconn

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

func oneRecordBatch(t *testing.T) *record.Batch {
	t.Helper()
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(1)
	bld.EndMap()
	batch.Append(bld.Finish())
	return batch
}

// TestConnectorDefinition pins the manifest the hub stores and the studio
// renders: one node with a get source and a post sink, both carrying the shared
// config schema, with secret-typed credential fields marked (ADR-0018).
func TestConnectorDefinition(t *testing.T) {
	c := Connector()
	if c.Name != "http" || c.Version == "" {
		t.Fatalf("name/version = %q/%q", c.Name, c.Version)
	}
	mkSrc, ok := c.Sources["get"]
	if !ok || mkSrc() == nil {
		t.Fatal("no usable get source")
	}
	mkSink, ok := c.Sinks["post"]
	if !ok || mkSink() == nil {
		t.Fatal("no usable post sink")
	}
	for _, action := range []string{"get", "post"} {
		var schema struct {
			Type       string   `json:"type"`
			Required   []string `json:"required"`
			Properties map[string]json.RawMessage
		}
		if err := json.Unmarshal(c.Schemas[action], &schema); err != nil {
			t.Fatalf("%s schema is not valid JSON: %v", action, err)
		}
		if schema.Type != "object" || len(schema.Required) != 1 || schema.Required[0] != "url" {
			t.Fatalf("%s schema = %+v, want object requiring url", action, schema)
		}
		for _, field := range []string{"url", "headers", "auth", "allow_local", "timeout_seconds"} {
			if _, ok := schema.Properties[field]; !ok {
				t.Errorf("%s schema omits %q, which commonConfig accepts", action, field)
			}
		}
	}
	// The credential fields must be flagged so the builder offers a secret
	// picker instead of a plaintext box.
	var doc struct {
		Properties struct {
			Auth struct {
				Properties map[string]map[string]any `json:"properties"`
			} `json:"auth"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(c.Schemas["get"], &doc); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"pass", "token"} {
		if secret, _ := doc.Properties.Auth.Properties[field]["x-shift-secret"].(bool); !secret {
			t.Errorf("auth.%s is not marked x-shift-secret", field)
		}
	}
	for _, field := range []string{"type", "user"} {
		if _, ok := doc.Properties.Auth.Properties[field]["x-shift-secret"]; ok {
			t.Errorf("auth.%s is marked secret but is not a credential", field)
		}
	}
}

func TestOpenRejectsMalformedConfigJSON(t *testing.T) {
	src := &getSource{}
	err := src.Open(context.Background(), []byte(`{"url":`))
	if err == nil || !strings.Contains(err.Error(), "bad config") {
		t.Fatalf("getSource.Open = %v, want bad-config error", err)
	}
	sink := &postSink{}
	err = sink.Open(context.Background(), []byte(`{"url":`))
	if err == nil || !strings.Contains(err.Error(), "bad config") {
		t.Fatalf("postSink.Open = %v, want bad-config error", err)
	}
	if err := sink.Open(context.Background(), []byte(`{}`)); err == nil ||
		!strings.Contains(err.Error(), "url is required") {
		t.Fatalf("postSink.Open without url = %v, want url-required error", err)
	}
}

// TestCustomHeadersAreSent: configured headers reach the target (and do not
// displace the connector's own Content-Type / Accept).
func TestCustomHeadersAreSent(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got["X-Trace"] = r.Header.Get("X-Trace")
		got["X-Tenant"] = r.Header.Get("X-Tenant")
		got["Content-Type"] = r.Header.Get("Content-Type")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := &postSink{}
	cfg := fmt.Sprintf(`{"url":%q,"allow_local":true,"headers":{"X-Trace":"t-1","X-Tenant":"acct-9"}}`, srv.URL)
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(context.Background(), oneRecordBatch(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got["X-Trace"] != "t-1" || got["X-Tenant"] != "acct-9" {
		t.Fatalf("custom headers = %v", got)
	}
	if got["Content-Type"] != "application/x-ndjson" {
		t.Fatalf("content-type = %q, want application/x-ndjson", got["Content-Type"])
	}
}

// TestPostSinkIdempotencyKeyPerBatch: the runner injects the hub task's
// idempotency key and each batch must carry it suffixed with its ordinal, so a
// re-dispatched attempt replays the identical key sequence (ADR-0002/0009).
func TestPostSinkIdempotencyKeyPerBatch(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	run := func() []string {
		s := &postSink{}
		cfg := fmt.Sprintf(`{"url":%q,"allow_local":true,"idempotency_key":"task-42"}`, srv.URL)
		if err := s.Open(context.Background(), []byte(cfg)); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if err := s.Write(context.Background(), oneRecordBatch(t)); err != nil {
				t.Fatal(err)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		out := append([]string(nil), keys...)
		keys = nil
		return out
	}
	want := []string{"task-42:0", "task-42:1", "task-42:2"}
	first, second := run(), run()
	for _, got := range [][]string{first, second} {
		if len(got) != len(want) {
			t.Fatalf("got %d keys, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("keys = %v, want %v", got, want)
			}
		}
	}
}

// TestPostSinkOmitsIdempotencyKeyWhenUnset: a flow run without a hub task key
// must not invent one.
func TestPostSinkOmitsIdempotencyKeyWhenUnset(t *testing.T) {
	var mu sync.Mutex
	var seen string
	var present bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		_, present = r.Header["Idempotency-Key"]
		seen = r.Header.Get("Idempotency-Key")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := &postSink{}
	if err := s.Open(context.Background(), fmt.Appendf(nil, `{"url":%q,"allow_local":true}`, srv.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(context.Background(), oneRecordBatch(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if present {
		t.Fatalf("Idempotency-Key sent as %q with no key configured", seen)
	}
}

// TestPostSinkRejectsUnencodableRecord: a record the NDJSON encoder cannot
// represent must fail the batch, not POST a truncated body.
func TestPostSinkRejectsUnencodableRecord(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := &postSink{}
	if err := s.Open(context.Background(), fmt.Appendf(nil, `{"url":%q,"allow_local":true}`, srv.URL)); err != nil {
		t.Fatal(err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("amount")
	bld.Float(math.NaN()) // JSON has no NaN
	bld.EndMap()
	batch.Append(bld.Finish())

	err := s.Write(context.Background(), batch)
	if err == nil || !strings.Contains(err.Error(), "unsupported float value") {
		t.Fatalf("Write = %v, want an encoding error", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 0 {
		t.Fatalf("%d requests sent for an unencodable batch, want 0", requests)
	}
}

// TestUnbuildableRequestURL: a URL that survives config validation but cannot
// form a request must be reported by both actions, before any dial.
func TestUnbuildableRequestURL(t *testing.T) {
	const bad = "http://exa mple.com/data" // space in host

	src := &getSource{}
	if err := src.Open(context.Background(), fmt.Appendf(nil, `{"url":%q}`, bad)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Next(context.Background()); err == nil ||
		!strings.HasPrefix(err.Error(), "http:") {
		t.Fatalf("getSource.Next = %v, want a request-construction error", err)
	}

	sink := &postSink{}
	if err := sink.Open(context.Background(), fmt.Appendf(nil, `{"url":%q}`, bad)); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), oneRecordBatch(t)); err == nil ||
		!strings.HasPrefix(err.Error(), "http:") {
		t.Fatalf("postSink.Write = %v, want a request-construction error", err)
	}
}

// TestPostSinkSSRFGuard: the guard covers the sink too — exfiltrating records
// to an internal address is refused pre-connect (issue #5). The literal IP means
// no DNS lookup and no connection is attempted.
func TestPostSinkSSRFGuard(t *testing.T) {
	for _, target := range []string{"http://127.0.0.1:1/", "http://10.1.2.3/", "http://100.64.0.1/"} {
		s := &postSink{}
		if err := s.Open(context.Background(), fmt.Appendf(nil, `{"url":%q}`, target)); err != nil {
			t.Fatalf("%s: open: %v", target, err)
		}
		err := s.Write(context.Background(), oneRecordBatch(t))
		if err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Fatalf("%s: Write = %v, want an SSRF refusal", target, err)
		}
	}
}

// TestGetSourceCloseBeforeRequest: closing a source that never issued a request
// is clean (the runner closes every opened action, successful or not).
func TestGetSourceCloseBeforeRequest(t *testing.T) {
	s := &getSource{}
	if err := s.Open(context.Background(), []byte(`{"url":"http://example.invalid/"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
}
