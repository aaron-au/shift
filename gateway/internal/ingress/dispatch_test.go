package ingress_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// The whole path over real HTTP, with a real (if tiny) runner: caller hits the
// public listener, a runner polling the control listener picks the work up,
// answers on deliver, and the answer reaches the caller. Nothing in this test
// dials the runner — that inversion is the reason the gateway can live in a
// DMZ (ADR-0038 §4).
func TestEndToEndPollThenDeliver(t *testing.T) {
	reg := runners.New()
	pub := ingress.New(reg, nil)
	// Labels come from the hub's roster, keyed by the identity the runner
	// PROVES with its client certificate (ADR-0041 §3) — never from its poll.
	cfg := &config.Config{Version: 1,
		Routes: []config.Route{{
			Path: "/orders", Flow: "ingest",
			Selector:      config.Selector{"environment": "production"},
			AuthPrincipal: "acme-erp",
		}},
		Runners: []config.Runner{{
			ID:     "rnr-1",
			Labels: map[string]string{"environment": "production", "workload": "api"},
		}},
	}
	if err := pub.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	ingress.NewDispatch(reg, nil, "").
		WithLabels(cfg.LabelsFor).
		WithPeerID(func(*http.Request) string { return "rnr-1" }). // stands in for mTLS
		Routes(mux)
	ctrl := httptest.NewServer(mux)
	defer ctrl.Close()
	public := httptest.NewServer(pub)
	defer public.Close()

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- fakeRunner(t, ctrl.URL) }()

	// Give the runner a moment to park. Without a parked runner the gateway
	// answers 503 immediately and never queues — by design.
	waitParked(t, reg)

	resp, err := post(t, public.URL+"/orders", strings.NewReader(`{"order":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if err := <-runnerDone; err != nil {
		t.Fatalf("runner: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202 (the runner's chosen status)", resp.StatusCode)
	}
	if got := string(body); got != `{"ok":true}` {
		t.Errorf("body = %q, want the runner's output", got)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want it carried back from the runner", ct)
	}
}

// fakeRunner is the runner half of the exchange: poll, read the work, deliver
// an answer. It asserts what the gateway is contractually obliged to hand it.
func fakeRunner(t *testing.T, ctrlURL string) error {
	t.Helper()
	pollBody, _ := json.Marshal(map[string]any{"wait_seconds": 5})
	resp, err := post(t, ctrlURL+"/api/v1/gw/poll", bytes.NewReader(pollBody))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return errf("poll status = %d, want 200", resp.StatusCode)
	}
	in, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if string(in) != `{"order":1}` {
		return errf("runner received body %q, want the caller's", in)
	}
	if got := resp.Header.Get("X-Shift-Flow"); got != "ingest" {
		return errf("flow = %q, want ingest", got)
	}
	// Identity arrives under the forward prefix, alongside the caller's own
	// headers, so the runner reads them all from one place.
	if got := resp.Header.Get("X-Shift-Fwd-X-Shift-Principal"); got != "acme-erp" {
		return errf("principal = %q, want acme-erp", got)
	}
	id := resp.Header.Get("X-Shift-Request-Id")
	if id == "" {
		return errf("no request id on the poll response")
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ctrlURL+"/api/v1/gw/deliver/"+id, strings.NewReader(`{"ok":true}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Shift-Status", "202")
	dresp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = dresp.Body.Close() }()
	if dresp.StatusCode != http.StatusNoContent {
		return errf("deliver status = %d, want 204", dresp.StatusCode)
	}
	return nil
}

func errf(format string, a ...any) error { return fmt.Errorf(format, a...) }

// post is http.Post with the test's context attached, which the linter
// requires and which also means an abandoned request dies with the test
// rather than outliving it.
func post(t *testing.T, url string, body io.Reader) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// Delivering against an id nobody is waiting on must say so rather than
// silently succeed: the runner is streaming, and it needs to stop.
func TestDeliverToUnknownRequestIsRejected(t *testing.T) {
	mux := http.NewServeMux()
	ingress.NewDispatch(runners.New(), nil, "").Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := post(t, srv.URL+"/api/v1/gw/deliver/nope", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusGone {
		t.Errorf("status = %d, want 410", resp.StatusCode)
	}
}

// An empty poll window is the NORMAL outcome, not an error: a runner that
// treated it as a failure would back off exactly when it should re-poll.
func TestEmptyPollWindowReturns204(t *testing.T) {
	mux := http.NewServeMux()
	ingress.NewDispatch(runners.New(), nil, "").Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body, _ := json.Marshal(map[string]any{"wait_seconds": 0.05})
	resp, err := post(t, srv.URL+"/api/v1/gw/poll", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}
}

// A runner must not be able to set an arbitrary status on the public
// response: a nonsense code would be written straight to the caller.
func TestDeliverRejectsAnImpossibleStatus(t *testing.T) {
	mux := http.NewServeMux()
	ingress.NewDispatch(runners.New(), nil, "").Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		srv.URL+"/api/v1/gw/deliver/x", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Shift-Status", "999")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// A malformed poll must be refused rather than parking a waiter with
// whatever partially-decoded labels survived — an unintended empty label set
// matches EVERY route, so the failure mode is a runner silently eligible for
// work it should never see.
func TestMalformedPollIsRejected(t *testing.T) {
	reg := runners.New()
	mux := http.NewServeMux()
	ingress.NewDispatch(reg, nil, "").Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := post(t, srv.URL+"/api/v1/gw/poll", strings.NewReader("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if n := reg.Parked(); n != 0 {
		t.Errorf("parked = %d after a rejected poll, want 0", n)
	}
}
