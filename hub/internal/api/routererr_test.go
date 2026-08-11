package api_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/aaron-au/shift/hub/internal/api"
)

// TC-030. ADR-0023 promises the envelope for ALL hub error responses, without
// qualification. The router answers two cases without ever reaching a handler —
// a known path with the wrong method (405) and an unknown path (404) — and both
// used to come back as `text/plain`.

func decodeEnvelope(t *testing.T, where, body string) (status int, message string) {
	t.Helper()
	var env struct {
		Error struct {
			Status  int    `json:"status"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("%s: body is not the ADR-0023 envelope (%v): %q", where, err, body)
	}
	return env.Error.Status, env.Error.Message
}

// TestAKnownPathWithTheWrongMethodAnswersInTheEnvelope. This is the case a
// client hits by typo, and it is how the gap was found: a test of ours asked
// for /api/v1/runners/lease instead of /api/v1/lease.
func TestAKnownPathWithTheWrongMethodAnswersInTheEnvelope(t *testing.T) {
	srv, _ := newFaultServer(t, api.Options{})

	// /api/v1/lease is registered for POST only. The method has to be one with
	// no catch-all pattern: `GET /` serves the dashboard, so a GET to any path
	// matches that instead of producing a 405.
	raw, code := call2t(t, http.MethodDelete, srv.URL+"/api/v1/lease", adminToken, "")
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE on a POST-only route = %d, want 405 (body %q)", code, raw)
	}
	status, msg := decodeEnvelope(t, "DELETE /api/v1/lease", raw)
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("envelope status = %d, want 405", status)
	}
	if msg == "" {
		t.Fatal("envelope carries no message")
	}
	if strings.Contains(raw, "Method Not Allowed\n") {
		t.Fatalf("the router's plain-text body survived alongside the envelope: %q", raw)
	}
}

// TestTheAllowHeaderSurvivesTheRewrite: the Allow header is the genuinely
// useful part of a 405, and it belongs to the mux. Rewriting the body must not
// cost it.
func TestTheAllowHeaderSurvivesTheRewrite(t *testing.T) {
	srv, _ := newFaultServer(t, api.Options{})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, srv.URL+"/api/v1/lease", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, http.MethodPost) {
		t.Fatalf("Allow = %q, want it to name POST; the rewrite dropped the mux's own header", allow)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func TestAnUnknownPathAnswersInTheEnvelope(t *testing.T) {
	srv, _ := newFaultServer(t, api.Options{})

	raw, code := call2t(t, http.MethodGet, srv.URL+"/api/v1/no-such-endpoint", adminToken, "")
	if code != http.StatusNotFound {
		t.Fatalf("unknown path = %d, want 404 (body %q)", code, raw)
	}
	if status, _ := decodeEnvelope(t, "GET /api/v1/no-such-endpoint", raw); status != http.StatusNotFound {
		t.Fatalf("envelope status = %d, want 404", status)
	}
}

// TestAHandlersOwn404IsNotRewritten is the guard on the mechanism. A handler
// that answers 404 through writeErr has already said something specific ("no
// such flow"), and replacing it with the router's generic message would destroy
// the diagnosis this middleware exists to provide elsewhere.
func TestAHandlersOwn404IsNotRewritten(t *testing.T) {
	srv, _ := newFaultServer(t, api.Options{})

	raw, code := call2t(t, http.MethodGet, srv.URL+"/api/v1/flows/definitely-not-a-flow", adminToken, "")
	if code != http.StatusNotFound {
		t.Fatalf("missing flow = %d, want 404 (body %q)", code, raw)
	}
	_, msg := decodeEnvelope(t, "GET /api/v1/flows/{name}", raw)
	if msg == "" {
		t.Fatal("envelope carries no message")
	}
	if msg == "no such endpoint" {
		t.Fatal("a handler's own 404 was replaced by the router's generic message; the specific diagnosis was lost")
	}
}

// TestASuccessfulResponseIsUntouched: the wrapper sits on every request, so the
// happy path has to be provably unaffected.
func TestASuccessfulResponseIsUntouched(t *testing.T) {
	srv, _ := newFaultServer(t, api.Options{})

	raw, code := call2t(t, http.MethodGet, srv.URL+"/api/v1/flows", adminToken, "")
	if code != http.StatusOK {
		t.Fatalf("GET /api/v1/flows = %d, want 200 (body %q)", code, raw)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("a successful response was altered by the router-error wrapper: %v (body %q)", err, raw)
	}
	if _, ok := body["flows"]; !ok {
		t.Fatalf("GET /api/v1/flows returned %q, which does not carry the flows key", raw)
	}
}
