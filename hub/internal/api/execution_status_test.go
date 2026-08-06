package api_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"testing"
)

// The endpoints a status URL is built on (ADR-0042 §3), over real HTTP in the
// runner realm — the hub never speaks to a caller directly.

// statusRig registers a runner through the real endpoint chain and returns the
// server URL plus that runner's credential.
type statusRig struct {
	url    string
	secret string
}

func newStatusRig(t *testing.T) statusRig {
	t.Helper()
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	var tok struct{ Token string }
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok); code != http.StatusCreated {
		t.Fatalf("minting a runner token = %d", code)
	}
	var reg struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); code != http.StatusCreated || reg.Secret == "" {
		t.Fatalf("registering a runner = %d %+v", code, reg)
	}
	return statusRig{url: srv.URL, secret: reg.Secret}
}

func (r statusRig) post(t *testing.T, path, body string, out any) int {
	t.Helper()
	return call(t, http.MethodPost, r.url+path, r.secret, body, out)
}

func (r statusRig) get(t *testing.T, path string, out any) int {
	t.Helper()
	return call(t, http.MethodGet, r.url+path, r.secret, "", out)
}

func TestExecutionStatusAcceptThenFinishThenRead(t *testing.T) {
	r := newStatusRig(t)
	id := newUUID(t)

	// Accept: the row must exist from here, so the 202's URL resolves the
	// instant the caller receives it rather than 404ing until the flow ends.
	if code := r.post(t, "/api/v1/execution-status",
		`{"id":"`+id+`","flow_name":"orders","route":"/orders","principal":"acme-erp"}`, nil); code != http.StatusCreated {
		t.Fatalf("accept status = %d, want 201", code)
	}

	read := "/api/v1/execution-status/" + id + "?route=%2Forders&principal=acme-erp"
	var got map[string]any
	if code := r.get(t, read, &got); code != http.StatusOK {
		t.Fatalf("read status = %d, want 200", code)
	}
	if got["state"] != "accepted" {
		t.Errorf("state = %v, want accepted", got["state"])
	}

	if code := r.post(t, "/api/v1/execution-status/"+id+"/finish",
		`{"state":"completed","records_in":12,"records_out":12}`, nil); code != http.StatusNoContent {
		t.Fatalf("finish status = %d, want 204", code)
	}
	got = nil
	if code := r.get(t, read, &got); code != http.StatusOK {
		t.Fatalf("terminal read = %d, want 200", code)
	}
	if got["state"] != "completed" {
		t.Errorf("state = %v, want completed", got["state"])
	}

	// A terminal read is consumed, so the SECOND one is Gone rather than
	// not-found: this caller already proved the capability, and a 404 would
	// read as "you got the id wrong".
	if code := r.get(t, read, nil); code != http.StatusGone {
		t.Errorf("second read = %d, want 410", code)
	}
}

// Every refusal is the same 404. A distinguishable response confirms that
// someone else's task exists under that id.
func TestExecutionStatusRefusalsAreIndistinguishable(t *testing.T) {
	r := newStatusRig(t)
	id := newUUID(t)

	if code := r.post(t, "/api/v1/execution-status",
		`{"id":"`+id+`","flow_name":"orders","route":"/orders","principal":"acme-erp"}`, nil); code != http.StatusCreated {
		t.Fatalf("accept = %d", code)
	}

	for _, tc := range []struct{ name, query string }{
		{"an unknown id", "/api/v1/execution-status/" + newUUID(t) + "?route=%2Forders&principal=acme-erp"},
		{"the wrong route", "/api/v1/execution-status/" + id + "?route=%2Fpayroll&principal=acme-erp"},
		{"the wrong principal", "/api/v1/execution-status/" + id + "?route=%2Forders&principal=someone-else"},
		{"no principal at all", "/api/v1/execution-status/" + id + "?route=%2Forders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := r.get(t, tc.query, nil); code != http.StatusNotFound {
				t.Errorf("status = %d, want 404 — every refusal must look the same", code)
			}
		})
	}
}

// An anonymous route's status URL is a capability URL: the token IS the
// authorisation, and a missing or wrong one is 404, never 401 or 403.
func TestAnonymousStatusRequiresItsCapabilityToken(t *testing.T) {
	r := newStatusRig(t)
	id := newUUID(t)
	digest := sha256Hex("capability-token")

	if code := r.post(t, "/api/v1/execution-status",
		`{"id":"`+id+`","flow_name":"hook","route":"/hooks/shopify","principal":"anonymous","token_sha256":"`+digest+`"}`,
		nil); code != http.StatusCreated {
		t.Fatalf("accept = %d", code)
	}

	base := "/api/v1/execution-status/" + id + "?route=%2Fhooks%2Fshopify&principal=anonymous"
	if code := r.get(t, base+"&token_sha256="+digest, nil); code != http.StatusOK {
		t.Fatalf("the right token was refused: %d", code)
	}
	if code := r.get(t, base, nil); code != http.StatusNotFound {
		t.Errorf("no token = %d, want 404", code)
	}
	if code := r.get(t, base+"&token_sha256="+sha256Hex("guess"), nil); code != http.StatusNotFound {
		t.Errorf("wrong token = %d, want 404", code)
	}
}

// A runner-minted id that collides gets a 409 so the runner mints another. A
// silent overwrite would make two requests share one status row and report each
// other's outcome.
func TestCollidingExecutionIDIsRejected(t *testing.T) {
	r := newStatusRig(t)
	id := newUUID(t)

	body := `{"id":"` + id + `","flow_name":"orders","route":"/orders","principal":"a"}`
	if code := r.post(t, "/api/v1/execution-status", body, nil); code != http.StatusCreated {
		t.Fatal("first accept failed")
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if code := r.post(t, "/api/v1/execution-status", body, &env); code != http.StatusConflict {
		t.Fatalf("second accept = %d, want 409", code)
	}
	if env.Error.Code != "status_id_taken" {
		t.Errorf("code = %q, want status_id_taken so the runner knows to retry with a new id", env.Error.Code)
	}
}

func TestFinishRejectsANonTerminalState(t *testing.T) {
	r := newStatusRig(t)
	id := newUUID(t)
	if code := r.post(t, "/api/v1/execution-status",
		`{"id":"`+id+`","flow_name":"orders","route":"/orders","principal":"a"}`, nil); code != http.StatusCreated {
		t.Fatal("accept failed")
	}
	if code := r.post(t, "/api/v1/execution-status/"+id+"/finish",
		`{"state":"accepted"}`, nil); code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422 — accepted is not a state to finish in", code)
	}
}

func TestFinishingAnUnknownExecutionIs404(t *testing.T) {
	r := newStatusRig(t)
	if code := r.post(t, "/api/v1/execution-status/"+newUUID(t)+"/finish",
		`{"state":"completed"}`, nil); code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", code)
	}
}

// These endpoints are the RUNNER realm. A human token must not reach them: the
// hub never serves a caller's status read directly, and admitting one would
// bypass the route/principal checks the runner passes through.
func TestExecutionStatusIsRunnerRealmOnly(t *testing.T) {
	r := newStatusRig(t)
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/execution-status"},
		{http.MethodGet, "/api/v1/execution-status/" + newUUID(t)},
		{http.MethodPost, "/api/v1/execution-status/" + newUUID(t) + "/finish"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			code := call(t, tc.method, r.url+tc.path, adminToken, `{"state":"completed"}`, nil)
			if code != http.StatusUnauthorized && code != http.StatusForbidden {
				t.Errorf("status = %d, want the admin realm refused", code)
			}
		})
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func newUUID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
