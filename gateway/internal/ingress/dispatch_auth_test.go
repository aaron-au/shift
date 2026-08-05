package ingress_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// The control listener is the runner-impersonation surface. A caller who
// reaches /poll unauthenticated parks a fake runner: it is handed real inbound
// payloads, and it can deliver forged responses to real callers. Interception
// and response forgery, from one open port.
func TestControlEndpointsRejectAnUnauthenticatedCaller(t *testing.T) {
	reg := runners.New()
	mux := http.NewServeMux()
	ingress.NewDispatch(reg, nil, "s3cret").Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cases := []struct {
		name, path, auth string
	}{
		{"poll, no credential", "/api/v1/gw/poll", ""},
		{"poll, wrong credential", "/api/v1/gw/poll", "Bearer nope"},
		{"poll, not a bearer", "/api/v1/gw/poll", "Basic czNjcmV0"},
		{"deliver, no credential", "/api/v1/gw/deliver/x", ""},
		{"deliver, wrong credential", "/api/v1/gw/deliver/x", "Bearer nope"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				srv.URL+c.path, strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}
			if c.auth != "" {
				req.Header.Set("Authorization", c.auth)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", resp.StatusCode)
			}
		})
	}

	// Nobody may be left parked by a rejected poll: an unauthenticated caller
	// that still ended up in the registry would receive the next request.
	if n := reg.Parked(); n != 0 {
		t.Errorf("parked = %d after rejected polls, want 0", n)
	}
}

// The correct credential still works, and the digest comparison is the only
// thing standing between the two cases above and this one.
func TestControlEndpointsAcceptTheConfiguredCredential(t *testing.T) {
	mux := http.NewServeMux()
	ingress.NewDispatch(runners.New(), nil, "s3cret").Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		srv.URL+"/api/v1/gw/poll", strings.NewReader(`{"wait_seconds":0.05}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer s3cret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204 (empty poll window)", resp.StatusCode)
	}
}

// An empty token leaves the endpoints open. That is only tenable on a loopback
// bind, which gatewayd enforces at start-up — this pins the handler behaviour
// the enforcement depends on.
func TestNoTokenLeavesControlEndpointsOpen(t *testing.T) {
	d := ingress.NewDispatch(runners.New(), nil, "")
	if d.Authenticated() {
		t.Fatal("Authenticated() = true with no token configured")
	}

	mux := http.NewServeMux()
	d.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		srv.URL+"/api/v1/gw/poll", strings.NewReader(`{"wait_seconds":0.05}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}
}
