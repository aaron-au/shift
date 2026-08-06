package ingress_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// A status read lands under the developer's OWN route (ADR-0042 §3), so it
// inherits that route's entire policy rather than needing a parallel one.
//
// The runner is told to ANSWER it rather than run a flow: a status read that
// executed the flow would be a side effect on a GET.
func TestStatusReadDispatchesUnderItsOwnRoute(t *testing.T) {
	reg := runners.New()
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{{
		Path: "/orders", Method: http.MethodPost, Flow: "ingest",
		AuthTokenSHA256: sha256Hex("s3cret"), AuthPrincipal: "acme-erp",
	}}}); err != nil {
		t.Fatal(err)
	}

	var got http.Header
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := reg.Poll(t.Context(), nil, 3*time.Second)
		if req == nil {
			t.Error("no work handed to the parked runner")
			return
		}
		got = req.Headers.Clone()
		_ = reg.Deliver(t.Context(), req.ID, &runners.Response{Status: http.StatusOK,
			Headers: http.Header{"Content-Type": {"application/json"}},
			Body:    io.NopCloser(strings.NewReader(`{"state":"running"}`))})
	}()
	waitParked(t, reg)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/_status/abc-123", nil)
	r.Header.Set("Authorization", "Bearer s3cret")
	h.ServeHTTP(w, r)
	<-done

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", w.Code, w.Body.String())
	}
	if op := got.Get(ingress.HeaderOp); op != ingress.OpStatus {
		t.Errorf("op = %q, want %q — the runner must answer this, not run a flow", op, ingress.OpStatus)
	}
	if id := got.Get(ingress.HeaderTask); id != "abc-123" {
		t.Errorf("task = %q, want abc-123", id)
	}
	if p := got.Get(ingress.HeaderPrincipal); p != "acme-erp" {
		t.Errorf("principal = %q, want the route's — the status read inherits its policy", p)
	}
	if rt := got.Get(ingress.HeaderRoute); rt != "/orders" {
		t.Errorf("route = %q, want /orders", rt)
	}
}

// The route's credential guards its status reads too. Without this the id
// alone would be the whole authorisation.
func TestStatusReadRequiresTheRoutesCredential(t *testing.T) {
	reg := runners.New()
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{{
		Path: "/orders", Method: http.MethodPost, Flow: "ingest",
		AuthTokenSHA256: sha256Hex("s3cret"),
	}}}); err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(),
		http.MethodGet, "/orders/_status/abc-123", nil))
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 without the route's token", w.Code)
	}
}

// A status path under a route that does not exist is an ordinary 404 — no
// hint that the shape is meaningful elsewhere.
func TestStatusReadOnAnUnknownRouteIs404(t *testing.T) {
	reg := runners.New()
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{{
		Path: "/orders", Flow: "ingest",
	}}}); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(),
		http.MethodGet, "/payroll/_status/abc-123", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// A route's method constrains how work is TRIGGERED, not how its status is
// read: a POST-only route whose status could not be read would be strange to
// ship. Equally, status is GET-only — a POST to a status path is not a status
// read and must not be treated as one.
func TestStatusIsGETOnlyRegardlessOfTheRoutesMethod(t *testing.T) {
	cfg := &config.Config{Version: 1, Routes: []config.Route{{
		Path: "/orders", Method: http.MethodPost, Flow: "ingest",
	}}}
	if route, id := cfg.StatusRequest(http.MethodGet, "/orders/_status/abc"); route == nil || id != "abc" {
		t.Errorf("GET on a POST-only route resolved to %v/%q, want the route and the id", route, id)
	}
	if route, _ := cfg.StatusRequest(http.MethodPost, "/orders/_status/abc"); route != nil {
		t.Error("POST to a status path was treated as a status read")
	}
	if route, _ := cfg.StatusRequest(http.MethodGet, "/orders/_status/"); route != nil {
		t.Error("an empty task id resolved to a route")
	}
	if route, _ := cfg.StatusRequest(http.MethodGet, "/orders/_status/a/b"); route != nil {
		t.Error("a multi-segment id resolved to a route")
	}
}

// A route that swallows another's status reads is a configuration that looks
// fine and silently breaks async status. Both shapes are refused at validation.
func TestConfigRefusesRoutesThatShadowStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		routes []config.Route
		want   string
	}{{
		name:   "a route using the reserved segment",
		routes: []config.Route{{Path: "/orders/_status/x", Flow: "f"}},
		want:   "reserved",
	}, {
		name: "a route sitting exactly where another's status lands",
		routes: []config.Route{
			{Path: "/orders", Flow: "f"},
			{Path: "/orders/_status", Flow: "g"},
		},
		want: "shadows",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := &config.Config{Version: 1, Routes: tc.routes}
			err := c.Validate()
			if err == nil {
				t.Fatalf("accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// The runner cannot derive the address the internet reached us on, so the
// gateway stamps it — otherwise every status URL would be relative or wrong.
func TestPublicBaseIsStamped(t *testing.T) {
	reg := runners.New()
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{{
		Path: "/orders", Flow: "ingest",
	}}}); err != nil {
		t.Fatal(err)
	}

	var got http.Header
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := reg.Poll(t.Context(), nil, 3*time.Second)
		if req == nil {
			t.Error("no work handed to the parked runner")
			return
		}
		got = req.Headers.Clone()
		_ = reg.Deliver(t.Context(), req.ID, &runners.Response{Status: http.StatusAccepted})
	}()
	waitParked(t, reg)

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders", strings.NewReader("{}"))
	r.Host = "api.example.com"
	// A gateway behind a TLS-terminating balancer sees plaintext for an https
	// service; a status URL on the wrong scheme is a broken link.
	r.Header.Set("X-Forwarded-Proto", "https")
	h.ServeHTTP(w, r)
	<-done

	if base := got.Get(ingress.HeaderPublicBase); base != "https://api.example.com" {
		t.Errorf("public base = %q, want https://api.example.com", base)
	}
}

// StatusPath and the runner's URL builder must agree: they are in separate
// modules (depguard) with no compiler between them, so the shape of a status
// URL is only as correct as this assertion.
func TestStatusPathMatchesWhatStatusRequestResolves(t *testing.T) {
	cfg := &config.Config{Version: 1, Routes: []config.Route{
		{Path: "/orders", Flow: "ingest"},
		{Path: "/", Flow: "root"},
	}}
	for _, routePath := range []string{"/orders", "/"} {
		t.Run(routePath, func(t *testing.T) {
			p := config.StatusPath(routePath, "abc-123")
			route, id := cfg.StatusRequest(http.MethodGet, p)
			if route == nil {
				t.Fatalf("StatusPath produced %q, which StatusRequest does not resolve", p)
			}
			if route.Path != routePath || id != "abc-123" {
				t.Errorf("%q resolved to route %q id %q", p, route.Path, id)
			}
		})
	}
}
