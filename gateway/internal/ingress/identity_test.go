package ingress_test

import (
	"crypto/sha256"
	"encoding/hex"
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

// THE security property of §4b. A caller that can forge X-Shift-Principal has
// an authentication bypass with an audit trail that lies about it, which is
// strictly worse than propagating no identity at all. The strip must be
// unconditional and case-insensitive, because HTTP header names are.
func TestInboundShiftHeadersAreStrippedBeforeStamping(t *testing.T) {
	reg := runners.New()
	h := ingress.New(reg, nil)
	route := config.Route{
		Path: "/hook", Flow: "orders",
		AuthTokenSHA256: sha256Hex("s3cret"), AuthPrincipal: "acme-erp",
	}
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{route}}); err != nil {
		t.Fatal(err)
	}

	got := make(chan *runners.Request, 1)
	go func() {
		if req := reg.Poll(t.Context(), nil, 3*time.Second); req != nil {
			_, _ = io.Copy(io.Discard, req.Body)
			got <- req
			_ = reg.Deliver(t.Context(), req.ID, &runners.Response{
				Status: http.StatusOK, Body: strings.NewReader("ok"),
			})
		}
	}()
	waitParked(t, reg)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer s3cret")
	// Every spelling a caller might reach for.
	r.Header.Set("X-Shift-Principal", "admin")
	r.Header.Set("x-shift-principal", "admin") // Set canonicalises; belt and braces
	r.Header["x-shift-client-ip"] = []string{"10.0.0.1"}
	r.Header["X-SHIFT-ROUTE"] = []string{"/admin"}
	r.Header.Set("X-Shift-Anything-At-All", "1")
	r.Header.Set("X-Normal-Header", "kept")
	h.ServeHTTP(httptest.NewRecorder(), r)

	req := <-got
	if p := req.Headers.Get(ingress.HeaderPrincipal); p != "acme-erp" {
		t.Errorf("principal = %q, want the CONFIGURED principal, not the caller's claim", p)
	}
	if v := req.Headers.Get(ingress.HeaderRoute); v != "/hook" {
		t.Errorf("route = %q, want /hook", v)
	}
	if v := req.Headers.Get("X-Shift-Anything-At-All"); v != "" {
		t.Errorf("unrecognised X-Shift-* header survived: %q", v)
	}
	if v := req.Headers.Get(ingress.HeaderRequestID); v == "" {
		t.Error("no request id stamped")
	}
	if v := req.Headers.Get("X-Normal-Header"); v != "kept" {
		t.Errorf("X-Normal-Header = %q, want it forwarded untouched", v)
	}
	// The credential itself must not reach the runner: the gateway already
	// consumed it, and the runner has no business seeing it.
	if v := req.Headers.Get("Authorization"); v != "" {
		t.Errorf("Authorization forwarded to the runner: %q", v)
	}
}

// A route with no credential still gets a principal, so "nobody
// authenticated" is distinguishable downstream from "the gateway forgot".
func TestOpenRouteStampsAnonymous(t *testing.T) {
	reg := runners.New()
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{
		{Path: "/open", Flow: "f"},
	}}); err != nil {
		t.Fatal(err)
	}

	got := make(chan *runners.Request, 1)
	go func() {
		if req := reg.Poll(t.Context(), nil, 3*time.Second); req != nil {
			_, _ = io.Copy(io.Discard, req.Body)
			got <- req
			_ = reg.Deliver(t.Context(), req.ID, &runners.Response{Status: http.StatusOK})
		}
	}()
	waitParked(t, reg)

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/open", strings.NewReader("{}")))

	if p := (<-got).Headers.Get(ingress.HeaderPrincipal); p != "anonymous" {
		t.Errorf("principal = %q, want %q", p, "anonymous")
	}
}

// A route whose selector no parked runner satisfies must 503 rather than be
// served by an ineligible runner. Placement is a correctness boundary
// (ADR-0030), not a preference.
func TestRouteSelectorGatesDispatch(t *testing.T) {
	reg := runners.New()
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{
		{Path: "/prod", Flow: "f", Selector: config.Selector{"environment": "production"}},
	}}); err != nil {
		t.Fatal(err)
	}

	// A staging runner is parked and willing; it must not be chosen.
	go reg.Poll(t.Context(), map[string]string{"environment": "staging"}, 2*time.Second)
	waitParked(t, reg)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/prod", strings.NewReader("{}")))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 — a staging runner served production work", w.Code)
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func waitParked(t *testing.T, reg *runners.Registry) {
	t.Helper()
	for range 200 {
		if reg.Parked() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("runner did not park")
}
