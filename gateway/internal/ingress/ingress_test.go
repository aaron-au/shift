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

func route() config.Route {
	return config.Route{Path: "/hook", Flow: "orders"}
}

func serve(t *testing.T, routes []config.Route, trusted ...string) (*ingress.Handler, *runners.Registry) {
	t.Helper()
	reg := runners.New()
	reg.DeliveryTimeout = 2 * time.Second
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: routes, TrustedProxies: trusted}); err != nil {
		t.Fatalf("config: %v", err)
	}
	return h, reg
}

// parkRunner answers one request with the given body, echoing what it received
// so a test can prove the request reached it intact.
func parkRunner(t *testing.T, reg *runners.Registry, reply string) chan *runners.Request {
	t.Helper()
	got := make(chan *runners.Request, 1)
	go func() {
		req := reg.Poll(t.Context(), nil, 3*time.Second)
		if req == nil {
			got <- nil
			return
		}
		body, _ := io.ReadAll(req.Body)
		req.Body = strings.NewReader(string(body)) // hand the read bytes back
		got <- req
		reg.Deliver(t.Context(), req.ID, &runners.Response{
			Status: 200, Headers: http.Header{"X-From": []string{"runner"}},
			Body: strings.NewReader(reply),
		})
	}()
	waitAvailable(t, reg, 1)
	return got
}

func waitAvailable(t *testing.T, reg *runners.Registry, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if reg.Parked() == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("runner did not park (available=%d)", reg.Parked())
}

// The happy path, end to end: request in, runner serves it, response streams
// back — and the runner actually received the body.
func TestRequestReachesTheRunnerAndTheResponseStreamsBack(t *testing.T) {
	h, reg := serve(t, []config.Route{route()})
	got := parkRunner(t, reg, "pong")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", strings.NewReader(`{"n":1}`)))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	if w.Body.String() != "pong" {
		t.Fatalf("body = %q, want the runner's response", w.Body.String())
	}
	if w.Header().Get("X-From") != "runner" {
		t.Error("runner response headers were not forwarded")
	}
	req := <-got
	if req == nil {
		t.Fatal("runner received nothing")
	}
	if req.Flow != "orders" {
		t.Errorf("flow = %q, want orders", req.Flow)
	}
	body, _ := io.ReadAll(req.Body)
	if string(body) != `{"n":1}` {
		t.Errorf("runner got body %q, want the caller's", body)
	}
}

// No runner is a 503 with Retry-After — never a queue, because a gateway that
// holds work is a gateway with durable state in the DMZ.
func TestNoRunnerIs503NotAQueue(t *testing.T) {
	h, _ := serve(t, []config.Route{route()})
	w := httptest.NewRecorder()
	start := time.Now()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After; a well-behaved sender should be told to come back")
	}
	if time.Since(start) > time.Second {
		t.Error("the gateway held the request waiting for a runner — that is a queue")
	}
}

// Until the hub pushes a configuration the gateway serves nothing. That is the
// correct state, not a degradation to paper over.
func TestUnconfiguredGatewayServes503(t *testing.T) {
	h := ingress.New(runners.New(), nil)
	if h.Configured() {
		t.Fatal("a fresh gateway reported itself configured")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

func TestBearerTokenIsCheckedAgainstItsDigest(t *testing.T) {
	sum := sha256.Sum256([]byte("s3cret"))
	r := route()
	r.AuthTokenSHA256 = hex.EncodeToString(sum[:])
	h, reg := serve(t, []config.Route{r})

	// Wrong token: rejected before any runner is involved.
	w := httptest.NewRecorder()
	bad := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil)
	bad.Header.Set("Authorization", "Bearer wrong")
	h.ServeHTTP(w, bad)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}

	parkRunner(t, reg, "ok")
	w = httptest.NewRecorder()
	good := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil)
	good.Header.Set("Authorization", "Bearer s3cret")
	h.ServeHTTP(w, good)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
}

// The caller's credential is the gateway's business, not the runner's.
func TestAuthorizationHeaderIsNotForwardedToTheRunner(t *testing.T) {
	h, reg := serve(t, []config.Route{route()})
	got := parkRunner(t, reg, "ok")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", strings.NewReader("x"))
	req.Header.Set("Authorization", "Bearer s3cret")
	req.Header.Set("X-Keep", "yes")
	h.ServeHTTP(httptest.NewRecorder(), req)

	fwd := <-got
	if fwd == nil {
		t.Fatal("runner received nothing")
	}
	if fwd.Headers.Get("Authorization") != "" {
		t.Error("the caller's credential was forwarded to the runner")
	}
	if fwd.Headers.Get("X-Keep") != "yes" {
		t.Error("ordinary headers must still be forwarded")
	}
}

// An IP allowlist that can be defeated by a header is not an allowlist. A
// forwarded header is believed ONLY from a configured trusted proxy.
func TestForwardedHeaderIsIgnoredFromAnUntrustedPeer(t *testing.T) {
	r := route()
	r.AllowCIDRs = []string{"10.0.0.0/8"}
	h, _ := serve(t, []config.Route{r}) // no trusted proxies

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil)
	req.RemoteAddr = "203.0.113.9:1234" // outside the allowlist
	req.Header.Set("X-Forwarded-For", "10.0.0.5")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — a spoofed X-Forwarded-For walked through the allowlist", w.Code)
	}
}

// ...and IS believed from one, which is how the gateway runs behind an F5/ALB.
func TestForwardedHeaderIsHonouredFromATrustedProxy(t *testing.T) {
	r := route()
	r.AllowCIDRs = []string{"10.0.0.0/8"}
	h, reg := serve(t, []config.Route{r}, "192.0.2.0/24")
	parkRunner(t, reg, "ok")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil)
	req.RemoteAddr = "192.0.2.7:443" // the trusted proxy
	req.Header.Set("X-Forwarded-For", "10.0.0.5, 192.0.2.7")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
}

// A blocked caller must not learn that the route exists.
func TestBlockedCallerCannotDistinguishFromAnUnknownPath(t *testing.T) {
	r := route()
	r.AllowCIDRs = []string{"10.0.0.0/8"}
	h, _ := serve(t, []config.Route{r})

	blocked := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil)
	blocked.RemoteAddr = "203.0.113.9:1234"
	wb := httptest.NewRecorder()
	h.ServeHTTP(wb, blocked)

	wu := httptest.NewRecorder()
	h.ServeHTTP(wu, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/nope", nil))

	if wb.Code != wu.Code {
		t.Fatalf("blocked = %d, unknown = %d; the difference is free reconnaissance", wb.Code, wu.Code)
	}
}

func TestRequiredHeadersAreEnforced(t *testing.T) {
	r := route()
	r.RequireHeaders = map[string]string{"X-Hub-Signature": "", "X-Env": "prod"}
	h, _ := serve(t, []config.Route{r})

	for name, set := range map[string]map[string]string{
		"missing both":      {},
		"missing one":       {"X-Hub-Signature": "abc"},
		"wrong exact value": {"X-Hub-Signature": "abc", "X-Env": "dev"},
	} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil)
		for k, v := range set {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, w.Code)
		}
	}
}

// A method-specific route must not be shadowed by a method-agnostic one.
func TestMethodSpecificRouteWinsOverWildcard(t *testing.T) {
	specific := config.Route{Path: "/hook", Flow: "post-flow", Method: http.MethodPost}
	wildcard := config.Route{Path: "/hook", Flow: "any-flow"}
	h, reg := serve(t, []config.Route{wildcard, specific})
	got := parkRunner(t, reg, "ok")

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", nil))
	req := <-got
	if req == nil || req.Flow != "post-flow" {
		t.Fatalf("flow = %v, want the method-specific route", req)
	}
}
