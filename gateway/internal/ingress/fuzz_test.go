package ingress_test

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// TC-017. The gateway is the only component of SHIFT that is meant to be
// publicly reachable (ADR-0038), so its request parsing is the one parser in
// the system eating bytes chosen by an unauthenticated stranger. Everything
// else — the format readers, the schema compiler — was fuzzed under TC-003;
// this was the gap that sweep left open.
//
// These targets assert PROPERTIES, not absence of crashes alone. A gateway that
// survives every input while forwarding a forged identity header has not
// passed.

// fuzzHandler builds a handler whose single route is token-protected and
// bound to a trusted-proxy range, so the interesting decisions (auth,
// allowlist, XFF handling, identity stamping) are all live.
func fuzzHandler(tb testing.TB) (*ingress.Handler, *runners.Registry) {
	tb.Helper()
	sum := sha256.Sum256([]byte("s3cret"))
	r := config.Route{
		Path:            "/hook",
		Flow:            "orders",
		AuthPrincipal:   "partner-a",
		AuthTokenSHA256: hex.EncodeToString(sum[:]),
		// The allowlist must ADMIT the peer these tests use, or every request
		// 404s at the allowlist and never reaches the forwarding path — which
		// is exactly how the first version of this file managed to assert
		// nothing while passing.
		AllowCIDRs: []string{"10.0.0.0/8", "192.0.2.0/24"},
	}
	reg := runners.New()
	reg.DeliveryTimeout = 50 * time.Millisecond
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{
		Version:        1,
		Routes:         []config.Route{r},
		TrustedProxies: []string{"192.0.2.0/24"},
	}); err != nil {
		tb.Fatalf("config: %v", err)
	}
	return h, reg
}

// FuzzIngressRequestLine drives whole requests parsed off the wire, which is
// the shape the DMZ actually sees — a malformed request line or header block
// must be refused by net/http or handled, never panic the process.
func FuzzIngressRequestLine(f *testing.F) {
	f.Add("GET /hook HTTP/1.1\r\nHost: x\r\n\r\n")
	f.Add("POST /hook HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer s3cret\r\nContent-Length: 2\r\n\r\nhi")
	f.Add("GET /hook/status/abc HTTP/1.1\r\nHost: x\r\n\r\n")
	f.Add("GET /hook HTTP/1.1\r\nHost: x\r\nX-Forwarded-For: 1.2.3.4, 5.6.7.8\r\n\r\n")
	f.Add("GET /hook HTTP/1.1\r\nHost: x\r\nX-Shift-Principal: admin\r\n\r\n")
	f.Add("GET /%2e%2e/hook HTTP/1.1\r\nHost: x\r\n\r\n")
	f.Add("GET /hook?a=%ff%fe HTTP/1.1\r\nHost: \x00\r\n\r\n")

	h, _ := fuzzHandler(f)
	f.Fuzz(func(t *testing.T, raw string) {
		req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
		if err != nil {
			return // net/http refused it before we ever saw it: that is a pass
		}
		req.RemoteAddr = "192.0.2.10:1234" // a trusted proxy, so XFF is honoured
		if req.URL == nil {
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code < 100 || rec.Code > 599 {
			t.Fatalf("status %d is not a valid HTTP code for request %q", rec.Code, raw)
		}
	})
}

// FuzzIngressNeverForwardsAClientIdentity is the property that matters most.
// The runner trusts X-Shift-* headers because the gateway promises to strip
// anything a caller sent and stamp its own (ingress.go's "strip, then stamp").
// If any spelling of those headers can survive from the client, the gateway is
// an authentication bypass with an audit trail that lies about it.
func FuzzIngressNeverForwardsAClientIdentity(f *testing.F) {
	f.Add("X-Shift-Principal", "admin")
	f.Add("x-shift-principal", "admin")
	f.Add("X-SHIFT-PRINCIPAL", "admin")
	f.Add("X-Shift-Client-Ip", "10.0.0.1")
	f.Add("X-Shift-Route", "/other")
	f.Add("X-Shift-Flow", "payroll")
	f.Add("Connection", "close")
	f.Add("X-Forwarded-For", "10.0.0.1")

	f.Fuzz(func(t *testing.T, name, value string) {
		// http.Header rejects nothing, but a name with a colon or control
		// character could never have arrived over the wire in the first place.
		if name == "" || strings.ContainsAny(name, ":\r\n\x00 \t") || strings.ContainsAny(value, "\r\n\x00") {
			return
		}

		h, reg := fuzzHandler(t)
		got := make(chan *runners.Request, 1)
		go func() {
			req := reg.Poll(t.Context(), nil, 3*time.Second)
			got <- req
			if req == nil {
				return
			}
			// Answer it. Without a delivery ServeHTTP waits out the delivery
			// timeout, which turns every case into a slow no-op.
			reg.Deliver(t.Context(), req.ID, &runners.Response{
				Status: 200, Body: strings.NewReader("ok"),
			})
		}()
		// Wait for the runner to actually park rather than sleeping and hoping.
		deadline := time.Now().Add(2 * time.Second)
		for reg.Parked() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("runner never parked; the request would not reach the forwarding path")
			}
			time.Sleep(time.Millisecond)
		}

		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", strings.NewReader("{}"))
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set("Authorization", "Bearer s3cret")
		req.Header[name] = []string{value} // raw assignment: skip canonicalisation
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code == http.StatusNotFound || rec.Code == http.StatusUnauthorized || rec.Code == http.StatusBadRequest {
			// Refused before forwarding. Legitimate for some fuzzed headers,
			// but it must not be the outcome for the seeds — see the guard below.
			return
		}
		select {
		case fwd := <-got:
			if fwd == nil {
				t.Fatal("the request was accepted but no runner received it")
			}
			if p := fwd.Headers.Get(ingress.HeaderPrincipal); p != "partner-a" {
				t.Fatalf("principal forwarded as %q, want the route's own %q (header %q: %q)",
					p, "partner-a", name, value)
			}
			// The property is about headers the CALLER named X-Shift-*. A
			// non-X-Shift header may legitimately influence a stamped value —
			// X-Forwarded-For becomes X-Shift-Client-Ip when the peer is a
			// trusted proxy, which is ADR-0038's design and which an earlier
			// version of this check reported as a survival. That was a false
			// positive found by the fuzzer, and narrowing the claim is the fix:
			// "derived from" and "forwarded verbatim" are different things.
			if !strings.HasPrefix(http.CanonicalHeaderKey(name), shiftHeaderPrefix) {
				return
			}
			for k, vs := range fwd.Headers {
				if !strings.HasPrefix(http.CanonicalHeaderKey(k), shiftHeaderPrefix) {
					continue
				}
				for _, v := range vs {
					if v == value {
						t.Fatalf("client header %q: %q survived to the runner as %q", name, value, k)
					}
				}
			}
		case <-time.After(3 * time.Second):
			t.Fatal("the request was accepted but never reached a runner")
		}
	})
}

// TestTheIdentityFuzzTargetActuallyReachesARunner is the non-vacuity guard for
// the target above, and it exists because the first version of that target
// passed while asserting nothing: the route's allowlist rejected the test's own
// peer, so every case 404'd before the forwarding path and the timeout branch
// swallowed it silently. A property test that cannot reach the code it targets
// is worse than no test, because it reports success.
func TestTheIdentityFuzzTargetActuallyReachesARunner(t *testing.T) {
	h, reg := fuzzHandler(t)
	got := make(chan *runners.Request, 1)
	go func() {
		req := reg.Poll(t.Context(), nil, 3*time.Second)
		got <- req
		if req != nil {
			reg.Deliver(t.Context(), req.ID, &runners.Response{Status: 200, Body: strings.NewReader("ok")})
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for reg.Parked() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("runner never parked")
		}
		time.Sleep(time.Millisecond)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/hook", strings.NewReader("{}"))
	req.RemoteAddr = "192.0.2.10:1234"
	req.Header.Set("Authorization", "Bearer s3cret")
	req.Header["X-Shift-Flow"] = []string{"payroll"} // a forged identity header
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: the fuzz target's own configuration refuses its own request", rec.Code)
	}
	fwd := <-got
	if fwd == nil {
		t.Fatal("no runner received the request")
	}
	if v := fwd.Headers.Get("X-Shift-Flow"); v == "payroll" {
		t.Fatalf("the caller's X-Shift-Flow %q reached the runner: strip-then-stamp is not holding", v)
	}
}

// shiftHeaderPrefix is the canonical spelling of the reserved header namespace
// the runner trusts. It is written out here rather than imported so the test
// would still fail if the production constant were changed to something that
// no longer covers the headers a caller might forge.
const shiftHeaderPrefix = "X-Shift-"

// FuzzStatusPathParsing targets the status-path split directly: it slices a
// caller-controlled path around a marker segment and hands the tail on as a
// task id. A tail that escaped its route would read another route's task.
func FuzzStatusPathParsing(f *testing.F) {
	f.Add("/hook/status/abc")
	f.Add("/hook/status/")
	f.Add("/hook/status/a/b")
	f.Add("//status/x")
	f.Add("/status/x")
	f.Add("/hook/status/status/x")
	f.Add("/hook/status/\x00")

	cfg := &config.Config{Version: 1, Routes: []config.Route{{Path: "/hook", Flow: "orders"}}}
	f.Fuzz(func(t *testing.T, path string) {
		route, id := cfg.StatusRequest(http.MethodGet, path)
		if route == nil {
			return
		}
		if id == "" {
			t.Fatalf("path %q resolved to a route with an EMPTY task id", path)
		}
		if strings.Contains(id, "/") {
			t.Fatalf("path %q yielded task id %q containing a separator: it can address another route's namespace", path, id)
		}
		if route.Path != "/hook" {
			t.Fatalf("path %q resolved to route %q, which is not configured", path, route.Path)
		}
	})
}

// FuzzClientIP targets the X-Forwarded-For split. The value is chosen by the
// caller whenever the peer is a trusted proxy, and the result is stamped as
// X-Shift-Client-Ip — which is what an allowlist and an audit trail believe.
func FuzzClientIP(f *testing.F) {
	f.Add("1.2.3.4")
	f.Add("1.2.3.4, 5.6.7.8")
	f.Add(" , , ")
	f.Add("::1")
	f.Add(strings.Repeat("1.2.3.4,", 1000))
	f.Add("\x00\r\n")

	cfg := &config.Config{Version: 1, TrustedProxies: []string{"192.0.2.0/24"},
		Routes: []config.Route{{Path: "/hook", Flow: "orders"}}}
	if err := cfg.Validate(); err != nil {
		f.Fatalf("seed config: %v", err)
	}

	f.Fuzz(func(t *testing.T, xff string) {
		if strings.ContainsAny(xff, "\r\n\x00") {
			return // could not have arrived over the wire
		}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/hook", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set("X-Forwarded-For", xff)

		ip := config.ClientIP(req, cfg.TrustedProxyNets())
		// The result is stamped into a header, so it must never be able to
		// inject one — that would let a caller append headers of their choosing
		// to what the runner receives.
		if strings.ContainsAny(ip, "\r\n\x00") {
			t.Fatalf("X-Forwarded-For %q produced client IP %q containing a control character: header injection", xff, ip)
		}
	})
}
