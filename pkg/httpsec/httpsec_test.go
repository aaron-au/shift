package httpsec

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHeadersAppliedToEveryResponse: the middleware hardens whatever it wraps,
// including error and non-HTML responses (the CSV exports rely on nosniff).
func TestHeadersAppliedToEveryResponse(t *testing.T) {
	h := Headers(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("a,b\n"))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/export.csv", nil))

	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d (middleware must not alter the response)", rec.Code, http.StatusTeapot)
	}
	want := map[string]string{
		"X-Content-Type-Options":       "nosniff",
		"X-Frame-Options":              "DENY",
		"Referrer-Policy":              "no-referrer",
		"Cross-Origin-Opener-Policy":   "same-origin",
		"Cross-Origin-Resource-Policy": "same-origin",
	}
	for k, v := range want {
		if got := rec.Header().Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if rec.Body.String() != "a,b\n" {
		t.Errorf("body = %q, want the wrapped handler's body", rec.Body.String())
	}
}

// TestCSPDirectives pins the directives that carry the security value, so a
// future edit cannot quietly drop one. frame-ancestors and object-src in
// particular are what block clickjacking and plugin-based script execution.
func TestCSPDirectives(t *testing.T) {
	rec := httptest.NewRecorder()
	SetHeaders(rec)
	csp := rec.Header().Get("Content-Security-Policy")
	for _, d := range []string{
		"default-src 'self'",
		"object-src 'none'",
		"base-uri 'none'",
		"frame-ancestors 'none'",
		"form-action 'self'",
		"connect-src 'self'",
	} {
		if !strings.Contains(csp, d) {
			t.Errorf("CSP missing %q\n  got: %s", d, csp)
		}
	}
	// The dashboards are no-build inline pages, so script/style keep
	// 'unsafe-inline' — but nothing else may. If a future CSP allows an
	// off-origin script source, that is a real regression.
	for _, forbidden := range []string{"'unsafe-eval'", "http://", "https://", "*"} {
		if strings.Contains(csp, forbidden) {
			t.Errorf("CSP must not contain %q\n  got: %s", forbidden, csp)
		}
	}
}
