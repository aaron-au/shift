// Package httpsec supplies the response-hardening headers shared by the hub's
// and the runner's HTTP surfaces. Both serve an embedded dashboard plus a JSON
// API to authenticated humans, so both want the same baseline; keeping it in
// one place stops the two from drifting.
//
// It is deliberately stdlib-only middleware — no policy decisions, no state.
package httpsec

import "net/http"

// CSP is the Content-Security-Policy served with every response.
//
// `script-src` and `style-src` still carry 'unsafe-inline' because the
// dashboards are vanilla no-build pages (ADR-0019) whose handlers and styles
// are inline attributes. That weakens CSP against injected script, so it is a
// second line of defense, not the first — the first is that every value
// interpolated into those pages is escaped for its sink (see the esc/escJS
// pair in ui.html). Moving the handlers to delegated listeners would let this
// tighten to a per-response nonce.
//
// The rest is strict: no plugins, no base-tag hijacking, no framing (which,
// with X-Frame-Options, blocks clickjacking of the deploy/publish/run
// controls), and no off-origin fetch, script, or style source.
const CSP = "default-src 'self'; " +
	"script-src 'self' 'unsafe-inline'; " +
	"style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; " +
	"font-src 'self'; " +
	"connect-src 'self'; " +
	"object-src 'none'; " +
	"base-uri 'none'; " +
	"form-action 'self'; " +
	"frame-ancestors 'none'"

// Headers wraps h so every response carries the hardening set.
//
// Notably X-Content-Type-Options: nosniff matters beyond the HTML — the hub
// exports audit and usage data as CSV, and without it a browser may sniff an
// attacker-influenced export back into HTML and execute it.
func Headers(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SetHeaders(w)
		h.ServeHTTP(w, r)
	})
}

// SetHeaders applies the hardening set to one response. Use it directly when a
// handler is not reached through Headers.
func SetHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Security-Policy", CSP)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	// The control planes are same-origin apps; no cross-origin page needs to
	// read a response, and none of them are meant to be embedded.
	h.Set("Cross-Origin-Opener-Policy", "same-origin")
	h.Set("Cross-Origin-Resource-Policy", "same-origin")
}
