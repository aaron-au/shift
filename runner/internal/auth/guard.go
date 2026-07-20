package auth

import "net/http"

// Guard enforces authentication + per-endpoint authorization. A nil
// Authenticator means the surface is open (loopback dev): every request
// passes. Configuring users turns enforcement on.
type Guard struct {
	auth Authenticator
}

// NewGuard builds a guard. Pass nil to leave the surface open.
func NewGuard(a Authenticator) *Guard { return &Guard{auth: a} }

// Enabled reports whether authentication is enforced.
func (g *Guard) Enabled() bool { return g != nil && g.auth != nil }

// Wrap guards next. For each request, perm derives the required permission;
// when it returns ok=false the request is unguarded (e.g. health checks,
// hook endpoints with their own token). Open (returns next) when no
// authenticator is configured.
func (g *Guard) Wrap(next http.Handler, perm func(*http.Request) (Permission, bool)) http.Handler {
	if !g.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		need, guarded := perm(r)
		if !guarded {
			next.ServeHTTP(w, r)
			return
		}
		p, ok := g.auth.Authenticate(r)
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="shift-runner"`)
			http.Error(w, `{"error":"authentication required"}`, http.StatusUnauthorized)
			return
		}
		if !p.Role.Can(need) {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
