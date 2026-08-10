package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aaron-au/shift/hub/internal/pki"
	"github.com/aaron-au/shift/hub/internal/ratelimit"
	"github.com/aaron-au/shift/hub/internal/store"
)

// identity is the authenticated principal attached to a request.
type identity struct {
	kind    string // "user" | "breakglass" | "runner"
	id      string // user id / runner id; empty for break-glass
	email   string
	role    string // admin | viewer
	account string
}

type identityKey struct{}

func withIdentity(ctx context.Context, id identity) context.Context {
	return store.WithAccount(context.WithValue(ctx, identityKey{}, id), id.account)
}

func requestIdentity(r *http.Request) identity {
	id, _ := r.Context().Value(identityKey{}).(identity)
	return id
}

// actor renders the audit-log actor for the request's identity.
func actor(r *http.Request) string {
	switch id := requestIdentity(r); id.kind {
	case "user":
		if id.email != "" {
			return "user:" + id.email
		}
		return "user:" + id.id
	case "breakglass":
		return "admin:break-glass"
	case "runner":
		return "runner:" + id.id
	default:
		return "unknown"
	}
}

const sessionCookie = "shift_session"

// adminCredential extracts the presented admin credential: bearer
// header first (API clients), session cookie second (dashboard).
func adminCredential(r *http.Request) string {
	if tok := bearer(r); tok != "" {
		return tok
	}
	if c, err := r.Cookie(sessionCookie); err == nil {
		return c.Value
	}
	return ""
}

// admin authenticates the human realm: the break-glass token when
// configured, else an OIDC token (header or session cookie) with JIT
// user provisioning. Viewers may only read.
func (a *api) admin(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := a.authenticateAdmin(r)
		if !ok {
			// One opaque failure for every path — no oracle. Still the
			// ADR-0023 envelope: a client that parses every other error
			// response one way should not need a second parser for the one it
			// meets most often. The message stays deliberately uninformative.
			writeErr(w, http.StatusUnauthorized, errUnauthorized)
			return
		}
		if id.role != "admin" && r.Method != http.MethodGet {
			writeErr(w, http.StatusForbidden, fmt.Errorf("role %q may not modify", id.role))
			return
		}
		if !a.opts.RateLimit.Allow("admin", rlKey(id)) {
			ratelimit.Reject(w)
			return
		}
		next(w, r.WithContext(withIdentity(r.Context(), id)))
	})
}

// rlKey is the rate-limit key for an authenticated identity (ADR-0021).
func rlKey(id identity) string {
	if id.kind == "user" && id.email != "" {
		return "user:" + id.email
	}
	return id.kind + ":" + id.id
}

// publicLimit rate-limits an unauthenticated route by client IP (the
// credential-stuffing / anonymous-flood surface, ADR-0021).
func (a *api) publicLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.opts.RateLimit.Allow("public", ratelimit.ClientIP(r)) {
			ratelimit.Reject(w)
			return
		}
		next(w, r)
	}
}

func (a *api) authenticateAdmin(r *http.Request) (identity, bool) {
	cred := adminCredential(r)
	if cred == "" {
		return identity{}, false
	}
	if a.opts.AdminToken != "" &&
		subtle.ConstantTimeCompare([]byte(cred), []byte(a.opts.AdminToken)) == 1 {
		return identity{kind: "breakglass", role: "admin", account: store.DefaultAccountID}, true
	}
	if a.opts.OIDC != nil {
		oid, err := a.opts.OIDC.Verify(r.Context(), cred)
		if err != nil {
			return identity{}, false
		}
		email := ""
		if oid.EmailVerified {
			email = oid.Email
		}
		u, err := a.st.UpsertUserByOIDC(r.Context(), oid.Issuer, oid.Subject, email, oid.Name)
		if err != nil {
			return identity{}, false
		}
		return identity{kind: "user", id: u.ID, email: u.Email, role: u.Role, account: u.AccountID}, true
	}
	return identity{}, false
}

// adminOrRunner admits either realm (runner first — its lookup is a
// hash probe; the admin path may hit the IdP).
func (a *api) adminOrRunner(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, account, err := a.authRunner(r)
		if err == nil {
			if !a.opts.RateLimit.Allow("runner", id) {
				ratelimit.Reject(w)
				return
			}
			ctx := withIdentity(context.WithValue(r.Context(), runnerKey{}, id),
				identity{kind: "runner", id: id, account: account})
			next(w, r.WithContext(ctx))
			return
		}
		// The admin path is tried REGARDLESS of why the runner check failed. An
		// admin request is expected to fail it, and short-circuiting on a store
		// error would deny an administrator whose own credential needs no
		// database — including during the very outage they are investigating.
		if adminID, ok := a.authenticateAdmin(r); ok {
			next(w, r.WithContext(withIdentity(r.Context(), adminID)))
			return
		}
		// Neither realm admitted the caller. If the runner check could not be
		// ANSWERED — overwhelmingly a database outage — say so, rather than
		// reporting the same 401 a bad credential gets (TC-031).
		if !errors.Is(err, store.ErrUnauthorized) {
			writeErr(w, http.StatusServiceUnavailable, errUnavailable)
			return
		}
		writeErr(w, http.StatusUnauthorized, errUnauthorized)
	})
}

type runnerKey struct{}

// authRunner resolves the runner making this request (ADR-0044).
//
// The certificate is tried FIRST and its failure is terminal for that request:
// a connection that presented a verified certificate has already said who it
// is, and falling back to a bearer token after that would let a runner whose
// certificate names a deleted runner keep working under a secret. The two
// credentials are alternatives, never a chain.
func (a *api) authRunner(r *http.Request) (id, account string, err error) {
	if a.opts.RunnerAuth.allowsMTLS() {
		if certID := pki.Subject(r.TLS); certID != "" {
			account, err = a.st.AuthRunnerCert(r.Context(), certID)
			if err != nil {
				return "", "", err
			}
			return certID, account, nil
		}
	}
	if !a.opts.RunnerAuth.allowsBearer() {
		return "", "", store.ErrUnauthorized
	}
	return a.st.AuthRunner(r.Context(), bearer(r))
}

// errUnavailable is what a runner is told when the hub cannot ANSWER the
// question of who it is, as opposed to answering "not you".
var errUnavailable = errors.New("service unavailable")

// runnerAuthFailure maps an authRunner error to a status.
//
// Only store.ErrUnauthorized means the credential was rejected. Anything else
// — overwhelmingly a database outage — means the hub could not check, and
// reporting that as 401 sends an operator hunting a credential problem that
// does not exist while every runner in the fleet reports the same thing
// (TC-031).
//
// This is diagnosability, not availability: hubclient does not treat 401 as
// terminal and leaseloop retries with backoff, so the fleet already recovers
// on its own. It recovers while telling the operator the wrong story.
func runnerAuthFailure(err error) (int, error) {
	if errors.Is(err, store.ErrUnauthorized) {
		return http.StatusUnauthorized, errUnauthorized
	}
	return http.StatusServiceUnavailable, errUnavailable
}

func (a *api) runner(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, account, err := a.authRunner(r)
		if err != nil {
			status, body := runnerAuthFailure(err)
			writeErr(w, status, body)
			return
		}
		if !a.opts.RateLimit.Allow("runner", id) {
			ratelimit.Reject(w)
			return
		}
		ctx := withIdentity(context.WithValue(r.Context(), runnerKey{}, id),
			identity{kind: "runner", id: id, account: account})
		next(w, r.WithContext(ctx))
	})
}

func runnerID(r *http.Request) string {
	id, _ := r.Context().Value(runnerKey{}).(string)
	return id
}

// --- browser login flow (dashboard) -----------------------------------------

const stateCookie = "shift_oauth_state"

// login redirects to the IdP. The anti-CSRF state rides a short-lived
// cookie.
// cookieSecure decides the Secure attribute for the session/state cookies.
//
// r.TLS alone is wrong for the documented deployment: behind a TLS-terminating
// proxy the hub speaks plaintext on the back side, so r.TLS is nil and the
// session cookie would ship WITHOUT Secure over a genuinely https site — free
// to leak on any plaintext request to the same host. The OIDC redirect URL is
// the authoritative, already-configured statement of the site's external
// scheme, so it decides. Falls back to r.TLS when no OIDC flow is configured
// (break-glass-token-only deployments, which ADR-0009 §5 keeps loopback).
func (a *api) cookieSecure(r *http.Request) bool {
	if a.opts.OIDCFlow != nil && strings.HasPrefix(a.opts.OIDCFlow.RedirectURL(), "https://") {
		return true
	}
	return r.TLS != nil
}

func (a *api) login(w http.ResponseWriter, r *http.Request) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	state := hex.EncodeToString(buf[:])
	//nolint:gosec // G124: HttpOnly+SameSite set; Secure follows the site's external scheme (cookieSecure)
	http.SetCookie(w, &http.Cookie{
		Name: stateCookie, Value: state, Path: "/auth",
		MaxAge: 300, HttpOnly: true, Secure: a.cookieSecure(r), SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, a.opts.OIDCFlow.AuthCodeURL(state), http.StatusFound)
}

// callback finishes the code exchange and installs the session cookie.
// The cookie value is the verified raw ID token: stateless, HA-safe
// (any replica validates it), and it expires with the token.
func (a *api) callback(w http.ResponseWriter, r *http.Request) {
	sc, err := r.Cookie(stateCookie)
	if err != nil || sc.Value == "" || r.URL.Query().Get("state") != sc.Value {
		writeErr(w, http.StatusBadRequest, errors.New("state mismatch"))
		return
	}
	//nolint:gosec // G124: deletion cookie (MaxAge -1), attributes mirror the set cookie
	http.SetCookie(w, &http.Cookie{Name: stateCookie, Value: "", Path: "/auth", MaxAge: -1,
		HttpOnly: true, Secure: a.cookieSecure(r), SameSite: http.SameSiteLaxMode})

	raw, oid, err := a.opts.OIDCFlow.Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		writeErr(w, http.StatusUnauthorized, errors.New("login failed"))
		return
	}
	email := ""
	if oid.EmailVerified {
		email = oid.Email
	}
	if _, err := a.st.UpsertUserByOIDC(r.Context(), oid.Issuer, oid.Subject, email, oid.Name); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	//nolint:gosec // G124: HttpOnly+SameSite set; Secure follows the site's external scheme (cookieSecure)
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: raw, Path: "/",
		HttpOnly: true, Secure: a.cookieSecure(r), SameSite: http.SameSiteLaxMode,
		Expires: time.Now().Add(12 * time.Hour), // token exp still governs
	})
	http.Redirect(w, r, "/", http.StatusFound)
}

func (a *api) logout(w http.ResponseWriter, r *http.Request) {
	//nolint:gosec // G124: deletion cookie (MaxAge -1), attributes mirror the set cookie
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: a.cookieSecure(r), SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, "/", http.StatusFound)
}

// me echoes the caller's identity (CLI/debug convenience).
func (a *api) me(w http.ResponseWriter, r *http.Request) {
	id := requestIdentity(r)
	writeJSON(w, http.StatusOK, map[string]string{
		"kind": id.kind, "id": id.id, "email": id.email, "role": id.role, "account": id.account,
	})
}

// errUnauthorized is the single message every authentication failure returns,
// in every realm. Deliberately uninformative: distinguishing "no such runner"
// from "wrong secret" from "expired session" hands an attacker an oracle. What
// it is NOT is a different response SHAPE from every other error the API emits
// — that was the ADR-0023 gap TC-012 found.
var errUnauthorized = errors.New("unauthorized")
