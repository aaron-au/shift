package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

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
			// One opaque failure for every path — no oracle.
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if id.role != "admin" && r.Method != http.MethodGet {
			writeErr(w, http.StatusForbidden, fmt.Errorf("role %q may not modify", id.role))
			return
		}
		next(w, r.WithContext(withIdentity(r.Context(), id)))
	})
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
		if id, account, err := a.st.AuthRunner(r.Context(), bearer(r)); err == nil {
			ctx := withIdentity(context.WithValue(r.Context(), runnerKey{}, id),
				identity{kind: "runner", id: id, account: account})
			next(w, r.WithContext(ctx))
			return
		}
		if id, ok := a.authenticateAdmin(r); ok {
			next(w, r.WithContext(withIdentity(r.Context(), id)))
			return
		}
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	})
}

type runnerKey struct{}

func (a *api) runner(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, account, err := a.st.AuthRunner(r.Context(), bearer(r))
		if err != nil {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
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
func (a *api) login(w http.ResponseWriter, r *http.Request) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	state := hex.EncodeToString(buf[:])
	//nolint:gosec // G124: HttpOnly+SameSite set; Secure follows TLS — plaintext hubd is loopback-only by policy (ADR-0009 §5)
	http.SetCookie(w, &http.Cookie{
		Name: stateCookie, Value: state, Path: "/auth",
		MaxAge: 300, HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteLaxMode,
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
		HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteLaxMode})

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
	//nolint:gosec // G124: HttpOnly+SameSite set; Secure follows TLS — plaintext hubd is loopback-only by policy (ADR-0009 §5)
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: raw, Path: "/",
		HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteLaxMode,
		Expires: time.Now().Add(12 * time.Hour), // token exp still governs
	})
	http.Redirect(w, r, "/", http.StatusFound)
}

func (a *api) logout(w http.ResponseWriter, r *http.Request) {
	//nolint:gosec // G124: deletion cookie (MaxAge -1), attributes mirror the set cookie
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, "/", http.StatusFound)
}

// me echoes the caller's identity (CLI/debug convenience).
func (a *api) me(w http.ResponseWriter, r *http.Request) {
	id := requestIdentity(r)
	writeJSON(w, http.StatusOK, map[string]string{
		"kind": id.kind, "id": id.id, "email": id.email, "role": id.role, "account": id.account,
	})
}
