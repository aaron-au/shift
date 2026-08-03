package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/oidcauth/oidctest"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

// noRedirect follows nothing: the login flow is asserted one 302 at a time.
func noRedirect() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

// newLoginServer builds a hub with the browser login flow (/auth/*) wired to a
// fake IdP whose redirect_uri points back at this very test server.
func newLoginServer(t *testing.T) (*httptest.Server, *oidctest.IdP) {
	t.Helper()
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}

	idp := oidctest.New(t, "shift-hub")
	// Unstarted: the listener (hence the callback URL) exists before the
	// handler that needs it.
	srv := httptest.NewUnstartedServer(nil)
	callbackURL := "http://" + srv.Listener.Addr().String() + "/auth/callback"

	verifier, err := oidcauth.New(t.Context(), oidcauth.Config{
		IssuerURL: idp.Issuer(), ClientID: "shift-hub",
	})
	if err != nil {
		t.Fatal(err)
	}
	flow, err := oidcauth.NewFlow(t.Context(), oidcauth.FlowConfig{
		Config:       oidcauth.Config{IssuerURL: idp.Issuer(), ClientID: "shift-hub"},
		ClientSecret: "hub-secret",
		RedirectURL:  callbackURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		OIDC:       verifier,
		OIDCFlow:   flow,
		LeaseTTL:   2 * time.Second,
		LeasePoll:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = h
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, idp
}

// httpResp is a drained response: status, headers and Set-Cookies, with the
// body already closed so no test can leak a connection.
type httpResp struct {
	StatusCode int
	Header     http.Header
	cookies    []*http.Cookie
}

// cookieNamed finds a Set-Cookie by name on a response.
func cookieNamed(resp httpResp, name string) *http.Cookie {
	for _, c := range resp.cookies {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// get issues an unfollowed GET with the given cookies.
func get(t *testing.T, url string, cookies ...*http.Cookie) httpResp {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cookies {
		req.AddCookie(c)
	}
	resp, err := noRedirect().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return httpResp{StatusCode: resp.StatusCode, Header: resp.Header, cookies: resp.Cookies()}
}

// TestBrowserLoginFlow walks the dashboard's OIDC login end to end: the state
// cookie is minted and checked, the code exchange installs a session cookie
// that authenticates the API, and logout revokes it.
func TestBrowserLoginFlow(t *testing.T) {
	srv, idp := newLoginServer(t)

	// The login page tells an anonymous browser that OIDC login exists.
	var ai struct {
		OIDCLogin  bool `json:"oidc_login"`
		BreakGlass bool `json:"break_glass"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/authinfo", "", "", &ai); c != 200 || !ai.OIDCLogin {
		t.Fatalf("authinfo = %d %+v, want oidc_login true", c, ai)
	}

	// 1. /auth/login redirects to the IdP and mints the anti-CSRF state cookie.
	resp := get(t, srv.URL+"/auth/login")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("login = %d, want 302", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.Header.Get("Location"), idp.Issuer()+"/auth") {
		t.Fatalf("login Location = %q, want the IdP authorize endpoint", resp.Header.Get("Location"))
	}
	stateCookie := cookieNamed(resp, "shift_oauth_state")
	if stateCookie == nil || stateCookie.Value == "" {
		t.Fatal("login did not set the shift_oauth_state cookie")
	}
	if !stateCookie.HttpOnly || stateCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("state cookie = %+v, want HttpOnly + SameSite=Lax", stateCookie)
	}
	state := loc.Query().Get("state")
	if state == "" || state != stateCookie.Value {
		t.Fatalf("redirect state %q does not match cookie state %q", state, stateCookie.Value)
	}

	// 2. A callback with no state cookie at all is rejected.
	resp = get(t, srv.URL+"/auth/callback?state="+state+"&code=x")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("callback without state cookie = %d, want 400", resp.StatusCode)
	}

	// 3. A callback whose query state does not match the cookie is rejected —
	//    this is the CSRF gate; it must fire before any code exchange.
	resp = get(t, srv.URL+"/auth/callback?state=attacker-chosen&code=x", stateCookie)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("callback with mismatched state = %d, want 400", resp.StatusCode)
	}

	// 4. Correct state but a code the IdP never issued → 401, no session.
	resp = get(t, srv.URL+"/auth/callback?state="+state+"&code=never-issued", stateCookie)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("callback with an unknown code = %d, want 401", resp.StatusCode)
	}
	if cookieNamed(resp, "shift_session") != nil {
		t.Fatal("a failed exchange installed a session cookie")
	}

	// 5. The happy path: a real code → session cookie + redirect home.
	idp.Authorize(t, "good-code", oidctest.Claims{
		Subject: "u-1", Email: "aaron@example.com", EmailVerified: true, Name: "Aaron",
	})
	resp = get(t, srv.URL+"/auth/callback?state="+state+"&code=good-code", stateCookie)
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/" {
		t.Fatalf("callback = %d -> %q, want 302 -> /", resp.StatusCode, resp.Header.Get("Location"))
	}
	session := cookieNamed(resp, "shift_session")
	if session == nil || session.Value == "" {
		t.Fatal("successful callback did not install a session cookie")
	}
	if !session.HttpOnly || session.SameSite != http.SameSiteLaxMode || session.Path != "/" {
		t.Fatalf("session cookie = %+v, want HttpOnly + SameSite=Lax on /", session)
	}
	// The one-shot state cookie is cleared on the way out.
	if cleared := cookieNamed(resp, "shift_oauth_state"); cleared == nil || cleared.MaxAge >= 0 {
		t.Fatalf("state cookie after callback = %+v, want a deletion cookie", cleared)
	}

	// 6. The session cookie alone authenticates the admin realm (no bearer).
	resp = get(t, srv.URL+"/api/v1/me", session)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("me with session cookie = %d, want 200", resp.StatusCode)
	}

	// 7. Logout clears the session cookie and sends the browser home.
	resp = get(t, srv.URL+"/auth/logout", session)
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/" {
		t.Fatalf("logout = %d -> %q, want 302 -> /", resp.StatusCode, resp.Header.Get("Location"))
	}
	gone := cookieNamed(resp, "shift_session")
	if gone == nil || gone.Value != "" || gone.MaxAge >= 0 {
		t.Fatalf("logout session cookie = %+v, want a deletion cookie", gone)
	}

	// 8. A forged session cookie is not a session.
	forged := &http.Cookie{
		Name: "shift_session", Value: "forged.token.value", Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	}
	resp = get(t, srv.URL+"/api/v1/me", forged)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("me with a forged session cookie = %d, want 401", resp.StatusCode)
	}
}

// TestLoginFlowRejectsTokenResponseWithoutIDToken: an IdP (or a MITM) that
// returns an OAuth2 token but no id_token must not yield a hub session.
func TestLoginFlowRejectsTokenResponseWithoutIDToken(t *testing.T) {
	srv, idp := newLoginServer(t)

	resp := get(t, srv.URL+"/auth/login")
	stateCookie := cookieNamed(resp, "shift_oauth_state")
	if stateCookie == nil {
		t.Fatal("no state cookie")
	}
	idp.AuthorizeRaw("no-idtoken", map[string]any{
		"access_token": "access-only", "token_type": "Bearer", "expires_in": 3600,
	})

	resp = get(t, srv.URL+"/auth/callback?state="+stateCookie.Value+"&code=no-idtoken", stateCookie)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("callback without id_token = %d, want 401", resp.StatusCode)
	}
	if cookieNamed(resp, "shift_session") != nil {
		t.Fatal("a token response without id_token produced a session")
	}
}

// TestAuditActorPerRealm pins how each realm is attributed in the audit log:
// a human by verified email, an unverified-email human by user id, and a
// runner by runner id. Attribution is the whole point of the log.
func TestAuditActorPerRealm(t *testing.T) {
	srv, idp, _ := newOIDCServer(t)

	verified := idp.Mint(t, oidctest.Claims{
		Subject: "u-verified", Email: "aaron@example.com", EmailVerified: true, Name: "Aaron",
	})
	// Same shape, but the IdP did not verify the address: the hub must not
	// record an unverified email as an identity.
	unverified := idp.Mint(t, oidctest.Claims{
		Subject: "u-unverified", Email: "spoofed@example.com", EmailVerified: false,
	})

	if c := call(t, "PUT", srv.URL+"/api/v1/flows/orders", verified, goodFlow, nil); c != 201 {
		t.Fatalf("deploy as verified user = %d", c)
	}
	var me struct{ ID string }
	if c := call(t, "GET", srv.URL+"/api/v1/me", unverified, "", &me); c != 200 || me.ID == "" {
		t.Fatalf("me (unverified email) = %d %+v", c, me)
	}
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/invoices", unverified,
		strings.Replace(goodFlow, `"name":"orders"`, `"name":"invoices"`, 1), nil); c != 201 {
		t.Fatalf("deploy as unverified-email user = %d", c)
	}

	// Runner realm: a secret resolution is audited against the runner id.
	if c := call(t, "PUT", srv.URL+"/api/v1/secrets/api_key", adminToken, `{"value":"v"}`, nil); c != 201 {
		t.Fatalf("put secret = %d", c)
	}
	secret := registerRunner(t, srv.URL, "actor-runner")
	if c := call(t, "POST", srv.URL+"/api/v1/secrets/resolve", secret, `{"names":["api_key"]}`, nil); c != 200 {
		t.Fatalf("resolve secret = %d", c)
	}

	var out struct {
		Audit []struct {
			Actor  string `json:"actor"`
			Action string `json:"action"`
			Entity string `json:"entity"`
		} `json:"audit"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/audit?limit=200", adminToken, "", &out); c != 200 {
		t.Fatalf("listAudit = %d", c)
	}
	actors := map[string]string{} // action+entity -> actor
	for _, e := range out.Audit {
		actors[e.Action+" "+e.Entity] = e.Actor
	}
	if got := actors["flow.deploy orders"]; got != "user:aaron@example.com" {
		t.Fatalf("verified-user actor = %q, want user:aaron@example.com", got)
	}
	// No email to attribute → the opaque user id, never the unverified claim.
	if got := actors["flow.deploy invoices"]; got != "user:"+me.ID {
		t.Fatalf("unverified-email actor = %q, want user:%s", got, me.ID)
	}
	if got := actors["flow.deploy invoices"]; strings.Contains(got, "spoofed@example.com") {
		t.Fatalf("audit recorded an unverified email as the actor: %q", got)
	}
	if got := actors["secret.access api_key"]; !strings.HasPrefix(got, "runner:") {
		t.Fatalf("runner actor = %q, want a runner:<id> prefix", got)
	}
	// Break-glass writes are attributed as such, not as a user.
	if got := actors["secret.put api_key"]; got != "admin:break-glass" {
		t.Fatalf("break-glass actor = %q, want admin:break-glass", got)
	}
}
