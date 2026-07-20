package oidcauth_test

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/oidcauth/oidctest"
)

func newFlow(t *testing.T, idp *oidctest.IdP) *oidcauth.Flow {
	t.Helper()
	f, err := oidcauth.NewFlow(t.Context(), oidcauth.FlowConfig{
		Config:       oidcauth.Config{IssuerURL: idp.Issuer(), ClientID: clientID},
		ClientSecret: "hub-secret",
		RedirectURL:  "https://hub.example:8400/auth/callback",
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestNewFlowRequiresRedirectURL(t *testing.T) {
	idp := oidctest.New(t, clientID)
	_, err := oidcauth.NewFlow(t.Context(), oidcauth.FlowConfig{
		Config: oidcauth.Config{IssuerURL: idp.Issuer(), ClientID: clientID},
	})
	if err == nil || !strings.Contains(err.Error(), "redirect URL") {
		t.Fatalf("missing redirect URL err = %v", err)
	}
}

func TestNewFlowDiscoveryFails(t *testing.T) {
	// A syntactically valid but non-OIDC issuer must fail discovery.
	_, err := oidcauth.NewFlow(t.Context(), oidcauth.FlowConfig{
		Config:      oidcauth.Config{IssuerURL: "https://127.0.0.1:1/nope", ClientID: clientID},
		RedirectURL: "https://hub.example/cb",
	})
	if err == nil || !strings.Contains(err.Error(), "discovering") {
		t.Fatalf("bad-issuer discovery err = %v", err)
	}
}

func TestAuthCodeURL(t *testing.T) {
	idp := oidctest.New(t, clientID)
	f := newFlow(t, idp)

	raw := f.AuthCodeURL("state-xyz")
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse auth URL: %v", err)
	}
	if !strings.HasPrefix(raw, idp.Issuer()+"/auth") {
		t.Errorf("auth URL %q not rooted at IdP auth endpoint", raw)
	}
	q := u.Query()
	if q.Get("state") != "state-xyz" {
		t.Errorf("state = %q", q.Get("state"))
	}
	if q.Get("client_id") != clientID {
		t.Errorf("client_id = %q", q.Get("client_id"))
	}
	if q.Get("response_type") != "code" {
		t.Errorf("response_type = %q", q.Get("response_type"))
	}
	if got := q.Get("scope"); !strings.Contains(got, "openid") || !strings.Contains(got, "email") {
		t.Errorf("scope = %q, want openid+email", got)
	}
	if q.Get("redirect_uri") != "https://hub.example:8400/auth/callback" {
		t.Errorf("redirect_uri = %q", q.Get("redirect_uri"))
	}
}

func TestExchangeSuccess(t *testing.T) {
	idp := oidctest.New(t, clientID)
	f := newFlow(t, idp)

	idp.Authorize(t, "good-code", oidctest.Claims{
		Subject: "user-9", Email: "bob@example.com", EmailVerified: true, Name: "Bob",
	})

	raw, id, err := f.Exchange(t.Context(), "good-code")
	if err != nil {
		t.Fatal(err)
	}
	if raw == "" {
		t.Error("empty raw id token")
	}
	if id.Subject != "user-9" || id.Email != "bob@example.com" || !id.EmailVerified || id.Name != "Bob" {
		t.Fatalf("identity = %+v", id)
	}
	if id.Issuer != idp.Issuer() {
		t.Errorf("issuer = %q", id.Issuer)
	}

	// The raw token handed back is the same one the shared verifier accepts.
	if _, err := f.Verifier().Verify(t.Context(), raw); err != nil {
		t.Errorf("verifier rejected the exchanged token: %v", err)
	}
}

func TestExchangeUnknownCode(t *testing.T) {
	// An unregistered code makes the token endpoint return 400 → oauth2 error.
	idp := oidctest.New(t, clientID)
	f := newFlow(t, idp)

	if _, _, err := f.Exchange(t.Context(), "nope"); err == nil ||
		!strings.Contains(err.Error(), "code exchange") {
		t.Fatalf("unknown-code exchange err = %v", err)
	}
}

func TestExchangeNoIDToken(t *testing.T) {
	// A token response lacking id_token must be rejected, not silently accepted.
	idp := oidctest.New(t, clientID)
	f := newFlow(t, idp)

	idp.AuthorizeRaw("no-idtoken", map[string]any{
		"access_token": "access-only",
		"token_type":   "Bearer",
		"expires_in":   3600,
	})

	if _, _, err := f.Exchange(t.Context(), "no-idtoken"); err == nil ||
		!strings.Contains(err.Error(), "no id_token") {
		t.Fatalf("no-id_token exchange err = %v", err)
	}
}

func TestExchangeUnverifiableIDToken(t *testing.T) {
	// A token response whose id_token fails verification (here: expired)
	// must surface the verifier's error out of Exchange.
	idp := oidctest.New(t, clientID)
	f := newFlow(t, idp)

	idp.AuthorizeRaw("expired-idtoken", map[string]any{
		"access_token": "access",
		"token_type":   "Bearer",
		"expires_in":   3600,
		"id_token":     idp.Mint(t, oidctest.Claims{Subject: "u", TTL: -time.Minute}),
	})

	if _, _, err := f.Exchange(t.Context(), "expired-idtoken"); err == nil {
		t.Fatal("expired id_token exchanged cleanly")
	}
}
