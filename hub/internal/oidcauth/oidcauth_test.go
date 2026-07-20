package oidcauth_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/oidcauth/oidctest"
)

const clientID = "shift-hub"

func newVerifier(t *testing.T, idp *oidctest.IdP) *oidcauth.Verifier {
	t.Helper()
	v, err := oidcauth.New(t.Context(), oidcauth.Config{IssuerURL: idp.Issuer(), ClientID: clientID})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestVerifyValidToken(t *testing.T) {
	idp := oidctest.New(t, clientID)
	v := newVerifier(t, idp)

	raw := idp.Mint(t, oidctest.Claims{
		Subject: "user-1", Email: "a@example.com", EmailVerified: true, Name: "Alice",
	})
	id, err := v.Verify(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if id.Subject != "user-1" || id.Email != "a@example.com" || !id.EmailVerified || id.Issuer != idp.Issuer() {
		t.Fatalf("identity = %+v", id)
	}
}

func TestVerifyRejections(t *testing.T) {
	idp := oidctest.New(t, clientID)
	v := newVerifier(t, idp)
	ctx := t.Context()

	cases := map[string]string{
		"expired":     idp.Mint(t, oidctest.Claims{Subject: "u", TTL: -time.Minute}),
		"wrong aud":   idp.Mint(t, oidctest.Claims{Subject: "u", Audience: "not-shift"}),
		"wrong iss":   idp.Mint(t, oidctest.Claims{Subject: "u", Issuer: "https://evil.example"}),
		"foreign key": idp.MintForeign(t, oidctest.Claims{Subject: "u"}),
		"garbage":     "not.a.jwt",
		"empty":       "",
	}
	for name, raw := range cases {
		if _, err := v.Verify(ctx, raw); err == nil {
			t.Errorf("%s token verified cleanly", name)
		}
	}
}

func TestNewRequiresConfig(t *testing.T) {
	if _, err := oidcauth.New(t.Context(), oidcauth.Config{}); err == nil ||
		!strings.Contains(err.Error(), "required") {
		t.Fatalf("empty config err = %v", err)
	}
}

func TestNewDiscoveryFails(t *testing.T) {
	// Config is complete but the issuer serves no OIDC discovery document,
	// so provider construction (a network call) must fail closed.
	_, err := oidcauth.New(t.Context(), oidcauth.Config{
		IssuerURL: "https://127.0.0.1:1/not-an-idp", ClientID: clientID,
	})
	if err == nil || !strings.Contains(err.Error(), "discovering") {
		t.Fatalf("bad-issuer discovery err = %v", err)
	}
}
