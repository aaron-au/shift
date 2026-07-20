// Package oidctest is a fake OIDC IdP for tests: it serves discovery
// and JWKS from an httptest server and mints RS256 ID tokens with a key
// generated at runtime (nothing sensitive is ever committed).
package oidctest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// IdP is a running fake identity provider.
type IdP struct {
	Server   *httptest.Server
	ClientID string
	key      *rsa.PrivateKey

	mu    sync.Mutex
	codes map[string]map[string]any // authorization code -> token-endpoint response body
}

// New starts a fake IdP. It shuts down with the test.
func New(t *testing.T, clientID string) *IdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &IdP{ClientID: clientID, key: key, codes: map[string]map[string]any{}}

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	idp.Server = srv

	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                srv.URL,
			"jwks_uri":                              srv.URL + "/keys",
			"authorization_endpoint":                srv.URL + "/auth",
			"token_endpoint":                        srv.URL + "/token",
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"response_types_supported":              []string{"code", "id_token"},
			"subject_types_supported":               []string{"public"},
		})
	})
	mux.HandleFunc("GET /keys", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key: key.Public(), KeyID: "test-key", Algorithm: "RS256", Use: "sig",
		}}})
	})
	// Token endpoint for the authorization-code flow: exchanges a code
	// registered via Authorize/AuthorizeRaw for its token response.
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		idp.mu.Lock()
		body, ok := idp.codes[r.Form.Get("code")]
		idp.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_grant"})
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	})
	return idp
}

// Issuer returns the IdP's issuer URL.
func (i *IdP) Issuer() string { return i.Server.URL }

// Authorize registers an authorization code whose token exchange returns a
// standard response carrying an id_token minted from the given claims.
func (i *IdP) Authorize(t *testing.T, code string, c Claims) {
	t.Helper()
	i.AuthorizeRaw(code, map[string]any{
		"access_token": "access-" + code,
		"token_type":   "Bearer",
		"expires_in":   3600,
		"id_token":     i.Mint(t, c),
	})
}

// AuthorizeRaw registers an authorization code with a verbatim token-endpoint
// response body — used to exercise malformed responses (e.g. no id_token).
func (i *IdP) AuthorizeRaw(code string, body map[string]any) {
	i.mu.Lock()
	i.codes[code] = body
	i.mu.Unlock()
}

// Claims are the mutable parts of a minted token.
type Claims struct {
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
	Issuer        string        // defaults to the IdP's own issuer
	Audience      string        // defaults to the IdP's client ID
	TTL           time.Duration // defaults to an hour; negative = already expired
}

// Mint signs an ID token for the given claims.
func (i *IdP) Mint(t *testing.T, c Claims) string {
	t.Helper()
	if c.Issuer == "" {
		c.Issuer = i.Server.URL
	}
	if c.Audience == "" {
		c.Audience = i.ClientID
	}
	if c.TTL == 0 {
		c.TTL = time.Hour
	}
	now := time.Now()
	payload, err := json.Marshal(map[string]any{
		"iss":            c.Issuer,
		"sub":            c.Subject,
		"aud":            c.Audience,
		"exp":            now.Add(c.TTL).Unix(),
		"iat":            now.Unix(),
		"email":          c.Email,
		"email_verified": c.EmailVerified,
		"name":           c.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: i.key},
		(&jose.SignerOptions{}).WithHeader("kid", "test-key"))
	if err != nil {
		t.Fatal(err)
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jws.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// MintForeign signs a token with a key the IdP does NOT publish —
// signature validation must reject it.
func (i *IdP) MintForeign(t *testing.T, c Claims) string {
	t.Helper()
	foreign, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	imposter := &IdP{ClientID: i.ClientID, key: foreign, Server: i.Server}
	return imposter.Mint(t, c)
}

// String satisfies fmt.Stringer for debugging.
func (i *IdP) String() string { return fmt.Sprintf("fake IdP at %s", i.Server.URL) }
