package api_test

import (
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/oidcauth/oidctest"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
)

// newOIDCServer builds a hub API with OIDC + secrets enabled (and the
// break-glass token retained).
func newOIDCServer(t *testing.T) (*httptest.Server, *oidctest.IdP, *store.Store) {
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
	verifier, err := oidcauth.New(t.Context(), oidcauth.Config{
		IssuerURL: idp.Issuer(), ClientID: "shift-hub",
	})
	if err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "kek.bin")
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := kek.NewLocalFiles(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		OIDC:       verifier,
		Secrets:    secrets.New(st, provider),
		LeaseTTL:   2 * time.Second,
		LeasePoll:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, idp, st
}

func TestOIDCAdminRealm(t *testing.T) {
	srv, idp, _ := newOIDCServer(t)

	valid := idp.Mint(t, oidctest.Claims{
		Subject: "u1", Email: "aaron@example.com", EmailVerified: true, Name: "Aaron",
	})

	// Valid OIDC token reaches admin endpoints and is JIT-provisioned.
	var me struct{ Kind, Email, Role string }
	if code := call(t, "GET", srv.URL+"/api/v1/me", valid, "", &me); code != 200 {
		t.Fatalf("me with OIDC token = %d", code)
	}
	if me.Kind != "user" || me.Email != "aaron@example.com" || me.Role != "admin" {
		t.Fatalf("me = %+v", me)
	}

	// Break-glass still works alongside.
	var me2 struct{ Kind string }
	if code := call(t, "GET", srv.URL+"/api/v1/me", adminToken, "", &me2); code != 200 || me2.Kind != "breakglass" {
		t.Fatalf("break-glass me = %d %+v", code, me2)
	}

	// Bad tokens are one opaque 401.
	for name, tok := range map[string]string{
		"expired": idp.Mint(t, oidctest.Claims{Subject: "u1", TTL: -time.Minute}),
		"foreign": idp.MintForeign(t, oidctest.Claims{Subject: "u1"}),
		"none":    "",
	} {
		if code := call(t, "GET", srv.URL+"/api/v1/me", tok, "", nil); code != 401 {
			t.Errorf("%s token: code = %d, want 401", name, code)
		}
	}
}

func TestViewerRoleReadOnly(t *testing.T) {
	srv, idp, st := newOIDCServer(t)

	tok := idp.Mint(t, oidctest.Claims{Subject: "v1", Email: "viewer@example.com", EmailVerified: true})
	// First call provisions; then demote to viewer directly in the store.
	if code := call(t, "GET", srv.URL+"/api/v1/me", tok, "", nil); code != 200 {
		t.Fatalf("provisioning call = %d", code)
	}
	if _, err := st.SetUserRole(t.Context(), "viewer@example.com", "viewer"); err != nil {
		t.Fatal(err)
	}

	if code := call(t, "GET", srv.URL+"/api/v1/flows", tok, "", nil); code != 200 {
		t.Fatalf("viewer GET = %d, want 200", code)
	}
	if code := call(t, "PUT", srv.URL+"/api/v1/secrets/x", tok, `{"value":"v"}`, nil); code != 403 {
		t.Fatalf("viewer PUT = %d, want 403", code)
	}
}

func TestSecretEndpoints(t *testing.T) {
	srv, _, st := newOIDCServer(t)

	// Put + list (metadata only, never values).
	var put struct {
		Name    string
		Version int
	}
	if code := call(t, "PUT", srv.URL+"/api/v1/secrets/api_key", adminToken,
		`{"value":"super-secret-value"}`, &put); code != 201 || put.Version != 1 {
		t.Fatalf("put = %d %+v", code, put)
	}
	req, _ := call2t(t, "GET", srv.URL+"/api/v1/secrets", adminToken, "")
	if strings.Contains(req, "super-secret-value") {
		t.Fatal("secret list leaked a value")
	}
	if !strings.Contains(req, `"api_key"`) {
		t.Fatalf("secret list missing name: %s", req)
	}

	// Bad name rejected.
	if code := call(t, "PUT", srv.URL+"/api/v1/secrets/bad%20name", adminToken, `{"value":"v"}`, nil); code != 422 {
		t.Fatalf("bad name = %d, want 422", code)
	}

	// Deploy referencing a missing secret → 422 naming it.
	doc := `{"name":"f","source":{"connector":"gen","action":"gen"},
	  "sink":{"connector":"http","action":"post","config":{"token":{"$secret":"nope"}}}}`
	body, code := call2t(t, "PUT", srv.URL+"/api/v1/flows/f", adminToken, doc)
	if code != 422 || !strings.Contains(body, "nope") {
		t.Fatalf("deploy with missing ref = %d %s", code, body)
	}
	// Existing ref deploys fine.
	doc = strings.ReplaceAll(doc, "nope", "api_key")
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/f", adminToken, doc, nil); code != 201 {
		t.Fatalf("deploy with valid ref = %d", code)
	}

	// Runner resolve: needs runner realm; admin token gets 401.
	if code := call(t, "POST", srv.URL+"/api/v1/secrets/resolve", adminToken,
		`{"names":["api_key"]}`, nil); code != 401 {
		t.Fatalf("resolve with admin token = %d, want 401", code)
	}
	tok, _, err := st.CreateRegistrationToken(t.Context(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, secret, err := st.RegisterRunner(t.Context(), tok, "r1")
	if err != nil {
		t.Fatal(err)
	}
	var resolved struct{ Secrets map[string]string }
	if code := call(t, "POST", srv.URL+"/api/v1/secrets/resolve", secret,
		`{"names":["api_key"]}`, &resolved); code != 200 || resolved.Secrets["api_key"] != "super-secret-value" {
		t.Fatalf("resolve = %d %+v", code, resolved)
	}
	// Missing name → 404 with names only.
	body, code = call2t(t, "POST", srv.URL+"/api/v1/secrets/resolve", secret, `{"names":["ghost"]}`)
	if code != 404 || !strings.Contains(body, "ghost") || strings.Contains(body, "super-secret-value") {
		t.Fatalf("resolve missing = %d %s", code, body)
	}

	// Delete.
	if code := call(t, "DELETE", srv.URL+"/api/v1/secrets/api_key", adminToken, "", nil); code != 204 {
		t.Fatalf("delete = %d", code)
	}
}

// call2t issues a request and returns the raw body + status for
// content assertions (leak checks need the exact bytes).
func call2t(t *testing.T, method, url, token, body string) (string, int) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw), resp.StatusCode
}
