package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func hashPw(t *testing.T, pw string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

func TestBasicAuthenticate(t *testing.T) {
	b, err := NewBasic("alice:" + hashPw(t, "secret") + ":viewer")
	if err != nil {
		t.Fatal(err)
	}
	req := func(u, p string, set bool) *http.Request {
		r := httptest.NewRequestWithContext(t.Context(), "GET", "/", nil)
		if set {
			r.SetBasicAuth(u, p)
		}
		return r
	}
	if p, ok := b.Authenticate(req("alice", "secret", true)); !ok || p.User != "alice" || p.Role.Name != "viewer" {
		t.Fatalf("valid creds: %+v ok=%v", p, ok)
	}
	if _, ok := b.Authenticate(req("alice", "wrong", true)); ok {
		t.Fatal("wrong password accepted")
	}
	if _, ok := b.Authenticate(req("bob", "secret", true)); ok {
		t.Fatal("unknown user accepted")
	}
	if _, ok := b.Authenticate(req("", "", false)); ok {
		t.Fatal("missing creds accepted")
	}
}

func TestNewBasicErrors(t *testing.T) {
	for _, bad := range []string{"", "nofields", "u:hash:badrole", "u:hash"} {
		if _, err := NewBasic(bad); err == nil {
			t.Errorf("NewBasic(%q) accepted", bad)
		}
	}
}

func TestGuard(t *testing.T) {
	b, _ := NewBasic("v:" + hashPw(t, "pw") + ":viewer")
	g := NewGuard(b)
	readOnly := func(r *http.Request) (Permission, bool) { return PermManage, true }
	h := g.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), readOnly)

	// No creds → 401 + challenge.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), "PUT", "/api/webhooks/x", nil))
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("no creds: %d %q", rec.Code, rec.Header().Get("WWW-Authenticate"))
	}

	// Viewer lacks manage → 403.
	rec = httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), "PUT", "/api/webhooks/x", nil)
	r.SetBasicAuth("v", "pw")
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("viewer manage: %d, want 403", rec.Code)
	}

	// Open guard passes everything through.
	open := NewGuard(nil)
	if open.Enabled() {
		t.Fatal("nil-auth guard should be disabled")
	}
	rec = httptest.NewRecorder()
	open.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }), readOnly).
		ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), "PUT", "/api/webhooks/x", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("open guard blocked: %d", rec.Code)
	}
}
