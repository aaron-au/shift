package hubclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadCredentialsEmptyPath(t *testing.T) {
	c, err := LoadCredentials("")
	if err != nil || c != (Credentials{}) {
		t.Fatalf("empty path: %+v %v", c, err)
	}
}

func TestLoadCredentialsMissingFile(t *testing.T) {
	c, err := LoadCredentials(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil || c != (Credentials{}) {
		t.Fatalf("missing file: %+v %v", c, err)
	}
}

func TestSaveLoadCredentialsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "creds.json")
	want := Credentials{HubURL: "https://hub", RunnerID: "r1", Secret: "rs_abc"}
	if err := SaveCredentials(path, want); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("creds mode = %v, want 0600", info.Mode().Perm())
	}
	got, err := LoadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round-trip: got %+v want %+v", got, want)
	}
}

func TestLoadCredentialsMalformed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCredentials(path); err == nil {
		t.Fatal("malformed credentials accepted")
	}
}

func TestHTTPClientEmpty(t *testing.T) {
	c, err := HTTPClient("")
	if err != nil || c == nil {
		t.Fatalf("empty CA: %v %v", c, err)
	}
	if c.Timeout != 90*time.Second {
		t.Fatalf("timeout = %s", c.Timeout)
	}
}

func TestHTTPClientMissingFile(t *testing.T) {
	if _, err := HTTPClient(filepath.Join(t.TempDir(), "nope.pem")); err == nil {
		t.Fatal("missing CA file accepted")
	}
}

func TestHTTPClientInvalidPEM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := HTTPClient(path); err == nil {
		t.Fatal("invalid PEM accepted")
	}
}

// A valid CA file yields a client whose transport trusts that CA — proven
// by successfully talking to a TLS test server signed with it.
func TestHTTPClientValidCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	hc, err := HTTPClient(caPath)
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := hc.Transport.(*http.Transport)
	if !ok || tr.TLSClientConfig == nil || tr.TLSClientConfig.RootCAs == nil {
		t.Fatalf("transport not configured with CA pool: %#v", hc.Transport)
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("min TLS = %x, want 1.2", tr.TLSClientConfig.MinVersion)
	}

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("request against CA-signed server failed: %v", err)
	}
	_ = resp.Body.Close()
}

func TestWithHTTPClient(t *testing.T) {
	c := New("https://hub", "sec")
	custom := &http.Client{Timeout: time.Second}
	if c.WithHTTPClient(custom) != c || c.hc != custom {
		t.Fatal("WithHTTPClient did not swap transport / return receiver")
	}
}

// Connect returns saved credentials without hitting the network when the
// stored HubURL matches.
func TestConnectUsesSavedCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "creds.json")
	if err := SaveCredentials(path, Credentials{HubURL: "https://hub", RunnerID: "r9", Secret: "rs_saved"}); err != nil {
		t.Fatal(err)
	}
	id, c, err := Connect(t.Context(), http.DefaultClient, "https://hub", path, "", "name")
	if err != nil {
		t.Fatal(err)
	}
	if id != "r9" || c.secret != "rs_saved" {
		t.Fatalf("id=%q secret=%q", id, c.secret)
	}
}

func TestConnectNoCredsNoToken(t *testing.T) {
	_, _, err := Connect(t.Context(), http.DefaultClient, "https://hub", "", "", "name")
	if err == nil {
		t.Fatal("Connect with no creds and no token succeeded")
	}
}

// With no saved credentials but a token, Connect registers and persists the
// issued identity.
func TestConnectRegistersAndPersists(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runners/register", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"runner_id": "r-new", "secret": "rs_issued"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "creds.json")
	id, c, err := Connect(t.Context(), http.DefaultClient, srv.URL, path, "reg-token", "name")
	if err != nil {
		t.Fatal(err)
	}
	if id != "r-new" || c.secret != "rs_issued" {
		t.Fatalf("id=%q secret=%q", id, c.secret)
	}
	saved, err := LoadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	if saved.RunnerID != "r-new" || saved.Secret != "rs_issued" || saved.HubURL != srv.URL {
		t.Fatalf("persisted creds = %+v", saved)
	}
}

// Saved credentials for a different hub are ignored: Connect falls through
// to registration against the new hub.
func TestConnectHubURLMismatchReregisters(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runners/register", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"runner_id": "r-fresh", "secret": "rs_fresh"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "creds.json")
	if err := SaveCredentials(path, Credentials{HubURL: "https://old-hub", RunnerID: "r-old", Secret: "rs_old"}); err != nil {
		t.Fatal(err)
	}
	id, _, err := Connect(t.Context(), http.DefaultClient, srv.URL, path, "reg-token", "name")
	if err != nil {
		t.Fatal(err)
	}
	if id != "r-fresh" {
		t.Fatalf("id = %q, want re-registered r-fresh", id)
	}
}

// A cancelled context ends the registration retry loop promptly rather than
// waiting out the 60s deadline.
func TestConnectContextCancelled(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runners/register", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // never ready
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // already done

	done := make(chan error, 1)
	go func() {
		_, _, err := Connect(ctx, http.DefaultClient, srv.URL, "", "reg-token", "name")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled Connect succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not honor cancelled context")
	}
}
