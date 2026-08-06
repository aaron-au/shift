package hubclient_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// The runner half of ADR-0044. These stand up a miniature CA and a hub that
// signs CSRs, because the properties under test are about what crosses the
// wire and what lands on disk — a mocked issuer would prove neither.

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test control-plane CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// sign issues a client certificate for id over the CSR's public key.
func (c *testCA) sign(t *testing.T, csrB64, id string, ttl time.Duration) string {
	t.Helper()
	der, err := base64.StdEncoding.DecodeString(csrB64)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.ParseCertificateRequest(der)
	if err != nil {
		t.Fatal(err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Fatalf("the runner sent a CSR it cannot prove it holds the key for: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: id},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(ttl),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	out, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, csr.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: out}))
}

// issuingHub serves register + renew, recording what it was sent.
type issuingHub struct {
	*httptest.Server
	ca       *testCA
	id       string
	ttl      time.Duration
	requests []map[string]string
	peerCN   string // CN of the client certificate on the last renewal
}

func newIssuingHub(t *testing.T, ca *testCA, id string, ttl time.Duration) *issuingHub {
	t.Helper()
	h := &issuingHub{ca: ca, id: id, ttl: ttl}
	mux := http.NewServeMux()
	handle := func(w http.ResponseWriter, r *http.Request, code int) {
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		h.requests = append(h.requests, req)
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			h.peerCN = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"runner_id":   h.id,
			"certificate": h.ca.sign(t, req["csr"], h.id, h.ttl),
			"ca":          string(h.ca.pem),
			"not_after":   time.Now().Add(h.ttl).UTC().Format(time.RFC3339),
		})
	}
	mux.HandleFunc("/api/v1/runners/register", func(w http.ResponseWriter, r *http.Request) {
		handle(w, r, http.StatusCreated)
	})
	mux.HandleFunc("/api/v1/runners/certificate", func(w http.ResponseWriter, r *http.Request) {
		handle(w, r, http.StatusOK)
	})
	srv := httptest.NewUnstartedServer(mux)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.pem)
	srv.TLS = &tls.Config{ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pool, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	h.Server = srv
	return h
}

// trust points an identity at the test hub's server certificate, standing in
// for the operator's -hub-ca.
func trust(t *testing.T, id *hubclient.Identity, srv *httptest.Server) *hubclient.Identity {
	t.Helper()
	if !id.TrustAlso(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})) {
		t.Fatal("the test hub certificate was not accepted as a root")
	}
	return id
}

// clientTrusting returns a client that trusts the test hub's server cert.
func clientTrusting(srv *httptest.Server) *http.Client {
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
	}}
}

func TestRegisterWithCSRWritesALoadableBundle(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()

	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	if id.RunnerID != "runner-1" {
		t.Errorf("runner id = %q", id.RunnerID)
	}
	// The bundle must survive a restart: registration tokens are single-use,
	// so a runner that cannot reload its identity is a runner that cannot
	// start twice.
	reloaded, err := hubclient.LoadIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded == nil || reloaded.RunnerID != "runner-1" {
		t.Fatalf("reloaded = %+v", reloaded)
	}
	for _, f := range []string{
		hubclient.IdentityCertFile, hubclient.IdentityKeyFile,
		hubclient.IdentityCAFile, hubclient.IdentityIDFile,
	} {
		info, err := os.Stat(filepath.Join(dir, f))
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("%s mode = %v, want 0600", f, info.Mode().Perm())
		}
	}
}

// The private key must never leave the process. This is the whole reason a CSR
// is used rather than the hub generating the pair.
func TestRegistrationSendsOnlyACSR(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()

	if _, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir); err != nil {
		t.Fatal(err)
	}
	if len(hub.requests) != 1 {
		t.Fatalf("requests = %d", len(hub.requests))
	}
	req := hub.requests[0]
	if req["csr"] == "" {
		t.Fatal("no csr was sent")
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, hubclient.IdentityKeyFile)) // #nosec G304 -- test-owned t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range req {
		if strings.Contains(v, "PRIVATE KEY") {
			t.Errorf("field %q carries private key material", k)
		}
	}
	// And the CSR is not simply the key re-encoded.
	if strings.Contains(req["csr"], string(keyPEM)) {
		t.Error("the request body contains the private key")
	}
}

// A missing bundle is "not registered yet", a normal first-boot state.
func TestLoadIdentityOfAnEmptyDirectory(t *testing.T) {
	id, err := hubclient.LoadIdentity(t.TempDir())
	if err != nil {
		t.Fatalf("an unregistered runner reported an error: %v", err)
	}
	if id != nil {
		t.Errorf("identity = %+v, want nil", id)
	}
	if id, err = hubclient.LoadIdentity(""); id != nil || err != nil {
		t.Errorf("LoadIdentity(\"\") = %v, %v", id, err)
	}
}

// The certificate subject IS the identity the hub authenticates, so a bundle
// whose id file disagrees would have the runner logging one identity and being
// treated as another.
func TestLoadIdentityRefusesAMismatchedIDFile(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	if _, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hubclient.IdentityIDFile), []byte("runner-999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := hubclient.LoadIdentity(dir)
	if err == nil {
		t.Fatal("a bundle whose id and certificate disagree was loaded")
	}
	if !strings.Contains(err.Error(), "runner-999") {
		t.Errorf("error %q does not name the disagreement", err)
	}
}

func TestLoadIdentityReportsAPartialBundle(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	if _, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir); err != nil {
		t.Fatal(err)
	}
	// A certificate with no key is not "not registered" — it is broken, and
	// silently re-registering would spend another single-use token.
	if err := os.Remove(filepath.Join(dir, hubclient.IdentityKeyFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := hubclient.LoadIdentity(dir); err == nil {
		t.Fatal("a bundle missing its key loaded cleanly")
	}
}

// Renewal swaps the certificate in place. The transport, and therefore the
// connection pool, is untouched — there is no window in which a caller holds a
// client wired to a certificate that has just been replaced.
func TestRenewSwapsTheCertificateInPlace(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := id.TLSConfig()
	before, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	firstExpiry := id.NotAfter()

	hub.ttl = 4 * time.Hour
	if err := trust(t, id, hub.Server).Renew(t.Context(), id.HTTPClient(), hub.URL); err != nil {
		t.Fatal(err)
	}
	// The SAME config object now yields the new certificate.
	after, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if string(after.Certificate[0]) == string(before.Certificate[0]) {
		t.Error("the certificate did not change")
	}
	if !id.NotAfter().After(firstExpiry) {
		t.Errorf("expiry did not move: %s then %s", firstExpiry, id.NotAfter())
	}
	// And it survives a restart.
	reloaded, err := hubclient.LoadIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	rcfg, err := reloaded.TLSConfig().GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if string(rcfg.Certificate[0]) != string(after.Certificate[0]) {
		t.Error("the renewed certificate was not persisted; a restart would go back to the old one")
	}
}

// A new key on every issuance: reusing one would mean a key compromised today
// stays useful for as long as the runner keeps renewing.
func TestRenewalUsesAFreshKey(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(dir, hubclient.IdentityKeyFile)) // #nosec G304 -- test-owned t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := trust(t, id, hub.Server).Renew(t.Context(), id.HTTPClient(), hub.URL); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(dir, hubclient.IdentityKeyFile)) // #nosec G304 -- test-owned t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) {
		t.Error("the renewal reused the existing private key")
	}
}

// Renewal authenticates with the CURRENT certificate — that is what makes an
// operator token unnecessary.
func TestRenewalPresentsTheCurrentCertificate(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := trust(t, id, hub.Server).Renew(t.Context(), id.HTTPClient(), hub.URL); err != nil {
		t.Fatal(err)
	}
	if hub.peerCN != "runner-1" {
		t.Errorf("the hub saw peer %q, want the runner's own certificate", hub.peerCN)
	}
	if len(hub.requests) < 2 {
		t.Fatal("no renewal request recorded")
	}
	if tok := hub.requests[1]["token"]; tok != "" {
		t.Errorf("the renewal carried a registration token (%q); renewal must not need one", tok)
	}
}

// The hub derives the subject from the authenticated connection, so this
// cannot happen against a correct hub — and if it ever did, the runner would
// be operating under an identity it did not think it had.
func TestRenewRefusesACertificateForAnotherRunner(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	before, err := id.TLSConfig().GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatal(err)
	}

	hub.id = "somebody-else"
	err = trust(t, id, hub.Server).Renew(t.Context(), id.HTTPClient(), hub.URL)
	if err == nil {
		t.Fatal("a certificate naming another runner was accepted")
	}
	if !strings.Contains(err.Error(), "somebody-else") {
		t.Errorf("error %q does not name what was issued", err)
	}
	// And the working identity is untouched: a failed renewal must not be an
	// outage.
	after, err := id.TLSConfig().GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if string(after.Certificate[0]) != string(before.Certificate[0]) {
		t.Error("a failed renewal replaced the working certificate")
	}
}

func TestRenewReportsAHubError(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	hub.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"status":400,"code":"csr_rejected","message":"nope"}}`))
	})
	if err := trust(t, id, hub.Server).Renew(t.Context(), id.HTTPClient(), hub.URL); err == nil {
		t.Fatal("a refused renewal was reported as success")
	}
}

// Half the REMAINING lifetime, floored at a minute: a runner that has been
// asleep, or that has failed a few attempts, converges on trying more often as
// the cliff approaches rather than waking on a schedule already missed.
func TestRenewAfterHalvesWhatIsLeft(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", 10*time.Hour)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	d := id.RenewAfter(now)
	if d < 4*time.Hour || d > 6*time.Hour {
		t.Errorf("RenewAfter = %s, want ~5h (half of what is left)", d)
	}
	// Past expiry it must still retry, not spin and not give up.
	if d := id.RenewAfter(id.NotAfter().Add(time.Hour)); d != time.Minute {
		t.Errorf("RenewAfter past expiry = %s, want a minute", d)
	}
	// And it never busy-loops as the remaining time shrinks.
	if d := id.RenewAfter(id.NotAfter().Add(-10 * time.Second)); d < time.Minute {
		t.Errorf("RenewAfter near expiry = %s; that is a spin", d)
	}
}

// An identity held only in memory would re-register on every restart, and
// registration tokens are single-use — so the second start fails, in
// production, at 3am.
func TestConnectMTLSInsistsOnSomewhereToPersist(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	_, _, err := hubclient.ConnectMTLS(t.Context(), clientTrusting(hub.Server), hub.URL, "", "srt_tok", "worker")
	if err == nil {
		t.Fatal("registration was allowed with nowhere to persist the identity")
	}
	if !strings.Contains(err.Error(), "directory") {
		t.Errorf("error %q does not say what is missing", err)
	}
}

func TestConnectMTLSNeedsATokenWhenUnregistered(t *testing.T) {
	_, _, err := hubclient.ConnectMTLS(t.Context(), http.DefaultClient, "https://hub.invalid", t.TempDir(), "", "worker")
	if err == nil {
		t.Fatal("an unregistered runner with no token connected")
	}
}

// The second start must reuse the bundle, not spend another token.
func TestConnectMTLSReusesASavedIdentity(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	hc := clientTrusting(hub.Server)

	if _, _, err := hubclient.ConnectMTLS(t.Context(), hc, hub.URL, dir, "srt_tok", "worker"); err != nil {
		t.Fatal(err)
	}
	// No token at all the second time round.
	id, client, err := hubclient.ConnectMTLS(t.Context(), hc, hub.URL, dir, "", "worker")
	if err != nil {
		t.Fatalf("a registered runner could not restart: %v", err)
	}
	if id.RunnerID != "runner-1" || client == nil {
		t.Errorf("identity = %+v", id)
	}
	if len(hub.requests) != 1 {
		t.Errorf("the hub saw %d registrations; a restart spent another single-use token", len(hub.requests))
	}
}

// End to end: the identity's client authenticates against a server that
// REQUIRES a client certificate, which is what the hub's runner realm is.
func TestTheIdentityClientAuthenticatesToAServerRequiringACertificate(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}

	var sawCN string
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.VerifiedChains) > 0 {
			sawCN = r.TLS.VerifiedChains[0][0].Subject.CommonName
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	defer srv.Close()

	// This server's certificate came from neither the system store nor the
	// control-plane CA, which is exactly what TrustAlso is for.
	trust(t, id, srv)
	hc := id.HTTPClient()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if sawCN != "runner-1" {
		t.Errorf("the server saw %q, want runner-1", sawCN)
	}
}

// The renewal loop is what keeps a fleet alive: it must actually renew, on a
// cadence derived from the certificate it holds, and stop when told.
func TestRenewLoopRenews(t *testing.T) {
	ca := newTestCA(t)
	// A six-second certificate renews after ~3s — the same halving a 24h one
	// gets, which is why the floor is proportional rather than fixed.
	hub := newIssuingHub(t, ca, "runner-1", 6*time.Second)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	trust(t, id, hub.Server)
	first := id.NotAfter()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); id.RenewLoop(ctx, hub.URL) }()

	deadline := time.Now().Add(20 * time.Second)
	for id.NotAfter().Equal(first) {
		if time.Now().After(deadline) {
			t.Fatal("the renewal loop never renewed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RenewLoop did not stop when its context ended")
	}
}

// A failing hub must not stop the loop: the certificate is still valid, and
// giving up is how a transient outage becomes a permanent one.
func TestRenewLoopKeepsGoingWhenTheHubRefuses(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", 6*time.Second)
	dir := t.TempDir()
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir)
	if err != nil {
		t.Fatal(err)
	}
	trust(t, id, hub.Server)
	var attempts atomic.Int32
	hub.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); id.RenewLoop(ctx, hub.URL) }()

	deadline := time.Now().Add(20 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the loop stopped after %d failed attempts", attempts.Load())
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	<-done
}

// The retry cadence is proportional to the certificate's lifetime: a minute is
// a pointless wait for a ten-minute certificate, and a second would be a
// busy-loop against a month-long one.
func TestRenewAfterFloorFollowsTheCertificateLifetime(t *testing.T) {
	ca := newTestCA(t)
	for _, tc := range []struct {
		name      string
		ttl       time.Duration
		wantFloor time.Duration
	}{
		{"long", 30 * 24 * time.Hour, time.Minute},
		{"short", 20 * time.Second, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub := newIssuingHub(t, ca, "runner-1", tc.ttl)
			id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			// Past expiry the wait collapses to the floor.
			if d := id.RenewAfter(id.NotAfter().Add(time.Hour)); d != tc.wantFloor {
				t.Errorf("floor = %s, want %s", d, tc.wantFloor)
			}
		})
	}
}

// The hub is often not up yet when a runner starts (compose ordering), so
// registration retries. A single early failure must not cost the runner its
// single-use token.
func TestRegistrationRetriesWhileTheHubComesUp(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	real := hub.Config.Handler
	var calls int
	hub.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "starting up", http.StatusServiceUnavailable)
			return
		}
		real.ServeHTTP(w, r)
	})
	dir := t.TempDir()

	id, _, err := hubclient.ConnectMTLS(t.Context(), clientTrusting(hub.Server), hub.URL, dir, "srt_tok", "worker")
	if err != nil {
		t.Fatalf("registration gave up on a hub that was still booting: %v", err)
	}
	if id.RunnerID != "runner-1" {
		t.Errorf("runner id = %q", id.RunnerID)
	}
	if calls < 2 {
		t.Errorf("calls = %d; the retry never happened", calls)
	}
}

// A refused registration is reported, not silently turned into a runner with
// no identity.
func TestRegistrationReportsARefusal(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"status":401,"code":"registration_token_invalid","message":"spent"}}`))
	}))
	defer srv.Close()

	_, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(srv), srv.URL, "srt_spent", "worker", t.TempDir())
	if err == nil {
		t.Fatal("a refused registration was reported as success")
	}
	if !strings.Contains(err.Error(), "registration_token_invalid") {
		t.Errorf("error %q loses the hub's machine-readable code", err)
	}
}

// A bundle that cannot be written is a failed registration, not a runner
// holding an identity it will lose on restart.
func TestRegistrationFailsWhenTheBundleCannotBeWritten(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	// A regular file where the bundle directory should be.
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", blocked); err == nil {
		t.Fatal("registration succeeded with nowhere to persist the identity")
	}
}

func TestTrustAlsoRejectsNonCertificateInput(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	id, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if id.TrustAlso(nil) {
		t.Error("TrustAlso(nil) reported success")
	}
	if id.TrustAlso([]byte("not a certificate")) {
		t.Error("TrustAlso accepted something that is not a certificate")
	}
}

// A corrupt bundle must be reported rather than treated as "unregistered",
// which would spend another single-use token.
func TestLoadIdentityReportsACorruptCertificate(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	if _, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hubclient.IdentityCertFile), []byte("-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hubclient.LoadIdentity(dir); err == nil {
		t.Fatal("a corrupt certificate loaded cleanly")
	}
	// Same for a CA file that contains no certificate at all.
	if err := os.WriteFile(filepath.Join(dir, hubclient.IdentityCAFile), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hubclient.LoadIdentity(dir); err == nil {
		t.Fatal("a bundle with an unusable CA loaded cleanly")
	}
}

// An empty id file is a broken bundle, not an anonymous runner.
func TestLoadIdentityRefusesAnEmptyID(t *testing.T) {
	ca := newTestCA(t)
	hub := newIssuingHub(t, ca, "runner-1", time.Hour)
	dir := t.TempDir()
	if _, err := hubclient.RegisterWithCSR(t.Context(), clientTrusting(hub.Server), hub.URL, "srt_tok", "worker", dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hubclient.IdentityIDFile), []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hubclient.LoadIdentity(dir); err == nil {
		t.Fatal("a bundle with no runner id loaded cleanly")
	}
}

// Before any certificate is loaded the config must not panic or offer
// something it does not have.
func TestTLSConfigWithNoCertificateYet(t *testing.T) {
	var zero hubclient.Identity
	cert, err := zero.TLSConfig().GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Certificate) != 0 {
		t.Error("an identity with no certificate offered one")
	}
}
