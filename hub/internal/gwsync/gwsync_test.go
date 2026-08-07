package gwsync_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/gwsync"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/pki"
	"github.com/aaron-au/shift/hub/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func testCA(t *testing.T) *pki.CA {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gateway ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	ca, err := pki.Load("gateway", certPath, keyPath, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func hubIdentity(t *testing.T, ca *pki.CA) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := ca.Sign(csr, gwpush.HubSubject, pki.UsageClient)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(issued.CertPEM)
	return tls.Certificate{Certificate: [][]byte{blk.Bytes}, PrivateKey: key}
}

// gateway is an adopted gateway: it holds a hub-issued identity and demands
// the hub's certificate in return.
type gateway struct {
	srv      *httptest.Server
	mux      *http.ServeMux
	URL      string
	renewals int
	pushes   int
}

// newGateway binds a listener FIRST so the record can be created with the real
// URL, then installs an identity for the id the hub assigned — the same order
// adoption happens in.
func newGateway(t *testing.T, ca *pki.CA) *gateway {
	t.Helper()
	g := &gateway{}
	mux := http.NewServeMux()
	g.srv = httptest.NewUnstartedServer(mux)
	g.mux = mux
	g.URL = "https://" + g.srv.Listener.Addr().String()
	t.Cleanup(g.srv.Close)
	return g
}

func (g *gateway) start(t *testing.T, ca *pki.CA, id string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := ca.Sign(csr, id, pki.UsageServer)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(issued.CertPEM)

	nextKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nextCSR, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, nextKey)
	if err != nil {
		t.Fatal(err)
	}

	mux := g.mux
	mux.HandleFunc("GET /csr", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gwpush.Hello{CSR: nextCSR})
	})
	mux.HandleFunc("POST /identity", func(w http.ResponseWriter, _ *http.Request) {
		g.renewals++
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /config", func(w http.ResponseWriter, _ *http.Request) {
		g.pushes++
		w.WriteHeader(http.StatusNoContent)
	})

	g.srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{blk.Bytes}, PrivateKey: key}},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.Pool(),
		MinVersion:   tls.VersionTLS13,
	}
	g.srv.StartTLS()
}

// The gateway cannot ask for a renewal, so the hub must notice. A pass that
// renewed nothing would leave it to expire and strand itself.
func TestATickRenewsAnIdentityBeforeItLapses(t *testing.T) {
	s := openStore(t)
	ca := testCA(t)
	hub := hubIdentity(t, ca)
	ctx := t.Context()

	g := newGateway(t, ca)
	gw, err := s.CreateGateway(ctx, "dmz", g.URL, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, ca, gw.ID)
	// An identity that expires inside the renewal window.
	if err := s.LearnGatewayFingerprint(ctx, gw.ID, strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("learn: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, gw.ID, "old-serial", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	l := gwsync.New(gwsync.Options{
		Store:       s,
		Client:      gwpush.New(ca, &hub, 5*time.Second),
		RenewBefore: time.Hour,
	})
	l.Tick(ctx)

	if g.renewals != 1 {
		t.Fatalf("renewals = %d, want 1 — the gateway would have expired unreachable", g.renewals)
	}
	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.CertSerial == "old-serial" {
		t.Fatal("the renewed identity was not recorded, so the hub would renew again every tick")
	}
}

// A healthy, converged gateway is left alone.
func TestATickLeavesAConvergedGatewayAlone(t *testing.T) {
	s := openStore(t)
	ca := testCA(t)
	hub := hubIdentity(t, ca)
	ctx := t.Context()

	g := newGateway(t, ca)
	gw, err := s.CreateGateway(ctx, "quiet", g.URL, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, ca, gw.ID)
	if err := s.LearnGatewayFingerprint(ctx, gw.ID, strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("learn: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, gw.ID, "serial", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	l := gwsync.New(gwsync.Options{
		Store:       s,
		Client:      gwpush.New(ca, &hub, 5*time.Second),
		RenewBefore: time.Hour,
	})
	l.Tick(ctx)

	if g.renewals != 0 || g.pushes != 0 {
		t.Fatalf("renewals=%d pushes=%d, want a converged gateway to be untouched", g.renewals, g.pushes)
	}
}

// Configuration drift is pushed, and the acknowledged version advances only on
// success — otherwise a gateway would look converged while running nothing.
func TestATickPushesDriftAndOnlyAdvancesOnSuccess(t *testing.T) {
	s := openStore(t)
	ca := testCA(t)
	hub := hubIdentity(t, ca)
	ctx := t.Context()

	g := newGateway(t, ca)
	gw, err := s.CreateGateway(ctx, "drifted", g.URL, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, ca, gw.ID)
	if err := s.LearnGatewayFingerprint(ctx, gw.ID, strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("learn: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, gw.ID, "serial", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := s.BumpGatewayConfig(ctx); err != nil {
		t.Fatalf("bump: %v", err)
	}

	// A configuration that cannot be built must not advance anything.
	failing := gwsync.New(gwsync.Options{
		Store: s, Client: gwpush.New(ca, &hub, 5*time.Second), RenewBefore: time.Hour,
		ConfigFor: func(context.Context, string) (any, error) {
			return nil, errors.New("no routes for this gateway")
		},
	})
	failing.Tick(ctx)
	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.PushedVersion != 0 {
		t.Fatal("a configuration that failed to build advanced the acknowledged version")
	}
	if got.LastPushError == "" {
		t.Fatal("the failure was not recorded, so the drift is invisible from the hub")
	}

	working := gwsync.New(gwsync.Options{
		Store: s, Client: gwpush.New(ca, &hub, 5*time.Second), RenewBefore: time.Hour,
		ConfigFor: func(context.Context, string) (any, error) {
			return map[string]int{"version": 1}, nil
		},
	})
	working.Tick(ctx)
	if g.pushes != 1 {
		t.Fatalf("pushes = %d, want 1", g.pushes)
	}
	got, err = s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.PushedVersion != got.ConfigVersion {
		t.Fatalf("acknowledged %d, intended %d — a successful push did not converge",
			got.PushedVersion, got.ConfigVersion)
	}
	if got.LastPushError != "" {
		t.Fatal("a successful push left the previous failure beside a healthy gateway")
	}
}

// Run must stop with its context rather than outliving the hub.
func TestRunStopsWithItsContext(t *testing.T) {
	s := openStore(t)
	ca := testCA(t)
	hub := hubIdentity(t, ca)
	l := gwsync.New(gwsync.Options{
		Store: s, Client: gwpush.New(ca, &hub, time.Second), Interval: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop when its context ended")
	}
}

// --- pairing ------------------------------------------------------------------

// pendingGateway is a gateway as the loop finds it: deployed, holding an
// install token, waiting to be dialled.
type pendingGateway struct {
	srv     *httptest.Server
	mux     *http.ServeMux
	URL     string
	token   string
	adopted bool
}

func newPendingGateway(t *testing.T) *pendingGateway {
	t.Helper()
	g := &pendingGateway{mux: http.NewServeMux()}
	g.srv = httptest.NewUnstartedServer(g.mux)
	g.URL = "https://" + g.srv.Listener.Addr().String()
	t.Cleanup(g.srv.Close)
	return g
}

func (g *pendingGateway) start(t *testing.T, token string) {
	t.Helper()
	g.token = token
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := pki.Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "unadopted"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	idKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, idKey)
	if err != nil {
		t.Fatal(err)
	}

	g.mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		var c gwpush.Challenge
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil || c.Proof == "" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if c.Proof != gwpush.Proof(g.token, gwpush.DomainHubHello, c.Nonce, fp, nil) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		sum := sha256.Sum256(csr)
		_ = json.NewEncoder(w).Encode(gwpush.Hello{
			Fingerprint: fp, CSR: csr,
			Proof: gwpush.Proof(g.token, gwpush.DomainGWHello, c.Nonce, fp, sum[:]),
		})
	})
	g.mux.HandleFunc("POST /adopt", func(w http.ResponseWriter, r *http.Request) {
		var a gwpush.Adoption
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if a.Proof != gwpush.Proof(g.token, gwpush.DomainInstall, a.Nonce, fp, gwpush.MaterialDigest(a)) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		g.adopted = true
		w.WriteHeader(http.StatusNoContent)
	})

	g.srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
	}
	g.srv.StartTLS()
}

// The loop adopts on its own, which is what makes the install one step: record,
// deploy with the token, done.
func TestATickPairsAPendingGateway(t *testing.T) {
	s := openStore(t)
	ca := testCA(t)
	hub := hubIdentity(t, ca)
	ctx := t.Context()

	g := newPendingGateway(t)
	gw, err := s.CreateGateway(ctx, "dmz", g.URL, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, gw.InstallToken)

	l := gwsync.New(gwsync.Options{
		Store: s, Client: gwpush.New(ca, &hub, 5*time.Second), RenewBefore: time.Hour,
		RunnerCA: func() []byte { return []byte("runner-ca") },
	})
	l.Tick(ctx)

	if !g.adopted {
		t.Fatal("the loop did not adopt a gateway that was waiting for it")
	}
	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.AdoptedAt == nil {
		t.Fatal("adoption was not recorded")
	}
	if got.Fingerprint == "" {
		t.Fatal("no fingerprint was learned; the hub would have no way back in once the identity lapsed")
	}
	if got.InstallToken != "" {
		t.Fatal("the install token survived pairing; it could be spent again")
	}
	if got.CertSerial == "" {
		t.Fatal("no identity was recorded, so renewal has nothing to compare against")
	}

	// A second pass must leave it alone rather than re-pairing.
	g.adopted = false
	l.Tick(ctx)
	if g.adopted {
		t.Fatal("the loop paired with an already-adopted gateway")
	}
}

// A gateway that is not deployed yet is the common case, not a fault: the loop
// records the failure and retries, and issues nothing.
func TestATickRetriesAGatewayThatIsNotThereYet(t *testing.T) {
	s := openStore(t)
	ca := testCA(t)
	hub := hubIdentity(t, ca)
	ctx := t.Context()

	gw, err := s.CreateGateway(ctx, "not-deployed", "https://127.0.0.1:1", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	l := gwsync.New(gwsync.Options{
		Store: s, Client: gwpush.New(ca, &hub, time.Second), RenewBefore: time.Hour,
	})
	l.Tick(ctx)

	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.AdoptedAt != nil {
		t.Fatal("a gateway that never answered was recorded as adopted")
	}
	if got.InstallToken == "" {
		t.Fatal("the install token was burned by a failed pairing, so the gateway could never be adopted")
	}
	if got.LastPushError == "" {
		t.Fatal("the failure was not recorded, so it is invisible from the hub")
	}
}

// A gateway holding a different token is not adopted, and nothing is issued to
// it.
func TestATickRefusesAGatewayWithTheWrongToken(t *testing.T) {
	s := openStore(t)
	ca := testCA(t)
	hub := hubIdentity(t, ca)
	ctx := t.Context()

	g := newPendingGateway(t)
	gw, err := s.CreateGateway(ctx, "impostor", g.URL, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, "sgt_a_completely_different_token")

	l := gwsync.New(gwsync.Options{
		Store: s, Client: gwpush.New(ca, &hub, 5*time.Second), RenewBefore: time.Hour,
	})
	l.Tick(ctx)

	if g.adopted {
		t.Fatal("a gateway that could not prove it holds the install token was adopted")
	}
	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.AdoptedAt != nil || got.Fingerprint != "" {
		t.Fatal("a failed pairing was recorded as an adoption")
	}
}
