package gwsync_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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
		_ = json.NewEncoder(w).Encode(gwpush.Bootstrap{CSR: nextCSR})
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
	gw, err := s.CreateGateway(ctx, "dmz", g.URL, "aa")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, ca, gw.ID)
	// An identity that expires inside the renewal window.
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
	gw, err := s.CreateGateway(ctx, "quiet", g.URL, "bb")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, ca, gw.ID)
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
	gw, err := s.CreateGateway(ctx, "drifted", g.URL, "cc")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	g.start(t, ca, gw.ID)
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
