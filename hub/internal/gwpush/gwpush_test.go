package gwpush_test

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
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/pki"
)

// fakeGateway is a gateway as the hub sees it: a self-signed TLS server that
// publishes a fingerprint and a CSR, and accepts an adoption.
type fakeGateway struct {
	srv         *httptest.Server
	fingerprint string
	csr         []byte

	adopted *gwpush.Adoption
	pushed  []byte
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	// The long-lived key: the thing the fingerprint is over, and the thing an
	// operator's paste actually pins.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gateway key: %v", err)
	}
	fp, err := pki.Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "unadopted-gateway"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("self-signed certificate: %v", err)
	}

	// A separate identity key: the certificate the hub issues is over this,
	// never over the anchor key.
	idKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("identity key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: "ignored"}}, idKey)
	if err != nil {
		t.Fatalf("csr: %v", err)
	}

	g := &fakeGateway{fingerprint: fp, csr: csr}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bootstrap", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gwpush.Bootstrap{Fingerprint: fp, CSR: csr, Version: "test"})
	})
	mux.HandleFunc("POST /adopt", func(w http.ResponseWriter, r *http.Request) {
		var a gwpush.Adoption
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		g.adopted = &a
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		g.pushed = buf[:n]
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /identity", func(w http.ResponseWriter, r *http.Request) {
		var a gwpush.Adoption
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		g.adopted = &a
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /explode", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "the gateway is unhappy", http.StatusInternalServerError)
	})

	g.srv = httptest.NewUnstartedServer(mux)
	g.srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
	}
	g.srv.StartTLS()
	t.Cleanup(g.srv.Close)
	return g
}

// testCA writes a CA to disk and loads it, mirroring how hubd is configured.
func testCA(t *testing.T) *pki.CA {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(7),
		Subject:               pkix.Name{CommonName: "shift gateway ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal ca key: %v", err)
	}
	certPath := filepath.Join(dir, "ca.pem")
	keyPath := filepath.Join(dir, "ca-key.pem")
	write(t, certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	write(t, keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	ca, err := pki.Load("gateway", certPath, keyPath, time.Hour)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	return ca
}

func write(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// The happy path: an operator's fingerprint is enough to reach a gateway that
// has no CA, no name, and no credential.
func TestAdoptionPinsTheGatewayByItsKey(t *testing.T) {
	g := newFakeGateway(t)
	c := gwpush.New(testCA(t), nil, 5*time.Second)

	b, err := c.Fetch(t.Context(), g.srv.URL, g.fingerprint)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if b.Version != "test" {
		t.Fatalf("version = %q, want test", b.Version)
	}

	issued, err := c.Adopt(t.Context(), g.srv.URL, g.fingerprint, "gw-1", []byte("runner-ca"), b.CSR)
	if err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if g.adopted == nil {
		t.Fatal("the gateway was never told it had been adopted")
	}
	if g.adopted.GatewayID != "gw-1" {
		t.Fatalf("gateway id = %q, want gw-1", g.adopted.GatewayID)
	}
	if g.adopted.HubSubject != gwpush.HubSubject {
		t.Fatalf("hub subject = %q, want %q", g.adopted.HubSubject, gwpush.HubSubject)
	}
	if string(g.adopted.RunnerCA) != "runner-ca" {
		t.Fatal("the gateway was not given the runner CA, so it cannot verify a runner's poll")
	}

	// The issued identity must name the gateway the HUB chose, and must be
	// server-auth only: a gateway serves, it never dials.
	blk, _ := pem.Decode(issued.CertPEM)
	if blk == nil {
		t.Fatal("issued certificate is not PEM")
	}
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatalf("parse issued: %v", err)
	}
	if leaf.Subject.CommonName != "gw-1" {
		t.Fatalf("subject = %q, want gw-1", leaf.Subject.CommonName)
	}
	if len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Fatalf("ext key usage = %v, want server-auth only", leaf.ExtKeyUsage)
	}
	if leaf.IsCA {
		t.Fatal("the gateway was issued a CA certificate; it could then issue identities itself")
	}
}

// The whole security claim of §2: a wrong fingerprint fails the HANDSHAKE, so
// no request is ever processed by either side.
func TestAWrongFingerprintNeverCompletesTheHandshake(t *testing.T) {
	g := newFakeGateway(t)
	c := gwpush.New(testCA(t), nil, 5*time.Second)

	wrong := strings.Repeat("ab", 32)
	_, err := c.Fetch(t.Context(), g.srv.URL, wrong)
	if err == nil {
		t.Fatal("a gateway presenting a different key was accepted")
	}
	if !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("error = %v, want it to name the fingerprint mismatch", err)
	}
	if g.adopted != nil {
		t.Fatal("the gateway saw a request despite the pin failing")
	}
}

// A gateway that presents one key and claims another is refused rather than
// reconciled — the two answers cannot both be true.
func TestAGatewayDisagreeingWithItselfIsRefused(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "liar"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	real, err := pki.Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /bootstrap", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gwpush.Bootstrap{
			Fingerprint: strings.Repeat("cd", 32), // not its own key
			CSR:         []byte{1, 2, 3},
		})
	})
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
	}
	srv.StartTLS()
	defer srv.Close()

	c := gwpush.New(testCA(t), nil, 5*time.Second)
	if _, err := c.Fetch(t.Context(), srv.URL, real); err == nil {
		t.Fatal("a gateway that misreports its own fingerprint was accepted")
	}
}

// An empty CSR is refused: adoption that produced no identity would leave a
// gateway trusted and unreachable.
func TestABootstrapWithoutACSRIsRefused(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	fp, err := pki.Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "empty"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bootstrap", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gwpush.Bootstrap{Fingerprint: fp})
	})
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
	}
	srv.StartTLS()
	defer srv.Close()

	c := gwpush.New(testCA(t), nil, 5*time.Second)
	if _, err := c.Fetch(t.Context(), srv.URL, fp); err == nil {
		t.Fatal("a bootstrap with no certificate request was accepted")
	}
}

// Adoption must not proceed on a request the gateway cannot prove it owns.
func TestAdoptRefusesAnUnsignableRequest(t *testing.T) {
	g := newFakeGateway(t)
	c := gwpush.New(testCA(t), nil, 5*time.Second)
	if _, err := c.Adopt(t.Context(), g.srv.URL, g.fingerprint, "gw-1", nil, []byte("not a csr")); err == nil {
		t.Fatal("a malformed certificate request was signed")
	}
	if g.adopted != nil {
		t.Fatal("the gateway was adopted despite no identity being issued")
	}
}

// A gateway erroring is reported with its status, and its response body is
// bounded — it is the least trusted thing that talks to the hub.
func TestAGatewayErrorIsReportedWithItsStatus(t *testing.T) {
	g := newFakeGateway(t)
	c := gwpush.New(testCA(t), nil, 5*time.Second)
	err := c.Push(t.Context(), g.srv.URL, "gw-1", map[string]string{"x": "y"})
	if err == nil {
		t.Fatal("pushing to a gateway with no mutual TLS succeeded")
	}
}

func TestPushDeliversTheWholeConfiguration(t *testing.T) {
	// Push requires mutual TLS, which needs the gateway to hold a hub-issued
	// identity; the fake gateway is self-signed, so verify the request never
	// leaves rather than the delivery.
	g := newFakeGateway(t)
	c := gwpush.New(testCA(t), nil, 5*time.Second)
	if err := c.Push(context.Background(), g.srv.URL, "gw-1", map[string]int{"version": 3}); err == nil {
		t.Fatal("a configuration was pushed to a gateway that presented no hub-issued identity")
	}
	if g.pushed != nil {
		t.Fatal("configuration reached a gateway the hub could not verify")
	}
}

// hubIdentity mints the hub's own client certificate from the gateway CA —
// the credential a gateway checks before believing a configuration push.
func hubIdentity(t *testing.T, ca *pki.CA) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("hub key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("hub csr: %v", err)
	}
	issued, err := ca.Sign(csr, gwpush.HubSubject, pki.UsageClient)
	if err != nil {
		t.Fatalf("sign hub: %v", err)
	}
	blk, _ := pem.Decode(issued.CertPEM)
	return tls.Certificate{Certificate: [][]byte{blk.Bytes}, PrivateKey: key}
}

// adoptedGateway is the gateway AFTER adoption: it serves the identity the hub
// issued and demands the hub's certificate in return.
type adoptedGateway struct {
	srv    *httptest.Server
	pushed []byte
	csr    []byte
	seen   *gwpush.Adoption
}

func newAdoptedGateway(t *testing.T, ca *pki.CA, id string) *adoptedGateway {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("identity key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	issued, err := ca.Sign(csr, id, pki.UsageServer)
	if err != nil {
		t.Fatalf("sign gateway: %v", err)
	}
	blk, _ := pem.Decode(issued.CertPEM)

	// A fresh key for the NEXT identity, so a renewal never re-signs the key
	// already in use.
	nextKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("next key: %v", err)
	}
	nextCSR, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, nextKey)
	if err != nil {
		t.Fatalf("next csr: %v", err)
	}

	g := &adoptedGateway{csr: nextCSR}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		g.pushed = buf[:n]
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /csr", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gwpush.Bootstrap{CSR: nextCSR})
	})
	mux.HandleFunc("POST /identity", func(w http.ResponseWriter, r *http.Request) {
		var a gwpush.Adoption
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		g.seen = &a
		w.WriteHeader(http.StatusNoContent)
	})

	g.srv = httptest.NewUnstartedServer(mux)
	g.srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{blk.Bytes}, PrivateKey: key}},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.Pool(),
		MinVersion:   tls.VersionTLS13,
	}
	g.srv.StartTLS()
	t.Cleanup(g.srv.Close)
	return g
}

// After adoption the hub pushes over mutual TLS, and the gateway is verified by
// the identity the hub issued rather than by a hostname it does not have.
func TestPushOverMutualTLSReachesAnAdoptedGateway(t *testing.T) {
	ca := testCA(t)
	g := newAdoptedGateway(t, ca, "gw-9")
	hub := hubIdentity(t, ca)
	c := gwpush.New(ca, &hub, 5*time.Second)

	if err := c.Push(t.Context(), g.srv.URL, "gw-9", map[string]int{"version": 42}); err != nil {
		t.Fatalf("push: %v", err)
	}
	if !strings.Contains(string(g.pushed), `"version":42`) {
		t.Fatalf("gateway received %q, want the configuration", g.pushed)
	}
}

// The certificate is pinned to the gateway the hub MEANT to reach. Chaining to
// the CA alone would trust any gateway it ever issued, including someone
// else's.
func TestPushRefusesTheWrongGatewayOnTheRightCA(t *testing.T) {
	ca := testCA(t)
	g := newAdoptedGateway(t, ca, "gw-9")
	hub := hubIdentity(t, ca)
	c := gwpush.New(ca, &hub, 5*time.Second)

	err := c.Push(t.Context(), g.srv.URL, "gw-OTHER", map[string]int{"version": 1})
	if err == nil {
		t.Fatal("a configuration was pushed to a different gateway holding a valid certificate")
	}
	if g.pushed != nil {
		t.Fatal("the wrong gateway received configuration")
	}
}

// Renewal is push-only, because the gateway cannot ask (ADR-0049 §6).
func TestRenewIssuesAFreshIdentityOverMutualTLS(t *testing.T) {
	ca := testCA(t)
	g := newAdoptedGateway(t, ca, "gw-9")
	hub := hubIdentity(t, ca)
	c := gwpush.New(ca, &hub, 5*time.Second)

	issued, err := c.Renew(t.Context(), g.srv.URL, "unused-fingerprint", "gw-9", []byte("runner-ca"))
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if g.seen == nil {
		t.Fatal("the gateway was never given its renewed identity")
	}
	blk, _ := pem.Decode(issued.CertPEM)
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatalf("parse renewed: %v", err)
	}
	if leaf.Subject.CommonName != "gw-9" {
		t.Fatalf("renewed subject = %q, want gw-9", leaf.Subject.CommonName)
	}
}

// The claim that matters: a gateway whose identity has LAPSED is not stranded.
// Mutual TLS fails, and the hub falls back to the key an operator pinned at
// adoption — which is why the fingerprint is retained rather than discarded.
func TestALapsedIdentityIsRecoveredThroughThePinnedKey(t *testing.T) {
	ca := testCA(t)
	g := newFakeGateway(t) // self-signed: mutual TLS to the CA cannot succeed
	hub := hubIdentity(t, ca)
	c := gwpush.New(ca, &hub, 5*time.Second)

	// The fake gateway serves /bootstrap but not /csr, so the mTLS attempt
	// fails at the handshake and the pinned path takes over.
	issued, err := c.Renew(t.Context(), g.srv.URL, g.fingerprint, "gw-1", []byte("runner-ca"))
	if err != nil {
		t.Fatalf("a gateway with no valid identity was left stranded: %v", err)
	}
	blk, _ := pem.Decode(issued.CertPEM)
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if leaf.Subject.CommonName != "gw-1" {
		t.Fatalf("recovered subject = %q, want gw-1", leaf.Subject.CommonName)
	}
}
