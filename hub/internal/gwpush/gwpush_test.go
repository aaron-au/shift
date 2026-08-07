package gwpush_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
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

//nolint:gosec // G101: a test fixture, not a credential.
const testToken = "sgt_test_install_token"

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
	certPath, keyPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem")
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

// fakeGateway is a gateway as the hub meets it before adoption: a self-signed
// key it generated itself, and an install token it was handed at deploy time.
type fakeGateway struct {
	srv         *httptest.Server
	fingerprint string
	token       string

	adopted *gwpush.Adoption
	// wrongFingerprint makes the gateway compute its proof over a key that is
	// not the one it is serving — what an interceptor's relay looks like.
	wrongFingerprint bool
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
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
		Subject:      pkix.Name{CommonName: "shift-gateway (unadopted)"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("self-signed certificate: %v", err)
	}

	idKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("identity key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, idKey)
	if err != nil {
		t.Fatalf("csr: %v", err)
	}

	g := &fakeGateway{fingerprint: fp, token: testToken}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		var c gwpush.Challenge
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if c.Proof == "" {
			// The hub's first call is a probe: it cannot compute a proof until
			// the handshake has revealed this gateway's key.
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if c.Proof != gwpush.Proof(g.token, gwpush.DomainHubHello, c.Nonce, fp, nil) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		proofFP := fp
		if g.wrongFingerprint {
			proofFP = strings.Repeat("00", 32)
		}
		sum := sha256.Sum256(csr)
		_ = json.NewEncoder(w).Encode(gwpush.Hello{
			Fingerprint: fp, CSR: csr, Version: "test",
			Proof: gwpush.Proof(g.token, gwpush.DomainGWHello, c.Nonce, proofFP, sum[:]),
		})
	})
	mux.HandleFunc("POST /adopt", func(w http.ResponseWriter, r *http.Request) {
		var a gwpush.Adoption
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		want := gwpush.Proof(g.token, gwpush.DomainInstall, a.Nonce, fp, gwpush.MaterialDigest(a))
		if a.Proof != want {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		g.adopted = &a
		w.WriteHeader(http.StatusNoContent)
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

// The happy path: the hub reaches a gateway whose key it has never seen, and
// learns it.
func TestPairingLearnsTheGatewaysKey(t *testing.T) {
	g := newFakeGateway(t)
	c := gwpush.New(testCA(t), nil, 5*time.Second)

	issued, fingerprint, err := c.Pair(t.Context(), g.srv.URL, testToken, "gw-1", []byte("runner-ca"))
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	if fingerprint != g.fingerprint {
		t.Fatalf("learned fingerprint %s, want %s", fingerprint, g.fingerprint)
	}
	if g.adopted == nil {
		t.Fatal("the gateway was never told it had been adopted")
	}
	if g.adopted.GatewayID != "gw-1" || g.adopted.HubSubject != gwpush.HubSubject {
		t.Fatal("the adoption did not carry the identity the hub assigned")
	}
	if string(g.adopted.RunnerCA) != "runner-ca" {
		t.Fatal("the gateway was not given the runner CA, so it cannot verify a runner's poll")
	}

	// Server-auth only, named by the hub, not a CA.
	blk, _ := pem.Decode(issued.CertPEM)
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

// Without the right token the hub gets nowhere — and, importantly, issues
// nothing.
func TestPairingWithTheWrongTokenIssuesNothing(t *testing.T) {
	g := newFakeGateway(t)
	c := gwpush.New(testCA(t), nil, 5*time.Second)

	if _, _, err := c.Pair(t.Context(), g.srv.URL, "sgt_wrong", "gw-1", nil); err == nil {
		t.Fatal("a gateway paired with a token it does not hold")
	}
	if g.adopted != nil {
		t.Fatal("an identity was delivered despite the pairing failing")
	}
}

// The gateway's proof is bound to the key it is serving. A proof computed over
// any other key is what a relayed exchange looks like, and the hub must refuse
// rather than issue an identity to whatever answered.
func TestAProofOverAnotherKeyIsRefused(t *testing.T) {
	g := newFakeGateway(t)
	g.wrongFingerprint = true
	c := gwpush.New(testCA(t), nil, 5*time.Second)

	_, _, err := c.Pair(t.Context(), g.srv.URL, testToken, "gw-1", nil)
	if err == nil {
		t.Fatal("the hub accepted a proof computed over a key other than the one on the wire")
	}
	if !strings.Contains(err.Error(), "install token") {
		t.Fatalf("error = %v, want it to name the failed proof", err)
	}
	if g.adopted != nil {
		t.Fatal("an identity was delivered to a peer that could not prove itself")
	}
}

// An unreachable gateway is an error, not a silent success.
func TestPairingAnUnreachableGatewayFails(t *testing.T) {
	c := gwpush.New(testCA(t), nil, time.Second)
	if _, _, err := c.Pair(t.Context(), "https://127.0.0.1:1", testToken, "gw-1", nil); err == nil {
		t.Fatal("pairing with nothing at all succeeded")
	}
}

// --- after adoption ----------------------------------------------------------

// hubIdentity mints the hub's own client certificate from the gateway CA — the
// credential a gateway checks before believing a configuration push.
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

	g := &adoptedGateway{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		g.pushed = buf[:n]
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /csr", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gwpush.Hello{CSR: nextCSR})
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
// the CA alone would trust any gateway it ever issued, including someone else's.
func TestPushRefusesTheWrongGatewayOnTheRightCA(t *testing.T) {
	ca := testCA(t)
	g := newAdoptedGateway(t, ca, "gw-9")
	hub := hubIdentity(t, ca)
	c := gwpush.New(ca, &hub, 5*time.Second)

	if err := c.Push(t.Context(), g.srv.URL, "gw-OTHER", map[string]int{"version": 1}); err == nil {
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

// A gateway that cannot be verified at all is a failure, not a fallback to
// something weaker.
func TestRenewFailsWhenTheGatewayCannotBeVerified(t *testing.T) {
	ca := testCA(t)
	g := newFakeGateway(t) // self-signed: neither the CA nor the pin will match
	hub := hubIdentity(t, ca)
	c := gwpush.New(ca, &hub, 5*time.Second)

	if _, err := c.Renew(t.Context(), g.srv.URL, strings.Repeat("11", 32), "gw-1", nil); err == nil {
		t.Fatal("an unverifiable gateway was renewed")
	}
}

// The proof construction is DUPLICATED in gateway/internal/adopt, because
// gateway/go.mod has zero dependencies and the hub cannot import from it —
// the same trade ADR-0046 §2 makes for logging. Duplicated crypto that silently
// disagreed would be far worse than the dependency, so both sides are pinned to
// this one vector. Its twin is TestTheProofConstructionMatchesTheHub in
// gateway/internal/adopt: if this value changes, that one must change with it,
// and a mismatch means no gateway can be adopted.
// to be identical on both sides of a duplicated implementation.
//
//nolint:gosec // G101: a fixed TEST VECTOR, not a credential — its whole job is
const (
	vectorToken       = "sgt_fixed_vector"
	vectorNonce       = "0123456789abcdef0123456789abcdef"
	vectorFingerprint = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	vectorHubHello    = "922c3fdfc328e5c2bbaabfa3508c61908edcc6da9c889c6f878bd3d54dbdf5e3"
)

func TestTheProofConstructionMatchesTheGateway(t *testing.T) {
	got := gwpush.Proof(vectorToken, gwpush.DomainHubHello, vectorNonce, vectorFingerprint, nil)
	if got != vectorHubHello {
		t.Fatalf("proof = %s, want %s — the hub and the gateway would no longer agree, "+
			"and no gateway could be adopted", got, vectorHubHello)
	}

	// The three properties the construction exists for.
	if gwpush.Proof(vectorToken, gwpush.DomainInstall, vectorNonce, vectorFingerprint, nil) == got {
		t.Fatal("two domains produce the same proof; a hello proof would satisfy an install")
	}
	if gwpush.Proof(vectorToken, gwpush.DomainHubHello, vectorNonce, strings.Repeat("00", 32), nil) == got {
		t.Fatal("the proof does not depend on the fingerprint, so it could not detect interception")
	}
	if gwpush.Proof("sgt_other", gwpush.DomainHubHello, vectorNonce, vectorFingerprint, nil) == got {
		t.Fatal("the proof does not depend on the token")
	}
}
