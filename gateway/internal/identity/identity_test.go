package identity_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/identity"
)

// The full mTLS handshake with real certificates, in both directions:
//
//   - the gateway REFUSES a client with no certificate, or one signed by
//     another CA — that is what stops an outsider parking a fake runner;
//   - the runner can VERIFY the gateway, which the shared secret never
//     allowed and which is what stops a runner handing its inbound payloads
//     to whatever answered on that address;
//   - the proven runner id comes from the certificate SUBJECT, never from
//     anything the runner sent (ADR-0041 §3).
func TestMutualTLS(t *testing.T) {
	ca := newCA(t)
	dir := t.TempDir()
	writeBundle(t, dir, ca, "gw-1")

	b, err := identity.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if b.ID != "gw-1" {
		t.Fatalf("bundle id = %q, want gw-1", b.ID)
	}

	// A server that reports back whichever runner id the client proved.
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The id comes from a CA-issued certificate subject, not from user
		// input; this handler exists only to echo it back to the test.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, identity.PeerRunnerID(r.TLS)) //nolint:gosec // G705: value is a verified certificate subject, not caller-controlled
	}))
	srv.TLS = b.ServerTLS()
	srv.StartTLS()
	defer srv.Close()

	t.Run("a runner with a CA-issued certificate is admitted and identified", func(t *testing.T) {
		cl := clientWith(t, ca.clientCert(t, "rnr-7"), ca.pool())
		got := get(t, cl, srv.URL)
		if got != "rnr-7" {
			t.Errorf("proven runner id = %q, want rnr-7", got)
		}
	})

	t.Run("no client certificate is refused", func(t *testing.T) {
		if err := handshake(t, clientWith(t, nil, ca.pool()), srv.URL); err == nil {
			t.Error("connection succeeded without a client certificate")
		}
	})

	t.Run("a certificate from another CA is refused", func(t *testing.T) {
		other := newCA(t)
		cl := clientWith(t, other.clientCert(t, "rnr-impostor"), ca.pool())
		if err := handshake(t, cl, srv.URL); err == nil {
			t.Error("a certificate from an unrelated CA was accepted")
		}
	})

	t.Run("the runner rejects a gateway it cannot verify", func(t *testing.T) {
		// The direction the shared secret never covered: trusting only an
		// unrelated CA, the runner must refuse this gateway.
		other := newCA(t)
		cl := clientWith(t, ca.clientCert(t, "rnr-7"), other.pool())
		if err := handshake(t, cl, srv.URL); err == nil {
			t.Error("the runner accepted a gateway certificate it could not verify")
		}
	})
}

// A bundle missing any part is refused at load, not at first connection: a
// gateway that starts and then cannot serve is far harder to diagnose.
func TestLoadRejectsAnIncompleteBundle(t *testing.T) {
	ca := newCA(t)
	for _, missing := range []string{identity.CertFile, identity.CAFile, identity.IDFile} {
		t.Run("missing "+missing, func(t *testing.T) {
			dir := t.TempDir()
			writeBundle(t, dir, ca, "gw-1")
			if err := os.Remove(filepath.Join(dir, missing)); err != nil {
				t.Fatal(err)
			}
			if _, err := identity.Load(dir); err == nil {
				t.Errorf("loaded a bundle with no %s", missing)
			}
		})
	}
}

// A bundle that loads but cannot authenticate anything is the worst outcome:
// the gateway starts, the listener binds, and every runner fails a handshake
// for reasons that look like a network fault. Both cases below are rejected at
// load, where the message can name the file.
func TestLoadRejectsAnUnusableBundle(t *testing.T) {
	ca := newCA(t)

	t.Run("a CA file with no certificate in it", func(t *testing.T) {
		dir := t.TempDir()
		writeBundle(t, dir, ca, "gw-1")
		// Plausible-looking and entirely useless: the client-cert pool would be
		// empty, so RequireAndVerifyClientCert would reject every runner.
		if err := os.WriteFile(filepath.Join(dir, identity.CAFile),
			[]byte("-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := identity.Load(dir); err == nil {
			t.Error("loaded a bundle whose CA file contains no usable certificate")
		}
	})

	t.Run("an empty gateway id", func(t *testing.T) {
		dir := t.TempDir()
		writeBundle(t, dir, ca, "gw-1")
		if err := os.WriteFile(filepath.Join(dir, identity.IDFile), []byte("\n  \n"), 0o600); err != nil {
			t.Fatal(err)
		}
		// An anonymous gateway cannot be named in an audit trail or revoked by
		// the hub, so it must not run at all.
		if _, err := identity.Load(dir); err == nil {
			t.Error("loaded a bundle with a blank gateway id")
		}
	})
}

// PeerRunnerID is called on every poll, including on connections that carry no
// certificate at all. It must answer "nobody" rather than panic — the caller
// turns that into a 403, and a panic in this path takes the gateway down from
// an unauthenticated request.
func TestPeerRunnerIDWithoutAProvenIdentity(t *testing.T) {
	if got := identity.PeerRunnerID(nil); got != "" {
		t.Errorf("PeerRunnerID(nil) = %q, want empty", got)
	}
	if got := identity.PeerRunnerID(&tls.ConnectionState{}); got != "" {
		t.Errorf("PeerRunnerID(no peer certificates) = %q, want empty", got)
	}
}

// --- test CA ---------------------------------------------------------------

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "SHIFT test control-plane CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
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
	return &testCA{cert: cert, key: key, der: der}
}

func (c *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(c.cert)
	return p
}

func (c *testCA) issue(t *testing.T, cn string, usage x509.ExtKeyUsage, dns []string, ips []net.IP) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dns,
		// IPs must go in IPAddresses, not DNSNames: an IP in DNSNames is
		// ignored by verification, which fails with "no IP SANs".
		IPAddresses: ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func (c *testCA) clientCert(t *testing.T, runnerID string) *tls.Certificate {
	t.Helper()
	cert := c.issue(t, runnerID, x509.ExtKeyUsageClientAuth, nil, nil)
	return &cert
}

func writeBundle(t *testing.T, dir string, ca *testCA, gatewayID string) {
	t.Helper()
	srv := ca.issue(t, gatewayID, x509.ExtKeyUsageServerAuth,
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")})
	key, err := x509.MarshalECPrivateKey(srv.PrivateKey.(*ecdsa.PrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		identity.CertFile: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate[0]}),
		identity.KeyFile:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: key}),
		identity.CAFile:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.der}),
		identity.IDFile:   []byte(gatewayID + "\n"),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func clientWith(t *testing.T, cert *tls.Certificate, roots *x509.CertPool) *http.Client {
	t.Helper()
	cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
}

// handshake attempts a request and returns only the error, closing any body.
// Every use expects the TLS handshake to fail before a response exists.
func handshake(t *testing.T, cl *http.Client, url string) error {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cl.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}

func get(t *testing.T, cl *http.Client, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cl.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
