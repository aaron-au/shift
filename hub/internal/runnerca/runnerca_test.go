package runnerca_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/runnerca"
)

// writeCA mints a throwaway CA on disk and returns its directory.
func writeCA(tb testing.TB, notAfter time.Time, isCA bool) string {
	tb.Helper()
	dir := tb.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test control-plane CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              notAfter,
		IsCA:                  isCA,
		BasicConstraintsValid: true,
	}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		tb.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		tb.Fatal(err)
	}
	write(tb, filepath.Join(dir, "ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	write(tb, filepath.Join(dir, "ca-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return dir
}

func write(tb testing.TB, path string, data []byte) {
	tb.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		tb.Fatal(err)
	}
}

func loadCA(tb testing.TB, dir string, ttl time.Duration) *runnerca.CA {
	tb.Helper()
	ca, err := runnerca.Load(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), ttl)
	if err != nil {
		tb.Fatal(err)
	}
	return ca
}

// csr returns a CSR and its key, with whatever subject the caller asks for —
// several tests exist to prove the subject is ignored.
func csr(tb testing.TB, commonName string) ([]byte, *ecdsa.PrivateKey) {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: commonName}}, key)
	if err != nil {
		tb.Fatal(err)
	}
	return der, key
}

func parse(tb testing.TB, certPEM []byte) *x509.Certificate {
	tb.Helper()
	blk, _ := pem.Decode(certPEM)
	if blk == nil {
		tb.Fatal("issued certificate is not PEM")
		return nil
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

func TestSignIssuesAClientCertificateForTheRunner(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "whatever")

	got, err := ca.Sign(der, "runner-7")
	if err != nil {
		t.Fatal(err)
	}
	cert := parse(t, got.CertPEM)
	if cert.Subject.CommonName != "runner-7" {
		t.Errorf("subject = %q, want the hub-assigned id", cert.Subject.CommonName)
	}
	if got.NotAfter.After(time.Now().Add(2 * time.Hour)) {
		t.Errorf("not_after = %s; the TTL was not honoured", got.NotAfter)
	}
	if got.Serial == "" {
		t.Error("no serial recorded; a certificate that cannot be named cannot be revoked")
	}
}

// The whole security property of ADR-0044 §5: the runner proves it holds a
// key, and the HUB says who that makes it. A runner that could name itself
// could name another runner.
func TestSignIgnoresTheRequestedSubject(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "admin")

	got, err := ca.Sign(der, "runner-7")
	if err != nil {
		t.Fatal(err)
	}
	if cn := parse(t, got.CertPEM).Subject.CommonName; cn != "runner-7" {
		t.Fatalf("subject = %q; a CSR claiming %q was honoured", cn, "admin")
	}
}

// A runner dials and never serves. A certificate that could also authenticate
// a SERVER would let a stolen key stand up something a peer would trust.
func TestIssuedCertificatesAreClientOnlyAndCannotIssue(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "")

	got, err := ca.Sign(der, "runner-7")
	if err != nil {
		t.Fatal(err)
	}
	cert := parse(t, got.CertPEM)
	if cert.IsCA {
		t.Error("the issued certificate is a CA; a runner could mint identities")
	}
	for _, u := range cert.ExtKeyUsage {
		if u == x509.ExtKeyUsageServerAuth {
			t.Error("the issued certificate is usable for server authentication")
		}
	}
	if len(cert.ExtKeyUsage) != 1 || cert.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth {
		t.Errorf("ext key usage = %v, want client auth only", cert.ExtKeyUsage)
	}
	if len(cert.DNSNames) != 0 || len(cert.IPAddresses) != 0 {
		t.Errorf("the certificate carries SANs (%v/%v); this identity is a name, not a host",
			cert.DNSNames, cert.IPAddresses)
	}
}

// Proof of possession. Without it, a valid registration token would let
// somebody have a certificate issued over ANOTHER party's public key.
func TestSignRefusesACSRNotSignedByItsOwnKey(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "runner")
	// Corrupt the signature: the last bytes of a CSR are its signature.
	tampered := append([]byte(nil), der...)
	tampered[len(tampered)-1] ^= 0xff

	if _, err := ca.Sign(tampered, "runner-7"); err == nil {
		t.Fatal("a CSR with a broken signature was signed")
	}
}

func TestSignRefusesAWeakKey(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	key, err := rsa.GenerateKey(rand.Reader, 1024) // #nosec G403 -- the point of the test is that a weak key is refused
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ca.Sign(der, "runner-7")
	if err == nil {
		t.Fatal("a 1024-bit RSA key was accepted")
	}
	if !strings.Contains(err.Error(), "2048") {
		t.Errorf("error %q does not say what the minimum is", err)
	}
}

func TestSignRefusesAnUnparseableRequest(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	if _, err := ca.Sign([]byte("not a csr"), "runner-7"); err == nil {
		t.Fatal("garbage was accepted as a CSR")
	}
}

func TestSignNeedsARunnerID(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "runner")
	if _, err := ca.Sign(der, ""); err == nil {
		t.Fatal("issued a certificate with no subject at all")
	}
}

// A leaf certificate configured as the CA would fail at the first
// registration, in a deployment that believed it had mTLS working.
func TestLoadRefusesACertificateThatIsNotACA(t *testing.T) {
	dir := writeCA(t, time.Now().Add(24*time.Hour), false)
	_, err := runnerca.Load(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour)
	if err == nil {
		t.Fatal("a non-CA certificate was accepted as the CA")
	}
	if !strings.Contains(err.Error(), "not a CA") {
		t.Errorf("error %q does not say what is wrong", err)
	}
}

// An expired CA cannot issue anything usable, so saying so at startup beats
// discovering it when the fleet stops renewing.
func TestLoadRefusesAnExpiredCA(t *testing.T) {
	dir := writeCA(t, time.Now().Add(-time.Hour), true)
	if _, err := runnerca.Load(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour); err == nil {
		t.Fatal("an expired CA was loaded")
	}
}

func TestLoadReportsMissingFiles(t *testing.T) {
	dir := writeCA(t, time.Now().Add(24*time.Hour), true)
	if _, err := runnerca.Load(filepath.Join(dir, "nope.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour); err == nil {
		t.Error("a missing certificate file was not reported")
	}
	if _, err := runnerca.Load(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "nope.pem"), time.Hour); err == nil {
		t.Error("a missing key file was not reported")
	}
}

func TestLoadDefaultsTheTTL(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(90*24*time.Hour), true), 0)
	der, _ := csr(t, "")
	got, err := ca.Sign(der, "runner-7")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Now().Add(runnerca.DefaultTTL)
	if got.NotAfter.Before(want.Add(-time.Minute)) || got.NotAfter.After(want.Add(time.Minute)) {
		t.Errorf("not_after = %s, want ~%s (DefaultTTL)", got.NotAfter, want)
	}
}

// An issued certificate must actually verify against the pool the hub hands
// out — the two are used by different processes and nothing else checks that
// they agree.
func TestIssuedCertificatesVerifyAgainstThePool(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "")
	got, err := ca.Sign(der, "runner-7")
	if err != nil {
		t.Fatal(err)
	}
	chains, err := parse(t, got.CertPEM).Verify(x509.VerifyOptions{
		Roots:     ca.Pool(),
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		t.Fatalf("the hub issued a certificate its own pool rejects: %v", err)
	}
	if len(chains) == 0 {
		t.Fatal("no chain")
	}
	if len(ca.CAPEM()) == 0 {
		t.Error("CAPEM is empty; a runner would have nothing to verify the hub with")
	}
	if ca.NotAfter().Before(time.Now()) {
		t.Error("NotAfter reports an expired CA after loading a valid one")
	}
}

// RunnerID reads the VERIFIED chain, never the presented certificate. An
// unverified peer certificate is an assertion, not an identity.
func TestRunnerIDRequiresAVerifiedChain(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "")
	got, err := ca.Sign(der, "runner-7")
	if err != nil {
		t.Fatal(err)
	}
	cert := parse(t, got.CertPEM)

	if id := runnerca.RunnerID(nil); id != "" {
		t.Errorf("RunnerID(nil) = %q", id)
	}
	// Presented but not verified: the field a forged peer controls.
	unverified := &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	if id := runnerca.RunnerID(unverified); id != "" {
		t.Errorf("RunnerID on an unverified chain = %q; an assertion was read as an identity", id)
	}
	verified := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
	if id := runnerca.RunnerID(verified); id != "runner-7" {
		t.Errorf("RunnerID = %q, want runner-7", id)
	}
}
