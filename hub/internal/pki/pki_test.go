package pki_test

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

	"github.com/aaron-au/shift/hub/internal/pki"
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

func loadCA(tb testing.TB, dir string, ttl time.Duration) *pki.CA {
	tb.Helper()
	ca, err := pki.Load("runner", filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), ttl)
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

	got, err := ca.Sign(der, "runner-7", pki.UsageClient)
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

	got, err := ca.Sign(der, "runner-7", pki.UsageClient)
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

	got, err := ca.Sign(der, "runner-7", pki.UsageClient)
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

	if _, err := ca.Sign(tampered, "runner-7", pki.UsageClient); err == nil {
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
	_, err = ca.Sign(der, "runner-7", pki.UsageClient)
	if err == nil {
		t.Fatal("a 1024-bit RSA key was accepted")
	}
	if !strings.Contains(err.Error(), "2048") {
		t.Errorf("error %q does not say what the minimum is", err)
	}
}

func TestSignRefusesAnUnparseableRequest(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	if _, err := ca.Sign([]byte("not a csr"), "runner-7", pki.UsageClient); err == nil {
		t.Fatal("garbage was accepted as a CSR")
	}
}

func TestSignNeedsARunnerID(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	der, _ := csr(t, "runner")
	if _, err := ca.Sign(der, "", pki.UsageClient); err == nil {
		t.Fatal("issued a certificate with no subject at all")
	}
}

// A leaf certificate configured as the CA would fail at the first
// registration, in a deployment that believed it had mTLS working.
func TestLoadRefusesACertificateThatIsNotACA(t *testing.T) {
	dir := writeCA(t, time.Now().Add(24*time.Hour), false)
	_, err := pki.Load("runner", filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour)
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
	if _, err := pki.Load("runner", filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour); err == nil {
		t.Fatal("an expired CA was loaded")
	}
}

func TestLoadReportsMissingFiles(t *testing.T) {
	dir := writeCA(t, time.Now().Add(24*time.Hour), true)
	if _, err := pki.Load("runner", filepath.Join(dir, "nope.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour); err == nil {
		t.Error("a missing certificate file was not reported")
	}
	if _, err := pki.Load("runner", filepath.Join(dir, "ca.pem"), filepath.Join(dir, "nope.pem"), time.Hour); err == nil {
		t.Error("a missing key file was not reported")
	}
}

func TestLoadDefaultsTheTTL(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(90*24*time.Hour), true), 0)
	der, _ := csr(t, "")
	got, err := ca.Sign(der, "runner-7", pki.UsageClient)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Now().Add(pki.DefaultTTL)
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
	got, err := ca.Sign(der, "runner-7", pki.UsageClient)
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
	got, err := ca.Sign(der, "runner-7", pki.UsageClient)
	if err != nil {
		t.Fatal(err)
	}
	cert := parse(t, got.CertPEM)

	if id := pki.Subject(nil); id != "" {
		t.Errorf("RunnerID(nil) = %q", id)
	}
	// Presented but not verified: the field a forged peer controls.
	unverified := &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	if id := pki.Subject(unverified); id != "" {
		t.Errorf("RunnerID on an unverified chain = %q; an assertion was read as an identity", id)
	}
	verified := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
	if id := pki.Subject(verified); id != "runner-7" {
		t.Errorf("RunnerID = %q, want runner-7", id)
	}
}

// --- gateway trust (ADR-0049) ------------------------------------------------

// The fingerprint is over the KEY, not the certificate: a gateway that rolls
// its self-signed certificate keeps the same anchor, which is the only reason
// the hub still has a way back in.
func TestFingerprintFollowsTheKeyNotTheCertificate(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	first, err := pki.Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	// A second certificate over the same key.
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(99),
		Subject:      pkix.Name{CommonName: "rolled"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	rolled, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	second, err := pki.Fingerprint(rolled.PublicKey)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if first != second {
		t.Fatal("rolling the certificate changed the fingerprint, which would strand the gateway")
	}
	if len(first) != 64 {
		t.Fatalf("fingerprint is %d hex chars, want 64 (SHA-256)", len(first))
	}
}

func TestFingerprintRefusesAKeyItCannotMarshal(t *testing.T) {
	if _, err := pki.Fingerprint("not a key"); err == nil {
		t.Fatal("a non-key was fingerprinted")
	}
}

// A server certificate is server-auth only, and carries no SANs — the identity
// is a name in the hub's namespace, not a host.
func TestAServerIdentityIsServerAuthOnly(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	issued, err := ca.Sign(csr, "gw-1", pki.UsageServer)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	blk, _ := pem.Decode(issued.CertPEM)
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Fatalf("ext key usage = %v, want server-auth only", leaf.ExtKeyUsage)
	}
	if len(leaf.DNSNames) != 0 || len(leaf.IPAddresses) != 0 {
		t.Fatal("a control-plane identity carries SANs, tying it to network topology the hub does not own")
	}
}

func TestVerifySubjectRejectsACertificateForAnotherGateway(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	issued, err := ca.Sign(csr, "gw-1", pki.UsageServer)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	blk, _ := pem.Decode(issued.CertPEM)

	if err := pki.VerifySubject(ca.Pool(), "gw-1")([][]byte{blk.Bytes}, nil); err != nil {
		t.Fatalf("the right gateway was rejected: %v", err)
	}
	// Same CA, valid certificate, WRONG gateway. Chaining alone would accept it.
	if err := pki.VerifySubject(ca.Pool(), "gw-2")([][]byte{blk.Bytes}, nil); err == nil {
		t.Fatal("a certificate for another gateway on the same CA was accepted")
	}
	if err := pki.VerifySubject(ca.Pool(), "gw-1")(nil, nil); err == nil {
		t.Fatal("a peer presenting nothing was accepted")
	}
	if err := pki.VerifySubject(ca.Pool(), "gw-1")([][]byte{[]byte("junk")}, nil); err == nil {
		t.Fatal("unparseable bytes were accepted as a certificate")
	}
	// A certificate from an unrelated CA must not chain.
	other := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	if err := pki.VerifySubject(other.Pool(), "gw-1")([][]byte{blk.Bytes}, nil); err == nil {
		t.Fatal("a certificate from a different CA was accepted")
	}
}

func TestVerifyFingerprintRejectsAnythingButThePinnedKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(5),
		Subject:      pkix.Name{CommonName: "gw"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	want, err := pki.Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if err := pki.VerifyFingerprint(want)([][]byte{der}, nil); err != nil {
		t.Fatalf("the pinned key was rejected: %v", err)
	}
	if err := pki.VerifyFingerprint(strings.Repeat("00", 32))([][]byte{der}, nil); err == nil {
		t.Fatal("a key that is not the pinned one was accepted")
	}
	if err := pki.VerifyFingerprint(want)(nil, nil); err == nil {
		t.Fatal("a gateway presenting no certificate was accepted")
	}
	if err := pki.VerifyFingerprint(want)([][]byte{[]byte("junk")}, nil); err == nil {
		t.Fatal("unparseable bytes were accepted as a certificate")
	}
}

// VerifyPeerCertificate is skipped on a RESUMED TLS session, so both pins are
// also expressed as VerifyConnection callbacks. If these ever diverge from the
// originals, a second dial to the same peer stops being checked.
func TestTheConnectionLevelPinsMatchTheCertificateLevelOnes(t *testing.T) {
	ca := loadCA(t, writeCA(t, time.Now().Add(24*time.Hour), true), time.Hour)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	req, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	issued, err := ca.Sign(req, "gw-1", pki.UsageServer)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	blk, _ := pem.Decode(issued.CertPEM)
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}

	if err := pki.VerifyConnSubject(ca.Pool(), "gw-1")(state); err != nil {
		t.Fatalf("the right gateway was rejected on a resumed connection: %v", err)
	}
	if err := pki.VerifyConnSubject(ca.Pool(), "gw-2")(state); err == nil {
		t.Fatal("a resumed session skipped the common-name pin")
	}
	if err := pki.VerifyConnSubject(ca.Pool(), "gw-1")(tls.ConnectionState{}); err == nil {
		t.Fatal("a resumed session with no peer certificate was accepted")
	}

	fp, err := pki.Fingerprint(leaf.PublicKey)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if err := pki.VerifyConnFingerprint(fp)(state); err != nil {
		t.Fatalf("the pinned key was rejected on a resumed connection: %v", err)
	}
	if err := pki.VerifyConnFingerprint(strings.Repeat("00", 32))(state); err == nil {
		t.Fatal("a resumed session skipped the fingerprint pin")
	}
	if err := pki.VerifyConnFingerprint(fp)(tls.ConnectionState{}); err == nil {
		t.Fatal("a resumed session with no peer certificate was accepted")
	}
}
