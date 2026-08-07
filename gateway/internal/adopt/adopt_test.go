package adopt_test

import (
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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/adopt"
)

// ca is a stand-in for the hub's signer.
type ca struct {
	key  *ecdsa.PrivateKey
	cert *x509.Certificate
	pem  []byte
}

func newCA(t *testing.T, name string) *ca {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
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
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &ca{key: key, cert: cert, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// sign issues a certificate over a CSR's key, the way the hub does.
func (c *ca) sign(t *testing.T, csrDER []byte, cn string, notAfter time.Time) []byte {
	t.Helper()
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatal(err)
	}
	return c.signKey(t, csr.PublicKey, cn, notAfter)
}

func (c *ca) signKey(t *testing.T, pub any, cn string, notAfter time.Time) []byte {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, pub, c.key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// testToken is the install token every test pairs with.
const testToken = "sgt_test_install_token"

// nonce is a hub nonce of the minimum acceptable length.
const nonce = "0123456789abcdef0123456789abcdef"

// pair runs the hub's half of a pairing against s, returning the hello.
func pair(t *testing.T, s *adopt.State, token string) *adopt.Hello {
	t.Helper()
	proof := adopt.Proof(token, "shift-gw-hub-hello", nonce, s.Fingerprint(), nil)
	hello, err := s.Pair(adopt.Challenge{Nonce: nonce, Proof: proof}, "test")
	if err != nil {
		t.Fatalf("pair: %v", err)
	}
	return hello
}

// install completes a pairing with a signed identity.
func install(t *testing.T, s *adopt.State, token string, a adopt.Adoption) error {
	t.Helper()
	a.Nonce = nonce
	a.Proof = adopt.Proof(token, "shift-gw-install", nonce, s.Fingerprint(), adopt.MaterialDigest(a))
	if err := s.CheckInstall(a); err != nil {
		return err
	}
	return s.Install(a)
}

// adoptOnce drives a full adoption against a fresh state directory.
func adoptOnce(t *testing.T, dir string, gwCA, rnCA *ca) *adopt.State {
	t.Helper()
	s, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	hello := pair(t, s, testToken)
	cert := gwCA.sign(t, hello.CSR, "gw-1", time.Now().Add(24*time.Hour))
	if err := install(t, s, testToken, adopt.Adoption{
		GatewayID: "gw-1", CertPEM: cert,
		GatewayCA: gwCA.pem, RunnerCA: rnCA.pem, HubSubject: "hub",
	}); err != nil {
		t.Fatalf("install: %v", err)
	}
	return s
}

// A fresh gateway is inert: it has a key nobody gave it, a fingerprint anyone
// may read, and no identity at all.
func TestAFreshGatewayIsUnadoptedAndSelfIdentifying(t *testing.T) {
	s, err := adopt.Open(t.TempDir(), testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if s.Adopted() {
		t.Fatal("a gateway with no state is adopted")
	}
	if len(s.Fingerprint()) != 64 {
		t.Fatalf("fingerprint = %q, want 64 hex characters", s.Fingerprint())
	}
	if s.GatewayID() != "" {
		t.Fatal("an unadopted gateway has an id; only the hub assigns one")
	}
}

// The anchor is what the operator's paste pins, so it must survive a restart.
// A gateway that regenerated its key on every boot could never be adopted.
func TestTheAnchorSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	first, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fp := first.Fingerprint()

	second, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if second.Fingerprint() != fp {
		t.Fatal("the fingerprint changed across a restart; the operator's paste would no longer match")
	}
}

// A restart between publishing a request and receiving the certificate must
// not lose the key, or the hub would issue an identity the gateway cannot use.
func TestAPendingRequestSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	first, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	csr, err := first.CSR()
	if err != nil {
		t.Fatalf("csr: %v", err)
	}

	second, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	again, err := second.CSR()
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	a, err := x509.ParseCertificateRequest(csr)
	if err != nil {
		t.Fatal(err)
	}
	b, err := x509.ParseCertificateRequest(again)
	if err != nil {
		t.Fatal(err)
	}
	ka, _ := a.PublicKey.(*ecdsa.PublicKey)
	kb, _ := b.PublicKey.(*ecdsa.PublicKey)
	if ka == nil || kb == nil || !ka.Equal(kb) {
		t.Fatal("the pending identity key changed across a restart; an issued certificate would be unusable")
	}
}

func TestAdoptionInstallsAnIdentityAndSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	s := adoptOnce(t, dir, gwCA, rnCA)

	if !s.Adopted() {
		t.Fatal("adoption did not take")
	}
	if s.GatewayID() != "gw-1" {
		t.Fatalf("gateway id = %q, want gw-1", s.GatewayID())
	}

	// ADR-0049 §3: the gateway comes back ADOPTED, and no bootstrap window
	// reopens.
	again, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !again.Adopted() {
		t.Fatal("a restarted gateway forgot it had been adopted")
	}
	if again.GatewayID() != "gw-1" {
		t.Fatalf("gateway id after restart = %q, want gw-1", again.GatewayID())
	}
}

// The install token is what stops pairing being a race won by whoever reaches
// the port first. Without it an attacker installs their own CA and pushes
// routes.
func TestPairingRefusesACallerWithoutTheToken(t *testing.T) {
	s, err := adopt.Open(t.TempDir(), testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fp := s.Fingerprint()

	for _, tc := range []struct {
		name string
		c    adopt.Challenge
	}{
		{"no proof", adopt.Challenge{Nonce: nonce}},
		{"wrong token", adopt.Challenge{Nonce: nonce,
			Proof: adopt.Proof("sgt_not_the_token", "shift-gw-hub-hello", nonce, fp, nil)}},
		// The proof is bound to the fingerprint of the certificate on the wire.
		// An interceptor terminates TLS with ITS key, so its proof is computed
		// over a different fingerprint and fails here — which is the entire
		// reason the HMAC covers the fingerprint rather than being a bare token.
		{"proof bound to another key", adopt.Challenge{Nonce: nonce,
			Proof: adopt.Proof(testToken, "shift-gw-hub-hello", nonce, strings.Repeat("00", 32), nil)}},
		// Domain separation: a proof minted for the install step must not open
		// the pairing step.
		{"proof for the wrong step", adopt.Challenge{Nonce: nonce,
			Proof: adopt.Proof(testToken, "shift-gw-install", nonce, fp, nil)}},
		{"short nonce", adopt.Challenge{Nonce: "abc",
			Proof: adopt.Proof(testToken, "shift-gw-hub-hello", "abc", fp, nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Pair(tc.c, "test"); err == nil {
				t.Fatal("a pairing challenge was accepted without the install token")
			}
		})
	}

	// The real thing works.
	if _, err := s.Pair(adopt.Challenge{
		Nonce: nonce, Proof: adopt.Proof(testToken, "shift-gw-hub-hello", nonce, fp, nil),
	}, "test"); err != nil {
		t.Fatalf("a valid pairing was refused: %v", err)
	}
}

// A gateway started with no token cannot be adopted at all, and says so rather
// than treating a missing configuration as a pass.
func TestAGatewayWithNoTokenCannotBePaired(t *testing.T) {
	s, err := adopt.Open(t.TempDir(), "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = s.Pair(adopt.Challenge{Nonce: nonce, Proof: "anything"}, "test")
	if !errors.Is(err, adopt.ErrNoToken) {
		t.Fatalf("pair with no token = %v, want ErrNoToken", err)
	}
}

// The install proof covers the MATERIAL. Without that a captured proof could be
// replayed with a substituted CA, and the gateway would trust an issuer nobody
// chose.
func TestTheInstallProofCommitsToTheMaterial(t *testing.T) {
	dir := t.TempDir()
	gwCA, attacker := newCA(t, "gateway ca"), newCA(t, "attacker ca")
	s, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	hello := pair(t, s, testToken)
	cert := gwCA.sign(t, hello.CSR, "gw-1", time.Now().Add(time.Hour))

	honest := adopt.Adoption{GatewayID: "gw-1", CertPEM: cert, GatewayCA: gwCA.pem, HubSubject: "hub"}
	honest.Nonce = nonce
	honest.Proof = adopt.Proof(testToken, "shift-gw-install", nonce, s.Fingerprint(), adopt.MaterialDigest(honest))

	// Same proof, swapped CA — the substitution an interceptor would attempt.
	swapped := honest
	swapped.GatewayCA = attacker.pem
	if err := s.CheckInstall(swapped); err == nil {
		t.Fatal("a proof was accepted over material it did not cover; the gateway would trust a substituted CA")
	}
	if err := s.CheckInstall(honest); err != nil {
		t.Fatalf("the honest material was refused: %v", err)
	}
}

// The token is burned on adoption. A surviving copy would be a standing
// credential on a DMZ host.
func TestTheInstallTokenIsBurnedOnAdoption(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	s := adoptOnce(t, dir, gwCA, rnCA)

	if _, err := s.Pair(adopt.Challenge{
		Nonce: nonce, Proof: adopt.Proof(testToken, "shift-gw-hub-hello", nonce, s.Fingerprint(), nil),
	}, "test"); err == nil {
		t.Fatal("an adopted gateway still answers pairing challenges")
	}
	a := adopt.Adoption{GatewayID: "gw-1", CertPEM: []byte("x"), GatewayCA: gwCA.pem}
	a.Nonce = nonce
	a.Proof = adopt.Proof(testToken, "shift-gw-install", nonce, s.Fingerprint(), adopt.MaterialDigest(a))
	if err := s.CheckInstall(a); !errors.Is(err, adopt.ErrNoToken) {
		t.Fatalf("install check after adoption = %v, want ErrNoToken (the token must be burned)", err)
	}
}

// A certificate over a key the gateway does not hold would leave the hub
// believing it was reachable while every handshake failed.
func TestACertificateOverAForeignKeyIsRefused(t *testing.T) {
	dir := t.TempDir()
	gwCA := newCA(t, "gateway ca")
	s, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.CSR(); err != nil {
		t.Fatalf("csr: %v", err)
	}
	foreign, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := gwCA.signKey(t, &foreign.PublicKey, "gw-1", time.Now().Add(time.Hour))

	err = s.Install(adopt.Adoption{GatewayID: "gw-1", CertPEM: cert, GatewayCA: gwCA.pem})
	if err == nil {
		t.Fatal("a certificate over a key this gateway does not hold was installed")
	}
	if s.Adopted() {
		t.Fatal("the gateway considers itself adopted after a failed install")
	}
}

func TestInstallRejectsMismatchedAndMalformedIdentities(t *testing.T) {
	dir := t.TempDir()
	gwCA := newCA(t, "gateway ca")
	s, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	csr, err := s.CSR()
	if err != nil {
		t.Fatalf("csr: %v", err)
	}

	if err := s.Install(adopt.Adoption{GatewayID: "", CertPEM: []byte("x")}); err == nil {
		t.Fatal("an adoption naming no gateway was accepted")
	}
	if err := s.Install(adopt.Adoption{GatewayID: "gw-1", CertPEM: []byte("not pem")}); err == nil {
		t.Fatal("a non-PEM certificate was accepted")
	}
	// A certificate whose subject is not the id the hub claims to be assigning.
	wrong := gwCA.sign(t, csr, "somebody-else", time.Now().Add(time.Hour))
	if err := s.Install(adopt.Adoption{GatewayID: "gw-1", CertPEM: wrong, GatewayCA: gwCA.pem}); err == nil {
		t.Fatal("a certificate naming a different gateway was accepted")
	}
	// No CA means the gateway could never verify the hub back.
	right := gwCA.sign(t, csr, "gw-1", time.Now().Add(time.Hour))
	if err := s.Install(adopt.Adoption{GatewayID: "gw-1", CertPEM: right}); err == nil {
		t.Fatal("an adoption with no gateway CA was accepted")
	}
}

// One gateway, one owner: a second hub cannot re-point an adopted gateway by
// offering it a different id.
func TestASecondHubCannotClaimAnAdoptedGateway(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	s := adoptOnce(t, dir, gwCA, rnCA)

	other := newCA(t, "attacker ca")
	csr, err := s.CSR()
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	cert := other.sign(t, csr, "gw-2", time.Now().Add(time.Hour))
	err = s.Install(adopt.Adoption{
		GatewayID: "gw-2", CertPEM: cert, GatewayCA: other.pem, HubSubject: "hub",
	})
	if err == nil {
		t.Fatal("an adopted gateway accepted an identity for a different gateway id")
	}
	if s.GatewayID() != "gw-1" {
		t.Fatalf("gateway id = %q, want gw-1 unchanged", s.GatewayID())
	}
}

// Renewal replaces the identity in place, under the same id.
func TestRenewalReplacesTheIdentity(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	s := adoptOnce(t, dir, gwCA, rnCA)
	before := s.NotAfter()

	csr, err := s.CSR()
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	renewed := gwCA.sign(t, csr, "gw-1", time.Now().Add(72*time.Hour))
	if err := s.Install(adopt.Adoption{
		GatewayID: "gw-1", CertPEM: renewed,
		GatewayCA: gwCA.pem, RunnerCA: rnCA.pem, HubSubject: "hub",
	}); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !s.NotAfter().After(before) {
		t.Fatal("renewal did not extend the identity")
	}
	// The promoted key must not still be offered as pending — the hub would
	// sign a key it had already signed.
	if _, err := os.Stat(filepath.Join(dir, "identity-key-pending.pem")); !os.IsNotExist(err) {
		t.Fatal("the pending key survived promotion")
	}
}

// The listener's whole posture changes with adoption, and it must change while
// running — a gateway adopted at 10:00 must not keep accepting anonymous
// callers until someone restarts it.
func TestTheListenerRequiresClientCertificatesOnlyOnceAdopted(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	s, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	get := s.ControlTLS().GetConfigForClient

	before, err := get(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("tls config: %v", err)
	}
	if before.ClientAuth != tls.NoClientCert {
		t.Fatal("an unadopted gateway demands a client certificate nobody can yet have")
	}

	csr, err := s.CSR()
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	cert := gwCA.sign(t, csr, "gw-1", time.Now().Add(24*time.Hour))
	if err := s.Install(adopt.Adoption{
		GatewayID: "gw-1", CertPEM: cert,
		GatewayCA: gwCA.pem, RunnerCA: rnCA.pem, HubSubject: "hub",
	}); err != nil {
		t.Fatalf("install: %v", err)
	}

	after, err := get(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("tls config: %v", err)
	}
	if after.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatal("an adopted gateway still accepts anonymous callers on the control listener")
	}
	if after.ClientCAs == nil {
		t.Fatal("no client CAs configured, so no peer could ever be verified")
	}
}

// The recovery path (ADR-0049 §6): once the identity has expired the listener
// must fall back to the ANCHOR, because that is the key the hub pins. A gateway
// still presenting a dead identity could never be dialled again.
func TestAnExpiredIdentityFallsBackToTheAnchor(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	s, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	csr, err := s.CSR()
	if err != nil {
		t.Fatalf("csr: %v", err)
	}
	// Already expired when issued.
	dead := gwCA.sign(t, csr, "gw-1", time.Now().Add(-time.Minute))
	if err := s.Install(adopt.Adoption{
		GatewayID: "gw-1", CertPEM: dead,
		GatewayCA: gwCA.pem, RunnerCA: rnCA.pem, HubSubject: "hub",
	}); err != nil {
		t.Fatalf("install: %v", err)
	}

	cfg, err := s.ControlTLS().GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("tls config: %v", err)
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Fatal("a gateway with an expired identity still demands a client certificate it cannot verify against a live channel")
	}
	served, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	fp, err := adopt.Fingerprint(served.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if fp != s.Fingerprint() {
		t.Fatal("a gateway with an expired identity does not present its anchor, so the hub's pinned dial would fail and it would be stranded")
	}
}

// Role comes from WHICH CA signed the peer, never from the name it carries.
// Both CAs are trusted on this listener, so a name-based check would let a
// runner named "hub" push configuration.
func TestRoleIsAttributedByIssuerNotByName(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	s := adoptOnce(t, dir, gwCA, rnCA)

	chain := func(c *ca, cn string) *tls.ConnectionState {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := c.signKey(t, &key.PublicKey, cn, time.Now().Add(time.Hour))
		blk, _ := pem.Decode(certPEM)
		leaf, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf, c.cert}}}
	}

	if role, id := s.PeerRole(chain(gwCA, "hub")); role != adopt.RoleHub || id != "hub" {
		t.Fatalf("hub certificate = role %v id %q, want RoleHub/hub", role, id)
	}
	if role, _ := s.PeerRole(chain(rnCA, "runner-7")); role != adopt.RoleRunner {
		t.Fatal("a runner certificate was not attributed to a runner")
	}
	// The attack the issuer check exists to stop.
	if role, _ := s.PeerRole(chain(rnCA, "hub")); role == adopt.RoleHub {
		t.Fatal("a RUNNER certificate named \"hub\" was treated as the hub; it could push routes and TLS keys")
	}
	// A certificate from the gateway CA that is not the hub subject.
	if role, _ := s.PeerRole(chain(gwCA, "not-the-hub")); role == adopt.RoleHub {
		t.Fatal("a gateway-CA certificate with the wrong subject was treated as the hub")
	}
	if role, _ := s.PeerRole(nil); role != adopt.RoleNone {
		t.Fatal("a connection with no certificate was given a role")
	}
	if role, _ := s.PeerRole(&tls.ConnectionState{}); role != adopt.RoleNone {
		t.Fatal("an unverified connection was given a role")
	}
}

// Rollback is what makes full-image replacement safe (ADR-0049 §5), so state
// written by a newer gateway must be refused rather than half-understood.
func TestStateFromANewerGatewayIsRefused(t *testing.T) {
	dir := t.TempDir()
	if _, err := adopt.Open(dir, testToken); err != nil {
		t.Fatalf("open: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "state.json")) // #nosec G304 -- a path built from t.TempDir()
	if err != nil {
		// No state file until adoption; write one directly.
		raw = []byte(`{"version":1}`)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	doc["version"] = adopt.StateVersion + 1
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), out, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := adopt.Open(dir, testToken); err == nil {
		t.Fatal("a gateway read state written by a newer build; a rollback would run a policy nobody chose")
	}
}

func TestOpenRefusesAnEmptyDirectoryName(t *testing.T) {
	if _, err := adopt.Open("", testToken); err == nil {
		t.Fatal("a gateway opened a state directory with no name")
	}
}

// --- corrupt and partial state ------------------------------------------------

// The state directory is on a DMZ host. It gets imaged, restored, half-copied
// and truncated, and every one of those must fail loudly at start-up rather
// than at the first handshake.
func TestCorruptStateIsRefusedAtStartup(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, dir string)
	}{
		{"unreadable state document", func(t *testing.T, dir string) {
			t.Helper()
			write(t, filepath.Join(dir, "state.json"), []byte("{not json"))
		}},
		{"anchor key is not PEM", func(t *testing.T, dir string) {
			t.Helper()
			write(t, filepath.Join(dir, "anchor-key.pem"), []byte("nonsense"))
		}},
		{"pending key is not PEM", func(t *testing.T, dir string) {
			t.Helper()
			seed(t, dir)
			write(t, filepath.Join(dir, "identity-key-pending.pem"), []byte("nonsense"))
		}},
		{"identity without its key", func(t *testing.T, dir string) {
			t.Helper()
			seed(t, dir)
			write(t, filepath.Join(dir, "identity.pem"), []byte("-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.setup(t, dir)
			if _, err := adopt.Open(dir, testToken); err == nil {
				t.Fatal("a corrupt state directory started cleanly")
			}
		})
	}
}

// seed creates a valid, unadopted state directory.
func seed(t *testing.T, dir string) {
	t.Helper()
	if _, err := adopt.Open(dir, testToken); err != nil {
		t.Fatal(err)
	}
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// The anchor CERTIFICATE is a wrapper for the key, so losing it is recoverable
// — the fingerprint is over the key and must not move.
func TestALostAnchorCertificateIsRebuiltWithoutMovingTheFingerprint(t *testing.T) {
	dir := t.TempDir()
	first, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatal(err)
	}
	fp := first.Fingerprint()

	if err := os.Remove(filepath.Join(dir, "anchor.pem")); err != nil {
		t.Fatal(err)
	}
	second, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("a gateway that lost its anchor certificate failed to start: %v", err)
	}
	if second.Fingerprint() != fp {
		t.Fatal("rebuilding the anchor certificate moved the fingerprint; the operator's paste would no longer match")
	}
}

// A gateway that lost its CA material cannot verify the hub, so it must not
// come back claiming to be adopted.
func TestAnIdentityWithoutItsCAIsRefused(t *testing.T) {
	dir := t.TempDir()
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	adoptOnce(t, dir, gwCA, rnCA)

	if err := os.Remove(filepath.Join(dir, "gateway-ca.pem")); err != nil {
		t.Fatal(err)
	}
	if _, err := adopt.Open(dir, testToken); err == nil {
		t.Fatal("a gateway with an identity but no CA started; it would require client certificates it cannot verify")
	}
}

// A runner CA is optional: a hub with no runner mTLS sends none, and the
// gateway then verifies only the hub.
func TestTheRunnerCAIsOptional(t *testing.T) {
	dir := t.TempDir()
	gwCA := newCA(t, "gateway ca")
	s, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := s.CSR()
	if err != nil {
		t.Fatal(err)
	}
	cert := gwCA.sign(t, csr, "gw-1", time.Now().Add(time.Hour))
	if err := s.Install(adopt.Adoption{
		GatewayID: "gw-1", CertPEM: cert, GatewayCA: gwCA.pem, HubSubject: "hub",
	}); err != nil {
		t.Fatalf("install without a runner CA: %v", err)
	}
	again, err := adopt.Open(dir, testToken)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !again.Adopted() {
		t.Fatal("a gateway adopted without a runner CA did not come back adopted")
	}
}

// The twin of TestTheProofConstructionMatchesTheGateway in
// hub/internal/gwpush. The construction is duplicated because gateway/go.mod
// has zero dependencies and the hub cannot import from it — the same trade
// ADR-0046 §2 makes for logging. Duplicated crypto that silently disagreed
// would be far worse than the dependency, so both sides are pinned to this one
// vector. A mismatch means no gateway can ever be adopted.
func TestTheProofConstructionMatchesTheHub(t *testing.T) {
	//nolint:gosec // G101: a fixed TEST VECTOR, not a credential — its whole job
	// is to be identical on both sides of a duplicated implementation.
	const (
		token       = "sgt_fixed_vector"
		fixedNonce  = "0123456789abcdef0123456789abcdef"
		fingerprint = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
		hubHello    = "922c3fdfc328e5c2bbaabfa3508c61908edcc6da9c889c6f878bd3d54dbdf5e3"
	)
	got := adopt.Proof(token, "shift-gw-hub-hello", fixedNonce, fingerprint, nil)
	if got != hubHello {
		t.Fatalf("proof = %s, want %s — the gateway and the hub would no longer agree, "+
			"and no gateway could be adopted", got, hubHello)
	}
	if adopt.Proof(token, "shift-gw-install", fixedNonce, fingerprint, nil) == got {
		t.Fatal("two domains produce the same proof; a hello proof would satisfy an install")
	}
	if adopt.Proof(token, "shift-gw-hub-hello", fixedNonce, strings.Repeat("00", 32), nil) == got {
		t.Fatal("the proof does not depend on the fingerprint, so it could not detect interception")
	}
}
