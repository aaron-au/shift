// Package pki issues the control-plane certificates the hub's peers
// authenticate with (ADR-0044 for runners, ADR-0049 for gateways).
//
// It is the ONE place in the platform that signs, which is deliberate: the CA
// key belongs at the hub, the component that already decides what a runner or a
// gateway is. Peers verify and never issue, and each peer holds only its own
// key.
//
// The private key never leaves the peer. A CSR proves possession of a key the
// hub never sees, which is the security difference between this and the bearer
// secrets it replaces: a shared secret on disk is replayable by anything that
// can read it — a log line, a proxy, a core dump — and a private key used only
// inside the TLS layer is not.
//
// The two CAs it loads never share a trust store (ADR-0044 §3). A runner
// certificate must not authenticate anything on the gateway's control listener
// and vice versa, so they are separate files, separate pools, and separate
// instances of this type.
package pki

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"
)

// DefaultTTL is how long an issued certificate lives.
//
// Short on purpose. ADR-0044's open question was CRL versus short lifetimes,
// and short lifetimes win for a fleet that can renew itself: a decommissioned
// peer stops being trusted within a day without any distribution mechanism,
// and there is no revocation list to publish, fetch, or fail to fetch.
const DefaultTTL = 24 * time.Hour

// MinRSABits refuses a key too small to be worth the TLS handshake.
const MinRSABits = 2048

// Usage is what an issued certificate is allowed to be.
//
// It is chosen per issuance rather than per CA because the gateway CA signs
// both halves of one conversation: the gateway serves (ADR-0049 §2 — the hub
// dials it, and runners long-poll it), while the hub presents a client
// certificate on that same dial so the gateway can verify who is pushing
// configuration at it.
type Usage uint8

const (
	// UsageClient dials and never serves. A runner's identity is always this:
	// a certificate that could also authenticate a SERVER would let a stolen
	// runner key stand up something a peer would trust.
	UsageClient Usage = iota
	// UsageServer serves and never dials.
	UsageServer
)

func (u Usage) ext() []x509.ExtKeyUsage {
	if u == UsageServer {
		return []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	return []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
}

// CA signs control-plane certificates.
type CA struct {
	name  string // "runner" | "gateway" — error prefix only
	cert  *x509.Certificate
	key   crypto.Signer
	caPEM []byte
	pool  *x509.CertPool
	ttl   time.Duration
}

// Load reads a control-plane CA from disk.
//
// From FILES, not from the database: a CA key stored beside the data it
// protects is one compromise away from being able to mint an identity for
// every peer in the fleet. Deployments that want it in a KMS can point these at
// a mounted, short-lived materialisation without this package knowing.
func Load(name, certFile, keyFile string, ttl time.Duration) (*CA, error) {
	certPEM, err := os.ReadFile(certFile) // #nosec G304 -- operator-configured path
	if err != nil {
		return nil, fmt.Errorf("%s CA certificate: %w", name, err)
	}
	keyPEM, err := os.ReadFile(keyFile) // #nosec G304 -- operator-configured path
	if err != nil {
		return nil, fmt.Errorf("%s CA key: %w", name, err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("%s CA certificate/key: %w", name, err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parsing the %s CA certificate: %w", name, err)
	}
	if !leaf.IsCA || leaf.KeyUsage&x509.KeyUsageCertSign == 0 {
		// A leaf certificate configured here would fail later, at the first
		// registration, in a deployment that believed it had mTLS working.
		return nil, fmt.Errorf("the configured %s certificate is not a CA (no certificate-signing usage)", name)
	}
	signer, ok := pair.PrivateKey.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("the %s CA key cannot sign", name)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		return nil, fmt.Errorf("the %s CA file contains no usable certificate", name)
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if notAfter := time.Until(leaf.NotAfter); notAfter <= 0 {
		return nil, fmt.Errorf("the %s CA certificate expired at %s", name, leaf.NotAfter.UTC().Format(time.RFC3339))
	}
	return &CA{name: name, cert: leaf, key: signer, caPEM: certPEM, pool: pool, ttl: ttl}, nil
}

// Pool is the trust root for verifying certificates this CA issued.
func (c *CA) Pool() *x509.CertPool { return c.pool }

// CAPEM is the certificate a peer needs in order to verify the other side.
func (c *CA) CAPEM() []byte { return c.caPEM }

// NotAfter is the CA's own expiry, surfaced so a deployment can see the cliff
// coming rather than discover it when the fleet stops renewing.
func (c *CA) NotAfter() time.Time { return c.cert.NotAfter }

// Issued describes a signed certificate.
type Issued struct {
	CertPEM  []byte
	Serial   string
	NotAfter time.Time
}

// Sign issues a certificate naming subject, from a CSR.
//
// The CSR's SUBJECT IS IGNORED. Only the public key is taken from it; the
// identity is the one the hub just assigned. This is ADR-0044 §5 and ADR-0041
// §3 in one line: the peer proves it holds a key, and the hub says who that
// makes it. A peer that could name itself could name another peer.
func (c *CA) Sign(csrDER []byte, subject string, usage Usage) (*Issued, error) {
	if subject == "" {
		return nil, fmt.Errorf("%s CA: no identity to issue for", c.name)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, fmt.Errorf("%s CA: parsing the request: %w", c.name, err)
	}
	if err := csr.CheckSignature(); err != nil {
		// Proof of possession. Without it, anyone with a valid registration
		// token could have a certificate issued over SOMEBODY ELSE's public
		// key, and that somebody would then be impersonable by the holder of
		// the matching private key.
		return nil, fmt.Errorf("%s CA: the request is not signed by its own key: %w", c.name, err)
	}
	if err := checkKey(csr.PublicKey); err != nil {
		return nil, fmt.Errorf("%s CA: %w", c.name, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("%s CA: serial: %w", c.name, err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: subject},
		NotBefore:             now.Add(-time.Minute), // absorb small clock skew
		NotAfter:              now.Add(c.ttl),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           usage.ext(),
		BasicConstraintsValid: true,
		// No SANs, even for a server certificate: this identity is a name in a
		// private namespace, not a host. A DMZ gateway has no stable DNS name
		// the hub can rely on, so peers verify the chain and then compare the
		// COMMON NAME against the identity the hub told them to expect
		// (see VerifySubject). Requiring a SAN would make the certificate
		// depend on network topology the hub does not own.
		//
		// No IsCA, so nothing issued here can issue anything to anybody.
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, csr.PublicKey, c.key)
	if err != nil {
		return nil, fmt.Errorf("%s CA: signing: %w", c.name, err)
	}
	return &Issued{
		CertPEM:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		Serial:   serial.Text(16),
		NotAfter: tmpl.NotAfter,
	}, nil
}

// checkKey refuses keys that are not worth the handshake.
func checkKey(pub any) error {
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		if k.Curve == nil || k.Curve.Params().BitSize < 256 {
			return errors.New("the ECDSA key is smaller than P-256")
		}
	case ed25519.PublicKey:
	case *rsa.PublicKey:
		if k.N.BitLen() < MinRSABits {
			return fmt.Errorf("the RSA key is %d bits; %d is the minimum", k.N.BitLen(), MinRSABits)
		}
	default:
		return fmt.Errorf("unsupported key type %T", pub)
	}
	return nil
}

// Subject returns the identity proven by a verified peer certificate, or ""
// when the connection carries none.
//
// It reads the SUBJECT of the verified chain, never anything the request body
// said. Callers must only pass a *verified* connection state — with Go's
// tls.VerifyClientCertIfGiven or RequireAndVerifyClientCert, VerifiedChains is
// non-empty exactly when the chain checked out, which is what this insists on.
func Subject(cs *tls.ConnectionState) string {
	if cs == nil || len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return ""
	}
	return cs.VerifiedChains[0][0].Subject.CommonName
}

// Fingerprint is the hex SHA-256 of a public key in DER SubjectPublicKeyInfo
// form — the value an administrator carries from a gateway to the hub
// (ADR-0049 §1).
//
// It is over the KEY, not over the certificate. A gateway rolls its self-signed
// certificate — on expiry, on a clock correction — without its key changing,
// and a fingerprint that moved when the certificate did would strand the hub's
// only way back in.
func Fingerprint(pub any) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("fingerprint: %w", err)
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:]), nil
}

// VerifySubject builds the peer-verification callback for a dial where the
// identity is a name in the hub's namespace rather than a hostname.
//
// Go's default verification checks the certificate against DNS names, which a
// DMZ gateway does not reliably have. So the caller sets InsecureSkipVerify —
// meaning "not the DEFAULT verification", not "no verification" — and this
// does the work instead: chain to the CA pool, then the common name must equal
// want. Skipping the second half would trust ANY certificate the CA ever
// issued, including one for a different gateway.
func VerifySubject(pool *x509.CertPool, want string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		certs, err := parseChain(rawCerts)
		if err != nil {
			return err
		}
		return checkSubject(pool, want, certs)
	}
}

// VerifyConnSubject is VerifySubject for tls.Config.VerifyConnection.
//
// Both must be set. VerifyPeerCertificate does NOT run on a RESUMED session —
// the handshake short-circuits to the cached state — so a peer that resumed an
// earlier ticket would skip the common-name pin entirely. VerifyConnection runs
// on every handshake, resumed or full, which is why it carries the same check.
func VerifyConnSubject(pool *x509.CertPool, want string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		return checkSubject(pool, want, cs.PeerCertificates)
	}
}

func checkSubject(pool *x509.CertPool, want string, certs []*x509.Certificate) error {
	if len(certs) == 0 {
		return errors.New("the peer presented no certificate")
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	if _, err := certs[0].Verify(x509.VerifyOptions{
		Roots:         pool,
		Intermediates: inter,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("the peer certificate does not chain to the expected CA: %w", err)
	}
	if certs[0].Subject.CommonName != want {
		return fmt.Errorf("the peer is %q, not %q", certs[0].Subject.CommonName, want)
	}
	return nil
}

func parseChain(rawCerts [][]byte) ([]*x509.Certificate, error) {
	if len(rawCerts) == 0 {
		return nil, errors.New("the peer presented no certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		c, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, fmt.Errorf("parsing the peer certificate: %w", err)
		}
		certs = append(certs, c)
	}
	return certs, nil
}

// VerifyFingerprint builds the peer-verification callback for the ADOPTION
// dial, where no CA exists yet (ADR-0049 §2).
//
// The gateway's certificate is self-signed, so there is nothing to chain to.
// What there is instead is a 256-bit public-key fingerprint an operator read
// off the gateway's own logs and carried to the hub by hand — trust-on-first-
// use with the first use moved OUT OF BAND, which is strictly stronger than
// TOFU, because a machine-in-the-middle would have to hold the gateway's
// private key rather than merely be first to answer.
func VerifyFingerprint(want string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		certs, err := parseChain(rawCerts)
		if err != nil {
			return err
		}
		return checkFingerprint(want, certs)
	}
}

// VerifyConnFingerprint is VerifyFingerprint for tls.Config.VerifyConnection.
// Both are needed for the same reason as VerifyConnSubject: a resumed session
// never reaches VerifyPeerCertificate, and an adoption dial that skipped the
// pin would trust whatever answered.
func VerifyConnFingerprint(want string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		return checkFingerprint(want, cs.PeerCertificates)
	}
}

func checkFingerprint(want string, certs []*x509.Certificate) error {
	if len(certs) == 0 {
		return errors.New("the gateway presented no certificate")
	}
	got, err := Fingerprint(certs[0].PublicKey)
	if err != nil {
		return err
	}
	// Fixed-length lowercase hex on both sides, and a mismatch is not a
	// secret — the fingerprint is public by construction (ADR-0049 §1), so
	// there is no timing channel worth closing here.
	if got != want {
		return fmt.Errorf("the gateway's key fingerprint is %s, not the adopted %s", got, want)
	}
	return nil
}
