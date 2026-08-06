// Package runnerca issues the control-plane client certificates a runner
// authenticates with (ADR-0044).
//
// It is the ONE place in the platform that signs, which is deliberate: the CA
// key belongs at the hub, the component that already decides what a runner is.
// The gateway verifies and never issues (its own bundle is placed by hand,
// ADR-0041), and the runner holds only its own key.
//
// The private key never leaves the runner. A CSR proves possession of a key
// the hub never sees, which is the security difference between this and the
// bearer secret it replaces: a shared secret on disk is replayable by anything
// that can read it — a log line, a proxy, a core dump — and a private key used
// only inside the TLS layer is not.
package runnerca

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"
)

// DefaultTTL is how long an issued runner certificate lives.
//
// Short on purpose. ADR-0044's open question was CRL versus short lifetimes,
// and short lifetimes win for a fleet that can renew itself: a decommissioned
// runner stops being trusted within a day without any distribution mechanism,
// and there is no revocation list to publish, fetch, or fail to fetch.
const DefaultTTL = 24 * time.Hour

// MinRSABits refuses a key too small to be worth the TLS handshake.
const MinRSABits = 2048

// CA signs runner certificates.
type CA struct {
	cert  *x509.Certificate
	key   crypto.Signer
	caPEM []byte
	pool  *x509.CertPool
	ttl   time.Duration
}

// Load reads the control-plane CA from disk.
//
// From FILES, not from the database: a CA key stored beside the data it
// protects is one compromise away from being able to mint an identity for
// every runner in the fleet. Deployments that want it in a KMS can point these
// at a mounted, short-lived materialisation without this package knowing.
func Load(certFile, keyFile string, ttl time.Duration) (*CA, error) {
	certPEM, err := os.ReadFile(certFile) // #nosec G304 -- operator-configured path
	if err != nil {
		return nil, fmt.Errorf("runnerca: CA certificate: %w", err)
	}
	keyPEM, err := os.ReadFile(keyFile) // #nosec G304 -- operator-configured path
	if err != nil {
		return nil, fmt.Errorf("runnerca: CA key: %w", err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("runnerca: CA certificate/key: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("runnerca: parsing the CA certificate: %w", err)
	}
	if !leaf.IsCA || leaf.KeyUsage&x509.KeyUsageCertSign == 0 {
		// A leaf certificate configured here would fail later, at the first
		// registration, in a deployment that believed it had mTLS working.
		return nil, errors.New("runnerca: the configured certificate is not a CA (no certificate-signing usage)")
	}
	signer, ok := pair.PrivateKey.(crypto.Signer)
	if !ok {
		return nil, errors.New("runnerca: the CA key cannot sign")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		return nil, errors.New("runnerca: the CA file contains no usable certificate")
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if notAfter := time.Until(leaf.NotAfter); notAfter <= 0 {
		return nil, fmt.Errorf("runnerca: the CA certificate expired at %s", leaf.NotAfter.UTC().Format(time.RFC3339))
	}
	return &CA{cert: leaf, key: signer, caPEM: certPEM, pool: pool, ttl: ttl}, nil
}

// Pool is the trust root for verifying runner certificates.
func (c *CA) Pool() *x509.CertPool { return c.pool }

// CAPEM is the certificate a runner needs in order to verify the hub.
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

// Sign issues a client certificate for runnerID from a CSR.
//
// The CSR's SUBJECT IS IGNORED. Only the public key is taken from it; the
// identity is the runnerID the hub just assigned. This is ADR-0044 §5 and
// ADR-0041 §3 in one line: the runner proves it holds a key, and the hub says
// who that makes it. A runner that could name itself could name another
// runner.
func (c *CA) Sign(csrDER []byte, runnerID string) (*Issued, error) {
	if runnerID == "" {
		return nil, errors.New("runnerca: no runner id to issue for")
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, fmt.Errorf("runnerca: parsing the request: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		// Proof of possession. Without it, anyone with a valid registration
		// token could have a certificate issued over SOMEBODY ELSE's public
		// key, and that somebody would then be impersonable by the holder of
		// the matching private key.
		return nil, fmt.Errorf("runnerca: the request is not signed by its own key: %w", err)
	}
	if err := checkKey(csr.PublicKey); err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("runnerca: serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: runnerID},
		NotBefore:    now.Add(-time.Minute), // absorb small clock skew
		NotAfter:     now.Add(c.ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		// ClientAuth ONLY. A runner dials; it never serves TLS in the control
		// plane. A certificate that could also authenticate a SERVER would let
		// a stolen runner key stand up something a peer would trust.
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		// No SANs: this identity is a name in a private namespace, not a host.
		// No IsCA, so a runner cannot issue anything to anybody.
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, csr.PublicKey, c.key)
	if err != nil {
		return nil, fmt.Errorf("runnerca: signing: %w", err)
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
			return errors.New("runnerca: the ECDSA key is smaller than P-256")
		}
	case ed25519.PublicKey:
	case *rsa.PublicKey:
		if k.N.BitLen() < MinRSABits {
			return fmt.Errorf("runnerca: the RSA key is %d bits; %d is the minimum", k.N.BitLen(), MinRSABits)
		}
	default:
		return fmt.Errorf("runnerca: unsupported key type %T", pub)
	}
	return nil
}

// RunnerID returns the runner id proven by a verified client certificate, or
// "" when the connection carries none.
//
// It reads the SUBJECT of the verified chain, never anything the request body
// said. Callers must only pass a *verified* connection state — with Go's
// tls.VerifyClientCertIfGiven or RequireAndVerifyClientCert, VerifiedChains is
// non-empty exactly when the chain checked out, which is what this insists on.
func RunnerID(cs *tls.ConnectionState) string {
	if cs == nil || len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return ""
	}
	return cs.VerifiedChains[0][0].Subject.CommonName
}
