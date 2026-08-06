// Package identity loads the gateway's mTLS material and turns a peer
// certificate into a runner id (ADR-0041).
//
// Deliberately stdlib-only, like the rest of the gateway. A shared PKI package
// across hub/runner/gateway would be convenient and would put more code — and
// a wider dependency surface — on the one box that may sit in a DMZ. Issuance
// lives at the hub; this side only verifies and reads.
//
// The bundle is placed BY HAND, once. A gateway cannot dial the hub (ADR-0038
// §2), so it cannot register itself; an administrator downloads the bundle and
// drops it on the host, the gateway binds and waits to be dialled, and
// everything after that — including certificate renewal — rides the channel
// the bundle establishes.
//
// A copied bundle is inert: the hub dials a configured address, so a second
// gateway holding the same identity is simply never contacted.
package identity

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// File names inside the bundle directory.
const (
	CertFile = "gateway.pem"
	KeyFile  = "gateway-key.pem"
	CAFile   = "ca.pem"
	IDFile   = "gateway-id"
)

// Bundle is a loaded gateway identity.
type Bundle struct {
	// ID is the gateway's hub-assigned identifier, used in its own logs and in
	// the hub's gateway record.
	ID string
	// Cert is this gateway's certificate and key.
	Cert tls.Certificate
	// CA verifies the hub and every runner. One CA for the whole control
	// plane, distinct from any public-facing TLS.
	CA *x509.CertPool
	// NotAfter is the certificate's expiry, surfaced so the gateway can report
	// it on health. Renewal is the HUB's job to push (ADR-0041 §4): a gateway
	// that lets its certificate lapse is stranded permanently, because
	// renewing would mean dialling out.
	NotAfter time.Time
}

// Load reads a bundle from dir.
func Load(dir string) (*Bundle, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, CertFile), filepath.Join(dir, KeyFile))
	if err != nil {
		return nil, fmt.Errorf("identity: certificate/key: %w", err)
	}
	// LoadX509KeyPair leaves Leaf nil; parse it once here so expiry and
	// subject are available without re-parsing per connection.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("identity: parsing certificate: %w", err)
	}
	cert.Leaf = leaf

	caPEM, err := os.ReadFile(filepath.Join(dir, CAFile)) // #nosec G304 -- operator-placed bundle
	if err != nil {
		return nil, fmt.Errorf("identity: CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("identity: CA file contains no usable certificate")
	}

	id, err := os.ReadFile(filepath.Join(dir, IDFile)) // #nosec G304 -- operator-placed bundle
	if err != nil {
		return nil, fmt.Errorf("identity: gateway id: %w", err)
	}

	b := &Bundle{
		ID:       strings.TrimSpace(string(id)),
		Cert:     cert,
		CA:       pool,
		NotAfter: leaf.NotAfter,
	}
	if b.ID == "" {
		return nil, errors.New("identity: gateway id is empty")
	}
	return b, nil
}

// ServerTLS returns the control listener's TLS configuration.
//
// RequireAndVerifyClientCert is the whole point: the listener carries runner
// poll/deliver, and an unauthenticated caller reaching /poll can park a fake
// runner, receive real inbound payloads, and deliver forged responses. There
// is no weaker mode offered, because there is no deployment in which one would
// be correct.
func (b *Bundle) ServerTLS() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{b.Cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    b.CA,
		MinVersion:   tls.VersionTLS13,
		// NextProtos lets Go negotiate HTTP/2 by ALPN. Beyond throughput it
		// collapses N parked polls from one runner onto ONE connection, which
		// is what caused runner-side port exhaustion under HTTP/1.1
		// (docs/bench-gateway.md).
		NextProtos: []string{"h2", "http/1.1"},
	}
}

// PeerRunnerID returns the runner id proven by the client certificate, or ""
// when the connection carries none.
//
// The id comes from the certificate SUBJECT, never from anything the runner
// sent in a request. That distinction is the entire security property of
// ADR-0041 §3: a runner proves WHO it is, and never states WHAT it is — its
// labels come from the hub's roster, so it cannot promote itself into a trust
// tier by claiming one.
func PeerRunnerID(cs *tls.ConnectionState) string {
	if cs == nil || len(cs.PeerCertificates) == 0 {
		return ""
	}
	return cs.PeerCertificates[0].Subject.CommonName
}
