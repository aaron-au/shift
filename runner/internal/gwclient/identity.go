package gwclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// Gateway is one gateway the hub has told this runner to poll: where it is,
// and WHO it will prove itself to be when the runner dials it.
//
// The identity travels with the address because a hub-issued gateway
// certificate carries NO subject alternative name — deliberately, because a
// DMZ box has no stable hostname the hub can commit to at issue time. So the
// usual TLS hostname check has nothing to match and the runner pins the
// COMMON NAME instead, which is the gateway's hub-assigned id.
//
// Without this the runner could verify only that a peer's certificate chained
// to the control-plane CA — and every runner in the fleet holds a certificate
// from that same CA. One compromised runner could then stand up a listener,
// be dialled as a gateway, and be handed inbound payload to answer. Pinning
// the id is what makes "this is gateway X" mean something.
type Gateway = hubclient.Gateway

// pins maps a dialled host to the gateway id expected on its certificate.
type pins struct {
	mu sync.RWMutex
	m  map[string]string
}

func newPins() *pins { return &pins{m: map[string]string{}} }

// set records the identity to expect at a gateway's address.
func (p *pins) set(rawURL, id string) {
	host := hostOf(rawURL)
	if host == "" || id == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.m[host] = id
}

func (p *pins) get(host string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	id, ok := p.m[host]
	return id, ok
}

// hostOf reduces a base URL to the host the TLS server name will carry.
func hostOf(rawURL string) string {
	u, err := url.Parse(strings.TrimSuffix(rawURL, "/"))
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// dialPinned builds a TLS dialler that verifies each gateway by CHAIN plus the
// COMMON NAME the hub named for that address.
//
// Per-dial rather than one shared tls.Config, because the identity to expect
// differs per gateway and the standard hooks cannot tell them apart reliably:
// a connection to an IP literal carries no SNI at all, so the server name a
// verification callback sees is empty. The dialler knows the address it was
// asked for, which is the fact that matters.
//
// An address the hub did NOT name falls back to standard verification — the
// operator's own -gateways flag, checked against the control-plane roots and
// the certificate's subject alternative names. That is a different assertion
// (DNS plus SAN, rather than a hub-issued id) and not a weaker one; what
// neither path permits is accepting a peer on chain alone, which would trust
// every runner in the fleet.
func dialPinned(base *tls.Config, p *pins) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		cfg := base.Clone()
		cfg.ServerName = host
		if want, ok := p.get(host); ok {
			roots := cfg.RootCAs
			// #nosec G402 -- not "no verification": verifyPinned below chains
			// to the control-plane roots and pins the common name. The default
			// check wants a hostname a hub-issued gateway certificate has no
			// SAN for, by design (a DMZ box has no stable name at issue time).
			cfg.InsecureSkipVerify = true
			// VerifyConnection, not VerifyPeerCertificate: the latter is
			// SKIPPED on a resumed session, so a peer presenting a session
			// ticket would bypass the pin entirely.
			cfg.VerifyConnection = func(cs tls.ConnectionState) error {
				return verifyPinned(roots, want, cs.PeerCertificates)
			}
		}
		d := &tls.Dialer{Config: cfg}
		return d.DialContext(ctx, network, addr)
	}
}

// verifyPinned checks the chain against the control-plane roots and the leaf's
// common name against the identity the hub named for this address.
func verifyPinned(roots *x509.CertPool, want string, certs []*x509.Certificate) error {
	if len(certs) == 0 {
		return errors.New("gwclient: the gateway presented no certificate")
	}
	leaf := certs[0]
	if leaf.Subject.CommonName != want {
		return fmt.Errorf("gwclient: gateway identity mismatch: expected %q, got %q", want, leaf.Subject.CommonName)
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: inter,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("gwclient: the gateway certificate does not chain to the control plane: %w", err)
	}
	return nil
}
