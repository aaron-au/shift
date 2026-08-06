// Package gwpush is the hub's side of the gateway control channel: the hub
// dials the gateway, adopts it, and pushes configuration to it (ADR-0038 §6,
// ADR-0049).
//
// Every connection here goes hub → gateway, never the reverse. That direction
// is the reason the gateway is safe in a DMZ (ADR-0038 §4): the DMZ box holds
// no hub credential, opens nothing inward, and cannot reach the control plane
// even if it is taken. It also means the hub owns liveness — a gateway that
// cannot be dialled is visibly failing a push, rather than quietly failing to
// check in.
//
// Two trust models live in this package, and they are not interchangeable:
//
//   - ADOPTION pins the gateway's long-lived public key by fingerprint. There
//     is no CA yet, so there is nothing to chain to; what there is instead is a
//     value an operator carried out of band.
//   - EVERYTHING AFTER pins the gateway CA plus the identity the hub itself
//     issued, and presents the hub's own client certificate so the gateway can
//     verify who is pushing policy at it.
package gwpush

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aaron-au/shift/hub/internal/pki"
)

// HubSubject is the common name on the hub's own client certificate — what a
// gateway checks before believing a configuration push.
const HubSubject = "hub"

// maxBody caps what the hub will read from a gateway. A DMZ box is the least
// trusted thing that talks to the hub, so its responses are bounded like any
// other untrusted input.
const maxBody = 1 << 20

// Bootstrap is what an unadopted gateway publishes about itself.
//
// All of it is public by construction: a key fingerprint, a certificate
// request, and a version string. None of it authorises anything, which is why
// the endpoint serving it needs no credential — requiring one would mean
// shipping a secret into the DMZ before trust exists, the exact inversion
// ADR-0049 removes.
type Bootstrap struct {
	Fingerprint string `json:"fingerprint"`
	CSR         []byte `json:"csr"` // DER
	Version     string `json:"version,omitempty"`
}

// Adoption is what the hub hands back to complete adoption.
type Adoption struct {
	GatewayID  string `json:"gateway_id"`
	CertPEM    []byte `json:"cert_pem"`
	GatewayCA  []byte `json:"gateway_ca_pem"` // verifies the hub, and the gateway to runners
	RunnerCA   []byte `json:"runner_ca_pem"`  // verifies runners polling for work
	HubSubject string `json:"hub_subject"`    // who the gateway must see on a push
}

// Client dials gateways.
type Client struct {
	ca   *pki.CA
	hub  *tls.Certificate // the hub's own identity, issued by ca
	http *http.Client
}

// New builds a client. hubCert is the hub's client certificate from the
// gateway CA; without it the hub can adopt but cannot push, because an adopted
// gateway requires mutual TLS.
func New(ca *pki.CA, hubCert *tls.Certificate, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{ca: ca, hub: hubCert, http: &http.Client{Timeout: timeout}}
}

// Fetch reads an unadopted gateway's bootstrap document over a connection
// pinned to want.
//
// InsecureSkipVerify here means "not the DEFAULT verification", not "no
// verification": the gateway's certificate is self-signed and its host has no
// name the hub can rely on, so VerifyPeerCertificate does the checking that
// matters — the presented key must hash to the fingerprint the operator
// carried. A wrong fingerprint fails the handshake, so nothing on either side
// processes a request.
func (c *Client) Fetch(ctx context.Context, url, want string) (*Bootstrap, error) {
	var b Bootstrap
	if err := c.call(ctx, c.pinnedTransport(want), http.MethodGet, url, "/bootstrap", nil, &b); err != nil {
		return nil, err
	}
	if b.Fingerprint != want {
		// The handshake already proved the key, so this can only be a gateway
		// disagreeing with itself. Refuse rather than reconcile.
		return nil, fmt.Errorf("gwpush: the gateway reports fingerprint %s but presented %s", b.Fingerprint, want)
	}
	if len(b.CSR) == 0 {
		return nil, errors.New("gwpush: the gateway offered no certificate request")
	}
	return &b, nil
}

// Adopt signs the gateway's request and installs the result, closing adoption.
//
// The identity is issued for UsageServer: a gateway serves the hub's pushes and
// the runners' long polls, and never dials. A certificate that could also
// authenticate a client would let a stolen gateway key speak to the hub as
// something the hub trusts.
func (c *Client) Adopt(ctx context.Context, url, fingerprint, gatewayID string, runnerCA []byte, csrDER []byte) (*pki.Issued, error) {
	issued, err := c.ca.Sign(csrDER, gatewayID, pki.UsageServer)
	if err != nil {
		return nil, err
	}
	body := Adoption{
		GatewayID:  gatewayID,
		CertPEM:    issued.CertPEM,
		GatewayCA:  c.ca.CAPEM(),
		RunnerCA:   runnerCA,
		HubSubject: HubSubject,
	}
	if err := c.call(ctx, c.pinnedTransport(fingerprint), http.MethodPost, url, "/adopt", body, nil); err != nil {
		return nil, err
	}
	return issued, nil
}

// Renew issues a fresh identity to an already-adopted gateway.
//
// This is push-only for the same reason everything else here is: the gateway
// cannot ask. An identity allowed to lapse before the hub replaced it would
// strand the gateway behind a certificate it has no way to renew — which is
// why the fingerprint is retained after adoption (ADR-0049 §6). If mutual TLS
// fails because the identity already expired, the hub falls back to the pinned
// dial and re-issues. Recovery, not a second trust model: the long-lived key
// was the anchor all along.
func (c *Client) Renew(ctx context.Context, url, fingerprint, gatewayID string, runnerCA []byte) (*pki.Issued, error) {
	var b Bootstrap
	err := c.call(ctx, c.mtlsTransport(gatewayID), http.MethodGet, url, "/csr", nil, &b)
	if err != nil {
		if !isTLSFailure(err) {
			return nil, err
		}
		// Expired or missing identity: fall back to the anchor.
		return c.reissuePinned(ctx, url, fingerprint, gatewayID, runnerCA)
	}
	issued, err := c.ca.Sign(b.CSR, gatewayID, pki.UsageServer)
	if err != nil {
		return nil, err
	}
	body := Adoption{GatewayID: gatewayID, CertPEM: issued.CertPEM,
		GatewayCA: c.ca.CAPEM(), RunnerCA: runnerCA, HubSubject: HubSubject}
	if err := c.call(ctx, c.mtlsTransport(gatewayID), http.MethodPost, url, "/identity", body, nil); err != nil {
		return nil, err
	}
	return issued, nil
}

func (c *Client) reissuePinned(ctx context.Context, url, fingerprint, gatewayID string, runnerCA []byte) (*pki.Issued, error) {
	b, err := c.Fetch(ctx, url, fingerprint)
	if err != nil {
		return nil, err
	}
	issued, err := c.ca.Sign(b.CSR, gatewayID, pki.UsageServer)
	if err != nil {
		return nil, err
	}
	body := Adoption{GatewayID: gatewayID, CertPEM: issued.CertPEM,
		GatewayCA: c.ca.CAPEM(), RunnerCA: runnerCA, HubSubject: HubSubject}
	if err := c.call(ctx, c.pinnedTransport(fingerprint), http.MethodPost, url, "/identity", body, nil); err != nil {
		return nil, err
	}
	return issued, nil
}

// Push delivers a whole configuration. Partial updates are deliberately not
// modelled anywhere in this path — a half-applied policy is worse than a stale
// one (see gateway/internal/config).
func (c *Client) Push(ctx context.Context, url, gatewayID string, cfg any) error {
	return c.call(ctx, c.mtlsTransport(gatewayID), http.MethodPost, url, "/config", cfg, nil)
}

// pinnedTransport verifies the gateway by public-key fingerprint alone.
func (c *Client) pinnedTransport(fingerprint string) *http.Transport {
	return &http.Transport{TLSClientConfig: &tls.Config{
		// #nosec G402 -- VerifyPeerCertificate pins the key; see Fetch.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: pki.VerifyFingerprint(fingerprint),
		// Both callbacks, deliberately: VerifyPeerCertificate is skipped on a
		// RESUMED session, so the pin would silently lapse on the second dial.
		VerifyConnection: pki.VerifyConnFingerprint(fingerprint),
		MinVersion:       tls.VersionTLS13,
	}}
}

// mtlsTransport verifies the gateway by the CA the hub controls AND by the
// identity the hub issued it, and presents the hub's own certificate in return.
func (c *Client) mtlsTransport(gatewayID string) *http.Transport {
	cfg := &tls.Config{
		// #nosec G402 -- VerifySubject chains to the gateway CA and pins the
		// common name; the default check wants a hostname a DMZ box lacks.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: pki.VerifySubject(c.ca.Pool(), gatewayID),
		// See pinnedTransport: a resumed session never reaches the callback
		// above, and an unpinned push would accept any gateway on this CA.
		VerifyConnection: pki.VerifyConnSubject(c.ca.Pool(), gatewayID),
		MinVersion:       tls.VersionTLS13,
		RootCAs:          c.ca.Pool(),
	}
	if c.hub != nil {
		cfg.Certificates = []tls.Certificate{*c.hub}
	}
	return &http.Transport{TLSClientConfig: cfg}
}

func (c *Client) call(ctx context.Context, tr *http.Transport, method, base, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("gwpush: encoding the request: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, body)
	if err != nil {
		return fmt.Errorf("gwpush: %w", err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := *c.http
	client.Transport = tr
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("gwpush: dialling the gateway: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		// Bounded, and never echoed anywhere a caller can see: a gateway's
		// error text is untrusted input.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("gwpush: the gateway answered %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	if out == nil {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return fmt.Errorf("gwpush: reading the gateway's response: %w", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("gwpush: decoding the gateway's response: %w", err)
	}
	return nil
}

// isTLSFailure reports whether an error is the handshake refusing, as opposed
// to the gateway being unreachable or answering badly.
//
// Only a handshake failure justifies the pinned fallback in Renew. Falling back
// on a timeout would turn every slow network into a re-issuance, and falling
// back on a 500 would let a gateway talk the hub out of mutual TLS by
// answering badly enough.
func isTLSFailure(err error) bool {
	var ce *tls.CertificateVerificationError
	if errors.As(err, &ce) {
		return true
	}
	var re *tls.RecordHeaderError
	if errors.As(err, &re) {
		return true
	}
	var ua x509.UnknownAuthorityError
	var ie x509.CertificateInvalidError
	return errors.As(err, &ua) || errors.As(err, &ie)
}
