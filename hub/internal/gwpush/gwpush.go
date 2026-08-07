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
//   - PAIRING has no key to pin — the gateway generated one after the hub last
//     looked. So the hub accepts what answers, LEARNS its fingerprint, and
//     proves the exchange with a one-time install token instead. Both proofs
//     are HMACs bound to the fingerprint on the wire, so an interceptor that
//     terminates TLS with its own key fails both checks.
//   - EVERY DIAL AFTER pins the learned fingerprint, or the gateway CA plus the
//     identity the hub issued.
//
// The hub presents its own client certificate on every dial after pairing, so
// the gateway can verify who is pushing policy at it.
package gwpush

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
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

// Adoption is what the hub delivers to complete a pairing, or to renew.
type Adoption struct {
	// Nonce and Proof carry the hub's half of the pairing. The proof covers a
	// digest of the material below, so a captured proof cannot be replayed with
	// a substituted CA. Both are empty on a renewal over mutual TLS, where the
	// hub's client certificate is the proof.
	Nonce      string `json:"nonce,omitempty"`
	Proof      string `json:"proof,omitempty"`
	GatewayID  string `json:"gateway_id"`
	CertPEM    []byte `json:"cert_pem"`
	GatewayCA  []byte `json:"gateway_ca_pem"` // verifies the hub, and the gateway to runners
	RunnerCA   []byte `json:"runner_ca_pem"`  // verifies runners polling for work
	HubSubject string `json:"hub_subject"`    // who the gateway must see on a push
}

// Hello is a gateway's answer to a pairing challenge.
type Hello struct {
	Fingerprint string `json:"fingerprint"`
	CSR         []byte `json:"csr"`
	Proof       string `json:"proof"`
	Version     string `json:"version,omitempty"`
}

// Challenge is the hub's opening move in a pairing.
type Challenge struct {
	Nonce string `json:"nonce"`
	Proof string `json:"proof"`
}

// Domain separators for the pairing proofs. Distinct strings stop a proof
// minted for one step satisfying another.
const (
	DomainHubHello = "shift-gw-hub-hello"
	DomainGWHello  = "shift-gw-gw-hello"
	DomainInstall  = "shift-gw-install"
)

// Proof computes an HMAC over a domain, a nonce, a certificate fingerprint and
// an optional payload digest.
//
// DUPLICATED from gateway/internal/adopt on purpose, for the reason ADR-0046 §2
// gives for duplicating the logging setup: gateway/go.mod has zero
// dependencies, which is an auditable security property of the one component
// that may sit in a DMZ, and the hub cannot import from it without breaking
// that.
//
// Duplicated crypto that silently disagrees would be far worse than the
// dependency, so TestTheProofConstructionMatchesTheGateway pins both
// implementations to one fixed vector.
func Proof(token, domain, nonce, fingerprint string, digest []byte) string {
	m := hmac.New(sha256.New, []byte(token))
	for _, part := range [][]byte{[]byte(domain), []byte(nonce), []byte(fingerprint), digest} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		m.Write(n[:])
		m.Write(part)
	}
	return hex.EncodeToString(m.Sum(nil))
}

// MaterialDigest is the digest the install proof commits to, so a captured
// proof cannot be replayed with substituted material.
func MaterialDigest(a Adoption) []byte {
	h := sha256.New()
	for _, part := range [][]byte{
		[]byte(a.GatewayID), a.CertPEM, a.GatewayCA, a.RunnerCA, []byte(a.HubSubject),
	} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		h.Write(n[:])
		h.Write(part)
	}
	return h.Sum(nil)
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

// Pair adopts an unadopted gateway using a one-time install token
// (ADR-0049 §1a), and returns the fingerprint it learned along with the issued
// identity.
//
// InsecureSkipVerify here means "not the DEFAULT verification", not "no
// verification". There is genuinely nothing to verify against yet — the
// gateway's key did not exist when the record was made — so the transport is
// used only to learn a fingerprint, and the HMAC exchange over it does the
// authenticating in both directions.
func (c *Client) Pair(ctx context.Context, url, token, gatewayID string, runnerCA []byte) (issued *pki.Issued, fingerprint string, err error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, "", fmt.Errorf("gwpush: nonce: %w", err)
	}
	n := hex.EncodeToString(nonce[:])

	// Learn the key the gateway is terminating TLS with. Everything below is
	// bound to it, so an interceptor's key produces proofs neither side accepts.
	var seen string
	tr := &http.Transport{TLSClientConfig: &tls.Config{
		// #nosec G402 -- see the doc comment: nothing exists to verify against
		// yet, and the HMAC exchange bound to this fingerprint is the check.
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("gwpush: the gateway presented no certificate")
			}
			fp, err := pki.Fingerprint(cs.PeerCertificates[0].PublicKey)
			if err != nil {
				return err
			}
			if seen != "" && seen != fp {
				// Every request in this pairing must land on the same key, or
				// the proofs are being computed against two different peers.
				return errors.New("gwpush: the gateway changed keys mid-pairing")
			}
			seen = fp
			return nil
		},
	}}

	var hello Hello
	challenge := Challenge{Nonce: n}
	// The proof cannot be computed until the handshake reveals the key, so the
	// call is made in two steps against the same transport: an empty probe to
	// learn the fingerprint, then the real challenge.
	if err := c.call(ctx, tr, http.MethodPost, url, "/pair", Challenge{Nonce: n}, &hello); err != nil && seen == "" {
		return nil, "", err
	}
	if seen == "" {
		return nil, "", errors.New("gwpush: the gateway's key was never observed")
	}
	challenge.Proof = Proof(token, DomainHubHello, n, seen, nil)
	if err := c.call(ctx, tr, http.MethodPost, url, "/pair", challenge, &hello); err != nil {
		return nil, "", err
	}
	if hello.Fingerprint != seen {
		return nil, "", fmt.Errorf("gwpush: the gateway reports fingerprint %s but presented %s", hello.Fingerprint, seen)
	}
	if len(hello.CSR) == 0 {
		return nil, "", errors.New("gwpush: the gateway offered no certificate request")
	}
	sum := sha256.Sum256(hello.CSR)
	if subtle.ConstantTimeCompare([]byte(hello.Proof),
		[]byte(Proof(token, DomainGWHello, n, seen, sum[:]))) != 1 {
		// Either the gateway does not hold this token, or something in the
		// middle is answering for it.
		return nil, "", errors.New("gwpush: the gateway did not prove it holds the install token")
	}

	issued, err = c.ca.Sign(hello.CSR, gatewayID, pki.UsageServer)
	if err != nil {
		return nil, "", err
	}
	body := Adoption{
		Nonce: n, GatewayID: gatewayID, CertPEM: issued.CertPEM,
		GatewayCA: c.ca.CAPEM(), RunnerCA: runnerCA, HubSubject: HubSubject,
	}
	body.Proof = Proof(token, DomainInstall, n, seen, MaterialDigest(body))
	if err := c.call(ctx, tr, http.MethodPost, url, "/adopt", body, nil); err != nil {
		return nil, "", err
	}
	return issued, seen, nil
}

// Renew issues a fresh identity to an already-adopted gateway.
//
// Push-only, for the same reason everything here is: the gateway cannot ask. An
// identity allowed to lapse before the hub replaced it would strand the gateway
// — which is why the learned fingerprint is RETAINED after adoption
// (ADR-0049 §6). If mutual TLS fails because the identity already expired, the
// hub falls back to the pinned fingerprint and re-issues. The gateway still
// verifies the hub's client certificate against the CA it kept from adoption,
// so nothing is unauthenticated on that path and no install token is needed.
func (c *Client) Renew(ctx context.Context, url, fingerprint, gatewayID string, runnerCA []byte) (*pki.Issued, error) {
	tr := c.mtlsTransport(gatewayID)
	var hello Hello
	if err := c.call(ctx, tr, http.MethodGet, url, "/csr", nil, &hello); err != nil {
		if !isTLSFailure(err) {
			return nil, err
		}
		// Expired identity: the gateway is serving its anchor, so pin that.
		tr = c.pinnedTransport(fingerprint)
		if err := c.call(ctx, tr, http.MethodGet, url, "/csr", nil, &hello); err != nil {
			return nil, err
		}
	}
	issued, err := c.ca.Sign(hello.CSR, gatewayID, pki.UsageServer)
	if err != nil {
		return nil, err
	}
	body := Adoption{GatewayID: gatewayID, CertPEM: issued.CertPEM,
		GatewayCA: c.ca.CAPEM(), RunnerCA: runnerCA, HubSubject: HubSubject}
	if err := c.call(ctx, tr, http.MethodPost, url, "/identity", body, nil); err != nil {
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
