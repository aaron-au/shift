package gwclient

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A hub-issued gateway certificate carries NO subject alternative name — a DMZ
// box has no stable hostname at issue time — so hostname verification has
// nothing to match. The runner pins the gateway's hub-assigned id from the same
// answer that gave it the address.
//
// That pin is the only thing between "this gateway" and "anything holding a
// control-plane certificate", which includes every other runner in the fleet:
// one compromised runner could otherwise stand up a listener, be dialled as a
// gateway, and be handed inbound payload to answer.
func TestAGatewayIsVerifiedByTheIdentityTheHubNamed(t *testing.T) {
	ca := newTestCA(t)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	// No SANs at all — exactly what hub/internal/pki issues.
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{ca.issue(t, "gw-real", x509.ExtKeyUsageServerAuth, nil, nil)},
		MinVersion:   tls.VersionTLS13,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "https://")

	dial := func(expect string) error {
		p := newPins()
		p.set(srv.URL, expect)
		conn, err := dialPinned(runnerTLS(t, ca, "rnr-1"), p)(t.Context(), "tcp", addr)
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}

	if err := dial("gw-real"); err != nil {
		t.Fatalf("a gateway presenting the identity the hub named was refused: %v", err)
	}

	err := dial("gw-something-else")
	if err == nil {
		t.Fatal("a control-plane certificate was accepted for a gateway it does not identify")
	}
	if !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("refused, but for the wrong reason: %v", err)
	}
}

// An address the hub never named falls back to STANDARD verification rather
// than to trust. It is a different assertion — the operator's own -gateways
// flag, checked against DNS and the certificate's SANs — and what neither path
// permits is accepting a peer on chain validity alone.
func TestAnUnpinnedAddressStillVerifiesTheHostname(t *testing.T) {
	ca := newTestCA(t)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{ca.issue(t, "gw-real", x509.ExtKeyUsageServerAuth, nil, nil)},
		MinVersion:   tls.VersionTLS13,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "https://")
	conn, err := dialPinned(runnerTLS(t, ca, "rnr-1"), newPins())(t.Context(), "tcp", addr)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("a SAN-less certificate was accepted for an address the hub never named")
	}
	// Either wording, depending on whether the address was a name or a
	// literal; both are the standard hostname check doing its job.
	if !strings.Contains(err.Error(), "not valid for any names") &&
		!strings.Contains(err.Error(), "doesn't contain any IP SANs") {
		t.Fatalf("refused, but for the wrong reason: %v", err)
	}
}

// The pin is keyed by HOST, so a gateway is verified the same way whether the
// hub named it by DNS name or by address.
func TestThePinSurvivesTheURLForm(t *testing.T) {
	p := newPins()
	p.set("https://gw-0.example:8444/", "gw-a")
	if got, ok := p.get("gw-0.example"); !ok || got != "gw-a" {
		t.Fatalf("get = %q, %v", got, ok)
	}
	if _, ok := p.get(net.JoinHostPort("gw-0.example", "8444")); ok {
		t.Fatal("the pin matched a host:port key; the dialler looks it up by host alone")
	}
}
