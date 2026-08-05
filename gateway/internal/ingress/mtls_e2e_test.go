package ingress_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/identity"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// ADR-0041 end to end, with NO stand-ins: a real TLS control listener built
// from a real identity bundle, real client certificates, and the default
// peer-id path (the certificate subject) rather than an injected function.
//
// The other tests in this package inject WithPeerID, which proves the ROSTER
// logic but leaves the join between "who TLS says you are" and "what the hub
// says you are" untested — and that join is the whole security property. If
// tlsPeerID read the wrong field, or the server config forgot to require a
// client certificate, every one of those tests would still pass.
func TestMutualTLSEndToEnd(t *testing.T) {
	ca := newTestCA(t)
	gwDir := t.TempDir()
	writeGatewayBundle(t, gwDir, ca, "gw-1")

	bundle, err := identity.Load(gwDir)
	if err != nil {
		t.Fatal(err)
	}

	// The hub's roster: rnr-prod is production, rnr-dev is not. Neither runner
	// is told this, and neither can influence it.
	cfg := &config.Config{
		Version: 7,
		Routes: []config.Route{{
			Path: "/orders", Method: http.MethodPost, Flow: "ingest",
			Selector: config.Selector{"environment": "production"}, AuthPrincipal: "acme-erp",
		}},
		Runners: []config.Runner{
			{ID: "rnr-prod", Labels: map[string]string{"environment": "production"}},
			{ID: "rnr-dev", Labels: map[string]string{"environment": "development"}},
		},
	}

	reg := runners.New()
	pub := ingress.New(reg, nil)
	if err := pub.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	// No WithPeerID: the identity comes from the certificate, through the same
	// code path gatewayd uses.
	ingress.NewDispatch(reg, nil, "").WithLabels(cfg.LabelsFor).Routes(mux)

	ctrl := httptest.NewUnstartedServer(mux)
	ctrl.TLS = bundle.ServerTLS()
	ctrl.StartTLS()
	defer ctrl.Close()

	public := httptest.NewServer(pub)
	defer public.Close()

	t.Run("a rostered production runner is identified by its certificate and served", func(t *testing.T) {
		cl := runnerClient(t, ca, "rnr-prod")
		done := make(chan error, 1)
		go func() { done <- pollAndDeliver(t, cl, ctrl.URL, `{"ok":true}`) }()
		waitParked(t, reg)

		resp, err := postTo(t, public.URL+"/orders", `{"order":1}`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)

		if err := <-done; err != nil {
			t.Fatalf("runner: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		if got := string(body); got != `{"ok":true}` {
			t.Errorf("body = %q, want the runner's output", got)
		}
	})

	t.Run("a valid certificate the hub never vouched for is refused", func(t *testing.T) {
		// The certificate verifies: same CA, same key usage, not expired. It is
		// refused purely because the hub's roster does not name it — a trusted
		// PKI is not by itself an authorization to serve traffic.
		cl := runnerClient(t, ca, "rnr-ghost")
		code, err := pollStatus(t, cl, ctrl.URL)
		if err != nil {
			t.Fatal(err)
		}
		if code != http.StatusForbidden {
			t.Errorf("poll status = %d, want 403 for an unrostered identity", code)
		}
		if n := reg.Parked(); n != 0 {
			t.Errorf("parked = %d, want 0 — a refused runner must not be waiting for work", n)
		}
	})

	t.Run("a runner cannot borrow another runner's placement", func(t *testing.T) {
		// rnr-dev parks successfully — it IS a known runner — but the roster
		// puts it in development, so the production route must not reach it.
		// Before ADR-0041 this runner could simply have claimed production in
		// its poll body, and nothing in the system could have contradicted it.
		cl := runnerClient(t, ca, "rnr-dev")
		parked := make(chan struct{})
		go func() {
			defer close(parked)
			_, _ = pollStatus(t, cl, ctrl.URL)
		}()
		waitParked(t, reg)

		resp, err := postTo(t, public.URL+"/orders", `{"order":2}`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503 — a development runner served a production route", resp.StatusCode)
		}
		<-parked
	})
}

// A runner that cannot verify the gateway's certificate must refuse to talk to
// it. This is the direction the shared secret never covered: a runner handing
// inbound payload to whatever answered on that address is a payload
// interception, and it is silent.
func TestRunnerRefusesAnUnverifiableGateway(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	writeGatewayBundle(t, dir, ca, "gw-1")
	bundle, err := identity.Load(dir)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	ingress.NewDispatch(runners.New(), nil, "").Routes(mux)
	ctrl := httptest.NewUnstartedServer(mux)
	ctrl.TLS = bundle.ServerTLS()
	ctrl.StartTLS()
	defer ctrl.Close()

	// A runner holding a valid client certificate but trusting a different CA.
	impostorCA := newTestCA(t)
	cl := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{ca.clientCert(t, "rnr-prod")},
		RootCAs:      impostorCA.pool(),
		MinVersion:   tls.VersionTLS13,
	}}}
	if _, err := pollStatus(t, cl, ctrl.URL); err == nil {
		t.Fatal("the runner polled a gateway whose certificate it could not verify")
	}
}

// --- runner side -----------------------------------------------------------

// runnerClient is a runner's HTTP client: its own client certificate, and the
// control-plane CA as the only trust root.
func runnerClient(t *testing.T, ca *testCA, runnerID string) *http.Client {
	t.Helper()
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{ca.clientCert(t, runnerID)},
		RootCAs:      ca.pool(),
		MinVersion:   tls.VersionTLS13,
	}}}
}

// pollStatus performs one poll and returns its status code, discarding any
// work. Used where the poll's OUTCOME is the assertion.
func pollStatus(t *testing.T, cl *http.Client, ctrlURL string) (int, error) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"wait_seconds": 2})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ctrlURL+"/api/v1/gw/poll", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cl.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// pollAndDeliver is the full runner cycle over mTLS: park, take the work,
// answer it.
func pollAndDeliver(t *testing.T, cl *http.Client, ctrlURL, out string) error {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"wait_seconds": 5})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ctrlURL+"/api/v1/gw/poll", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return errf("poll status = %d, want 200", resp.StatusCode)
	}
	id := resp.Header.Get("X-Shift-Request-Id")
	if id == "" {
		return errf("no request id on the poll response")
	}
	_, _ = io.Copy(io.Discard, resp.Body)

	dreq, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ctrlURL+"/api/v1/gw/deliver/"+id, strings.NewReader(out))
	if err != nil {
		return err
	}
	dreq.Header.Set("Content-Type", "application/json")
	dresp, err := cl.Do(dreq)
	if err != nil {
		return err
	}
	defer func() { _ = dresp.Body.Close() }()
	if dresp.StatusCode != http.StatusNoContent {
		return errf("deliver status = %d, want 204", dresp.StatusCode)
	}
	return nil
}

func postTo(t *testing.T, url, body string) (*http.Response, error) {
	t.Helper()
	return post(t, url, strings.NewReader(body))
}

// --- test CA ---------------------------------------------------------------
//
// A throwaway control-plane CA. When the hub gains real issuance (ADR-0041 §2)
// this moves to a shared package and these tests use that, so the thing under
// test is the code that ships rather than a parallel implementation.

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func newTestCA(tb testing.TB) *testCA {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "SHIFT test control-plane CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		tb.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		tb.Fatal(err)
	}
	return &testCA{cert: cert, key: key, der: der}
}

func (c *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(c.cert)
	return p
}

func (c *testCA) issue(tb testing.TB, cn string, usage x509.ExtKeyUsage, dns []string, ips []net.IP) tls.Certificate {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dns,
		// IPs belong in IPAddresses; an IP left in DNSNames is ignored by
		// verification, which then fails with "no IP SANs".
		IPAddresses: ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		tb.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		tb.Fatal(err)
	}
	pair, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		tb.Fatal(err)
	}
	return pair
}

// clientCert issues a runner identity. The runner id IS the subject common
// name — the one field the runner cannot choose for itself.
func (c *testCA) clientCert(tb testing.TB, runnerID string) tls.Certificate {
	tb.Helper()
	return c.issue(tb, runnerID, x509.ExtKeyUsageClientAuth, nil, nil)
}

func writeGatewayBundle(tb testing.TB, dir string, ca *testCA, gatewayID string) {
	tb.Helper()
	srv := ca.issue(tb, gatewayID, x509.ExtKeyUsageServerAuth,
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")})
	key, err := x509.MarshalECPrivateKey(srv.PrivateKey.(*ecdsa.PrivateKey))
	if err != nil {
		tb.Fatal(err)
	}
	files := map[string][]byte{
		identity.CertFile: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate[0]}),
		identity.KeyFile:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: key}),
		identity.CAFile:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.der}),
		identity.IDFile:   []byte(gatewayID + "\n"),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			tb.Fatal(err)
		}
	}
}
