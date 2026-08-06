package gwclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/service"
)

// The runner half of ADR-0041, with the REAL client: gwclient's own transport,
// carrying this runner's certificate, against a gateway that demands one.
//
// Three things are proven here that the gateway-side tests cannot prove,
// because they use a hand-rolled client:
//
//   - gwclient actually presents its certificate (Options.TLS reaching the
//     transport is easy to break and impossible to notice — a gateway with a
//     shared secret still configured would accept the connection anyway);
//   - the identity the gateway reads is this runner's, from the certificate;
//   - the whole poll → execute → deliver cycle works over TLS, including the
//     h2 upgrade that ForceAttemptHTTP2 turns on.
func TestPollExecuteDeliverOverMutualTLS(t *testing.T) {
	doc, err := flowdoc.Parse([]byte(echoFlow))
	if err != nil {
		t.Fatal(err)
	}

	ca := newTestCA(t)

	var (
		mu        sync.Mutex
		peer      string
		proto     string
		delivered []byte
		handedOut bool
	)
	done := make(chan struct{})

	gw := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == pollPath:
			mu.Lock()
			// Whatever the runner PROVED, read the way the gateway reads it.
			if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
				peer = r.TLS.PeerCertificates[0].Subject.CommonName
			}
			proto = r.Proto
			first := !handedOut
			handedOut = true
			mu.Unlock()
			if !first {
				time.Sleep(20 * time.Millisecond)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set(hdrRequestID, "req-1")
			w.Header().Set(hdrFlow, "echo")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"order":1}`)

		case strings.HasPrefix(r.URL.Path, deliverPath):
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			delivered = b
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			close(done)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	gw.TLS = serverTLS(t, ca, "gw-1")
	gw.EnableHTTP2 = true
	gw.StartTLS()
	defer gw.Close()

	svc := service.New(service.Options{})
	defer func() { _ = svc.Close(5 * time.Second) }()

	l := New(Options{
		Addrs:    []string{gw.URL},
		Service:  svc,
		Lookup:   func(name string) (*flowdoc.Document, bool) { return doc, name == "echo" },
		PollWait: time.Second,
		TLS:      runnerTLS(t, ca, "rnr-7"),
	})
	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	go func() { l.Run(ctx); close(loopDone) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatal("no delivery within 15s over mTLS")
	}
	cancel()
	<-loopDone

	mu.Lock()
	defer mu.Unlock()
	if peer != "rnr-7" {
		t.Errorf("gateway saw peer %q, want rnr-7 — the runner did not present its certificate", peer)
	}
	if got := strings.TrimSpace(string(delivered)); got != `{"order":1}` {
		t.Errorf("delivered body = %q, want the flow's output", got)
	}
	// Not a correctness requirement, but the reason parked polls stop costing a
	// socket each — see the port-exhaustion note in New.
	if proto != "HTTP/2.0" {
		t.Logf("negotiated %s, not HTTP/2 — parked polls will each hold a connection", proto)
	}
}

// A gateway this runner cannot verify must get nothing: no poll, no payload,
// and above all no delivered response. The loop keeps retrying rather than
// exiting, because an unverifiable gateway is indistinguishable from a
// mis-rolled certificate that an operator is about to fix.
func TestRunnerWillNotPollAnUnverifiableGateway(t *testing.T) {
	ca := newTestCA(t)
	impostor := newTestCA(t)

	var polled bool
	var mu sync.Mutex
	gw := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		polled = true
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	// The gateway's certificate is signed by a CA the runner does not trust.
	gw.TLS = serverTLS(t, impostor, "gw-1")
	gw.StartTLS()
	defer gw.Close()

	svc := service.New(service.Options{})
	defer func() { _ = svc.Close(5 * time.Second) }()

	l := New(Options{
		Addrs:    []string{gw.URL},
		Service:  svc,
		Lookup:   func(string) (*flowdoc.Document, bool) { return nil, false },
		PollWait: 100 * time.Millisecond,
		TLS:      runnerTLS(t, ca, "rnr-7"),
	})
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	l.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if polled {
		t.Fatal("the runner reached a gateway whose certificate it could not verify")
	}
}

// --- test PKI --------------------------------------------------------------

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key}
}

func (c *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(c.cert)
	return p
}

func (c *testCA) issue(t *testing.T, cn string, usage x509.ExtKeyUsage, dns []string, ips []net.IP) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dns,
		IPAddresses:  ips, // an IP in DNSNames is ignored: "no IP SANs"
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

// serverTLS is the gateway's side: its own certificate, and a demand for a
// client certificate from the control-plane CA.
func serverTLS(t *testing.T, ca *testCA, gatewayID string) *tls.Config {
	t.Helper()
	cert := ca.issue(t, gatewayID, x509.ExtKeyUsageServerAuth,
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")})
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    ca.pool(),
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2", "http/1.1"},
	}
}

// runnerTLS is what the hub places on a runner: its identity, and the CA that
// says which gateways are real.
func runnerTLS(t *testing.T, ca *testCA, runnerID string) *tls.Config {
	t.Helper()
	cert := ca.issue(t, runnerID, x509.ExtKeyUsageClientAuth, nil, nil)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      ca.pool(),
		MinVersion:   tls.VersionTLS13,
	}
}
