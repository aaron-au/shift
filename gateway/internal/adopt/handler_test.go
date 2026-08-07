package adopt_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/adopt"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// peer synthesises the verified connection state the TLS layer would produce.
func peer(t *testing.T, c *ca, cn string) *tls.ConnectionState {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(c.signKey(t, &key.PublicKey, cn, time.Now().Add(time.Hour)))
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf, c.cert}}}
}

type harness struct {
	state   *adopt.State
	mux     *http.ServeMux
	applied []byte
	reject  bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	s, err := adopt.Open(t.TempDir(), testToken)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{state: s, mux: http.NewServeMux()}
	adopt.Handler(h.mux, s, "test", func(raw []byte) error {
		if h.reject {
			return io.ErrUnexpectedEOF
		}
		h.applied = raw
		return nil
	}, quietLogger())
	return h
}

func (h *harness) do(t *testing.T, method, path string, body any, cs *tls.ConnectionState) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequestWithContext(t.Context(), method, path, rd)
	req.TLS = cs
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, req)
	return w
}

// hubPair runs the hub's half of a pairing.
func (h *harness) hubPair(t *testing.T, token string) *adopt.Hello {
	t.Helper()
	w := h.do(t, http.MethodPost, "/pair", adopt.Challenge{
		Nonce: nonce,
		Proof: adopt.Proof(token, "shift-gw-hub-hello", nonce, h.state.Fingerprint(), nil),
	}, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("pair = %d (%s), want 200", w.Code, w.Body.String())
	}
	var hello adopt.Hello
	if err := json.Unmarshal(w.Body.Bytes(), &hello); err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	return &hello
}

// hubAdopt completes a pairing with signed material.
func (h *harness) hubAdopt(t *testing.T, token string, a adopt.Adoption) *httptest.ResponseRecorder {
	t.Helper()
	a.Nonce = nonce
	a.Proof = adopt.Proof(token, "shift-gw-install", nonce, h.state.Fingerprint(), adopt.MaterialDigest(a))
	return h.do(t, http.MethodPost, "/adopt", a, nil)
}

// pairAndAdopt drives a complete, honest pairing and returns the CAs used.
func (h *harness) pairAndAdopt(t *testing.T) (gwCA, rnCA *ca) {
	t.Helper()
	gwCA, rnCA = newCA(t, "gateway ca"), newCA(t, "runner ca")
	hello := h.hubPair(t, testToken)
	cert := gwCA.sign(t, hello.CSR, "gw-1", time.Now().Add(time.Hour))
	if w := h.hubAdopt(t, testToken, adopt.Adoption{
		GatewayID: "gw-1", CertPEM: cert,
		GatewayCA: gwCA.pem, RunnerCA: rnCA.pem, HubSubject: "hub",
	}); w.Code != http.StatusNoContent {
		t.Fatalf("adopt = %d (%s), want 204", w.Code, w.Body.String())
	}
	return gwCA, rnCA
}

// The pairing endpoints answer only while unadopted, and only to the token.
func TestPairingIsOpenBeforeAdoptionAndClosedAfter(t *testing.T) {
	h := newHarness(t)

	hello := h.hubPair(t, testToken)
	if hello.Fingerprint != h.state.Fingerprint() || len(hello.CSR) == 0 {
		t.Fatal("the gateway did not answer with its fingerprint and a certificate request")
	}
	// The gateway's proof is what tells the hub it reached the real gateway
	// rather than something answering on its behalf.
	if hello.Proof == "" {
		t.Fatal("the gateway offered no proof, so the hub could not tell it from an interceptor")
	}

	h.pairAndAdopt(t)

	// Closed. Leaving them open would keep offering a certificate request for a
	// gateway that already has an owner.
	if w := h.do(t, http.MethodPost, "/pair", adopt.Challenge{Nonce: nonce, Proof: "x"}, nil); w.Code != http.StatusForbidden {
		t.Fatalf("pair after adoption = %d, want 403", w.Code)
	}
	if w := h.do(t, http.MethodPost, "/adopt", adopt.Adoption{Nonce: nonce}, nil); w.Code != http.StatusConflict {
		t.Fatalf("second adoption = %d, want 409", w.Code)
	}
}

// Without the token, pairing is a race won by whoever reaches the port first.
func TestPairingWithoutTheTokenIsRefused(t *testing.T) {
	h := newHarness(t)
	attacker := newCA(t, "attacker ca")
	fp := h.state.Fingerprint()

	cases := []adopt.Challenge{
		{Nonce: nonce},
		{Nonce: nonce, Proof: "not-a-proof"},
		{Nonce: nonce, Proof: adopt.Proof("sgt_wrong", "shift-gw-hub-hello", nonce, fp, nil)},
		// An interceptor computes its proof over ITS OWN key, not the
		// gateway's — which is exactly what the binding catches.
		{Nonce: nonce, Proof: adopt.Proof(testToken, "shift-gw-hub-hello", nonce, strings.Repeat("00", 32), nil)},
	}
	for i, c := range cases {
		if w := h.do(t, http.MethodPost, "/pair", c, nil); w.Code != http.StatusForbidden {
			t.Fatalf("case %d: pair = %d, want 403", i, w.Code)
		}
	}

	// Adoption cannot be forced without a matching install proof either.
	w := h.hubAdopt(t, "sgt_wrong", adopt.Adoption{
		GatewayID: "gw-evil", CertPEM: []byte("x"), GatewayCA: attacker.pem, HubSubject: "hub",
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("adopt with the wrong token = %d, want 403", w.Code)
	}
	if h.state.Adopted() {
		t.Fatal("a gateway was adopted by a caller that did not hold the install token")
	}
}

func TestPairingRejectsMalformedBodies(t *testing.T) {
	h := newHarness(t)

	for _, path := range []string{"/pair", "/adopt"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader("{not json"))
		w := httptest.NewRecorder()
		h.mux.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("malformed body to %s = %d, want 400", path, w.Code)
		}
	}

	// A correct proof over material that cannot be installed.
	if w := h.hubAdopt(t, testToken, adopt.Adoption{
		GatewayID: "gw-1", CertPEM: []byte("not pem"), GatewayCA: []byte("x"),
	}); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("uninstallable adoption = %d, want 422", w.Code)
	}
}

// The hub-only endpoints are gated on adoption AND on the issuer of the
// caller's certificate.
func TestTheHubEndpointsRefuseEveryoneElse(t *testing.T) {
	h := newHarness(t)

	// Before adoption they answer to nobody.
	if w := h.do(t, http.MethodGet, "/csr", nil, nil); w.Code != http.StatusForbidden {
		t.Fatalf("csr before adoption = %d, want 403", w.Code)
	}
	if w := h.do(t, http.MethodPost, "/config", nil, nil); w.Code != http.StatusForbidden {
		t.Fatalf("config before adoption = %d, want 403", w.Code)
	}

	gwCA, rnCA := h.pairAndAdopt(t)
	hubPeer := peer(t, gwCA, "hub")

	if w := h.do(t, http.MethodGet, "/csr", nil, hubPeer); w.Code != http.StatusOK {
		t.Fatalf("csr as the hub = %d, want 200", w.Code)
	}
	if w := h.do(t, http.MethodPost, "/config", map[string]int{"version": 4}, hubPeer); w.Code != http.StatusNoContent {
		t.Fatalf("config as the hub = %d, want 204", w.Code)
	}
	if !bytes.Contains(h.applied, []byte(`"version":4`)) {
		t.Fatalf("applied %q, want the pushed configuration", h.applied)
	}

	// A RUNNER — verified, on a trusted CA — must not push configuration, even
	// when its certificate is named "hub". Both CAs are trusted on this
	// listener, so a name check would be no check at all.
	for _, cn := range []string{"runner-7", "hub"} {
		p := peer(t, rnCA, cn)
		if w := h.do(t, http.MethodPost, "/config", map[string]int{"version": 9}, p); w.Code != http.StatusForbidden {
			t.Fatalf("config as a runner named %q = %d, want 403", cn, w.Code)
		}
		if w := h.do(t, http.MethodGet, "/csr", nil, p); w.Code != http.StatusForbidden {
			t.Fatalf("csr as a runner named %q = %d, want 403", cn, w.Code)
		}
		if w := h.do(t, http.MethodPost, "/identity", adopt.Adoption{GatewayID: "gw-1"}, p); w.Code != http.StatusForbidden {
			t.Fatalf("identity as a runner named %q = %d, want 403", cn, w.Code)
		}
	}
	if bytes.Contains(h.applied, []byte(`"version":9`)) {
		t.Fatal("a runner's configuration was applied")
	}

	// A rejected configuration is a rejection, not a silent acknowledgement:
	// the hub must not believe it converged.
	h.reject = true
	if w := h.do(t, http.MethodPost, "/config", map[string]int{"version": 5}, hubPeer); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("rejected configuration = %d, want 422", w.Code)
	}
}

// Renewal needs a verified hub certificate — including on the recovery path,
// where the gateway serves its anchor because its identity lapsed. It can still
// verify the hub with the CA it kept from adoption, which is why the install
// token can be burned.
func TestRenewalRequiresAVerifiedHub(t *testing.T) {
	h := newHarness(t)
	gwCA, _ := h.pairAndAdopt(t)
	hubPeer := peer(t, gwCA, "hub")

	w := h.do(t, http.MethodGet, "/csr", nil, hubPeer)
	var hello adopt.Hello
	if err := json.Unmarshal(w.Body.Bytes(), &hello); err != nil {
		t.Fatalf("decode: %v", err)
	}
	renewed := gwCA.sign(t, hello.CSR, "gw-1", time.Now().Add(72*time.Hour))
	body := adopt.Adoption{GatewayID: "gw-1", CertPEM: renewed, GatewayCA: gwCA.pem, HubSubject: "hub"}

	// No certificate at all — an anchor-served connection with nobody verified.
	if w := h.do(t, http.MethodPost, "/identity", body, nil); w.Code != http.StatusForbidden {
		t.Fatalf("identity with no hub certificate = %d, want 403", w.Code)
	}
	if w := h.do(t, http.MethodPost, "/identity", body, hubPeer); w.Code != http.StatusNoContent {
		t.Fatalf("identity as the hub = %d (%s), want 204", w.Code, w.Body.String())
	}
	if !h.state.NotAfter().After(time.Now().Add(48 * time.Hour)) {
		t.Fatal("the renewed identity was not installed")
	}
}

// A gateway wired with no applier says so rather than acknowledging a push it
// cannot honour.
func TestConfigWithoutAnApplierIsNotImplemented(t *testing.T) {
	s, err := adopt.Open(t.TempDir(), testToken)
	if err != nil {
		t.Fatal(err)
	}
	gwCA, rnCA := newCA(t, "gateway ca"), newCA(t, "runner ca")
	mux := http.NewServeMux()
	adopt.Handler(mux, s, "test", nil, quietLogger())

	hello := pair(t, s, testToken)
	cert := gwCA.sign(t, hello.CSR, "gw-1", time.Now().Add(time.Hour))
	if err := install(t, s, testToken, adopt.Adoption{
		GatewayID: "gw-1", CertPEM: cert,
		GatewayCA: gwCA.pem, RunnerCA: rnCA.pem, HubSubject: "hub",
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/config", strings.NewReader("{}"))
	req.TLS = peer(t, gwCA, "hub")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("config with no applier = %d, want 501", w.Code)
	}
}
