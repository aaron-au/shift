package api_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/pki"
	"github.com/aaron-au/shift/hub/internal/store"
)

// The gateway realm is administrator-facing only (ADR-0049). Every assertion
// here is about what an operator can and cannot get wrong with a paste.

const goodFingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestRecordingAGatewayValidatesThePaste(t *testing.T) {
	srv := newServer(t)

	cases := []struct {
		name, body string
		want       int
	}{
		{"valid", `{"name":"dmz","url":"https://gw.example:8444","fingerprint":"` + goodFingerprint + `"}`, http.StatusCreated},
		{"no name", `{"url":"https://gw.example","fingerprint":"` + goodFingerprint + `"}`, http.StatusUnprocessableEntity},
		{"no url", `{"name":"x","fingerprint":"` + goodFingerprint + `"}`, http.StatusUnprocessableEntity},
		// Adoption pins a key inside the TLS handshake; over plaintext there is
		// no handshake to pin and the trust argument evaporates.
		{"plaintext url", `{"name":"x","url":"http://gw.example","fingerprint":"` + goodFingerprint + `"}`, http.StatusUnprocessableEntity},
		{"truncated fingerprint", `{"name":"x","url":"https://gw.example","fingerprint":"abcd"}`, http.StatusUnprocessableEntity},
		{"non-hex fingerprint", `{"name":"x","url":"https://gw.example","fingerprint":"` + strings.Repeat("zz", 32) + `"}`, http.StatusUnprocessableEntity},
		{"not json", `{`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, tc.body, nil); got != tc.want {
				t.Fatalf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

// Operators copy fingerprints out of terminals, where they are usually
// colon-separated. Rejecting that spelling would be a papercut on the one
// action that has to be got right by hand.
func TestAColonSeparatedFingerprintIsAccepted(t *testing.T) {
	srv := newServer(t)
	pairs := make([]string, 0, 32)
	for i := 0; i < 64; i += 2 {
		pairs = append(pairs, goodFingerprint[i:i+2])
	}
	body := `{"name":"colons","url":"https://gw.example","fingerprint":"` + strings.ToUpper(strings.Join(pairs, ":")) + `"}`

	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, body, &created); got != http.StatusCreated {
		t.Fatalf("status = %d, want 201", got)
	}
	if created["fingerprint"] != goodFingerprint {
		t.Fatalf("stored fingerprint = %v, want it normalised to lowercase hex", created["fingerprint"])
	}
}

func TestGatewayListGetAndDelete(t *testing.T) {
	srv := newServer(t)
	var created map[string]any
	body := `{"name":"dmz-1","url":"https://gw.example:8444/","fingerprint":"` + goodFingerprint + `"}`
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, body, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatal("no gateway id returned")
	}
	if created["url"] != "https://gw.example:8444" {
		t.Fatalf("url = %v, want the trailing slash trimmed", created["url"])
	}
	if created["adopted_at"] != nil {
		t.Fatal("a newly recorded gateway is adopted; nothing has dialled it")
	}

	var list []map[string]any
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways", adminToken, "", &list); got != http.StatusOK {
		t.Fatalf("list = %d", got)
	}
	if len(list) != 1 {
		t.Fatalf("listed %d gateways, want 1", len(list))
	}

	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", nil); got != http.StatusOK {
		t.Fatalf("get = %d", got)
	}
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/00000000-0000-0000-0000-000000000000", adminToken, "", nil); got != http.StatusNotFound {
		t.Fatalf("get missing = %d, want 404", got)
	}

	// Deletion is revocation: the hub stops dialling and stops renewing.
	if got := call(t, http.MethodDelete, srv.URL+"/api/v1/gateways/"+id, adminToken, "", nil); got != http.StatusNoContent {
		t.Fatalf("delete = %d", got)
	}
	if got := call(t, http.MethodDelete, srv.URL+"/api/v1/gateways/"+id, adminToken, "", nil); got != http.StatusNotFound {
		t.Fatalf("second delete = %d, want 404", got)
	}
}

// A hub with no gateway CA cannot issue an identity. Saying so is better than
// dialling and failing halfway through a trust exchange.
func TestAdoptingWithoutAGatewayCAIsUnavailable(t *testing.T) {
	srv := newServer(t)
	var created map[string]any
	body := `{"name":"dmz","url":"https://gw.example","fingerprint":"` + goodFingerprint + `"}`
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, body, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusServiceUnavailable {
		t.Fatalf("adopt with no CA = %d, want 503", got)
	}
}

// Rotation is the recovery path for a gateway that lost its state directory,
// and it validates the new paste exactly like the first one.
func TestRotatingAdoptionValidatesTheNewFingerprint(t *testing.T) {
	srv := newServer(t)
	var created map[string]any
	body := `{"name":"dmz","url":"https://gw.example","fingerprint":"` + goodFingerprint + `"}`
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, body, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)

	rotated := strings.Repeat("ab", 32)
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/rotate", adminToken,
		`{"fingerprint":"`+rotated+`"}`, nil); got != http.StatusNoContent {
		t.Fatalf("rotate = %d, want 204", got)
	}
	var after map[string]any
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", &after); got != http.StatusOK {
		t.Fatalf("get = %d", got)
	}
	if after["fingerprint"] != rotated {
		t.Fatalf("fingerprint = %v, want the rotated one", after["fingerprint"])
	}
	if after["name"] != "dmz" || after["url"] != "https://gw.example" {
		t.Fatal("rotation lost the record the administrator configured")
	}

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/rotate", adminToken,
		`{"fingerprint":"nope"}`, nil); got != http.StatusUnprocessableEntity {
		t.Fatalf("rotate with a bad paste = %d, want 422", got)
	}
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/rotate", adminToken, `{`, nil); got != http.StatusBadRequest {
		t.Fatalf("rotate with malformed json = %d, want 400", got)
	}
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/00000000-0000-0000-0000-000000000000/rotate",
		adminToken, `{"fingerprint":"`+rotated+`"}`, nil); got != http.StatusNotFound {
		t.Fatalf("rotate a missing gateway = %d, want 404", got)
	}
}

// The gateway realm is admin-only. A runner credential must not reach it —
// runners are the thing gateways serve, not the thing that configures them.
func TestTheGatewayRealmRefusesAnUnauthenticatedCaller(t *testing.T) {
	srv := newServer(t)
	for _, path := range []string{"/api/v1/gateways", "/api/v1/gateways/x"} {
		if got := call(t, http.MethodGet, srv.URL+path, "", "", nil); got != http.StatusUnauthorized {
			t.Fatalf("GET %s unauthenticated = %d, want 401", path, got)
		}
	}
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", "", "{}", nil); got != http.StatusUnauthorized {
		t.Fatal("an unauthenticated caller recorded a gateway")
	}
}

// --- adoption end to end ------------------------------------------------------

// unadoptedGateway is a gateway in the state ADR-0049 §1 describes: a
// self-signed key, a published fingerprint, and no credential of any kind.
type unadoptedGateway struct {
	srv         *httptest.Server
	fingerprint string
	adopted     bool
}

func newUnadoptedGateway(t *testing.T) *unadoptedGateway {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := pki.Fingerprint(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "unadopted"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	idKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, idKey)
	if err != nil {
		t.Fatal(err)
	}

	g := &unadoptedGateway{fingerprint: fp}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bootstrap", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gwpush.Bootstrap{Fingerprint: fp, CSR: csr})
	})
	mux.HandleFunc("POST /adopt", func(w http.ResponseWriter, _ *http.Request) {
		g.adopted = true
		w.WriteHeader(http.StatusNoContent)
	})
	g.srv = httptest.NewUnstartedServer(mux)
	g.srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
	}
	g.srv.StartTLS()
	t.Cleanup(g.srv.Close)
	return g
}

func gatewayCA(t *testing.T) *pki.CA {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "gateway ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	ca, err := pki.Load("gateway", certPath, keyPath, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func newServerWithGateways(t *testing.T, ca *pki.CA) *httptest.Server {
	t.Helper()
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		LeaseTTL:   2 * time.Second,
		LeasePoll:  20 * time.Millisecond,
		Gateways:   gwpush.New(ca, nil, 5*time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// The whole adoption exchange, driven the way an administrator drives it.
func TestAdoptionDialsTheGatewayAndRecordsTheIdentity(t *testing.T) {
	ca := gatewayCA(t)
	srv := newServerWithGateways(t, ca)
	g := newUnadoptedGateway(t)

	var created map[string]any
	body := `{"name":"dmz","url":"` + g.srv.URL + `","fingerprint":"` + g.fingerprint + `"}`
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, body, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)

	var adopted map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", &adopted); got != http.StatusOK {
		t.Fatalf("adopt = %d, want 200", got)
	}
	if !g.adopted {
		t.Fatal("the gateway was never told it had been adopted")
	}
	if adopted["adopted_at"] == nil {
		t.Fatal("adoption was not recorded, so the reconcile loop would never pick the gateway up")
	}
	if adopted["cert_serial"] == "" || adopted["cert_serial"] == nil {
		t.Fatal("no identity serial recorded; renewal has nothing to compare against")
	}

	// One gateway, one owner. A second attempt is refused, not applied.
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusConflict {
		t.Fatalf("second adoption = %d, want 409", got)
	}
}

// A wrong paste fails at the handshake. It must surface as a gateway-side
// failure the administrator can act on, not a 500.
func TestAdoptingWithTheWrongFingerprintFailsAtTheDial(t *testing.T) {
	ca := gatewayCA(t)
	srv := newServerWithGateways(t, ca)
	g := newUnadoptedGateway(t)

	var created map[string]any
	body := `{"name":"dmz","url":"` + g.srv.URL + `","fingerprint":"` + strings.Repeat("ab", 32) + `"}`
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, body, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusBadGateway {
		t.Fatalf("adopt with a wrong fingerprint = %d, want 502", got)
	}
	if g.adopted {
		t.Fatal("the gateway processed an adoption despite the pin failing")
	}

	var after map[string]any
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", &after); got != http.StatusOK {
		t.Fatalf("get = %d", got)
	}
	if after["adopted_at"] != nil {
		t.Fatal("a failed adoption was recorded as successful")
	}
}

func TestAdoptingAGatewayThatDoesNotExist(t *testing.T) {
	srv := newServerWithGateways(t, gatewayCA(t))
	if got := call(t, http.MethodPost,
		srv.URL+"/api/v1/gateways/00000000-0000-0000-0000-000000000000/adopt", adminToken, "", nil); got != http.StatusNotFound {
		t.Fatalf("adopt a missing gateway = %d, want 404", got)
	}
}
