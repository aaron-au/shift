package api_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
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
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/pki"
	"github.com/aaron-au/shift/hub/internal/store"
)

// The gateway realm is administrator-facing only (ADR-0049). There is no
// gateway-facing endpoint here and there must never be one — a gateway that
// could call the hub would be a DMZ box holding a hub credential.

func TestRecordingAGatewayValidatesTheInput(t *testing.T) {
	srv := newServer(t)

	cases := []struct {
		name, body string
		want       int
	}{
		{"valid", `{"name":"dmz","url":"https://gw.example:8444"}`, http.StatusCreated},
		{"no name", `{"url":"https://gw.example"}`, http.StatusUnprocessableEntity},
		{"no url", `{"name":"x"}`, http.StatusUnprocessableEntity},
		// Pairing binds its proofs to the fingerprint of the certificate on the
		// wire. Over plaintext there is no certificate to bind to, and the
		// exchange degrades to a token anyone on the path can copy.
		{"plaintext url", `{"name":"x","url":"http://gw.example"}`, http.StatusUnprocessableEntity},
		{"not a url", `{"name":"x","url":"://nonsense"}`, http.StatusUnprocessableEntity},
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

// The install token is served exactly once, by create. Anywhere else would be a
// standing way to read a bootstrap secret out of the API.
func TestTheInstallTokenIsReturnedOnceAndNeverAgain(t *testing.T) {
	srv := newServer(t)

	var created map[string]any
	body := `{"name":"dmz-1","url":"https://gw.example:8444/"}`
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken, body, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	token, _ := created["install_token"].(string)
	if token == "" {
		t.Fatal("no install token was returned, so the gateway could never be deployed")
	}
	if created["token_expires_at"] == nil {
		t.Fatal("the install token never expires; an unclaimed one would stand forever")
	}
	if created["url"] != "https://gw.example:8444" {
		t.Fatalf("url = %v, want the trailing slash trimmed", created["url"])
	}
	if created["adopted_at"] != nil {
		t.Fatal("a newly recorded gateway is adopted; nothing has dialled it")
	}
	if created["fingerprint"] != "" {
		t.Fatal("a fingerprint was recorded before the gateway existed to have one")
	}
	id, _ := created["id"].(string)

	var got map[string]any
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", &got); code != http.StatusOK {
		t.Fatalf("get = %d", code)
	}
	if _, ok := got["install_token"]; ok {
		t.Fatal("the install token is served by get; it must be returned once and only once")
	}

	var list []map[string]any
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/gateways", adminToken, "", &list); code != http.StatusOK {
		t.Fatalf("list = %d", code)
	}
	if len(list) != 1 {
		t.Fatalf("listed %d gateways, want 1", len(list))
	}
	if _, ok := list[0]["install_token"]; ok {
		t.Fatal("the install token is served by list")
	}
}

func TestGatewayGetAndDelete(t *testing.T) {
	srv := newServer(t)
	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"https://gw.example"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)

	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/00000000-0000-0000-0000-000000000000",
		adminToken, "", nil); got != http.StatusNotFound {
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

// A hub with no gateway CA cannot issue an identity. Saying so beats dialling
// and failing halfway through a trust exchange.
func TestAdoptingWithoutAGatewayCAIsUnavailable(t *testing.T) {
	srv := newServer(t)
	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"https://gw.example"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusServiceUnavailable {
		t.Fatalf("adopt with no CA = %d, want 503", got)
	}
}

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

// --- pairing end to end -------------------------------------------------------

// unadoptedGateway is a gateway in the state ADR-0049 §1 describes: a key it
// generated itself, an install token handed to it at deploy time, and no
// credential from anybody.
type unadoptedGateway struct {
	srv     *httptest.Server
	token   string
	adopted bool
}

func newUnadoptedGateway(t *testing.T, token string) *unadoptedGateway {
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

	g := &unadoptedGateway{token: token}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		var c gwpush.Challenge
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil || c.Proof == "" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if c.Proof != gwpush.Proof(g.token, gwpush.DomainHubHello, c.Nonce, fp, nil) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		sum := sha256.Sum256(csr)
		_ = json.NewEncoder(w).Encode(gwpush.Hello{
			Fingerprint: fp, CSR: csr,
			Proof: gwpush.Proof(g.token, gwpush.DomainGWHello, c.Nonce, fp, sum[:]),
		})
	})
	mux.HandleFunc("POST /adopt", func(w http.ResponseWriter, r *http.Request) {
		var a gwpush.Adoption
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if a.Proof != gwpush.Proof(g.token, gwpush.DomainInstall, a.Nonce, fp, gwpush.MaterialDigest(a)) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
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

// The whole exchange, driven the way an administrator drives it: record,
// deploy with the token, pair.
func TestAdoptionPairsWithTheGatewayAndLearnsItsKey(t *testing.T) {
	srv := newServerWithGateways(t, gatewayCA(t))

	// The record comes first — the token does not exist until it does. The
	// gateway then reads that token at deploy time, which is what g.token
	// stands in for here.
	g := newUnadoptedGateway(t, "not-yet-deployed")
	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"`+g.srv.URL+`"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)
	g.token, _ = created["install_token"].(string)

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
	if adopted["fingerprint"] == "" || adopted["fingerprint"] == nil {
		t.Fatal("no fingerprint was learned; the hub would have no way back in after the identity lapsed")
	}
	if adopted["cert_serial"] == "" || adopted["cert_serial"] == nil {
		t.Fatal("no identity serial recorded; renewal has nothing to compare against")
	}

	// The token is spent. A second adoption cannot replay it.
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusConflict {
		t.Fatalf("second adoption = %d, want 409", got)
	}
}

// A gateway holding a different token is not adopted, and nothing is issued.
func TestAdoptingAGatewayWithTheWrongTokenFails(t *testing.T) {
	srv := newServerWithGateways(t, gatewayCA(t))
	g := newUnadoptedGateway(t, "sgt_some_other_token")

	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"`+g.srv.URL+`"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusBadGateway {
		t.Fatalf("adopt with a mismatched token = %d, want 502", got)
	}
	if g.adopted {
		t.Fatal("the gateway was adopted despite the pairing failing")
	}
	var after map[string]any
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", &after); got != http.StatusOK {
		t.Fatalf("get = %d", got)
	}
	if after["adopted_at"] != nil {
		t.Fatal("a failed pairing was recorded as an adoption")
	}
}

func TestAdoptingAGatewayThatDoesNotExist(t *testing.T) {
	srv := newServerWithGateways(t, gatewayCA(t))
	if got := call(t, http.MethodPost,
		srv.URL+"/api/v1/gateways/00000000-0000-0000-0000-000000000000/adopt", adminToken, "", nil); got != http.StatusNotFound {
		t.Fatalf("adopt a missing gateway = %d, want 404", got)
	}
}

// Rotation is the recovery path for a gateway redeployed without its state
// directory: a fresh token, the record intact, the identity reset.
func TestRotatingAdoptionIssuesANewToken(t *testing.T) {
	srv := newServerWithGateways(t, gatewayCA(t))
	g := newUnadoptedGateway(t, "placeholder")

	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"`+g.srv.URL+`"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)
	g.token, _ = created["install_token"].(string)
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusOK {
		t.Fatalf("adopt = %d", got)
	}

	var rotated map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/rotate", adminToken, "", &rotated); got != http.StatusOK {
		t.Fatalf("rotate = %d, want 200", got)
	}
	newToken, _ := rotated["install_token"].(string)
	if newToken == "" || newToken == g.token {
		t.Fatal("rotation did not issue a new install token")
	}

	var after map[string]any
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", &after); got != http.StatusOK {
		t.Fatalf("get = %d", got)
	}
	if after["name"] != "dmz" || after["url"] != g.srv.URL {
		t.Fatal("rotation lost the record the administrator configured")
	}
	if after["adopted_at"] != nil {
		t.Fatal("rotation left the gateway adopted, so it could never pair again")
	}
	if after["fingerprint"] != "" {
		t.Fatal("rotation kept the old fingerprint; the hub would pin a key the redeployed gateway no longer has")
	}

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/00000000-0000-0000-0000-000000000000/rotate",
		adminToken, "", nil); got != http.StatusNotFound {
		t.Fatalf("rotate a missing gateway = %d, want 404", got)
	}
}

// The token's lifetime is deployment policy, so it is configurable — short by
// default because it will sit in a manifest until somebody deploys.
func TestTheInstallTokenLifetimeIsConfigurable(t *testing.T) {
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken:      adminToken,
		LeaseTTL:        2 * time.Second,
		LeasePoll:       20 * time.Millisecond,
		GatewayTokenTTL: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"https://gw.example"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	raw, _ := created["token_expires_at"].(string)
	expires, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("token_expires_at = %q: %v", raw, err)
	}
	if d := time.Until(expires); d > 20*time.Minute || d < 10*time.Minute {
		t.Fatalf("token expires in %s, want roughly the configured 15m", d)
	}
}

// An unadopted gateway whose token is gone cannot be paired with, and says so
// rather than dialling and failing obscurely.
func TestAdoptingWithASpentTokenIsAConflict(t *testing.T) {
	srv := newServerWithGateways(t, gatewayCA(t))
	g := newUnadoptedGateway(t, "placeholder")

	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"`+g.srv.URL+`"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	id, _ := created["id"].(string)
	g.token, _ = created["install_token"].(string)

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusOK {
		t.Fatalf("adopt = %d", got)
	}
	// Adopted, so the token is spent; a second attempt is a conflict rather
	// than a retry.
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); got != http.StatusConflict {
		t.Fatalf("adopt with a spent token = %d, want 409", got)
	}
}

// The gateway realm is the ADMIN realm. A runner's credential must not reach
// it: runners are the thing gateways serve, never the thing that configures
// them, and a compromised runner that could mint gateway records or read a
// gateway's URL would have escalated out of its own realm.
func TestARunnerCredentialCannotReachTheGatewayRealm(t *testing.T) {
	srv := newServer(t)

	var tok struct{ Token string }
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok); code != http.StatusCreated {
		t.Fatalf("runner token = %d", code)
	}
	var reg struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); code != http.StatusCreated || reg.Secret == "" {
		t.Fatalf("register = %d", code)
	}

	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways", reg.Secret, "", nil); got != http.StatusUnauthorized {
		t.Fatalf("list as a runner = %d, want 401", got)
	}
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", reg.Secret,
		`{"name":"x","url":"https://gw.example"}`, nil); got != http.StatusUnauthorized {
		t.Fatalf("create as a runner = %d, want 401", got)
	}

	// And the record a runner tried to create must not exist.
	var list []map[string]any
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/gateways", adminToken, "", &list); got != http.StatusOK {
		t.Fatalf("list = %d", got)
	}
	if len(list) != 0 {
		t.Fatalf("listed %d gateways, want none — a runner created one", len(list))
	}
}

// A malformed id is a client mistake, not a broken hub. Postgres rejects it
// while parsing the UUID, and letting that surface as a 500 would tell an
// operator the server had failed when their URL was simply wrong — and would
// bury real 500s among the typos.
func TestAMalformedIDIsNotFoundRatherThanAServerError(t *testing.T) {
	srv := newServer(t)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/gateways/not-a-uuid"},
		{http.MethodDelete, "/api/v1/gateways/not-a-uuid"},
		{http.MethodPost, "/api/v1/gateways/not-a-uuid/rotate"},
	} {
		if got := call(t, tc.method, srv.URL+tc.path, adminToken, "", nil); got != http.StatusNotFound {
			t.Fatalf("%s %s = %d, want 404", tc.method, tc.path, got)
		}
	}
}
