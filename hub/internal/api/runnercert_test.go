package api_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/runnerca"
	"github.com/aaron-au/shift/hub/internal/store"
)

// The runner realm authenticated by client certificate (ADR-0044).
//
// These run over real TLS with a real handshake, because the property under
// test is exactly what Go's TLS stack decides: an identity that comes from a
// VERIFIED chain and from nowhere else. A fake ConnectionState would test the
// test.

type mtlsHub struct {
	srv *httptest.Server
	ca  *runnerca.CA
}

// newMTLSHub starts a TLS hub that trusts a fresh control-plane CA for client
// certificates, mirroring hubd's VerifyClientCertIfGiven configuration.
func newMTLSHub(t *testing.T, mode api.RunnerAuthMode) *mtlsHub {
	t.Helper()
	dir := t.TempDir()
	writeTestCA(t, dir)
	ca, err := runnerca.Load(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}

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
		RunnerCA:   ca,
		RunnerAuth: mode,
		LeaseTTL:   2 * time.Second,
		LeasePoll:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{
		ClientAuth: tls.VerifyClientCertIfGiven,
		ClientCAs:  ca.Pool(),
		MinVersion: tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return &mtlsHub{srv: srv, ca: ca}
}

func writeTestCA(t *testing.T, dir string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test control-plane CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ca-key.pem"),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

// client returns an HTTP client that trusts the hub and optionally presents a
// client certificate.
func (m *mtlsHub) client(t *testing.T, cert *tls.Certificate) *http.Client {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(m.srv.Certificate())
	cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
}

func (m *mtlsHub) do(t *testing.T, c *http.Client, method, path, token, body string, out any) int {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, m.srv.URL+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
			t.Fatalf("decoding %s %s: %v", method, path, err)
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode
}

// newCSR returns a base64 DER CSR and its key.
func newCSR(t *testing.T, cn string) (string, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: cn}}, key)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(der), key
}

type registration struct {
	RunnerID    string `json:"runner_id"`
	Certificate string `json:"certificate"`
	CA          string `json:"ca"`
	NotAfter    string `json:"not_after"`
	Secret      string `json:"secret"`
}

// registerWithCert takes a registration token through the CSR path and returns
// the runner id and a usable client certificate.
func (m *mtlsHub) registerWithCert(t *testing.T, name string) (string, *tls.Certificate) {
	t.Helper()
	admin := m.client(t, nil)
	var tok struct {
		Token string `json:"token"`
	}
	if code := m.do(t, admin, http.MethodPost, "/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("minting a registration token = %d", code)
	}
	csrB64, key := newCSR(t, "ignored")
	var reg registration
	body := `{"token":"` + tok.Token + `","name":"` + name + `","csr":"` + csrB64 + `"}`
	if code := m.do(t, admin, http.MethodPost, "/api/v1/runners/register", "", body, &reg); code != http.StatusCreated {
		t.Fatalf("registering with a CSR = %d", code)
	}
	if reg.Secret != "" {
		t.Error("a certificate registration also handed out a bearer secret; the replayable credential was supposed to disappear")
	}
	return reg.RunnerID, keyPair(t, reg.Certificate, key)
}

func keyPair(t *testing.T, certPEM string, key *ecdsa.PrivateKey) *tls.Certificate {
	t.Helper()
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair([]byte(certPEM),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	return &pair
}

func TestRegisterWithACSRIssuesAUsableIdentity(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	id, cert := m.registerWithCert(t, "runner-a")
	if id == "" {
		t.Fatal("no runner id")
	}
	// The certificate must authenticate the RUNNER realm on a real handshake.
	if code := m.do(t, m.client(t, cert), http.MethodPost, "/api/v1/lease", "", `{"wait_seconds":0}`, nil); code == http.StatusUnauthorized {
		t.Fatal("the issued certificate did not authenticate the runner realm")
	}
	if leaf := cert.Certificate[0]; len(leaf) == 0 {
		t.Fatal("empty certificate")
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Subject.CommonName != id {
		t.Errorf("subject = %q, want the assigned id %q", parsed.Subject.CommonName, id)
	}
}

// The bearer secret is what mTLS exists to remove. A cert-registered runner
// must have none — not an unused one, none.
func TestACertificateRegisteredRunnerHasNoBearerSecret(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	_, cert := m.registerWithCert(t, "runner-b")

	// The same runner presenting NO certificate and NO token is unauthorized,
	// which is the observable form of "there is no secret to steal".
	if code := m.do(t, m.client(t, nil), http.MethodPost, "/api/v1/lease", "", `{"wait_seconds":0}`, nil); code != http.StatusUnauthorized {
		t.Errorf("unauthenticated lease = %d, want 401", code)
	}
	if code := m.do(t, m.client(t, cert), http.MethodPost, "/api/v1/lease", "", `{"wait_seconds":0}`, nil); code == http.StatusUnauthorized {
		t.Error("the certificate stopped working")
	}
}

// A certificate signed by somebody else must not authenticate anything. The
// handshake itself refuses it, which is the property worth pinning: this is
// not a check the application code can forget to make.
func TestAForeignCertificateIsRefusedAtTheHandshake(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	id, _ := m.registerWithCert(t, "runner-c")

	// A second, untrusted CA issuing a certificate for the SAME runner id.
	dir := t.TempDir()
	writeTestCA(t, dir)
	rogue, err := runnerca.Load(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca-key.pem"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	csrB64, key := newCSR(t, "")
	der, err := base64.StdEncoding.DecodeString(csrB64)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := rogue.Sign(der, id)
	if err != nil {
		t.Fatal(err)
	}
	forged := keyPair(t, string(issued.CertPEM), key)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		m.srv.URL+"/api/v1/lease", strings.NewReader(`{"wait_seconds":0}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := m.client(t, forged).Do(req)
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("a certificate from an untrusted CA got %d", resp.StatusCode)
		}
		return
	}
	// The expected outcome: the server aborts the handshake.
	if !strings.Contains(err.Error(), "certificate") && !strings.Contains(err.Error(), "tls") {
		t.Fatalf("unexpected failure: %v", err)
	}
}

// Deleting a runner must be an effective revocation: the signature is still
// valid, and the name it carries no longer means anything.
func TestACertificateForADeletedRunnerIsRefused(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	id, cert := m.registerWithCert(t, "runner-d")

	// Decommissioning is the operator action; under mTLS it IS revocation.
	if code := m.do(t, m.client(t, nil), http.MethodDelete, "/api/v1/runners/"+id, adminToken, "", nil); code != http.StatusNoContent {
		t.Fatalf("deleting the runner = %d", code)
	}
	if code := m.do(t, m.client(t, cert), http.MethodPost, "/api/v1/lease", "", `{"wait_seconds":0}`, nil); code != http.StatusUnauthorized {
		t.Errorf("a certificate naming a deleted runner = %d, want 401", code)
	}
}

// Renewal rides the CURRENT certificate — no operator token, or a fleet needs
// a human in the loop every day and the renewal path quietly never gets used.
func TestRenewalNeedsNoRegistrationToken(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	id, cert := m.registerWithCert(t, "runner-e")

	csrB64, key := newCSR(t, "")
	var got registration
	if code := m.do(t, m.client(t, cert), http.MethodPost, "/api/v1/runners/certificate", "",
		`{"csr":"`+csrB64+`"}`, &got); code != http.StatusOK {
		t.Fatalf("renewal = %d", code)
	}
	if got.RunnerID != id {
		t.Errorf("renewed as %q, want %q", got.RunnerID, id)
	}
	renewed := keyPair(t, got.Certificate, key)
	if code := m.do(t, m.client(t, renewed), http.MethodPost, "/api/v1/lease", "", `{"wait_seconds":0}`, nil); code == http.StatusUnauthorized {
		t.Error("the renewed certificate does not authenticate")
	}
}

// The renewed identity comes from the AUTHENTICATED connection, never from the
// request. A runner renewing on behalf of another runner is the one thing this
// endpoint must not permit.
func TestRenewalCannotNameAnotherRunner(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	idA, certA := m.registerWithCert(t, "runner-f")
	idB, _ := m.registerWithCert(t, "runner-g")

	csrB64, _ := newCSR(t, idB) // the CSR asks to be runner B
	var got registration
	if code := m.do(t, m.client(t, certA), http.MethodPost, "/api/v1/runners/certificate", "",
		`{"csr":"`+csrB64+`"}`, &got); code != http.StatusOK {
		t.Fatalf("renewal = %d", code)
	}
	if got.RunnerID != idA {
		t.Fatalf("runner A renewed as %q; a CSR naming another runner was honoured", got.RunnerID)
	}
	block, _ := pem.Decode([]byte(got.Certificate))
	parsed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Subject.CommonName != idA {
		t.Errorf("issued subject = %q, want %q", parsed.Subject.CommonName, idA)
	}
}

func TestRenewalRefusesAMalformedCSR(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	_, cert := m.registerWithCert(t, "runner-h")
	c := m.client(t, cert)

	if code := m.do(t, c, http.MethodPost, "/api/v1/runners/certificate", "", `{"csr":"!!!"}`, nil); code != http.StatusBadRequest {
		t.Errorf("non-base64 csr = %d, want 400", code)
	}
	if code := m.do(t, c, http.MethodPost, "/api/v1/runners/certificate", "", `{"csr":""}`, nil); code != http.StatusBadRequest {
		t.Errorf("empty csr = %d, want 400", code)
	}
	garbage := base64.StdEncoding.EncodeToString([]byte("not a csr"))
	if code := m.do(t, c, http.MethodPost, "/api/v1/runners/certificate", "", `{"csr":"`+garbage+`"}`, nil); code != http.StatusBadRequest {
		t.Errorf("unparseable csr = %d, want 400", code)
	}
}

func TestRenewalRequiresAnAuthenticatedRunner(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	csrB64, _ := newCSR(t, "")
	if code := m.do(t, m.client(t, nil), http.MethodPost, "/api/v1/runners/certificate", "",
		`{"csr":"`+csrB64+`"}`, nil); code != http.StatusUnauthorized {
		t.Errorf("unauthenticated renewal = %d, want 401", code)
	}
}

// A deployment that has cut over refuses the weaker credential where it would
// be ISSUED, not only where it would be accepted — otherwise the secret exists
// on disk and merely does not work yet.
func TestMTLSOnlyRefusesToIssueABearerSecret(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthMTLS)
	admin := m.client(t, nil)
	var tok struct {
		Token string `json:"token"`
	}
	if code := m.do(t, admin, http.MethodPost, "/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("token = %d", code)
	}
	var errBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	body := `{"token":"` + tok.Token + `","name":"legacy"}`
	if code := m.do(t, admin, http.MethodPost, "/api/v1/runners/register", "", body, &errBody); code != http.StatusBadRequest {
		t.Fatalf("bearer registration on an mtls-only hub = %d, want 400", code)
	}
	if errBody.Error.Code != "csr_required" {
		t.Errorf("code = %q, want csr_required", errBody.Error.Code)
	}
}

// And it refuses to ACCEPT one, including a secret issued before the cutover.
func TestMTLSOnlyRefusesABearerSecret(t *testing.T) {
	both := newMTLSHub(t, api.RunnerAuthBoth)
	admin := both.client(t, nil)
	var tok struct {
		Token string `json:"token"`
	}
	if code := both.do(t, admin, http.MethodPost, "/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("token = %d", code)
	}
	var reg registration
	if code := both.do(t, admin, http.MethodPost, "/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"legacy"}`, &reg); code != http.StatusCreated {
		t.Fatalf("bearer registration = %d", code)
	}
	if reg.Secret == "" {
		t.Fatal(`"both" did not issue a bearer secret`)
	}
	// The secret works on this hub...
	if code := both.do(t, admin, http.MethodPost, "/api/v1/lease", reg.Secret, `{"wait_seconds":0}`, nil); code == http.StatusUnauthorized {
		t.Fatal("a freshly issued bearer secret was refused by a hub that accepts both")
	}
}

// Configuration that cannot work must fail at startup, not one 401 at a time.
func TestMTLSOnlyNeedsACA(t *testing.T) {
	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_, err = api.Handler(st, api.Options{AdminToken: adminToken, RunnerAuth: api.RunnerAuthMTLS})
	if err == nil {
		t.Fatal(`"mtls" was accepted with no CA to verify or issue anything`)
	}
	if !strings.Contains(err.Error(), "CA") {
		t.Errorf("error %q does not say what is missing", err)
	}
}

func TestRunnerAuthModeIsValidated(t *testing.T) {
	if _, err := api.ParseRunnerAuthMode("sometimes"); err == nil {
		t.Error("an unknown runner auth mode was accepted")
	}
	if m, err := api.ParseRunnerAuthMode(""); err != nil || m != api.RunnerAuthBoth {
		t.Errorf(`ParseRunnerAuthMode("") = %q, %v; the migration default is "both"`, m, err)
	}
}

// A hub with no CA cannot issue, and must say so rather than 500.
func TestRegistrationWithACSRNeedsAConfiguredCA(t *testing.T) {
	srv := newServer(t) // plain hub: no runner CA
	var tok struct {
		Token string `json:"token"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("token = %d", code)
	}
	csrB64, _ := newCSR(t, "")
	var errBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	body := `{"token":"` + tok.Token + `","name":"r","csr":"` + csrB64 + `"}`
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "", body, &errBody); code != http.StatusBadRequest {
		t.Fatalf("CSR registration against a CA-less hub = %d, want 400", code)
	}
	if errBody.Error.Code != "mtls_unavailable" {
		t.Errorf("code = %q, want mtls_unavailable", errBody.Error.Code)
	}
}

func TestRegistrationRefusesAMalformedCSR(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	admin := m.client(t, nil)
	var tok struct {
		Token string `json:"token"`
	}
	if code := m.do(t, admin, http.MethodPost, "/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("token = %d", code)
	}
	body := `{"token":"` + tok.Token + `","name":"r","csr":"!!!not-base64!!!"}`
	if code := m.do(t, admin, http.MethodPost, "/api/v1/runners/register", "", body, nil); code != http.StatusBadRequest {
		t.Errorf("non-base64 csr = %d, want 400", code)
	}
}

// A spent or unknown registration token must fail the same way it always did.
// The CSR path is a different credential, not a different door.
func TestCertificateRegistrationStillNeedsAValidToken(t *testing.T) {
	m := newMTLSHub(t, api.RunnerAuthBoth)
	csrB64, _ := newCSR(t, "")
	var errBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	body := `{"token":"srt_nope","name":"r","csr":"` + csrB64 + `"}`
	if code := m.do(t, m.client(t, nil), http.MethodPost, "/api/v1/runners/register", "", body, &errBody); code != http.StatusUnauthorized {
		t.Fatalf("registration with an unknown token = %d, want 401", code)
	}
	if errBody.Error.Code != "registration_token_invalid" {
		t.Errorf("code = %q", errBody.Error.Code)
	}
}

// A CA-less hub must say so rather than 500, including on the renewal path a
// bearer-authenticated runner could still reach.
func TestRenewalOnACALessHubIsRefusedCleanly(t *testing.T) {
	srv := newServer(t)
	var tok struct {
		Token string `json:"token"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("token = %d", code)
	}
	var reg registration
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"legacy"}`, &reg); code != http.StatusCreated {
		t.Fatalf("registration = %d", code)
	}
	csrB64, _ := newCSR(t, "")
	var errBody struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/certificate", reg.Secret,
		`{"csr":"`+csrB64+`"}`, &errBody)
	if code != http.StatusBadRequest {
		t.Fatalf("renewal on a CA-less hub = %d, want 400", code)
	}
	if errBody.Error.Code != "mtls_unavailable" {
		t.Errorf("code = %q, want mtls_unavailable", errBody.Error.Code)
	}
}

// Decommissioning is an admin action on a real runner, and both halves of that
// sentence are enforced.
func TestDeleteRunner(t *testing.T) {
	srv := newServer(t)
	var tok struct {
		Token string `json:"token"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("token = %d", code)
	}
	var reg registration
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"doomed"}`, &reg); code != http.StatusCreated {
		t.Fatalf("registration = %d", code)
	}
	if code := call(t, http.MethodDelete, srv.URL+"/api/v1/runners/"+reg.RunnerID, "", "", nil); code != http.StatusUnauthorized {
		t.Errorf("unauthenticated delete = %d, want 401", code)
	}
	if code := call(t, http.MethodDelete, srv.URL+"/api/v1/runners/"+reg.RunnerID, adminToken, "", nil); code != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204", code)
	}
	// Deleting is revocation: the credential it held stops working.
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/lease", reg.Secret, `{"wait_seconds":0}`, nil); code != http.StatusUnauthorized {
		t.Errorf("a deleted runner's secret = %d, want 401", code)
	}
	if code := call(t, http.MethodDelete, srv.URL+"/api/v1/runners/"+reg.RunnerID, adminToken, "", nil); code != http.StatusNotFound {
		t.Errorf("deleting an already-deleted runner = %d, want 404", code)
	}
}

// The zombie-result rejection, from the wire (ADR-0009). A runner whose lease
// has gone must not be able to heartbeat, complete or fail a task that now
// belongs to somebody else — loosening this is how two runners both "finish"
// one task.
func TestATaskCannotBeTouchedByARunnerThatDoesNotHoldItsLease(t *testing.T) {
	srv := newServer(t)
	// Two runners, one task, and only one of them holds the lease.
	holder := registerBearerRunner(t, srv.URL, "holder")
	stranger := registerBearerRunner(t, srv.URL, "stranger")

	if code := call(t, http.MethodPut, srv.URL+"/api/v1/flows/zombie", adminToken,
		`{"name":"zombie","source":{"connector":"gen","action":"records"},"sink":{"connector":"@discard"}}`, nil); code != http.StatusCreated {
		t.Fatal("deploy failed")
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/flows/zombie/versions/1/publish", adminToken, "", nil); code != http.StatusOK {
		t.Fatal("publish failed")
	}
	var enq struct {
		TaskID string `json:"task_id"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/flows/zombie/execute", adminToken, `{}`, &enq); code != http.StatusAccepted {
		t.Fatal("enqueue failed")
	}
	var leased struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/lease", holder, `{"wait_seconds":2}`, &leased); code != http.StatusOK {
		t.Fatalf("lease = %d", code)
	}
	id := leased.Task.ID
	if id == "" {
		t.Fatal("no task leased")
	}
	for _, tc := range []struct{ name, path, body string }{
		{"heartbeat", "/api/v1/tasks/" + id + "/heartbeat", ""},
		{"complete", "/api/v1/tasks/" + id + "/complete", `{"records_in":1,"records_out":1}`},
		{"fail", "/api/v1/tasks/" + id + "/fail", `{"error":"nope"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := call(t, http.MethodPost, srv.URL+tc.path, stranger, tc.body, nil); code != http.StatusConflict {
				t.Errorf("%s by a runner without the lease = %d, want 409", tc.name, code)
			}
		})
	}
	// The holder is unaffected.
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/tasks/"+id+"/heartbeat", holder, "", nil); code != http.StatusNoContent {
		t.Errorf("the lease holder's heartbeat = %d, want 204", code)
	}
}

// A malformed body is a 400 on every runner endpoint that reads one, not a 500
// and not a silently-defaulted request.
func TestRunnerEndpointsRejectAMalformedBody(t *testing.T) {
	srv := newServer(t)
	secret := registerBearerRunner(t, srv.URL, "picky")
	for _, path := range []string{
		"/api/v1/lease",
		"/api/v1/tasks/00000000-0000-0000-0000-000000000000/heartbeat",
		"/api/v1/tasks/00000000-0000-0000-0000-000000000000/complete",
	} {
		if code := call(t, http.MethodPost, srv.URL+path, secret, `{"broken`, nil); code != http.StatusBadRequest {
			t.Errorf("%s with a malformed body = %d, want 400", path, code)
		}
	}
}

// registerBearerRunner registers a runner the old way and returns its secret.
func registerBearerRunner(t *testing.T, base, name string) string {
	t.Helper()
	var tok struct {
		Token string `json:"token"`
	}
	if code := call(t, http.MethodPost, base+"/api/v1/runner-tokens", adminToken, `{"ttl_seconds":300}`, &tok); code != http.StatusCreated {
		t.Fatalf("token = %d", code)
	}
	var reg registration
	if code := call(t, http.MethodPost, base+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"`+name+`"}`, &reg); code != http.StatusCreated {
		t.Fatalf("registration = %d", code)
	}
	return reg.Secret
}
