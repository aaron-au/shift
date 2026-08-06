package hubclient

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// The runner's control-plane certificate identity (ADR-0044).
//
// The private key is generated HERE and never leaves this process except as a
// public key inside a CSR. That is the difference from the bearer secret it
// replaces: a shared secret on disk is replayable by anything that can read it
// — a log line, a proxy, a core dump — and a key used only inside the TLS
// layer is not.
//
// The bundle layout mirrors the gateway's (ADR-0041) on purpose: one control
// plane, one shape, so an operator inspecting either finds the same four
// files.

// Identity file names inside the bundle directory.
const (
	IdentityCertFile = "runner.pem"
	IdentityKeyFile  = "runner-key.pem"
	IdentityCAFile   = "ca.pem"
	IdentityIDFile   = "runner-id"
)

// Identity is a runner's issued certificate and the trust root that came with
// it.
type Identity struct {
	dir      string
	RunnerID string
	// CA is the control-plane root alone: it verifies the gateways a runner
	// polls (ADR-0041), and nothing else uses it.
	CA *x509.CertPool
	// roots verify the HUB's server certificate: the system trust store PLUS
	// the control-plane CA, plus whatever the operator adds.
	//
	// Not the control CA alone. A hub fronted by a public or corporate CA —
	// which is most of them — would fail every handshake, and the failure
	// would look like a runner problem.
	roots *x509.CertPool
	// caPEM is kept so a renewal can rewrite the bundle unchanged.
	caPEM []byte

	// cert is swapped in place on renewal. An atomic pointer read by
	// GetClientCertificate means one transport, one connection pool, and no
	// window in which a caller holds a client wired to a certificate that has
	// just been replaced.
	cert atomic.Pointer[tls.Certificate]
	// notAfter / notBefore mirror the current certificate's validity window for
	// the renewal loop.
	notAfter  atomic.Int64
	notBefore atomic.Int64
}

// NotAfter is the current certificate's expiry.
func (i *Identity) NotAfter() time.Time { return time.Unix(i.notAfter.Load(), 0).UTC() }

// TLSConfig returns the client configuration presenting this identity.
func (i *Identity) TLSConfig() *tls.Config {
	return &tls.Config{
		RootCAs:    i.roots,
		MinVersion: tls.VersionTLS12,
		// Fetched per handshake rather than fixed at construction, so a
		// renewal takes effect on the next connection without rebuilding
		// anything.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if c := i.cert.Load(); c != nil {
				return c, nil
			}
			return &tls.Certificate{}, nil
		},
	}
}

// HTTPClient builds the hub client transport for this identity.
func (i *Identity) HTTPClient() *http.Client {
	return &http.Client{
		Timeout:   90 * time.Second,
		Transport: &http.Transport{TLSClientConfig: i.TLSConfig()},
	}
}

// TrustAlso adds a PEM certificate to the roots used to verify the HUB —
// the operator's -hub-ca, for a deployment whose hub certificate comes from
// neither the system store nor the control-plane CA.
//
// It never touches Identity.CA: the control plane's trust root is not
// something an operator flag should be able to widen (ADR-0044 §3 — the three
// PKIs never share a trust store).
//
// Call it at start-up, before the identity's client is used: it mutates the
// pool a live transport reads, so widening trust under in-flight handshakes
// would be a data race.
func (i *Identity) TrustAlso(pemBytes []byte) bool {
	if len(pemBytes) == 0 {
		return false
	}
	return i.roots.AppendCertsFromPEM(pemBytes)
}

// LoadIdentity reads a bundle. A missing bundle is (nil, nil): "not registered
// yet" is a normal state on first boot, not an error.
func LoadIdentity(dir string) (*Identity, error) {
	if dir == "" {
		return nil, nil
	}
	certPEM, err := os.ReadFile(filepath.Join(dir, IdentityCertFile)) // #nosec G304 -- operator-configured bundle
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("hubclient: identity certificate: %w", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, IdentityKeyFile)) // #nosec G304 -- operator-configured bundle
	if err != nil {
		return nil, fmt.Errorf("hubclient: identity key: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Join(dir, IdentityCAFile)) // #nosec G304 -- operator-configured bundle
	if err != nil {
		return nil, fmt.Errorf("hubclient: identity CA: %w", err)
	}
	idBytes, err := os.ReadFile(filepath.Join(dir, IdentityIDFile)) // #nosec G304 -- operator-configured bundle
	if err != nil {
		return nil, fmt.Errorf("hubclient: runner id: %w", err)
	}
	return newIdentity(dir, strings.TrimSpace(string(idBytes)), certPEM, keyPEM, caPEM)
}

// newIdentity assembles and validates a bundle's parts.
func newIdentity(dir, runnerID string, certPEM, keyPEM, caPEM []byte) (*Identity, error) {
	if runnerID == "" {
		return nil, errors.New("hubclient: identity has no runner id")
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("hubclient: identity certificate/key: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("hubclient: parsing the identity certificate: %w", err)
	}
	pair.Leaf = leaf
	if leaf.Subject.CommonName != runnerID {
		// The subject IS the identity the hub authenticates (ADR-0044 §5), so
		// a bundle whose id file disagrees with its certificate would have the
		// runner logging one identity and being treated as another.
		return nil, fmt.Errorf("hubclient: identity certificate names %q but the bundle says %q",
			leaf.Subject.CommonName, runnerID)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("hubclient: identity CA contains no usable certificate")
	}
	// Start from the system store so a hub with an ordinary certificate works
	// without configuration, and add the control-plane CA so a self-signed
	// bundle works without disabling verification.
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	roots.AppendCertsFromPEM(caPEM)
	i := &Identity{dir: dir, RunnerID: runnerID, CA: pool, roots: roots, caPEM: caPEM}
	i.cert.Store(&pair)
	i.notAfter.Store(leaf.NotAfter.Unix())
	i.notBefore.Store(leaf.NotBefore.Unix())
	return i, nil
}

// save writes the bundle, 0600. The key is written first and the id last, so
// an interrupted write leaves a bundle that fails to LOAD rather than one that
// loads with a certificate and key that do not match.
func (i *Identity) save(certPEM, keyPEM []byte) error {
	if i.dir == "" {
		return nil
	}
	if err := os.MkdirAll(i.dir, 0o700); err != nil {
		return err
	}
	for _, f := range []struct {
		name string
		data []byte
	}{
		{IdentityKeyFile, keyPEM},
		{IdentityCertFile, certPEM},
		{IdentityCAFile, i.caPEM},
		{IdentityIDFile, []byte(i.RunnerID + "\n")},
	} {
		if err := os.WriteFile(filepath.Join(i.dir, f.name), f.data, 0o600); err != nil {
			return fmt.Errorf("hubclient: writing %s: %w", f.name, err)
		}
	}
	return nil
}

// newKeyAndCSR generates a fresh key and a CSR over it.
//
// A NEW key on every issuance, including renewals: reusing one would mean a
// key compromised today stays useful for as long as the runner keeps renewing,
// which is exactly the property short certificate lifetimes exist to remove.
func newKeyAndCSR(commonName string) (csrDER, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	// The subject is ignored by the hub, which assigns the identity itself.
	// It is filled in anyway so a CSR captured in a log is legible.
	csrDER, err = x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: commonName}}, key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return csrDER, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

// issuance is the hub's response to a registration or a renewal.
type issuance struct {
	RunnerID    string `json:"runner_id"`
	Certificate string `json:"certificate"`
	CA          string `json:"ca"`
	NotAfter    string `json:"not_after"`
}

// RegisterWithCSR consumes a single-use registration token and returns a
// certificate identity, written to dir (ADR-0044 §1).
func RegisterWithCSR(ctx context.Context, hc *http.Client, baseURL, token, name, dir string) (*Identity, error) {
	csrDER, keyPEM, err := newKeyAndCSR(name)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]string{
		"token": token, "name": name,
		"csr": base64.StdEncoding.EncodeToString(csrDER),
	})
	if err != nil {
		return nil, err
	}
	var out issuance
	if err := postJSON(ctx, hc, strings.TrimRight(baseURL, "/")+"/api/v1/runners/register",
		"", body, http.StatusCreated, &out); err != nil {
		return nil, fmt.Errorf("hubclient: register: %w", err)
	}
	id, err := newIdentity(dir, out.RunnerID, []byte(out.Certificate), keyPEM, []byte(out.CA))
	if err != nil {
		return nil, err
	}
	if err := id.save([]byte(out.Certificate), keyPEM); err != nil {
		return nil, err
	}
	return id, nil
}

// Renew asks the hub for a fresh certificate, authenticating with the current
// one, and swaps it in on success.
//
// The identity is unchanged if this fails: the existing certificate keeps
// working until it expires, which is the whole point of renewing early.
func (i *Identity) Renew(ctx context.Context, hc *http.Client, baseURL string) error {
	csrDER, keyPEM, err := newKeyAndCSR(i.RunnerID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"csr": base64.StdEncoding.EncodeToString(csrDER)})
	if err != nil {
		return err
	}
	var out issuance
	if err := postJSON(ctx, hc, strings.TrimRight(baseURL, "/")+"/api/v1/runners/certificate",
		"", body, http.StatusOK, &out); err != nil {
		return fmt.Errorf("hubclient: renew: %w", err)
	}
	pair, err := tls.X509KeyPair([]byte(out.Certificate), keyPEM)
	if err != nil {
		return fmt.Errorf("hubclient: renewed certificate/key: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fmt.Errorf("hubclient: parsing the renewed certificate: %w", err)
	}
	if leaf.Subject.CommonName != i.RunnerID {
		// The hub derives the subject from the authenticated connection, so
		// this cannot happen against a correct hub — and if it ever did, the
		// runner would be operating under an identity it did not think it had.
		return fmt.Errorf("hubclient: the hub issued a certificate for %q, not %q",
			leaf.Subject.CommonName, i.RunnerID)
	}
	pair.Leaf = leaf
	// Persist BEFORE swapping: a restart after a swap-but-no-write would come
	// back holding the old certificate while the new one is what the hub last
	// recorded, and the confusing case is worse than a redundant renewal.
	if err := i.save([]byte(out.Certificate), keyPEM); err != nil {
		return err
	}
	i.cert.Store(&pair)
	i.notAfter.Store(leaf.NotAfter.Unix())
	i.notBefore.Store(leaf.NotBefore.Unix())
	return nil
}

// RenewAfter returns how long to wait before the next renewal attempt.
//
// Half of the REMAINING lifetime: renewing at 50% is the standard shape, and
// deriving it from what is left rather than from the original lifetime means a
// runner that has been asleep, or that failed a few attempts, converges on
// trying more often as the cliff approaches instead of waking on a fixed
// schedule it has already missed.
//
// The floor is a tenth of the certificate's lifetime, clamped to [1s, 1m]. A
// fixed floor would be wrong at both ends: a minute is a pointless wait for a
// ten-minute certificate, and a second would be a busy-loop against a
// month-long one.
func (i *Identity) RenewAfter(now time.Time) time.Duration {
	floor := i.retryFloor()
	remaining := i.NotAfter().Sub(now)
	if remaining <= 0 {
		return floor
	}
	if d := remaining / 2; d > floor {
		return d
	}
	return floor
}

// retryFloor bounds how often the loop retries, derived from how long the
// certificate was issued for.
func (i *Identity) retryFloor() time.Duration {
	lifetime := i.NotAfter().Sub(time.Unix(i.notBefore.Load(), 0))
	switch d := lifetime / 10; {
	case d > time.Minute:
		return time.Minute
	case d < time.Second:
		return time.Second
	default:
		return d
	}
}

// ConnectMTLS resolves a runner's certificate identity: the saved bundle when
// present, otherwise a registration with the single-use token (retrying while
// the hub boots — compose ordering).
//
// hc is used only for the registration call, which happens before any identity
// exists to present. Everything after it goes through the identity's own
// client.
func ConnectMTLS(ctx context.Context, hc *http.Client, hubURL, dir, token, name string) (*Identity, *Client, error) {
	id, err := LoadIdentity(dir)
	if err != nil {
		return nil, nil, err
	}
	if id == nil {
		if token == "" {
			return nil, nil, errors.New("hubclient: no saved identity and no registration token")
		}
		if dir == "" {
			// An identity held only in memory would re-register on every
			// restart, and registration tokens are single-use — so the second
			// start of that runner fails, in production, at 3am.
			return nil, nil, errors.New("hubclient: mTLS registration needs an identity directory to persist into")
		}
		if id, err = registerWithRetry(ctx, hc, hubURL, token, name, dir); err != nil {
			return nil, nil, err
		}
	}
	return id, New(hubURL, "").WithHTTPClient(id.HTTPClient()), nil
}

func registerWithRetry(ctx context.Context, hc *http.Client, hubURL, token, name, dir string) (*Identity, error) {
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for attempt := 1; ; attempt++ {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		id, err := RegisterWithCSR(rctx, hc, hubURL, token, name, dir)
		cancel()
		if err == nil {
			return id, nil
		}
		lastErr = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("hubclient: registration failed after %d attempts: %w", attempt, lastErr)
		}
		slog.Warn("registration attempt failed, retrying",
			"event", "runner.register.retry", "attempt", attempt, "error", err.Error())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// RenewLoop keeps the identity current until ctx ends (ADR-0044).
//
// Policy on repeated failure, which the ADR left open: the runner KEEPS
// WORKING and keeps retrying, more often as expiry approaches, and says so
// loudly. It does not refuse new work early. A renewal outage is already a
// problem; converting it into a fleet-wide work stoppage before the
// certificates have actually expired would make the platform's response to a
// hub hiccup worse than the hiccup.
func (i *Identity) RenewLoop(ctx context.Context, hubURL string) {
	hc := i.HTTPClient()
	for {
		wait := i.RenewAfter(time.Now())
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := i.Renew(rctx, hc, hubURL)
		cancel()
		notAfter := i.NotAfter().Format(time.RFC3339)
		switch {
		case err == nil:
			slog.Info("certificate renewed",
				"event", "runner.cert.renewed", "runner", i.RunnerID, "cert_not_after", notAfter)
		case time.Until(i.NotAfter()) < time.Hour:
			// The last hour is when "renewal has been failing quietly" turns
			// into "this runner is about to go silent", so it stops being a
			// warning (ADR-0044: "the fleet went quiet overnight").
			slog.Error("certificate renewal is failing and the certificate is about to expire",
				"event", "runner.cert.renew_failed", "runner", i.RunnerID,
				"cert_not_after", notAfter, "error", err.Error())
		default:
			slog.Warn("certificate renewal failed, will retry",
				"event", "runner.cert.renew_failed", "runner", i.RunnerID,
				"cert_not_after", notAfter, "error", err.Error())
		}
	}
}

// postJSON posts a body and decodes a JSON response, insisting on want.
func postJSON(ctx context.Context, hc *http.Client, url, bearer string, body []byte, want int, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != want {
		return errors.New(readErr(resp))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
