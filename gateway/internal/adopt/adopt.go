// Package adopt is the gateway's side of adoption (ADR-0049).
//
// A gateway cannot dial the hub — ADR-0038 §4 forbids anything at the edge
// from initiating inward — so it cannot register itself the way a runner does.
// Adoption is a PAIRING, keyed by a one-time install token the hub mints and
// the operator supplies at deploy time. The hub cannot know the gateway's key
// in advance — the gateway generates it on first start — so the hub does not
// need to: it learns the key on the first dial, and the token is what makes
// learning it safe. Afterwards the key is pinned forever and the token is
// worthless, so it is burned at both ends.
//
// Both proofs are HMACs bound to the FINGERPRINT OF THE CERTIFICATE ON THE
// WIRE, not bare comparisons of the token. That is what defeats an active
// machine-in-the-middle: an interceptor terminates TLS with its own key, so the
// hub's proof (computed over the interceptor's fingerprint) fails the gateway's
// check, and the gateway's proof (over its own) fails the hub's. A plain token
// comparison would be relayed and copied.
//
// Two states, and the difference is visible in what the listener will even
// talk to:
//
//   - UNADOPTED — serves the anchor certificate, asks for no client
//     certificate, and answers only the pairing endpoints, and only to a caller
//     holding the install token. It has no configuration, so it routes nothing.
//     An unadopted gateway is inert.
//   - ADOPTED — serves the hub-issued identity, REQUIRES a client certificate,
//     and the pairing endpoints stop answering. A second adoption is refused
//     rather than applied: one gateway, one owner.
//
// Deliberately stdlib-only, like the rest of this module. A shared PKI package
// would be convenient and would put more code, and a wider dependency surface,
// on the one box that may sit in a DMZ.
package adopt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// StateVersion is the on-disk format version.
//
// ADDITIVE-ONLY within a major. A gateway upgrade is a new image over the same
// state directory (ADR-0049 §5), and rollback is the previous image over that
// same directory — so a newer gateway that wrote state an older one cannot read
// would break the exact thing that makes full-image replacement safe. Adding
// fields is fine; changing the meaning of one is not.
const StateVersion = 1

// File names inside the state directory.
const (
	stateFile       = "state.json"
	anchorKeyFile   = "anchor-key.pem"
	anchorCertFile  = "anchor.pem"
	identityKeyFile = "identity-key.pem"
	identityFile    = "identity.pem"
	pendingKeyFile  = "identity-key-pending.pem"
	gatewayCAFile   = "gateway-ca.pem"
	runnerCAFile    = "runner-ca.pem"
)

// Role is what a verified peer on the control listener is.
type Role int

const (
	// RoleNone is an unverified or unrecognised peer.
	RoleNone Role = iota
	// RoleHub is the hub, pushing configuration or an identity.
	RoleHub
	// RoleRunner is a runner polling for inbound work.
	RoleRunner
)

// state is the persisted document.
type state struct {
	Version    int       `json:"version"`
	GatewayID  string    `json:"gateway_id,omitempty"`
	HubSubject string    `json:"hub_subject,omitempty"`
	AdoptedAt  time.Time `json:"adopted_at,omitzero"`
}

// State is the gateway's adoption state, safe for concurrent use.
//
// Configuration is deliberately NOT part of it. Identity survives a reboot;
// routes and customer TLS keys do not survive an imaged disk (ADR-0049 §3). On
// restart the gateway has an identity and no configuration, and waits, inert,
// for the hub's reconcile push.
type State struct {
	dir string

	mu        sync.RWMutex
	doc       state
	anchor    tls.Certificate // long-lived; the fingerprint is over this key
	anchorFP  string
	identity  *tls.Certificate // nil until adopted
	pending   *ecdsa.PrivateKey
	gatewayCA *x509.CertPool
	runnerCA  *x509.CertPool
	gwRoot    *x509.Certificate // for role attribution — see PeerRole
	rnRoot    *x509.Certificate

	// token is the one-time install token, supplied by the environment at
	// deploy time and NEVER written to disk. It is burned the moment adoption
	// completes: a standing credential on a DMZ host is exactly what this
	// design exists to avoid, and afterwards the pinned key does everything
	// the token did.
	token string
}

// Open loads a state directory, creating the anchor key on first start.
//
// installToken is empty for an already-adopted gateway; it is only consulted
// while unadopted, and a gateway with neither an identity nor a token can
// never be adopted (it will say so and serve nothing).
func Open(dir, installToken string) (*State, error) {
	if dir == "" {
		return nil, errors.New("adopt: no state directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("adopt: state directory: %w", err)
	}
	s := &State{dir: dir, doc: state{Version: StateVersion}, token: installToken}

	if raw, err := os.ReadFile(s.path(stateFile)); err == nil {
		if err := json.Unmarshal(raw, &s.doc); err != nil {
			return nil, fmt.Errorf("adopt: %s: %w", stateFile, err)
		}
		if s.doc.Version > StateVersion {
			// Refusing beats guessing. A rollback that silently ignored fields
			// it did not understand would run with a policy nobody chose.
			return nil, fmt.Errorf("adopt: state was written by a newer gateway (format %d, this build reads %d); "+
				"roll forward or start from an empty state directory", s.doc.Version, StateVersion)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("adopt: %s: %w", stateFile, err)
	}

	if err := s.loadOrCreateAnchor(); err != nil {
		return nil, err
	}
	if err := s.loadIdentity(); err != nil {
		return nil, err
	}
	if err := s.loadPending(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *State) path(name string) string { return filepath.Join(s.dir, name) }

// loadOrCreateAnchor reads the long-lived key, generating it on first start.
//
// The anchor is what the operator's paste actually pins, and it never changes:
// the self-signed certificate over it may be reissued (on expiry, on a clock
// correction) without the fingerprint moving, because the fingerprint is over
// the KEY. A fingerprint that moved with the certificate would strand the hub's
// only way back in (ADR-0049 §6).
func (s *State) loadOrCreateAnchor() error {
	keyPEM, err := os.ReadFile(s.path(anchorKeyFile)) // #nosec G304 -- state directory
	switch {
	case err == nil:
		key, err := parseECKey(keyPEM)
		if err != nil {
			return fmt.Errorf("adopt: anchor key: %w", err)
		}
		certPEM, err := os.ReadFile(s.path(anchorCertFile)) // #nosec G304 -- state directory
		if err != nil {
			// The key is the anchor; a missing certificate is re-derivable.
			certPEM, err = selfSign(key)
			if err != nil {
				return err
			}
			if err := s.write(anchorCertFile, certPEM); err != nil {
				return err
			}
		}
		return s.setAnchor(certPEM, keyPEM, key)
	case errors.Is(err, os.ErrNotExist):
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return fmt.Errorf("adopt: generating the anchor key: %w", err)
		}
		keyPEM, err := encodeECKey(key)
		if err != nil {
			return err
		}
		certPEM, err := selfSign(key)
		if err != nil {
			return err
		}
		if err := s.write(anchorKeyFile, keyPEM); err != nil {
			return err
		}
		if err := s.write(anchorCertFile, certPEM); err != nil {
			return err
		}
		return s.setAnchor(certPEM, keyPEM, key)
	default:
		return fmt.Errorf("adopt: anchor key: %w", err)
	}
}

func (s *State) setAnchor(certPEM, keyPEM []byte, key *ecdsa.PrivateKey) error {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("adopt: anchor certificate/key: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fmt.Errorf("adopt: parsing the anchor certificate: %w", err)
	}
	pair.Leaf = leaf
	fp, err := Fingerprint(&key.PublicKey)
	if err != nil {
		return err
	}
	s.anchor, s.anchorFP = pair, fp
	return nil
}

func (s *State) loadIdentity() error {
	certPEM, err := os.ReadFile(s.path(identityFile)) // #nosec G304 -- state directory
	if errors.Is(err, os.ErrNotExist) {
		return nil // not adopted yet
	}
	if err != nil {
		return fmt.Errorf("adopt: identity: %w", err)
	}
	keyPEM, err := os.ReadFile(s.path(identityKeyFile)) // #nosec G304 -- state directory
	if err != nil {
		return fmt.Errorf("adopt: identity key: %w", err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("adopt: identity certificate/key: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fmt.Errorf("adopt: parsing the identity: %w", err)
	}
	pair.Leaf = leaf
	s.identity = &pair

	gw, gwRoot, err := s.loadCA(gatewayCAFile)
	if err != nil {
		return err
	}
	s.gatewayCA, s.gwRoot = gw, gwRoot
	// The runner CA is optional: a hub with no runner mTLS configured sends
	// none, and the gateway then verifies only the hub.
	if rn, rnRoot, err := s.loadCA(runnerCAFile); err == nil {
		s.runnerCA, s.rnRoot = rn, rnRoot
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *State) loadCA(name string) (*x509.CertPool, *x509.Certificate, error) {
	raw, err := os.ReadFile(s.path(name)) // #nosec G304 -- state directory
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, nil, fmt.Errorf("adopt: %s contains no usable certificate", name)
	}
	blk, _ := pem.Decode(raw)
	if blk == nil {
		return nil, nil, fmt.Errorf("adopt: %s is not PEM", name)
	}
	root, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("adopt: %s: %w", name, err)
	}
	return pool, root, nil
}

func (s *State) loadPending() error {
	raw, err := os.ReadFile(s.path(pendingKeyFile)) // #nosec G304 -- state directory
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("adopt: pending identity key: %w", err)
	}
	key, err := parseECKey(raw)
	if err != nil {
		return fmt.Errorf("adopt: pending identity key: %w", err)
	}
	s.pending = key
	return nil
}

// Adopted reports whether adoption has completed.
func (s *State) Adopted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.identity != nil
}

// Fingerprint is the value an administrator carries to the hub.
func (s *State) Fingerprint() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.anchorFP
}

// GatewayID is the hub-assigned identity, empty until adopted.
func (s *State) GatewayID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.doc.GatewayID
}

// NotAfter is the identity's expiry, zero when unadopted. Surfaced for health:
// renewal is the hub's to push, so a gateway watching its own expiry is
// reporting, not acting.
func (s *State) NotAfter() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.identity == nil {
		return time.Time{}
	}
	return s.identity.Leaf.NotAfter
}

// CSR returns a certificate request the hub can sign.
//
// The key is generated fresh and PERSISTED before the request is handed out.
// Holding it only in memory would mean a gateway that restarted between
// publishing a request and receiving the certificate would be issued an
// identity over a key it no longer had — adopted from the hub's point of view,
// and unable to serve.
func (s *State) CSR() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("adopt: generating an identity key: %w", err)
		}
		keyPEM, err := encodeECKey(key)
		if err != nil {
			return nil, err
		}
		if err := s.write(pendingKeyFile, keyPEM); err != nil {
			return nil, err
		}
		s.pending = key
	}
	// The subject is ignored by the hub — it assigns the identity (ADR-0044
	// §5) — so there is nothing meaningful to put here.
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, s.pending)
	if err != nil {
		return nil, fmt.Errorf("adopt: certificate request: %w", err)
	}
	return csr, nil
}

// Adoption is what the hub delivers.
type Adoption struct {
	// Nonce and Proof carry the hub's half of the pairing. Proof is an HMAC
	// over the nonce, this gateway's fingerprint and a digest of the material
	// below — so an interceptor can neither substitute a CA of its own nor
	// replay a captured proof against different material.
	//
	// Both are empty on a renewal delivered over mutual TLS, where the hub's
	// client certificate is the proof.
	Nonce      string `json:"nonce,omitempty"`
	Proof      string `json:"proof,omitempty"`
	GatewayID  string `json:"gateway_id"`
	CertPEM    []byte `json:"cert_pem"`
	GatewayCA  []byte `json:"gateway_ca_pem"`
	RunnerCA   []byte `json:"runner_ca_pem"`
	HubSubject string `json:"hub_subject"`
}

// ErrAlreadyAdopted is a second adoption against a gateway that has an owner.
var ErrAlreadyAdopted = errors.New("adopt: already adopted")

// Install accepts an identity from the hub.
//
// The certificate must be over a key this gateway actually holds — the pending
// one, or the current one on a re-delivery. Installing a certificate whose key
// we do not have would leave the hub believing the gateway was reachable while
// every handshake failed, which is the worst of both states.
func (s *State) Install(a Adoption) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if a.GatewayID == "" {
		return errors.New("adopt: the hub named no gateway id")
	}
	if s.doc.GatewayID != "" && s.doc.GatewayID != a.GatewayID {
		// One gateway, one owner. A different id means a different hub, or a
		// record that was recreated rather than rotated.
		return fmt.Errorf("adopt: this gateway is %s; the hub offered an identity for %s",
			s.doc.GatewayID, a.GatewayID)
	}
	blk, _ := pem.Decode(a.CertPEM)
	if blk == nil {
		return errors.New("adopt: the issued certificate is not PEM")
	}
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return fmt.Errorf("adopt: parsing the issued certificate: %w", err)
	}
	if leaf.Subject.CommonName != a.GatewayID {
		return fmt.Errorf("adopt: the certificate names %q, not %q", leaf.Subject.CommonName, a.GatewayID)
	}

	key, keyFile, err := s.keyFor(leaf)
	if err != nil {
		return err
	}
	if len(a.GatewayCA) == 0 {
		return errors.New("adopt: the hub sent no gateway CA, so the gateway could not verify it back")
	}

	// Write the CA material first: a certificate installed without the CA that
	// verifies its peers would leave the listener requiring client certificates
	// it cannot check.
	if err := s.write(gatewayCAFile, a.GatewayCA); err != nil {
		return err
	}
	if len(a.RunnerCA) > 0 {
		if err := s.write(runnerCAFile, a.RunnerCA); err != nil {
			return err
		}
	}
	if keyFile == pendingKeyFile {
		keyPEM, err := encodeECKey(key)
		if err != nil {
			return err
		}
		if err := s.write(identityKeyFile, keyPEM); err != nil {
			return err
		}
	}
	if err := s.write(identityFile, a.CertPEM); err != nil {
		return err
	}

	keyPEM, err := encodeECKey(key)
	if err != nil {
		return err
	}
	pair, err := tls.X509KeyPair(a.CertPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("adopt: the issued certificate does not match its key: %w", err)
	}
	pair.Leaf = leaf

	gw, gwRoot, err := s.loadCA(gatewayCAFile)
	if err != nil {
		return err
	}
	s.gatewayCA, s.gwRoot = gw, gwRoot
	if len(a.RunnerCA) > 0 {
		rn, rnRoot, err := s.loadCA(runnerCAFile)
		if err != nil {
			return err
		}
		s.runnerCA, s.rnRoot = rn, rnRoot
	}

	first := s.identity == nil
	s.identity = &pair
	s.doc.GatewayID = a.GatewayID
	s.doc.HubSubject = a.HubSubject
	s.doc.Version = StateVersion
	if first {
		s.doc.AdoptedAt = time.Now().UTC()
	}
	if err := s.save(); err != nil {
		return err
	}
	// The pending key has been promoted; a stale one left behind would be
	// handed out by the next CSR call over a key the hub already signed.
	s.pending = nil
	_ = os.Remove(s.path(pendingKeyFile))
	// Burn the install token. It was never on disk, and from here the pinned
	// key does everything it did — a surviving copy would only be a standing
	// credential on a DMZ host.
	s.token = ""
	return nil
}

// keyFor finds the private key matching an issued certificate.
func (s *State) keyFor(leaf *x509.Certificate) (*ecdsa.PrivateKey, string, error) {
	pub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, "", fmt.Errorf("adopt: unsupported key type %T in the issued certificate", leaf.PublicKey)
	}
	if s.pending != nil && s.pending.PublicKey.Equal(pub) {
		return s.pending, pendingKeyFile, nil
	}
	if s.identity != nil {
		if cur, ok := s.identity.PrivateKey.(*ecdsa.PrivateKey); ok && cur.PublicKey.Equal(pub) {
			return cur, identityKeyFile, nil
		}
	}
	return nil, "", errors.New("adopt: the issued certificate is over a key this gateway does not hold")
}

// ControlTLS is the control listener's configuration.
//
// GetConfigForClient rather than a static config, because adoption flips the
// listener's whole posture WHILE IT IS RUNNING: before, it serves the anchor
// and asks for no client certificate; after, it serves the hub-issued identity
// and requires one. A config captured at start-up would leave a freshly
// adopted gateway still accepting anonymous callers until it restarted.
func (s *State) ControlTLS() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			s.mu.RLock()
			defer s.mu.RUnlock()
			cfg := &tls.Config{
				MinVersion: tls.VersionTLS13,
				// h2 collapses N parked polls from one runner onto one
				// connection (docs/bench-gateway.md).
				NextProtos: []string{"h2", "http/1.1"},
			}
			// The ANCHOR is served when there is no identity, and also when the
			// identity has EXPIRED. Continuing to present a dead certificate
			// would defeat the recovery path: the hub falls back to dialling
			// the pinned key, and the pin is over the anchor, so a gateway
			// still presenting its expired identity could never be reached
			// again (ADR-0049 §6).
			if s.identity == nil || time.Now().After(s.identity.Leaf.NotAfter) {
				cfg.Certificates = []tls.Certificate{s.anchor}
				cfg.ClientAuth = tls.NoClientCert
				return cfg, nil
			}
			cfg.Certificates = []tls.Certificate{*s.identity}
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
			// BOTH control CAs. The hub authenticates with a certificate from
			// the gateway CA; runners with one from the runner CA. They are
			// separate issuers on one listener, which is why authorisation
			// below attributes a role by ROOT rather than by common name.
			pool := x509.NewCertPool()
			if s.gwRoot != nil {
				pool.AddCert(s.gwRoot)
			}
			if s.rnRoot != nil {
				pool.AddCert(s.rnRoot)
			}
			cfg.ClientCAs = pool
			return cfg, nil
		},
	}
}

// PeerRole attributes a verified connection to the hub or to a runner, and
// returns the identity it proved.
//
// The role comes from WHICH CA the chain terminated at, not from the common
// name. Both CAs are trusted on this listener, so a name-based check would let
// a runner whose certificate happened to be named "hub" push configuration —
// and configuration is what carries route policy and TLS private keys. The
// issuer is the thing neither peer can choose.
func (s *State) PeerRole(cs *tls.ConnectionState) (Role, string) {
	if cs == nil || len(cs.VerifiedChains) == 0 {
		return RoleNone, ""
	}
	s.mu.RLock()
	gwRoot, rnRoot, hubSubject := s.gwRoot, s.rnRoot, s.doc.HubSubject
	s.mu.RUnlock()

	for _, chain := range cs.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		root := chain[len(chain)-1]
		leaf := chain[0]
		switch {
		case gwRoot != nil && root.Equal(gwRoot):
			// Issued by the gateway CA. Only the hub is issued from it as a
			// client, and the subject it must carry was fixed at adoption.
			if hubSubject != "" && leaf.Subject.CommonName != hubSubject {
				continue
			}
			return RoleHub, leaf.Subject.CommonName
		case rnRoot != nil && root.Equal(rnRoot):
			return RoleRunner, leaf.Subject.CommonName
		}
	}
	return RoleNone, ""
}

// --- pairing (ADR-0049 §1a) --------------------------------------------------

// Domain separators. Distinct strings stop a proof minted for one step being
// accepted at another — without them, the hub's hello proof would also satisfy
// the install check.
const (
	domainHubHello = "shift-gw-hub-hello"
	domainGWHello  = "shift-gw-gw-hello"
	domainInstall  = "shift-gw-install"
)

// ErrNoToken is a pairing attempt against a gateway that was given no install
// token. It is not an authentication failure — there is simply nothing to
// check against, and treating it as a pass would adopt on a missing config.
var ErrNoToken = errors.New("adopt: this gateway was started with no install token")

// Hello is what a gateway answers a pairing challenge with.
type Hello struct {
	Fingerprint string `json:"fingerprint"`
	CSR         []byte `json:"csr"`
	Proof       string `json:"proof"`
	Version     string `json:"version,omitempty"`
}

// Challenge is the hub's opening move.
type Challenge struct {
	Nonce string `json:"nonce"`
	Proof string `json:"proof"`
}

// Proof computes an HMAC over a domain, a nonce, a certificate fingerprint and
// an optional payload digest.
//
// Exported so the hub computes it with the same code path rather than a second
// implementation that agrees today and drifts later.
func Proof(token, domain, nonce, fingerprint string, digest []byte) string {
	m := hmac.New(sha256.New, []byte(token))
	// Length-prefixed, so no combination of fields can be re-split into a
	// different one that hashes the same.
	for _, part := range [][]byte{[]byte(domain), []byte(nonce), []byte(fingerprint), digest} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		m.Write(n[:])
		m.Write(part)
	}
	return hex.EncodeToString(m.Sum(nil))
}

// MaterialDigest is the digest the install proof commits to.
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

// Pair answers a hub's challenge.
//
// The hub's proof is checked against THIS gateway's fingerprint, which is what
// makes the exchange resistant to interception: a machine-in-the-middle
// terminates TLS with its own key, so the hub computes its proof over the
// interceptor's fingerprint and the check here fails.
func (s *State) Pair(c Challenge, version string) (*Hello, error) {
	s.mu.RLock()
	token, fp, adopted := s.token, s.anchorFP, s.identity != nil
	s.mu.RUnlock()

	if adopted {
		return nil, ErrAlreadyAdopted
	}
	if token == "" {
		return nil, ErrNoToken
	}
	if len(c.Nonce) < 32 {
		// The nonce is the hub's; a short one would let a captured proof be
		// replayed against a guessable challenge.
		return nil, errors.New("adopt: the pairing nonce is too short")
	}
	want := Proof(token, domainHubHello, c.Nonce, fp, nil)
	if subtle.ConstantTimeCompare([]byte(c.Proof), []byte(want)) != 1 {
		return nil, errors.New("adopt: the caller does not hold this gateway's install token")
	}
	csr, err := s.CSR()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(csr)
	return &Hello{
		Fingerprint: fp,
		CSR:         csr,
		Proof:       Proof(token, domainGWHello, c.Nonce, fp, sum[:]),
		Version:     version,
	}, nil
}

// CheckInstall verifies the hub's proof over the material it is delivering.
func (s *State) CheckInstall(a Adoption) error {
	s.mu.RLock()
	token, fp := s.token, s.anchorFP
	s.mu.RUnlock()
	if token == "" {
		return ErrNoToken
	}
	if len(a.Nonce) < 32 {
		return errors.New("adopt: the pairing nonce is too short")
	}
	want := Proof(token, domainInstall, a.Nonce, fp, MaterialDigest(a))
	if subtle.ConstantTimeCompare([]byte(a.Proof), []byte(want)) != 1 {
		return errors.New("adopt: the offered identity is not vouched for by this gateway's install token")
	}
	return nil
}

// --- helpers -----------------------------------------------------------------

// Fingerprint is the hex SHA-256 of a public key in DER SubjectPublicKeyInfo
// form — the value an operator carries to the hub.
func Fingerprint(pub any) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("adopt: fingerprint: %w", err)
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:]), nil
}

// selfSign builds the anchor's certificate. It is a wrapper for a key, nothing
// more: nobody verifies it by chain or by name, only by fingerprint.
func selfSign(key *ecdsa.PrivateKey) ([]byte, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("adopt: serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "shift-gateway (unadopted)"},
		NotBefore:    now.Add(-time.Minute),
		// Long-lived because it is never trusted on its own merits. A short
		// expiry here would only add a way for the anchor to stop working.
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("adopt: self-signing the anchor: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

func encodeECKey(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("adopt: encoding a key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), nil
}

func parseECKey(raw []byte) (*ecdsa.PrivateKey, error) {
	blk, _ := pem.Decode(raw)
	if blk == nil {
		return nil, errors.New("not PEM")
	}
	return x509.ParseECPrivateKey(blk.Bytes)
}

// write persists a file through a temporary name, so a crash mid-write leaves
// the previous contents rather than a truncated one.
func (s *State) write(name string, data []byte) error {
	// name is always one of this package's file-name constants; nothing from
	// the network reaches it.
	tmp := s.path(name + ".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil { // #nosec G703 -- constant file name, see above
		return fmt.Errorf("adopt: writing %s: %w", name, err)
	}
	if err := os.Rename(tmp, s.path(name)); err != nil { // #nosec G703 -- constant file name, see above
		return fmt.Errorf("adopt: writing %s: %w", name, err)
	}
	return nil
}

func (s *State) save() error {
	raw, err := json.MarshalIndent(s.doc, "", "  ")
	if err != nil {
		return fmt.Errorf("adopt: encoding state: %w", err)
	}
	return s.write(stateFile, append(raw, '\n'))
}
