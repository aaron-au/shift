// Command shift-bootstrap prepares the "just runs" compose bundle
// (M4b): a Go one-shot instead of curl/jq shell fragility.
//
//	shift-bootstrap certs  # pre-hubd: self-signed CA + hub cert + KEK into -dir
//	shift-bootstrap seed   # post-hubd: publisher key, sign+upload connectors,
//	                       # mint a runner token, optional demo flow
//
// Everything it writes stays inside the bootstrap volume; the dev
// publisher PRIVATE key never reaches the hub.
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aaron-au/shift/pkg/consign"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("shift-bootstrap: usage: shift-bootstrap certs|seed [flags]")
	}
	var err error
	switch os.Args[1] {
	case "certs":
		err = certs(os.Args[2:])
	case "seed":
		err = seed(os.Args[2:])
	default:
		err = fmt.Errorf("unknown mode %q (want certs|seed)", os.Args[1])
	}
	if err != nil {
		log.Fatalf("shift-bootstrap: %v", err)
	}
}

// certs generates the bundle's crypto material: a throwaway CA, a hub
// server cert (SANs: hubd, localhost), and the hub's KEK file.
// Idempotent: existing material is kept so restarts don't invalidate
// runner CA trust.
func certs(args []string) error {
	fs := flag.NewFlagSet("certs", flag.ExitOnError)
	dir := fs.String("dir", "/bootstrap", "output directory (the shared volume)")
	_ = fs.Parse(args)

	if _, err := os.Stat(filepath.Join(*dir, "hub.pem")); err == nil {
		log.Print("certs: material exists, keeping it")
		return nil
	}
	if err := os.MkdirAll(*dir, 0o700); err != nil {
		return err
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "SHIFT dev bundle CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(5, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "hubd"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(5, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"hubd", "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		return err
	}

	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		return err
	}

	for _, f := range []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644},
		{"hub.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}), 0o644},
		{"hub.key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER}), 0o600},
		{"kek.bin", kek, 0o600},
	} {
		if err := os.WriteFile(filepath.Join(*dir, f.name), f.data, f.mode); err != nil {
			return err
		}
	}
	log.Print("certs: wrote ca.pem, hub.pem, hub.key, kek.bin")
	return nil
}

// seed provisions the running hub: trusted publisher key, signed
// connector artifacts, a runner registration token, and (optionally) a
// demo flow on a minutely schedule.
func seed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	dir := fs.String("dir", "/bootstrap", "bootstrap volume")
	hubURL := fs.String("hub", envOr("SHIFT_HUB_URL", "https://hubd:8400"), "hub base URL")
	connDir := fs.String("connectors", "/usr/local/bin", "directory holding shift-connector-* binaries to publish")
	_ = fs.Parse(args)
	adminToken := os.Getenv("SHIFT_HUB_ADMIN_TOKEN")
	if adminToken == "" {
		return fmt.Errorf("seed: SHIFT_HUB_ADMIN_TOKEN is required")
	}

	client, err := tlsClient(filepath.Join(*dir, "ca.pem"))
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err := waitReady(ctx, client, *hubURL); err != nil {
		return err
	}
	hub := &hubAPI{client: client, base: strings.TrimRight(*hubURL, "/"), token: adminToken}

	// Publisher keypair (idempotent across restarts: reuse the saved seed).
	priv, pub, err := loadOrCreatePublisherKey(filepath.Join(*dir, "publisher.key"))
	if err != nil {
		return err
	}
	if err := hub.post("/api/v1/publisher-keys", map[string]string{
		"name": "bundle-dev", "public_key": base64.StdEncoding.EncodeToString(pub),
	}, true /* tolerate conflict on re-run */); err != nil {
		return err
	}

	// Sign + upload every connector binary present.
	entries, err := os.ReadDir(*connDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name, ok := strings.CutPrefix(e.Name(), "shift-connector-")
		if !ok {
			continue
		}
		if err := hub.uploadArtifact(filepath.Join(*connDir, e.Name()), name, "0.1.0", priv); err != nil {
			return fmt.Errorf("seed: publish %s: %w", name, err)
		}
		log.Printf("seed: published connector %q (signed)", name)
	}

	// Runner registration token → file (runnerd reads SHIFT_HUB_REG_TOKEN_FILE).
	var tok struct {
		Token string `json:"token"`
	}
	if err := hub.postInto("/api/v1/runner-tokens", map[string]int{"ttl_seconds": 3600}, &tok); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*dir, "runner-token"), []byte(tok.Token), 0o600); err != nil {
		return err
	}
	log.Print("seed: minted runner registration token")

	if os.Getenv("SHIFT_BOOTSTRAP_DEMO") == "1" {
		// A v2 step graph so the studio graph view has real content out of
		// the box: source → filter → sink on the happy path, with a
		// dead-letter handler off the source's onFailure.
		demo := `{"name":"demo","start":"in","steps":[` +
			`{"id":"in","type":"source","connector":"gen","action":"gen","config":{"records":10000},"onSuccess":"keep","onFailure":"dead"},` +
			`{"id":"keep","type":"filter","path":"$.active","op":"eq","value":true,"onComplete":"out"},` +
			`{"id":"out","type":"sink","connector":"gen","action":"discard"},` +
			`{"id":"dead","type":"sink","connector":"gen","action":"discard"}]}`
		if err := hub.put("/api/v1/flows/demo", demo); err != nil {
			return err
		}
		if err := hub.post("/api/v1/flows/demo/versions/1/publish", nil, true); err != nil {
			return err
		}
		if err := hub.put("/api/v1/flows/demo/schedule", `{"cron":"* * * * *"}`); err != nil {
			return err
		}
		log.Print("seed: demo flow deployed, published, scheduled every minute")
	}
	log.Print("seed: done")
	return nil
}

// --- helpers -----------------------------------------------------------------

type hubAPI struct {
	client *http.Client
	base   string
	token  string
}

func (h *hubAPI) do(method, path string, body []byte, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(context.Background(), method, h.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return h.client.Do(req)
}

func (h *hubAPI) post(path string, v any, tolerateConflict bool) error {
	var body []byte
	if v != nil {
		body, _ = json.Marshal(v)
	}
	resp, err := h.do(http.MethodPost, path, body, "application/json")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 300 {
		return nil
	}
	if tolerateConflict && (resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusUnprocessableEntity) {
		return nil // idempotent re-run (e.g. key already registered)
	}
	return fmt.Errorf("POST %s: %s", path, readBody(resp))
}

func (h *hubAPI) postInto(path string, v, out any) error {
	body, _ := json.Marshal(v)
	resp, err := h.do(http.MethodPost, path, body, "application/json")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: %s", path, readBody(resp))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (h *hubAPI) put(path, body string) error {
	resp, err := h.do(http.MethodPut, path, []byte(body), "application/json")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s: %s", path, readBody(resp))
	}
	return nil
}

func (h *hubAPI) uploadArtifact(path, name, version string, priv ed25519.PrivateKey) error {
	data, err := os.ReadFile(path) //nolint:gosec // G304: bundle's own binaries
	if err != nil {
		return err
	}
	digest, _, err := consign.HashReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	m := consign.Manifest{Name: name, Version: version, OS: runtime.GOOS, Arch: runtime.GOARCH, Digest: digest}

	// Extract the connector's action-catalog descriptor (ADR-0018) so the
	// studio builder can render config forms. Done by shelling out to the
	// connectors-module tool (the hub module must not import sdk/host) —
	// best-effort: a binary that can't be described is signed v1.
	descriptor := extractDescriptor(path)
	if len(descriptor) > 0 {
		m.DescriptorDigest = sha256.Sum256(descriptor)
	}
	sig := consign.Sign(priv, m)

	url := fmt.Sprintf("%s/api/v1/connectors/%s/versions/%s?os=%s&arch=%s", h.base, name, version, m.OS, m.Arch)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("X-Shift-Publisher-Key", "bundle-dev")
	req.Header.Set("X-Shift-Signature", base64.StdEncoding.EncodeToString(sig))
	if len(descriptor) > 0 {
		req.Header.Set("X-Shift-Descriptor", base64.StdEncoding.EncodeToString(descriptor))
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("upload: %s", readBody(resp))
	}
	return nil
}

// extractDescriptor runs `<connector> describe` to obtain its canonical
// action-catalog bytes (ADR-0018). Shelling out keeps the hub module free
// of any sdk/host dependency. Best-effort: on failure the artifact is
// signed v1 (schema-less).
func extractDescriptor(path string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "describe").Output() //nolint:gosec // G204: path is the bundle's own connector binary being published
	if err != nil {
		log.Printf("warn: descriptor extraction for %s failed, publishing v1: %v", path, err)
		return nil
	}
	// describeToStdout appends one trailing newline; strip it so the bytes
	// match the connector's canonical descriptor exactly (digest stability).
	return bytes.TrimSuffix(out, []byte("\n"))
}

func loadOrCreatePublisherKey(path string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	if raw, err := os.ReadFile(path); err == nil { //nolint:gosec // G304: bootstrap volume path
		seed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil || len(seed) != ed25519.SeedSize {
			return nil, nil, fmt.Errorf("publisher key file %s is corrupt", path)
		}
		priv := ed25519.NewKeyFromSeed(seed)
		return priv, priv.Public().(ed25519.PublicKey), nil
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(priv.Seed())+"\n"), 0o600); err != nil {
		return nil, nil, err
	}
	return priv, pub, nil
}

func tlsClient(caFile string) (*http.Client, error) {
	pemBytes, err := os.ReadFile(caFile) //nolint:gosec // G304: bootstrap volume path
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no certs in %s", caFile)
	}
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}, nil
}

func waitReady(ctx context.Context, client *http.Client, base string) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/readyz", nil)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("hub at %s never became ready: %w", base, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func readBody(resp *http.Response) string {
	raw := make([]byte, 2048)
	n, _ := resp.Body.Read(raw)
	return fmt.Sprintf("%d: %s", resp.StatusCode, strings.TrimSpace(string(raw[:n])))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
