package e2e

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/consign"
	"github.com/jackc/pgx/v5"
)

const genFlow = `{"name":"signed",
  "source":{"connector":"gen","action":"gen","config":{"records":1000}},
  "sink":{"connector":"gen","action":"discard"}}`

// TestSignedArtifactPath proves the registry supply chain end to end:
// a connector binary is signed at "build time", published through the
// hub registry, and a real runnerd with NO local connectors and
// -require-signed fetches, verifies, and executes it. Then the DB blob
// is tampered with and the same path fails closed.
func TestSignedArtifactPath(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("needs postgres + real processes")
	}

	dsn := pgtest.DSN(t)
	st, err := store.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		LeaseTTL:   5 * time.Second,
		LeasePoll:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub := httptest.NewServer(h)
	t.Cleanup(hub.Close)

	// "Publisher build machine": build the gen connector and sign it.
	bin := t.TempDir()
	build(t, bin, "runnerd", "github.com/aaron-au/shift/runner/cmd/runnerd")
	build(t, bin, "shift-connector-gen", "github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := os.ReadFile(filepath.Join(bin, "shift-connector-gen")) //nolint:gosec // test file we built
	if err != nil {
		t.Fatal(err)
	}
	digest, _, err := consign.HashReader(bytes.NewReader(artifact))
	if err != nil {
		t.Fatal(err)
	}
	// Extract the connector's descriptor (ADR-0018) and publish a v2-signed
	// artifact — the descriptor digest is bound into the signature.
	descriptor := describeConnector(t, filepath.Join(bin, "shift-connector-gen"))
	manifest := consign.Manifest{Name: "gen", Version: "1.0.0", OS: runtime.GOOS, Arch: runtime.GOARCH, Digest: digest}
	manifest.DescriptorDigest = sha256.Sum256(descriptor)
	sig := consign.Sign(priv, manifest)

	// Register the publisher key and upload the signed artifact.
	doJSON(t, hub.URL, "POST", "/api/v1/publisher-keys",
		fmt.Sprintf(`{"name":"e2e","public_key":%q}`, base64.StdEncoding.EncodeToString(pub)), nil)
	uploadArtifact(t, hub.URL, manifest, sig, artifact, descriptor, http.StatusCreated)

	// A tampered upload (bit-flipped signature) is rejected outright.
	badSig := append([]byte{}, sig...)
	badSig[0] ^= 0xff
	uploadArtifact(t, hub.URL, manifest, badSig, artifact, descriptor, http.StatusForbidden)

	// resolve serves the descriptor back (base64 of the exact signed bytes)
	// so the studio builder can render config forms with no runner online.
	var resolved struct {
		Descriptor string `json:"descriptor"`
	}
	doJSON(t, hub.URL, "GET",
		fmt.Sprintf("/api/v1/connectors/gen/resolve?os=%s&arch=%s", runtime.GOOS, runtime.GOARCH), "", &resolved)
	if resolved.Descriptor == "" {
		t.Fatal("resolve did not return a descriptor")
	}
	if got, err := base64.StdEncoding.DecodeString(resolved.Descriptor); err != nil || !bytes.Equal(got, descriptor) {
		t.Fatalf("resolved descriptor mismatch (err=%v)", err)
	}

	// Runner with an EMPTY connector dir: everything must come signed
	// from the registry.
	emptyDir := t.TempDir()
	cache := t.TempDir()
	runner := startSignedRunner(t, hub.URL, bin, emptyDir, cache, "signed-runner", "127.0.0.1:8391")
	defer func() { _ = runner.Process.Kill() }()

	doJSON(t, hub.URL, "PUT", "/api/v1/flows/signed", genFlow, nil)
	doJSON(t, hub.URL, "POST", "/api/v1/flows/signed/versions/1/publish", "", nil)
	var acc struct {
		TaskID string `json:"task_id"`
	}
	doJSON(t, hub.URL, "POST", "/api/v1/flows/signed/execute", `{"idempotency_key":"signed-1"}`, &acc)

	waitFor(t, 60*time.Second, func() (bool, string) {
		tk := getTask(t, hub.URL, acc.TaskID)
		return tk.Task.State == "completed", "task " + tk.Task.State
	})

	// The cached artifact came through the verified path.
	entries, err := os.ReadDir(cache)
	if err != nil || len(entries) == 0 {
		t.Fatalf("connector cache empty: %v", err)
	}

	// --- fail-closed: corrupt the registry blob directly in Postgres ---
	// A fresh runner (fresh cache, no pooled connector process) must
	// refetch — and refuse the tampered bytes before anything executes.
	_ = runner.Process.Kill()
	_ = runner.Wait()

	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	if _, err := conn.Exec(t.Context(),
		`UPDATE connector_blobs SET data = '\x6576696c'::bytea`); err != nil { // "evil"
		t.Fatal(err)
	}

	cache2 := t.TempDir()
	runner2 := startSignedRunner(t, hub.URL, bin, emptyDir, cache2, "signed-runner-2", "127.0.0.1:8392")
	defer func() { _ = runner2.Process.Kill() }()

	doJSON(t, hub.URL, "POST", "/api/v1/flows/signed/execute", `{"idempotency_key":"signed-2","max_attempts":1}`, &acc)
	waitFor(t, 60*time.Second, func() (bool, string) {
		tk := getTask(t, hub.URL, acc.TaskID)
		return tk.Task.State == "failed", "task " + tk.Task.State
	})
	tk := getTask(t, hub.URL, acc.TaskID)
	if !strings.Contains(tk.Task.Error, "digest mismatch") {
		t.Fatalf("tampered blob error = %q, want digest mismatch", tk.Task.Error)
	}
	// Nothing executable landed in the fresh cache.
	entries, _ = os.ReadDir(cache2)
	for _, e := range entries {
		if info, _ := e.Info(); info != nil && info.Mode().Perm()&0o100 != 0 {
			t.Fatalf("executable residue after tamper: %s", e.Name())
		}
	}
}

// describeConnector shells out to the connector's `describe` mode to get
// its canonical descriptor bytes — the same path shift-bootstrap uses.
func describeConnector(t *testing.T, bin string) []byte {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), bin, "describe").Output() //nolint:gosec // G204: binary we just built
	if err != nil {
		t.Fatalf("describe %s: %v", bin, err)
	}
	return bytes.TrimSuffix(out, []byte("\n"))
}

func uploadArtifact(t *testing.T, hubURL string, m consign.Manifest, sig, data, descriptor []byte, wantCode int) {
	t.Helper()
	url := fmt.Sprintf("%s/api/v1/connectors/%s/versions/%s?os=%s&arch=%s",
		hubURL, m.Name, m.Version, m.OS, m.Arch)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("X-Shift-Publisher-Key", "e2e")
	req.Header.Set("X-Shift-Signature", base64.StdEncoding.EncodeToString(sig))
	if len(descriptor) > 0 {
		req.Header.Set("X-Shift-Descriptor", base64.StdEncoding.EncodeToString(descriptor))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantCode {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		t.Fatalf("upload = %d, want %d: %s", resp.StatusCode, wantCode, raw)
	}
}

// startSignedRunner boots runnerd with -require-signed and a registry
// cache — no locally trusted binaries.
func startSignedRunner(t *testing.T, hubURL, bin, connectorDir, cache, name, listen string) *exec.Cmd {
	t.Helper()
	var tok struct {
		Token string `json:"token"`
	}
	doJSON(t, hubURL, "POST", "/api/v1/runner-tokens", `{}`, &tok)

	cmd := exec.CommandContext(t.Context(), filepath.Join(bin, "runnerd"), //nolint:gosec // G204: binary we just built
		"-hub", hubURL, "-listen", listen, "-connector-dir", connectorDir,
		"-connector-cache", cache, "-require-signed", "-name", name)
	cmd.Env = append(os.Environ(), "SHIFT_HUB_REG_TOKEN="+tok.Token)
	cmd.Stdout = testWriter{t, name}
	cmd.Stderr = testWriter{t, name}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return cmd
}
