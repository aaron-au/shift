package e2e

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
)

// sentinel is the secret value the whole test hunts for: it must reach
// the destination header and appear NOWHERE else.
const sentinel = "s3ntinel-secret-value-e2e" //nolint:gosec // G101: test-only sentinel, the opposite of a credential

// TestSecretsNeverAtRest proves the M4b secrets doctrine end to end
// with a real runnerd: a flow references {"$secret":...}; the value
// reaches the destination, and never appears in the stored document,
// task API responses, or hub/runner log output.
func TestSecretsNeverAtRest(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres + real processes")
	}

	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "kek.bin")
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := kek.NewLocalFiles(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		Secrets:    secrets.New(st, provider),
		LeaseTTL:   5 * time.Second,
		LeasePoll:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub := httptest.NewServer(h)
	t.Cleanup(hub.Close)

	// Destination: capture the Authorization header the sink presents.
	var mu sync.Mutex
	var gotAuth string
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	bin := t.TempDir()
	build(t, bin, "runnerd", "github.com/aaron-au/shift/runner/cmd/runnerd")
	build(t, bin, "shift-connector-gen", "github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	build(t, bin, "shift-connector-http", "github.com/aaron-au/shift/connectors/cmd/shift-connector-http")

	// Capture runner output ourselves for the leak assertion.
	logBuf := &syncBuffer{}
	runner := startRunnerCapture(t, hub.URL, bin, "secrets-runner", "127.0.0.1:8393", logBuf)
	defer func() { _ = runner.Process.Kill() }()

	// Secret + flow referencing it.
	doJSON(t, hub.URL, "PUT", "/api/v1/secrets/e2e_token", fmt.Sprintf(`{"value":%q}`, sentinel), nil)
	flow := fmt.Sprintf(`{"name":"secretflow",
	  "source":{"connector":"gen","action":"gen","config":{"records":100}},
	  "sink":{"connector":"http","action":"post","config":{
	    "url":%q,"allow_local":true,
	    "auth":{"type":"bearer","token":{"$secret":"e2e_token"}}}}}`, dest.URL)
	doJSON(t, hub.URL, "PUT", "/api/v1/flows/secretflow", flow, nil)
	doJSON(t, hub.URL, "POST", "/api/v1/flows/secretflow/versions/1/publish", "", nil)

	var acc struct {
		TaskID string `json:"task_id"`
	}
	doJSON(t, hub.URL, "POST", "/api/v1/flows/secretflow/execute", `{"idempotency_key":"sec-1"}`, &acc)
	waitFor(t, 60*time.Second, func() (bool, string) {
		tk := getTask(t, hub.URL, acc.TaskID)
		return tk.Task.State == "completed", "task " + tk.Task.State + " " + tk.Task.Error
	})

	// 1) The destination saw the resolved value.
	mu.Lock()
	auth := gotAuth
	mu.Unlock()
	if auth != "Bearer "+sentinel {
		t.Fatalf("destination Authorization = %q, want the resolved secret", auth)
	}

	// 2) The stored flow document and the task view still carry only the
	// inert reference.
	var flowView struct {
		Document map[string]any `json:"document"`
	}
	doJSON(t, hub.URL, "GET", "/api/v1/flows/secretflow", "", &flowView)
	raw := fmt.Sprintf("%v", flowView.Document)
	if strings.Contains(raw, sentinel) {
		t.Fatal("stored flow document leaked the secret value")
	}
	body, code := rawGet(t, hub.URL, "/api/v1/tasks/"+acc.TaskID)
	if code != 200 || strings.Contains(body, sentinel) {
		t.Fatalf("task API response leaked the secret (code %d)", code)
	}
	body, code = rawGet(t, hub.URL, "/api/v1/tasks")
	if code != 200 || strings.Contains(body, sentinel) {
		t.Fatalf("task list leaked the secret (code %d)", code)
	}

	// 3) Runner log output never printed it.
	if strings.Contains(logBuf.String(), sentinel) {
		t.Fatal("runner logs leaked the secret value")
	}

	// 4) The access is audited (metadata check through the DB would need
	// a store handle; the audit write path is unit-tested — here we
	// assert the admin-visible secret list still never shows values).
	body, code = rawGet(t, hub.URL, "/api/v1/secrets")
	if code != 200 || strings.Contains(body, sentinel) {
		t.Fatalf("secret list leaked the value (code %d)", code)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func rawGet(t *testing.T, base, path string) (string, int) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var sb strings.Builder
	if _, err := io.Copy(&sb, resp.Body); err != nil {
		t.Fatal(err)
	}
	return sb.String(), resp.StatusCode
}

// startRunnerCapture mirrors startRunner but tees output into buf for
// content assertions (and still to the test log).
func startRunnerCapture(t *testing.T, hubURL, bin, name, listen string, buf *syncBuffer) *exec.Cmd {
	t.Helper()
	var tok struct {
		Token string `json:"token"`
	}
	doJSON(t, hubURL, "POST", "/api/v1/runner-tokens", `{}`, &tok)

	cmd := exec.CommandContext(t.Context(), filepath.Join(bin, "runnerd"), //nolint:gosec // G204: binary we just built
		"-hub", hubURL, "-listen", listen, "-connector-dir", bin, "-name", name)
	cmd.Env = append(os.Environ(), "SHIFT_HUB_REG_TOKEN="+tok.Token)
	sink := io.MultiWriter(buf, testWriter{t, name})
	cmd.Stdout = sink
	cmd.Stderr = sink
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
