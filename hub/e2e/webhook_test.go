package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

// webhookPayload is the distinctive marker the whole test hunts for. It rides
// in the webhook request body (the data plane, runner-only) and must appear
// NOWHERE in anything the hub stores or serves — the ADR-0016 doctrine that
// payload never touches the hub.
const webhookPayload = "WEBHOOK-PAYLOAD-DO-NOT-LEAK-e2e"

// hookToken is the per-webhook credential guarding the public trigger.
const hookToken = "hook-s3cret-e2e-token" //nolint:gosec // G101: test-only value, not a credential

// webhookFlow is the flow the webhook drives: an @webhook source (the request
// body is the input, parsed as NDJSON) into a discard sink. It carries only
// config, never payload — the sentinel below never appears here.
const webhookFlow = `{"name":"webhook-ingest",
  "source":{"connector":"@webhook","action":"ndjson"},
  "sink":{"connector":"gen","action":"discard"}}`

// TestWebhookIngressReportedAsMetadataOnly proves the ADR-0016 webhook
// ingress path end to end with a real runnerd process:
//
//  1. A webhook POST to the runner's public trigger executes the flow on the
//     runner (returns 202 + task_id; wrong/missing token → 401; unknown hook
//     → 404).
//  2. The runner reports the execution to the hub as METADATA ONLY
//     (flow_name, trigger=webhook, terminal state, record counts).
//  3. The distinctive request payload NEVER appears in anything the hub
//     stored or serves — payload never crosses the control plane.
func TestWebhookIngressReportedAsMetadataOnly(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("needs postgres + real processes")
	}

	// Hub: real store + API over a fresh database.
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
		LeaseTTL:   5 * time.Second,
		LeasePoll:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub := httptest.NewServer(h)
	t.Cleanup(hub.Close)

	// Build runnerd + the gen connector once into a shared bin dir.
	bin := t.TempDir()
	build(t, bin, "runnerd", "github.com/aaron-au/shift/runner/cmd/runnerd")
	build(t, bin, "shift-connector-gen", "github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")

	// Configure the webhook on the HUB — the authoritative, doctrine-correct
	// path for a hub-attached runner. The hub carries only metadata + the
	// flow config (never payload): deploy + publish the @webhook flow, then
	// bind a token-protected webhook to it. The runner's config-sync loop
	// pulls this and keeps the local registry populated, so there is no race
	// with a locally-PUT hook being clobbered by a sync.
	doJSON(t, hub.URL, "PUT", "/api/v1/flows/webhook-ingest", webhookFlow, nil)
	doJSON(t, hub.URL, "POST", "/api/v1/flows/webhook-ingest/versions/1/publish", "", nil)
	doJSON(t, hub.URL, "PUT", "/api/v1/webhooks/ingest",
		fmt.Sprintf(`{"flow_name":"webhook-ingest","token":%q}`, hookToken), nil)

	// A real runnerd attached to the hub, reachable over HTTP on its listen
	// address (its control API + public /hooks trigger).
	const listen = "127.0.0.1:18351"
	runnerURL := "http://" + listen
	startRunner(t, hub.URL, bin, "webhook-runner", listen)

	// Wait for the runner's HTTP surface to come up AND for the hub-configured
	// webhook to arrive over the config-sync plane (it stays present — every
	// sync Replace includes it).
	waitFor(t, 30*time.Second, func() (bool, string) {
		code, body := runnerGet(t, runnerURL+"/api/webhooks")
		if code != http.StatusOK {
			return false, "runner not ready / webhooks list unavailable"
		}
		var list struct {
			Webhooks []string `json:"webhooks"`
		}
		_ = json.Unmarshal([]byte(body), &list)
		for _, n := range list.Webhooks {
			if n == "ingest" {
				return true, ""
			}
		}
		return false, "hook not yet synced: " + body
	})

	// --- negative auth/routing checks on the public trigger ---

	body := webhookNDJSON()

	// Unknown hook name → 404.
	if code, _ := runnerReq(t, http.MethodPost, runnerURL+"/hooks/does-not-exist", hookToken, body); code != http.StatusNotFound {
		t.Fatalf("unknown hook = %d, want 404", code)
	}
	// Missing token → 401.
	if code, _ := runnerReq(t, http.MethodPost, runnerURL+"/hooks/ingest", "", body); code != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", code)
	}
	// Wrong token → 401.
	if code, _ := runnerReq(t, http.MethodPost, runnerURL+"/hooks/ingest", "wrong-token", body); code != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d, want 401", code)
	}

	// --- the real trigger: correct token → 202 + task id ---
	code, respBody := runnerReq(t, http.MethodPost, runnerURL+"/hooks/ingest", hookToken, body)
	if code != http.StatusAccepted {
		t.Fatalf("trigger = %d, want 202: %s", code, respBody)
	}
	var acc struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(respBody), &acc); err != nil || acc.TaskID == "" {
		t.Fatalf("trigger response = %q (err %v), want a task_id", respBody, err)
	}

	// The runner reports the execution to the hub as metadata once it
	// finishes. Poll the hub's admin executions list until it lands.
	type exec = store.DirectExecution
	var got exec
	waitFor(t, 60*time.Second, func() (bool, string) {
		var out struct {
			Executions []exec `json:"executions"`
		}
		doJSON(t, hub.URL, "GET", "/api/v1/executions?limit=50", "", &out)
		for _, e := range out.Executions {
			if e.FlowName == "webhook-ingest" && e.Trigger == "webhook" {
				got = e
				return true, ""
			}
		}
		return false, "no webhook execution reported yet"
	})

	// Metadata is what we expect: webhook trigger, terminal completed state,
	// and the record count of the 3-line NDJSON body.
	if got.State != "completed" {
		t.Errorf("reported state = %q, want completed (err %q)", got.State, got.Error)
	}
	if got.RecordsIn != 3 {
		t.Errorf("reported records_in = %d, want 3 (the NDJSON body lines)", got.RecordsIn)
	}

	// --- the doctrine-critical assertion: payload NEVER reached the hub ---
	//
	// The executions list serializes every column RecordDirectExecution
	// stores, so a scan of its raw response effectively proves no stored
	// column holds the payload. Also scan the queued-tasks list (a webhook
	// run never enters the queue, so this is a second, orthogonal check).
	execRaw, ec := rawGet(t, hub.URL, "/api/v1/executions?limit=50")
	if ec != http.StatusOK || strings.Contains(execRaw, webhookPayload) {
		t.Fatalf("hub executions leaked the webhook payload (code %d)", ec)
	}
	// A webhook run is never queued, so the task list must not exist for it —
	// and certainly must not contain the payload.
	taskRaw, tc := rawGet(t, hub.URL, "/api/v1/tasks")
	if tc != http.StatusOK || strings.Contains(taskRaw, webhookPayload) {
		t.Fatalf("hub task list leaked the webhook payload (code %d)", tc)
	}
	if strings.Contains(taskRaw, "webhook-ingest") {
		t.Fatalf("webhook flow appeared in the hub queue; it must be direct-only: %s", taskRaw)
	}
}

// webhookNDJSON builds a 3-record NDJSON body, every line carrying the
// distinctive payload marker.
func webhookNDJSON() string {
	line := `{"marker":"` + webhookPayload + `","n":`
	return line + `1}` + "\n" + line + `2}` + "\n" + line + `3}` + "\n"
}

// runnerReq issues a request to the runner. A non-empty token is sent as the
// per-webhook credential (X-Webhook-Token). Returns status code and body.
func runnerReq(t *testing.T, method, url, token, body string) (int, string) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-Webhook-Token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Transport error (e.g. runner not listening yet): return a
		// non-fatal sentinel so waitFor-based probes can retry.
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(raw)
}

// runnerGet is a bodyless GET against the runner.
func runnerGet(t *testing.T, url string) (int, string) {
	t.Helper()
	return runnerReq(t, http.MethodGet, url, "", "")
}
