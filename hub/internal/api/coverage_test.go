package api_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/consign"
)

// registerRunner drives the real token→register chain and returns the
// runner's bearer secret.
func registerRunner(t *testing.T, url, name string) string {
	t.Helper()
	var tok struct{ Token string }
	if code := call(t, "POST", url+"/api/v1/runner-tokens", adminToken, `{}`, &tok); code != 201 {
		t.Fatalf("runner-token = %d", code)
	}
	var reg struct {
		Secret string `json:"secret"`
	}
	if code := call(t, "POST", url+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"`+name+`"}`, &reg); code != 201 || reg.Secret == "" {
		t.Fatalf("register = %d", code)
	}
	return reg.Secret
}

// callHdr issues a request with arbitrary headers and returns raw body + status.
func callHdr(t *testing.T, method, url string, hdr map[string]string, body []byte) (string, int) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw), resp.StatusCode
}

// deployPublish deploys and publishes goodFlow under name "orders".
func deployPublish(t *testing.T, url string) {
	t.Helper()
	if c := call(t, "PUT", url+"/api/v1/flows/orders", adminToken, goodFlow, nil); c != 201 {
		t.Fatalf("deploy = %d", c)
	}
	if c := call(t, "POST", url+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil); c != 200 {
		t.Fatalf("publish = %d", c)
	}
}

// --- simple list/get handlers ------------------------------------------------

func TestListAndGetHandlers(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	deployPublish(t, srv.URL)

	// listRunners
	if c := call(t, "GET", srv.URL+"/api/v1/runners", adminToken, "", nil); c != 200 {
		t.Fatalf("listRunners = %d", c)
	}
	// getFlow: present + missing
	if c := call(t, "GET", srv.URL+"/api/v1/flows/orders", adminToken, "", nil); c != 200 {
		t.Fatalf("getFlow present = %d", c)
	}
	if c := call(t, "GET", srv.URL+"/api/v1/flows/ghost", adminToken, "", nil); c != 404 {
		t.Fatalf("getFlow missing = %d", c)
	}
	// getFlowGraph missing → 404
	if c := call(t, "GET", srv.URL+"/api/v1/flows/ghost/graph", adminToken, "", nil); c != 404 {
		t.Fatalf("getFlowGraph missing = %d", c)
	}
	// listTasks with limit param
	if c := call(t, "GET", srv.URL+"/api/v1/tasks?limit=5", adminToken, "", nil); c != 200 {
		t.Fatalf("listTasks = %d", c)
	}
	// getTask missing → 404
	if c := call(t, "GET", srv.URL+"/api/v1/tasks/00000000-0000-0000-0000-000000000000", adminToken, "", nil); c != 404 {
		t.Fatalf("getTask missing = %d", c)
	}
	// publishFlow: bad version → 400, missing flow → 404
	if c := call(t, "POST", srv.URL+"/api/v1/flows/orders/versions/notanum/publish", adminToken, "", nil); c != 400 {
		t.Fatalf("publish bad version = %d", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/flows/ghost/versions/1/publish", adminToken, "", nil); c != 404 {
		t.Fatalf("publish missing flow = %d", c)
	}
	// dashboard root page + authinfo (public).
	if _, c := callHdr(t, "GET", srv.URL+"/", nil, nil); c != 200 {
		t.Fatalf("dashboard = %d", c)
	}
	var ai struct {
		OIDCLogin  bool `json:"oidc_login"`
		BreakGlass bool `json:"break_glass"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/authinfo", "", "", &ai); c != 200 || !ai.BreakGlass {
		t.Fatalf("authinfo = %d %+v", c, ai)
	}
	// Unknown path under "/" → 404.
	if _, c := callHdr(t, "GET", srv.URL+"/nope", nil, nil); c != 404 {
		t.Fatalf("unknown path = %d", c)
	}
}

// --- malformed bodies / bad requests -----------------------------------------

func TestMalformedBodies(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	deployPublish(t, srv.URL)

	// Malformed JSON → 400 on body-decoding handlers.
	bad := `{not json`
	if c := call(t, "POST", srv.URL+"/api/v1/runner-tokens", adminToken, bad, nil); c != 400 {
		t.Fatalf("createRunnerToken bad json = %d", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken, bad, nil); c != 400 {
		t.Fatalf("execute bad json = %d", c)
	}
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/orders/schedule", adminToken, bad, nil); c != 400 {
		t.Fatalf("putSchedule bad json = %d", c)
	}
	// register with missing fields → 400.
	if c := call(t, "POST", srv.URL+"/api/v1/runners/register", "", `{"token":"","name":""}`, nil); c != 400 {
		t.Fatalf("register empty = %d", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/runners/register", "", bad, nil); c != 400 {
		t.Fatalf("register bad json = %d", c)
	}
}

// --- schedules ---------------------------------------------------------------

func TestScheduleHandlers(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	// Schedule on a missing flow → 404.
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/ghost/schedule", adminToken,
		`{"cron":"* * * * *"}`, nil); c != 404 {
		t.Fatalf("schedule missing flow = %d", c)
	}

	// Deploy but do NOT publish → schedule is 409 (nothing to schedule).
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, nil); c != 201 {
		t.Fatalf("deploy = %d", c)
	}
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/orders/schedule", adminToken,
		`{"cron":"* * * * *"}`, nil); c != 409 {
		t.Fatalf("schedule unpublished = %d, want 409", c)
	}

	// Publish then schedule with an invalid cron → 422.
	if c := call(t, "POST", srv.URL+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil); c != 200 {
		t.Fatalf("publish = %d", c)
	}
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/orders/schedule", adminToken,
		`{"cron":"not a cron"}`, nil); c != 422 {
		t.Fatalf("bad cron = %d, want 422", c)
	}

	// Valid schedule → 201.
	if c := call(t, "PUT", srv.URL+"/api/v1/flows/orders/schedule", adminToken,
		`{"cron":"*/5 * * * *","enabled":true,"max_attempts":3}`, nil); c != 201 {
		t.Fatalf("putSchedule = %d, want 201", c)
	}
	// getSchedule present → 200.
	if c := call(t, "GET", srv.URL+"/api/v1/flows/orders/schedule", adminToken, "", nil); c != 200 {
		t.Fatalf("getSchedule = %d", c)
	}
	// getSchedule for a flow with no schedule → 404.
	if c := call(t, "GET", srv.URL+"/api/v1/flows/ghost/schedule", adminToken, "", nil); c != 404 {
		t.Fatalf("getSchedule missing = %d", c)
	}
	// listSchedules → 200.
	if c := call(t, "GET", srv.URL+"/api/v1/schedules", adminToken, "", nil); c != 200 {
		t.Fatalf("listSchedules = %d", c)
	}
	// deleteSchedule present → 204, then missing → 404.
	if c := call(t, "DELETE", srv.URL+"/api/v1/flows/orders/schedule", adminToken, "", nil); c != 204 {
		t.Fatalf("deleteSchedule = %d", c)
	}
	if c := call(t, "DELETE", srv.URL+"/api/v1/flows/orders/schedule", adminToken, "", nil); c != 404 {
		t.Fatalf("deleteSchedule missing = %d", c)
	}
}

// --- webhooks ----------------------------------------------------------------

func TestWebhookHandlers(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	deployPublish(t, srv.URL)

	// Missing flow_name → 422.
	if c := call(t, "PUT", srv.URL+"/api/v1/webhooks/hook1", adminToken, `{}`, nil); c != 422 {
		t.Fatalf("putWebhook no flow = %d, want 422", c)
	}
	// Unknown flow → 422.
	if c := call(t, "PUT", srv.URL+"/api/v1/webhooks/hook1", adminToken,
		`{"flow_name":"ghost"}`, nil); c != 422 {
		t.Fatalf("putWebhook unknown flow = %d, want 422", c)
	}
	// Valid, token-protected webhook → 200.
	var wh struct {
		Name      string `json:"name"`
		Protected bool   `json:"protected"`
	}
	if c := call(t, "PUT", srv.URL+"/api/v1/webhooks/hook1", adminToken,
		`{"flow_name":"orders","token":"sekret","enabled":true}`, &wh); c != 200 || !wh.Protected {
		t.Fatalf("putWebhook = %d %+v", c, wh)
	}
	// list shows metadata only (no token hash).
	body, c := call2t(t, "GET", srv.URL+"/api/v1/webhooks", adminToken, "")
	if c != 200 || !strings.Contains(body, "hook1") || strings.Contains(body, hashTokenTest("sekret")) {
		t.Fatalf("listWebhooks = %d body=%s", c, body)
	}
	// Runner-realm sync → 200.
	secret := registerRunner(t, srv.URL, "wh-runner")
	if c := call(t, "GET", srv.URL+"/api/v1/webhooks/sync", secret, "", nil); c != 200 {
		t.Fatalf("syncWebhooks = %d", c)
	}
	// Admin token cannot use the runner-only sync route → 401.
	if c := call(t, "GET", srv.URL+"/api/v1/webhooks/sync", adminToken, "", nil); c != 401 {
		t.Fatalf("syncWebhooks admin = %d, want 401", c)
	}
	// delete present → 204, then missing → 404.
	if c := call(t, "DELETE", srv.URL+"/api/v1/webhooks/hook1", adminToken, "", nil); c != 204 {
		t.Fatalf("deleteWebhook = %d", c)
	}
	if c := call(t, "DELETE", srv.URL+"/api/v1/webhooks/hook1", adminToken, "", nil); c != 404 {
		t.Fatalf("deleteWebhook missing = %d", c)
	}
}

// hashTokenTest mirrors webhooks.hashToken (hex SHA-256) so we can assert the
// stored hash never leaks through the list endpoint.
func hashTokenTest(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// --- direct executions (runner realm) ----------------------------------------

func TestDirectExecutionReporting(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	secret := registerRunner(t, srv.URL, "exec-runner")

	// Valid report → 201.
	var rep struct {
		ID string `json:"id"`
	}
	if c := call(t, "POST", srv.URL+"/api/v1/executions", secret,
		`{"flow_name":"orders","state":"completed","trigger":"webhook","records_in":3,"records_out":3}`, &rep); c != 201 || rep.ID == "" {
		t.Fatalf("reportExecution = %d %+v", c, rep)
	}
	// Trigger defaults to "api" when omitted.
	if c := call(t, "POST", srv.URL+"/api/v1/executions", secret,
		`{"flow_name":"orders","state":"failed"}`, nil); c != 201 {
		t.Fatalf("reportExecution default trigger = %d", c)
	}
	// Missing flow_name / bad state → 422.
	if c := call(t, "POST", srv.URL+"/api/v1/executions", secret,
		`{"state":"completed"}`, nil); c != 422 {
		t.Fatalf("report no flow = %d, want 422", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/executions", secret,
		`{"flow_name":"orders","state":"weird"}`, nil); c != 422 {
		t.Fatalf("report bad state = %d, want 422", c)
	}
	// Malformed JSON → 400.
	if c := call(t, "POST", srv.URL+"/api/v1/executions", secret, `{bad`, nil); c != 400 {
		t.Fatalf("report bad json = %d, want 400", c)
	}
	// Admin lists them → 200.
	var out struct {
		Executions []store.DirectExecution `json:"executions"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/executions?limit=10", adminToken, "", &out); c != 200 {
		t.Fatalf("listDirectExecutions = %d", c)
	}
	if len(out.Executions) < 2 {
		t.Fatalf("expected >=2 executions, got %d", len(out.Executions))
	}
}

// --- runner fail / heartbeat lease-lost paths --------------------------------

func TestRunnerFailAndLeaseLost(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	secret := registerRunner(t, srv.URL, "fail-runner")
	deployPublish(t, srv.URL)

	// Heartbeat/complete/fail against an unleased task → 409 (lease lost).
	if c := call(t, "POST", srv.URL+"/api/v1/tasks/00000000-0000-0000-0000-000000000000/heartbeat", secret, "", nil); c != 409 {
		t.Fatalf("heartbeat unleased = %d, want 409", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/tasks/00000000-0000-0000-0000-000000000000/complete", secret, `{}`, nil); c != 409 {
		t.Fatalf("complete unleased = %d, want 409", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/tasks/00000000-0000-0000-0000-000000000000/fail", secret, `{"error":"x"}`, nil); c != 409 {
		t.Fatalf("fail unleased = %d, want 409", c)
	}
	// Malformed fail body → 400.
	if c := call(t, "POST", srv.URL+"/api/v1/tasks/00000000-0000-0000-0000-000000000000/fail", secret, `{bad`, nil); c != 400 {
		t.Fatalf("fail bad json = %d, want 400", c)
	}

	// Enqueue with max_attempts=1, lease, then fail → terminal "failed".
	var acc struct {
		TaskID string `json:"task_id"`
	}
	if c := call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken,
		`{"idempotency_key":"fail-1","max_attempts":1}`, &acc); c != 202 {
		t.Fatalf("execute = %d", c)
	}
	var lease struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if c := call(t, "POST", srv.URL+"/api/v1/lease", secret, `{"wait_seconds":5}`, &lease); c != 200 {
		t.Fatalf("lease = %d", c)
	}
	var fr struct {
		State string `json:"state"`
	}
	if c := call(t, "POST", srv.URL+"/api/v1/tasks/"+lease.Task.ID+"/fail", secret,
		`{"error":"boom"}`, &fr); c != 200 || fr.State != "failed" {
		t.Fatalf("fail terminal = %d %+v", c, fr)
	}

	// Enqueue with retries, lease, fail → requeued "queued".
	if c := call(t, "POST", srv.URL+"/api/v1/flows/orders/execute", adminToken,
		`{"idempotency_key":"fail-2","max_attempts":3}`, &acc); c != 202 {
		t.Fatalf("execute retry = %d", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/lease", secret, `{"wait_seconds":5}`, &lease); c != 200 {
		t.Fatalf("lease retry = %d", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/tasks/"+lease.Task.ID+"/fail", secret,
		`{"error":"transient"}`, &fr); c != 200 || fr.State != "queued" {
		t.Fatalf("fail requeue = %d %+v", c, fr)
	}
}

// --- audit -------------------------------------------------------------------

func TestAuditListing(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	// Generate some audit rows (deploy + publish + runner-token).
	deployPublish(t, srv.URL)
	_ = registerRunner(t, srv.URL, "audit-runner")

	// JSON listing with filters + limit.
	var out struct {
		Audit []struct {
			ID     int64  `json:"id"`
			Action string `json:"action"`
		} `json:"audit"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/audit?limit=50", adminToken, "", &out); c != 200 {
		t.Fatalf("listAudit = %d", c)
	}
	if len(out.Audit) == 0 {
		t.Fatal("expected audit rows")
	}
	// Family-prefix filter.
	if c := call(t, "GET", srv.URL+"/api/v1/audit?action=flow.&limit=10", adminToken, "", nil); c != 200 {
		t.Fatalf("listAudit filtered = %d", c)
	}
	// before cursor.
	before := out.Audit[0].ID
	if c := call(t, "GET", srv.URL+"/api/v1/audit?before="+strconv.FormatInt(before, 10), adminToken, "", nil); c != 200 {
		t.Fatalf("listAudit before = %d", c)
	}
	// CSV export.
	body, c := call2t(t, "GET", srv.URL+"/api/v1/audit?format=csv", adminToken, "")
	if c != 200 || !strings.HasPrefix(body, "id,at,actor,action,entity,detail") {
		t.Fatalf("audit csv = %d body=%q", c, body[:min(40, len(body))])
	}
}

// --- publisher keys + connector registry -------------------------------------

func TestPublisherKeysAndConnectors(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	// addPublisherKey: bad body → 400.
	if c := call(t, "POST", srv.URL+"/api/v1/publisher-keys", adminToken, `{bad`, nil); c != 400 {
		t.Fatalf("addPublisherKey bad json = %d", c)
	}
	// Missing name / bad base64 → 400.
	if c := call(t, "POST", srv.URL+"/api/v1/publisher-keys", adminToken,
		`{"name":"","public_key":"!!!"}`, nil); c != 400 {
		t.Fatalf("addPublisherKey bad = %d", c)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var keyResp struct {
		ID string `json:"id"`
	}
	if c := call(t, "POST", srv.URL+"/api/v1/publisher-keys", adminToken,
		`{"name":"pub1","public_key":"`+base64.StdEncoding.EncodeToString(pub)+`"}`, &keyResp); c != 201 || keyResp.ID == "" {
		t.Fatalf("addPublisherKey = %d %+v", c, keyResp)
	}

	// listPublisherKeys via admin AND runner realm (adminOrRunner).
	if c := call(t, "GET", srv.URL+"/api/v1/publisher-keys", adminToken, "", nil); c != 200 {
		t.Fatalf("listPublisherKeys admin = %d", c)
	}
	secret := registerRunner(t, srv.URL, "reg-runner")
	if c := call(t, "GET", srv.URL+"/api/v1/publisher-keys", secret, "", nil); c != 200 {
		t.Fatalf("listPublisherKeys runner = %d", c)
	}
	// adminOrRunner rejects a bogus credential → 401.
	if c := call(t, "GET", srv.URL+"/api/v1/publisher-keys", "totally-bogus-cred", "", nil); c != 401 {
		t.Fatalf("listPublisherKeys bogus = %d, want 401", c)
	}

	// --- uploadConnector validation branches ---
	art := []byte("fake-connector-artifact-bytes")
	sum := sha256.Sum256(art)
	m := consign.Manifest{Name: "myconn", Version: "1.0.0", OS: "linux", Arch: "amd64"}
	copy(m.Digest[:], sum[:])
	sig := consign.Sign(priv, m)
	sigB64 := base64.StdEncoding.EncodeToString(sig)

	base := srv.URL + "/api/v1/connectors/myconn/versions/1.0.0?os=linux&arch=amd64"
	hdr := func(extra map[string]string) map[string]string {
		h := map[string]string{"Authorization": "Bearer " + adminToken}
		for k, v := range extra {
			h[k] = v
		}
		return h
	}

	// Bad connector name → 422.
	if _, c := callHdr(t, "PUT", srv.URL+"/api/v1/connectors/Bad_Name/versions/1?os=linux&arch=amd64",
		hdr(map[string]string{"X-Shift-Publisher-Key": "pub1", "X-Shift-Signature": sigB64}), art); c != 422 {
		t.Fatalf("upload bad name = %d, want 422", c)
	}
	// Unsupported os/arch → 422.
	if _, c := callHdr(t, "PUT", srv.URL+"/api/v1/connectors/myconn/versions/1.0.0?os=plan9&arch=amd64",
		hdr(map[string]string{"X-Shift-Publisher-Key": "pub1", "X-Shift-Signature": sigB64}), art); c != 422 {
		t.Fatalf("upload bad os = %d, want 422", c)
	}
	// Missing signature headers → 400.
	if _, c := callHdr(t, "PUT", base, hdr(nil), art); c != 400 {
		t.Fatalf("upload no sig = %d, want 400", c)
	}
	// Unknown publisher key → 403.
	if _, c := callHdr(t, "PUT", base,
		hdr(map[string]string{"X-Shift-Publisher-Key": "nope", "X-Shift-Signature": sigB64}), art); c != 403 {
		t.Fatalf("upload unknown key = %d, want 403", c)
	}
	// Empty artifact → 400.
	if _, c := callHdr(t, "PUT", base,
		hdr(map[string]string{"X-Shift-Publisher-Key": "pub1", "X-Shift-Signature": sigB64}), nil); c != 400 {
		t.Fatalf("upload empty = %d, want 400", c)
	}
	// Bad descriptor header → 400.
	if _, c := callHdr(t, "PUT", base,
		hdr(map[string]string{"X-Shift-Publisher-Key": "pub1", "X-Shift-Signature": sigB64, "X-Shift-Descriptor": "!!!"}), art); c != 400 {
		t.Fatalf("upload bad descriptor = %d, want 400", c)
	}
	// Tampered signature (valid base64, wrong bytes) → 403.
	badSig := append([]byte(nil), sig...)
	badSig[0] ^= 0xff
	if _, c := callHdr(t, "PUT", base,
		hdr(map[string]string{"X-Shift-Publisher-Key": "pub1", "X-Shift-Signature": base64.StdEncoding.EncodeToString(badSig)}), art); c != 403 {
		t.Fatalf("upload bad sig = %d, want 403", c)
	}
	// Valid upload → 201.
	var up struct {
		Name   string `json:"name"`
		Digest string `json:"digest"`
	}
	body, c := callHdr(t, "PUT", base,
		hdr(map[string]string{"X-Shift-Publisher-Key": "pub1", "X-Shift-Signature": sigB64}), art)
	if c != 201 {
		t.Fatalf("upload = %d body=%s", c, body)
	}
	_ = up

	// listConnectors → 200 and includes the uploaded one.
	list, c := call2t(t, "GET", srv.URL+"/api/v1/connectors", adminToken, "")
	if c != 200 || !strings.Contains(list, "myconn") {
		t.Fatalf("listConnectors = %d body=%s", c, list)
	}
	// resolveConnector present (via runner realm) → 200.
	if c := call(t, "GET", srv.URL+"/api/v1/connectors/myconn/resolve?os=linux&arch=amd64", secret, "", nil); c != 200 {
		t.Fatalf("resolveConnector = %d", c)
	}
	// resolveConnector missing → 404.
	if c := call(t, "GET", srv.URL+"/api/v1/connectors/ghost/resolve?os=linux&arch=amd64", adminToken, "", nil); c != 404 {
		t.Fatalf("resolveConnector missing = %d", c)
	}
	// downloadConnector present → 200 with verification headers.
	dhdr := map[string]string{"Authorization": "Bearer " + adminToken}
	dbody, dc := callHdrResp(t, "GET",
		srv.URL+"/api/v1/connectors/myconn/versions/1.0.0/artifact?os=linux&arch=amd64", dhdr)
	if dc.status != 200 || dbody != string(art) {
		t.Fatalf("downloadConnector = %d len=%d", dc.status, len(dbody))
	}
	if dc.digest == "" || dc.sig == "" {
		t.Fatalf("download missing verification headers: %+v", dc)
	}
	// downloadConnector missing → 404.
	if c := call(t, "GET", srv.URL+"/api/v1/connectors/ghost/versions/1/artifact?os=linux&arch=amd64", adminToken, "", nil); c != 404 {
		t.Fatalf("downloadConnector missing = %d", c)
	}

	// revokePublisherKey present → 204, missing → 404.
	if c := call(t, "DELETE", srv.URL+"/api/v1/publisher-keys/"+keyResp.ID, adminToken, "", nil); c != 204 {
		t.Fatalf("revokePublisherKey = %d", c)
	}
	if c := call(t, "DELETE", srv.URL+"/api/v1/publisher-keys/00000000-0000-0000-0000-000000000000", adminToken, "", nil); c != 404 {
		t.Fatalf("revokePublisherKey missing = %d", c)
	}
}

// TestConnectorVersionsAndYank (M6e) covers the version-history listing and
// the yank/restore lifecycle: a yanked version disappears from resolve/list
// (fail closed) but stays in the history; restore brings it back.
func TestConnectorVersionsAndYank(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/publisher-keys", adminToken,
		`{"name":"pub1","public_key":"`+base64.StdEncoding.EncodeToString(pub)+`"}`, nil); c != 201 {
		t.Fatalf("addPublisherKey = %d", c)
	}
	upload := func(version string) {
		art := []byte("artifact-" + version)
		sum := sha256.Sum256(art)
		m := consign.Manifest{Name: "myconn", Version: version, OS: "linux", Arch: "amd64"}
		copy(m.Digest[:], sum[:])
		h := map[string]string{
			"Authorization":         "Bearer " + adminToken,
			"X-Shift-Publisher-Key": "pub1",
			"X-Shift-Signature":     base64.StdEncoding.EncodeToString(consign.Sign(priv, m)),
		}
		url := srv.URL + "/api/v1/connectors/myconn/versions/" + version + "?os=linux&arch=amd64"
		if _, c := callHdr(t, "PUT", url, h, art); c != 201 {
			t.Fatalf("upload %s = %d", version, c)
		}
	}
	upload("1.0.0")
	upload("1.1.0")

	// Version history lists both, newest first.
	var vs struct {
		Versions []struct {
			Version  string `json:"version"`
			YankedAt string `json:"yanked_at"`
		} `json:"versions"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/connectors/myconn/versions", adminToken, "", &vs); c != 200 {
		t.Fatalf("versions = %d", c)
	}
	if len(vs.Versions) != 2 || vs.Versions[0].Version != "1.1.0" {
		t.Fatalf("versions = %+v", vs.Versions)
	}
	// Unknown connector → 404.
	if c := call(t, "GET", srv.URL+"/api/v1/connectors/ghost/versions", adminToken, "", nil); c != 404 {
		t.Fatalf("versions ghost = %d, want 404", c)
	}

	// Yank 1.1.0 → resolve now falls back to 1.0.0 (fail closed on the yanked).
	if c := call(t, "POST", srv.URL+"/api/v1/connectors/myconn/versions/1.1.0/yank", adminToken,
		`{"os":"linux","arch":"amd64","yanked":true}`, nil); c != 200 {
		t.Fatalf("yank = %d", c)
	}
	var res struct {
		Version string `json:"version"`
	}
	if c := call(t, "GET", srv.URL+"/api/v1/connectors/myconn/resolve?os=linux&arch=amd64", adminToken, "", &res); c != 200 || res.Version != "1.0.0" {
		t.Fatalf("resolve after yank = %d %q, want 1.0.0", c, res.Version)
	}
	// History still shows 1.1.0, now marked yanked.
	call(t, "GET", srv.URL+"/api/v1/connectors/myconn/versions", adminToken, "", &vs)
	var found bool
	for _, v := range vs.Versions {
		if v.Version == "1.1.0" {
			found = v.YankedAt != ""
		}
	}
	if !found {
		t.Fatalf("1.1.0 not marked yanked in history: %+v", vs.Versions)
	}
	// Yank a nonexistent version → 404.
	if c := call(t, "POST", srv.URL+"/api/v1/connectors/myconn/versions/9.9.9/yank", adminToken,
		`{"os":"linux","arch":"amd64"}`, nil); c != 404 {
		t.Fatalf("yank missing = %d, want 404", c)
	}
	// Restore 1.1.0 → resolve returns it again.
	if c := call(t, "POST", srv.URL+"/api/v1/connectors/myconn/versions/1.1.0/yank", adminToken,
		`{"os":"linux","arch":"amd64","yanked":false}`, nil); c != 200 {
		t.Fatalf("restore = %d", c)
	}
	if c := call(t, "GET", srv.URL+"/api/v1/connectors/myconn/resolve?os=linux&arch=amd64", adminToken, "", &res); c != 200 || res.Version != "1.1.0" {
		t.Fatalf("resolve after restore = %d %q, want 1.1.0", c, res.Version)
	}
}

type dlmeta struct {
	status      int
	digest, sig string
}

// callHdrResp is like callHdr but also captures the artifact verification
// response headers.
func callHdrResp(t *testing.T, method, url string, hdr map[string]string) (string, dlmeta) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw), dlmeta{
		status: resp.StatusCode,
		digest: resp.Header.Get("X-Shift-Digest"),
		sig:    resp.Header.Get("X-Shift-Signature"),
	}
}

// --- rotateKEK + secrets validation (needs OIDC/secrets server) --------------

func TestRotateKEKAndSecretValidation(t *testing.T) {
	srv, _, _ := newOIDCServer(t)

	// Store a secret, then rotate the KEK — every envelope is rewrapped.
	if c := call(t, "PUT", srv.URL+"/api/v1/secrets/api_key", adminToken,
		`{"value":"v1"}`, nil); c != 201 {
		t.Fatalf("put secret = %d", c)
	}
	// The test server uses a single static KEK, so nothing is stale to
	// rewrap — the handler still runs and returns a count (200).
	var rot struct {
		Rewrapped int `json:"rewrapped"`
	}
	if c := call(t, "POST", srv.URL+"/api/v1/keys/rotate", adminToken, "", &rot); c != 200 {
		t.Fatalf("rotateKEK = %d %+v", c, rot)
	}

	// putSecret with empty value → 400.
	if c := call(t, "PUT", srv.URL+"/api/v1/secrets/api_key", adminToken, `{"value":""}`, nil); c != 400 {
		t.Fatalf("put empty value = %d, want 400", c)
	}
	// putSecret malformed body → 400.
	if c := call(t, "PUT", srv.URL+"/api/v1/secrets/api_key", adminToken, `{bad`, nil); c != 400 {
		t.Fatalf("put bad json = %d, want 400", c)
	}
	// deleteSecret missing → 404.
	if c := call(t, "DELETE", srv.URL+"/api/v1/secrets/ghost", adminToken, "", nil); c != 404 {
		t.Fatalf("delete missing secret = %d, want 404", c)
	}
	// resolveSecrets: empty names → 400; too-many handled by count.
	secret := registerRunner(t, srv.URL, "sec-runner")
	if c := call(t, "POST", srv.URL+"/api/v1/secrets/resolve", secret, `{"names":[]}`, nil); c != 400 {
		t.Fatalf("resolve empty names = %d, want 400", c)
	}
	if c := call(t, "POST", srv.URL+"/api/v1/secrets/resolve", secret, `{bad`, nil); c != 400 {
		t.Fatalf("resolve bad json = %d, want 400", c)
	}
}
