package e2e

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

// capturePayload is the distinctive marker this test hunts for. It exists
// ONLY in the data plane: the origin server below serves it, the runner's
// test-mode capture retains it, and nothing the hub stores or logs may ever
// contain it (ADR-0014: the sample is runner-only; the hub never sees payload).
//
// Deliberately not present anywhere in the flow document — a marker that rode
// in config would prove nothing about payload.
const capturePayload = "CAPTURE-PAYLOAD-DO-NOT-LEAK-e2e"

// captureFlowName is the flow the runner executes in test mode. Unlike the
// payload it IS hub-visible (the runner reports the execution as metadata),
// which makes it the positive control for the hub-state sweep: if the sweep
// cannot find the flow name, it is not looking where the run was recorded and
// its silence about the payload would mean nothing.
const captureFlowName = "capture-testmode-e2e"

// fillerFlow is the cheapest task the runner can run — the built-in @webhook
// source into the built-in @discard sink, no connector subprocess at all. It
// exists only to push tasks through the runner's bounded ring so the captured
// task is evicted.
const fillerFlow = `{"name":"capture-filler",
  "source":{"connector":"@webhook","action":"ndjson"},
  "sink":{"connector":"@discard"}}`

// fillerToken guards the filler webhook.
const fillerToken = "filler-hook-token-e2e" //nolint:gosec // G101: test-only value, not a credential

// taskRingLimit mirrors task.NewStore's default (500). The runner exposes no
// flag for it, so evicting a task means genuinely running that many more.
const taskRingLimit = 500

// TestTestModeCaptureStaysOnTheRunnerAndNeverReachesTheHub proves the
// hub-blindness half of ADR-0014 end to end, with a real runnerd process:
//
//  1. A flow runs on the runner in TEST MODE (capture on) over a payload the
//     hub never supplied and never sees.
//  2. The capture IS retrievable from the RUNNER's own API — without this the
//     rest of the test would pass trivially with the feature switched off.
//     Capture is the feature most likely to break hub-blindness precisely
//     because its whole job is to retain payload.
//  3. That payload appears in NOTHING the hub persisted (every row of every
//     table, raw and hex- and base64-encoded) or logged, even though the hub
//     did record the run as metadata.
//  4. The capture is ephemeral: it is evicted with its task from the runner's
//     bounded in-memory ring, so it cannot outlive the runner.
func TestTestModeCaptureStaysOnTheRunnerAndNeverReachesTheHub(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("needs postgres + real processes")
	}

	// Hub: real store + API over a fresh database. The DSN is kept so the
	// sweep below can read every table directly rather than trusting the API
	// to serialize everything it stores.
	dsn := pgtest.DSN(t)
	st, err := store.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The hub logs through the default slog logger, so redirecting it here
	// captures everything the hub process would have written to stdout.
	hubLog := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(hubLog, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

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

	// The data plane: an origin the RUNNER pulls from directly. The hub never
	// touches this server, which is the point — the marker enters the system
	// through payload alone.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i := 1; i <= 5; i++ {
			_, _ = fmt.Fprintf(w, "{\"marker\":%q,\"n\":%d}\n", capturePayload, i)
		}
	}))
	t.Cleanup(origin.Close)

	bin := t.TempDir()
	build(t, bin, "runnerd", "github.com/aaron-au/shift/runner/cmd/runnerd")
	build(t, bin, "shift-connector-http", "github.com/aaron-au/shift/connectors/cmd/shift-connector-http")

	// The filler flow is registered on the HUB (the config-sync plane owns the
	// registry on an attached runner; a locally-PUT hook would be clobbered by
	// the next sync).
	doJSON(t, hub.URL, "PUT", "/api/v1/flows/capture-filler", fillerFlow, nil)
	doJSON(t, hub.URL, "POST", "/api/v1/flows/capture-filler/versions/1/publish", "", nil)
	doJSON(t, hub.URL, "PUT", "/api/v1/webhooks/capture-filler",
		fmt.Sprintf(`{"flow_name":"capture-filler","token":%q}`, fillerToken), nil)

	// Capture the runner's own log output: the sample is redacted and
	// runner-only, and it must not be printed even there.
	runnerLog := &syncBuffer{}
	const listen = "127.0.0.1:18361"
	runnerURL := "http://" + listen
	startRunnerCapture(t, hub.URL, bin, "capture-runner", listen, runnerLog)

	waitFor(t, 30*time.Second, func() (bool, string) {
		code, body := runnerGet(t, runnerURL+"/api/status")
		return code == http.StatusOK, "runner not ready: " + body
	})

	// --- the test-mode run: capture ON, over the distinctive payload ---
	//
	// The document goes straight to the runner (ADR-0016 direct execution), so
	// the hub sees only the metadata report that follows.
	doc := fmt.Sprintf(`{"name":%q,
	  "source":{"connector":"http","action":"get","config":{"url":%q,"allow_local":true}},
	  "sink":{"connector":"@discard"}}`, captureFlowName, origin.URL+"/data.ndjson")
	code, body := runnerReq(t, http.MethodPost, runnerURL+"/api/flows/execute?capture=1&capture_max=5", "", doc)
	if code != http.StatusAccepted {
		t.Fatalf("execute = %d, want 202: %s", code, body)
	}
	var acc struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(body), &acc); err != nil || acc.TaskID == "" {
		t.Fatalf("execute response = %q (err %v), want a task_id", body, err)
	}

	waitFor(t, 60*time.Second, func() (bool, string) {
		_, tb := runnerGet(t, runnerURL+"/api/tasks/"+acc.TaskID)
		var tv struct {
			State string `json:"state"`
			Error string `json:"error"`
		}
		_ = json.Unmarshal([]byte(tb), &tv)
		if tv.State == "failed" {
			t.Fatalf("test-mode run failed: %s", tv.Error)
		}
		return tv.State == "completed", "task state " + tv.State
	})

	// (a) The capture happened. Everything after this is only meaningful
	// because payload really was retained on the runner: with capture off the
	// hub-blindness assertions below would hold for the trivial reason that
	// nothing anywhere held the payload.
	code, captureBody := runnerGet(t, runnerURL+"/api/tasks/"+acc.TaskID+"/capture")
	if code != http.StatusOK {
		t.Fatalf("runner capture endpoint = %d: %s", code, captureBody)
	}
	var capt struct {
		Captured []struct {
			StepID  string            `json:"step_id"`
			Records []json.RawMessage `json:"records"`
		} `json:"captured"`
	}
	if err := json.Unmarshal([]byte(captureBody), &capt); err != nil {
		t.Fatalf("capture response: %v: %s", err, captureBody)
	}
	if len(capt.Captured) == 0 {
		t.Fatal("the runner captured no steps: test mode did not take a sample, " +
			"so the hub-blindness assertions that follow would pass vacuously")
	}
	if !strings.Contains(captureBody, capturePayload) {
		t.Fatalf("the capture does not contain the payload it sampled; "+
			"the rest of this test would be vacuous: %s", captureBody)
	}

	// The hub must have recorded the run — as METADATA. Without this the
	// sweep would be searching a hub that knows nothing about the execution.
	var reported store.DirectExecution
	waitFor(t, 60*time.Second, func() (bool, string) {
		var out struct {
			Executions []store.DirectExecution `json:"executions"`
		}
		doJSON(t, hub.URL, "GET", "/api/v1/executions?limit=100", "", &out)
		for _, e := range out.Executions {
			if e.FlowName == captureFlowName {
				reported = e
				return true, ""
			}
		}
		return false, "no direct execution reported yet"
	})
	if reported.State != "completed" {
		t.Errorf("reported state = %q, want completed (err %q)", reported.State, reported.Error)
	}
	if reported.RecordsIn != 5 {
		t.Errorf("reported records_in = %d, want 5", reported.RecordsIn)
	}

	// (b) The doctrine assertion: the payload reached nothing the hub kept.

	// b.1 — everything the hub serves back about executions and tasks.
	for _, path := range []string{"/api/v1/executions?limit=100", "/api/v1/tasks", "/api/v1/flows"} {
		raw, sc := rawGet(t, hub.URL, path)
		if sc != http.StatusOK {
			t.Fatalf("GET %s = %d", path, sc)
		}
		if strings.Contains(raw, capturePayload) {
			t.Fatalf("hub API %s leaked the captured payload", path)
		}
	}

	// b.2 — every row of every table, not just the columns the API chooses to
	// serialize. Encodings are checked too: a payload written into a bytea
	// column renders as hex, and one that travelled through a JSON envelope
	// could be base64 — a plain-text-only search would miss both.
	needles := []string{
		capturePayload,
		hex.EncodeToString([]byte(capturePayload)),
		base64.StdEncoding.EncodeToString([]byte(capturePayload)),
	}
	for _, needle := range needles {
		if where := sweepDatabase(t, dsn, needle); len(where) > 0 {
			t.Fatalf("the captured payload is at rest in the hub database: %v", where)
		}
	}
	// Positive control for the sweep itself: the flow NAME is metadata the hub
	// legitimately stores, so the sweep must find it. If it does not, the sweep
	// is looking in the wrong place and its silence above proves nothing.
	if where := sweepDatabase(t, dsn, captureFlowName); len(where) == 0 {
		t.Fatal("the database sweep found no trace of the flow name; it is not " +
			"inspecting the tables the execution was recorded in, so its " +
			"payload result is meaningless")
	}

	// b.3 — logs, on both sides of the control plane.
	if !strings.Contains(hubLog.String(), "hub.request") {
		t.Fatal("captured no hub log output; the log assertion below is vacuous")
	}
	if strings.Contains(hubLog.String(), capturePayload) {
		t.Fatal("hub logs leaked the captured payload")
	}
	if strings.Contains(runnerLog.String(), capturePayload) {
		t.Fatal("runner logs leaked the captured payload (the sample is for the API, not the log)")
	}

	// --- ephemerality: the capture dies with its task ---
	//
	// The sample lives in the runner's bounded in-memory ring and nowhere else
	// (ADR-0014). Push enough tasks through to evict it and the capture is
	// gone — which is also why it cannot outlive the runner process.
	waitFor(t, 60*time.Second, func() (bool, string) {
		code, list := runnerGet(t, runnerURL+"/api/webhooks")
		return code == http.StatusOK && strings.Contains(list, "capture-filler"),
			"filler hook not yet synced from the hub"
	})
	for range taskRingLimit + 20 {
		code, resp := runnerReq(t, http.MethodPost, runnerURL+"/hooks/capture-filler", fillerToken, "{\"n\":1}\n")
		if code != http.StatusAccepted {
			t.Fatalf("filler trigger = %d: %s", code, resp)
		}
	}
	waitFor(t, 60*time.Second, func() (bool, string) {
		code, resp := runnerGet(t, runnerURL+"/api/tasks/"+acc.TaskID+"/capture")
		if code == http.StatusNotFound {
			return true, ""
		}
		return false, fmt.Sprintf("capture still retrievable (%d): %.120s", code, resp)
	})
}

// sweepDatabase returns the tables whose rows contain needle anywhere, in any
// column. Casting the whole row to text is what makes this a sweep rather than
// a spot check: it covers columns added after this test was written, which is
// exactly when a payload leak would slip in unnoticed.
func sweepDatabase(t *testing.T, dsn, needle string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("sweep: connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx, `SELECT table_schema, table_name FROM information_schema.tables
	  WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')`)
	if err != nil {
		t.Fatalf("sweep: list tables: %v", err)
	}
	type tbl struct{ schema, name string }
	var tables []tbl
	for rows.Next() {
		var tb tbl
		if err := rows.Scan(&tb.schema, &tb.name); err != nil {
			t.Fatalf("sweep: scan table list: %v", err)
		}
		tables = append(tables, tb)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("sweep: list tables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("sweep: the hub database has no tables; the migration did not run")
	}

	var found []string
	for _, tb := range tables {
		// Identifiers come from information_schema and are sanitized; the
		// needle is a bound parameter.
		q := "SELECT count(*) FROM " + pgx.Identifier{tb.schema, tb.name}.Sanitize() + " r WHERE r::text LIKE $1"
		var n int64
		if err := conn.QueryRow(ctx, q, "%"+needle+"%").Scan(&n); err != nil {
			t.Fatalf("sweep %s.%s: %v", tb.schema, tb.name, err)
		}
		if n > 0 {
			found = append(found, fmt.Sprintf("%s.%s (%d rows)", tb.schema, tb.name, n))
		}
	}
	return found
}
