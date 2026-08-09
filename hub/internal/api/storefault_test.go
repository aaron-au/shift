package api_test

// TC-008: the store-failure error arms of the control API.
//
// These are the paths a coverage report cannot reach by asking politely — a
// statement failing in the middle of a multi-statement store operation. What
// each test asserts is not "an error came back" but what an operator or a
// runner would actually observe afterwards: the status, the ADR-0023 envelope,
// and above all that the half-finished operation left NOTHING behind. A 500
// that also burned a single-use token, spent an attempt or metered a task
// twice is a far worse outcome than the 500 itself.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

// newFaultServer is newServer plus a handle on the same database, so a test
// can break one statement inside the store and then look at what survived.
func newFaultServer(t *testing.T, opts api.Options) (*httptest.Server, *faultDB) {
	t.Helper()
	dsn := pgtest.DSN(t)
	st, err := store.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if opts.AdminToken == "" {
		opts.AdminToken = adminToken
	}
	if opts.LeaseTTL == 0 {
		// Long enough that no lease expires under a test that deliberately
		// stalls a statement — a reaped lease would look like the fault.
		opts.LeaseTTL = 60 * time.Second
	}
	if opts.LeasePoll == 0 {
		opts.LeasePoll = 20 * time.Millisecond
	}
	h, err := api.Handler(st, opts)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, newFaultDB(t, dsn)
}

// wantErrorEnvelope asserts the ADR-0023 shape on a body a client would have
// to branch on. Checked here rather than assumed: an error arm that returns
// the right status with the wrong body is still a broken API.
func wantErrorEnvelope(t *testing.T, where, body string, status int) {
	t.Helper()
	var env struct {
		Error struct {
			Status  int    `json:"status"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("%s: body %q is not the ADR-0023 error envelope: %v", where, body, err)
	}
	if env.Error.Status != status {
		t.Errorf("%s: envelope status = %d, want %d (body %q)", where, env.Error.Status, status, body)
	}
	if env.Error.Message == "" {
		t.Errorf("%s: envelope carries no message, so nothing is actionable (body %q)", where, body)
	}
	wantNoInternalLeak(t, where, body)
}

// wantNoInternalLeak keeps a store failure from becoming an information
// disclosure. A 500 travels to whoever called; the statement that failed, the
// file it lives in and any credential in the request must not travel with it.
func wantNoInternalLeak(t *testing.T, where, body string, secrets ...string) {
	t.Helper()
	for _, bad := range []string{
		"SELECT ", "INSERT INTO", "UPDATE ", "DELETE FROM", " WHERE ",
		"github.com/aaron-au/shift", ".go:", "goroutine ", "postgres://", "password=",
	} {
		if strings.Contains(body, bad) {
			t.Errorf("%s: response leaks internal detail %q: %s", where, bad, body)
		}
	}
	for _, s := range secrets {
		if s != "" && strings.Contains(body, s) {
			t.Errorf("%s: response echoes a credential from the request: %s", where, body)
		}
	}
}

// A registration token is single-use. If the runners INSERT fails after the
// token row has been consumed in the same transaction, the operator is left
// holding a token that the hub has silently spent — the runner can never join,
// and nothing says why. The transaction is what prevents that, so this is the
// test that proves the transaction is really there.
func TestAStoreFailureMidRegistrationDoesNotBurnTheSingleUseToken(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{})
	faults.install(pgFault{
		Label: "runner_insert", Table: "runners", Op: "INSERT",
		SQLState: sqlStateSerializationFailure, Message: "injected: registration lost a race",
	})

	var tok struct{ Token string }
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok); c != http.StatusCreated {
		t.Fatalf("runner-token = %d", c)
	}

	faults.enable("runner_insert")
	body, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`)
	if code != http.StatusInternalServerError {
		t.Fatalf("register during a store failure = %d, want 500 (body %q)", code, body)
	}
	wantErrorEnvelope(t, "POST /api/v1/runners/register", body, http.StatusInternalServerError)
	wantNoInternalLeak(t, "POST /api/v1/runners/register", body, tok.Token)
	if n := faults.fired("runner_insert"); n != 1 {
		t.Fatalf("the fault fired %d times; the 500 came from somewhere else", n)
	}

	// Nothing partial: no runner row, and the token is still unspent.
	var list struct{ Runners []map[string]any }
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/runners", adminToken, "", &list); c != http.StatusOK {
		t.Fatalf("runners = %d", c)
	}
	if len(list.Runners) != 0 {
		t.Fatalf("a failed registration left %d runner(s) behind", len(list.Runners))
	}
	if n := faults.count(`SELECT count(*) FROM runner_registration_tokens WHERE used_at IS NOT NULL`); n != 0 {
		t.Fatal("the failed registration spent the single-use token; the operator can never deploy that runner")
	}

	// And the SAME token still works once the database is healthy, which is
	// the whole point of not having burned it.
	faults.disable("runner_insert")
	var reg struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); c != http.StatusCreated || reg.Secret == "" {
		t.Fatalf("retry after recovery = %d, want 201 with a secret", c)
	}
}

// Claim leases the task and writes its attempt row in one transaction. If the
// attempt row fails, a task left "leased" with an incremented attempt would be
// stranded until the lease expired, and would have silently consumed one of
// its at-least-once retries (ADR-0002).
func TestAStoreFailureMidClaimLeavesTheTaskQueuedAndItsAttemptUnspent(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{})
	deployPublish(t, srv.URL)
	secret := registerRunner(t, srv.URL, "claim-runner")

	var acc struct {
		TaskID string `json:"task_id"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/flows/orders/execute", adminToken,
		`{"idempotency_key":"claim-fault"}`, &acc); c != http.StatusAccepted {
		t.Fatalf("execute = %d", c)
	}

	faults.install(pgFault{
		Label: "attempt_insert", Table: "task_attempts", Op: "INSERT",
		SQLState: sqlStateUniqueViolation, Message: "injected: attempt row already exists",
	})
	faults.enable("attempt_insert")

	body, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/lease", secret, `{"wait_seconds":0}`)
	if code != http.StatusInternalServerError {
		t.Fatalf("lease during a store failure = %d, want 500 (body %q)", code, body)
	}
	wantErrorEnvelope(t, "POST /api/v1/lease", body, http.StatusInternalServerError)
	wantNoInternalLeak(t, "POST /api/v1/lease", body, secret)

	var got struct {
		Task struct {
			State    string `json:"state"`
			Attempt  int    `json:"attempt"`
			LeasedBy string `json:"leased_by"`
		} `json:"task"`
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/tasks/"+acc.TaskID, adminToken, "", &got); c != http.StatusOK {
		t.Fatalf("get task = %d", c)
	}
	if got.Task.State != "queued" || got.Task.Attempt != 0 || got.Task.LeasedBy != "" {
		t.Fatalf("after a failed claim the task is %+v, want it queued, unattempted and unleased", got.Task)
	}
	if n := faults.count(`SELECT count(*) FROM task_attempts`); n != 0 {
		t.Fatalf("a failed claim left %d attempt row(s) behind", n)
	}

	// Recovered: the task is claimable, on its FIRST attempt.
	faults.disable("attempt_insert")
	var lease struct {
		Task struct {
			ID      string `json:"id"`
			Attempt int    `json:"attempt"`
		} `json:"task"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/lease", secret, `{"wait_seconds":5}`, &lease); c != http.StatusOK {
		t.Fatalf("lease after recovery = %d, want 200", c)
	}
	if lease.Task.ID != acc.TaskID || lease.Task.Attempt != 1 {
		t.Fatalf("claimed %+v, want task %s on attempt 1 — the failed claim consumed a retry",
			lease.Task, acc.TaskID)
	}
}

// A completion writes the task's terminal state, its attempt row and its
// metering row in one transaction (M6d). If metering fails, a task recorded as
// completed but never metered would be invisible to billing forever — and the
// runner, seeing a 500, would report it again.
func TestAStoreFailureMidResultReportLeavesTheTaskLeasedAndUnmetered(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{})
	deployPublish(t, srv.URL)
	secret := registerRunner(t, srv.URL, "result-runner")
	taskID := leaseOneTask(t, srv.URL, secret)

	faults.install(pgFault{
		Label: "usage_insert", Table: "usage_events", Op: "INSERT",
		SQLState: sqlStateConnectionFailure, Message: "injected: connection lost while metering",
	})
	faults.enable("usage_insert")

	body, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/tasks/"+taskID+"/complete", secret,
		`{"records_in":10,"records_out":10}`)
	if code != http.StatusInternalServerError {
		t.Fatalf("complete during a store failure = %d, want 500 (body %q)", code, body)
	}
	wantErrorEnvelope(t, "POST /api/v1/tasks/{id}/complete", body, http.StatusInternalServerError)

	var got struct {
		Task struct {
			State  string          `json:"state"`
			Result json.RawMessage `json:"result"`
		} `json:"task"`
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/tasks/"+taskID, adminToken, "", &got); c != http.StatusOK {
		t.Fatalf("get task = %d", c)
	}
	if got.Task.State != "leased" {
		t.Fatalf("task state = %q after a failed completion, want it still leased so the runner can retry", got.Task.State)
	}
	if len(got.Task.Result) != 0 && string(got.Task.Result) != "null" {
		t.Fatalf("a failed completion stored a result (%s); the task would report work it never finished", got.Task.Result)
	}
	if n := faults.count(`SELECT count(*) FROM usage_events`); n != 0 {
		t.Fatalf("a failed completion metered %d event(s)", n)
	}

	// The runner's retry succeeds and meters EXACTLY once — the at-least-once
	// contract must not become bill-twice.
	faults.disable("usage_insert")
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/tasks/"+taskID+"/complete", secret,
		`{"records_in":10,"records_out":10}`, nil); c != http.StatusNoContent {
		t.Fatalf("complete after recovery = %d, want 204", c)
	}
	if n := faults.count(`SELECT count(*) FROM usage_events`); n != 1 {
		t.Fatalf("usage events = %d, want exactly 1", n)
	}
	if n := faults.count(`SELECT count(*) FROM task_attempts WHERE finished_at IS NULL`); n != 0 {
		t.Fatal("the attempt row was never finished, so the task's history says it is still running")
	}
}

// The same seam, reached the other way: the caller gives up (or the hub's own
// deadline fires) while a statement is in flight. Nothing answers the client,
// so the only thing that can be asserted is the thing that matters — the
// database is not left half-written.
func TestAResultReportCancelledMidStatementLeavesNoHalfFinishedTask(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{})
	deployPublish(t, srv.URL)
	secret := registerRunner(t, srv.URL, "cancel-runner")
	taskID := leaseOneTask(t, srv.URL, secret)

	// No SQLSTATE: the statement stalls rather than failing, so the failure
	// the store sees is the CALLER's context expiring mid-transaction.
	faults.install(pgFault{
		Label: "usage_stall", Table: "usage_events", Op: "INSERT", DelayMS: 3000,
	})
	faults.enable("usage_stall")

	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		srv.URL+"/api/v1/tasks/"+taskID+"/complete", strings.NewReader(`{"records_in":1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
		t.Fatalf("the stalled completion answered %d; the client should have given up first", resp.StatusCode)
	}

	// Give the server's own goroutine a moment to notice the cancellation and
	// roll back before looking at what it left.
	faults.disable("usage_stall")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if n := faults.count(`SELECT count(*) FROM tasks WHERE state = 'leased'`); n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("after a cancelled completion the task is not leased; the abandoned request left a terminal state behind")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := faults.count(`SELECT count(*) FROM usage_events`); n != 0 {
		t.Fatalf("a cancelled completion metered %d event(s)", n)
	}

	// And the runner's retry still works.
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/tasks/"+taskID+"/complete", secret, `{}`, nil); c != http.StatusNoContent {
		t.Fatalf("complete after the cancellation = %d, want 204", c)
	}
}

// The race named in coverage.thresholds: adoption learns the gateway's
// fingerprint and then marks the record adopted, in two statements. If the
// second fails, the hub must NOT report an adoption it did not record — an
// operator who believed it would leave a gateway the reconcile loop never
// picks up, and never know.
func TestAnAdoptionThatFailsWhileMarkingAdoptedDoesNotClaimTheGatewayIsAdopted(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{Gateways: gwpush.New(gatewayCA(t), nil, 5*time.Second)})
	g := newUnadoptedGateway(t, "placeholder")

	var created map[string]any
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"`+g.srv.URL+`"}`, &created); c != http.StatusCreated {
		t.Fatalf("create = %d", c)
	}
	id, _ := created["id"].(string)
	g.token, _ = created["install_token"].(string)

	// WHEN narrows the trigger to MarkGatewayAdopted: the fingerprint UPDATE
	// that runs first leaves adopted_at alone and slips past.
	faults.install(pgFault{
		Label: "mark_adopted", Table: "gateways", Op: "UPDATE",
		When:     "NEW.adopted_at IS NOT NULL AND OLD.adopted_at IS NULL",
		SQLState: sqlStateSerializationFailure, Message: "injected: marking adoption lost a race",
	})
	faults.enable("mark_adopted")

	body, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "")
	if code != http.StatusInternalServerError {
		t.Fatalf("adopt with the adoption mark failing = %d, want 500 (body %q)", code, body)
	}
	wantErrorEnvelope(t, "POST /api/v1/gateways/{id}/adopt", body, http.StatusInternalServerError)
	wantNoInternalLeak(t, "POST /api/v1/gateways/{id}/adopt", body, g.token)
	if n := faults.fired("mark_adopted"); n != 1 {
		t.Fatalf("the fault fired %d times; the 500 came from somewhere else", n)
	}

	var after map[string]any
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", &after); c != http.StatusOK {
		t.Fatalf("get = %d", c)
	}
	if after["adopted_at"] != nil {
		t.Fatal("the hub recorded an adoption whose write failed")
	}
	if s, _ := after["cert_serial"].(string); s != "" {
		t.Fatalf("cert_serial = %q after a failed adoption; renewal would chase an identity the hub never recorded", s)
	}
	// The install token WAS spent by the statement that succeeded, so a retry
	// is a conflict rather than a silent half-repair. That is the documented
	// state (ADR-0049 §4): rotation, not retry, is the way out.
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); c != http.StatusConflict {
		t.Fatalf("retrying the adoption = %d, want 409 — the install token was spent", c)
	}

	faults.disable("mark_adopted")
	var rotated map[string]string
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/rotate", adminToken, "", &rotated); c != http.StatusOK {
		t.Fatalf("rotate = %d, want 200 — the documented recovery from a failed adoption", c)
	}
	g.token = rotated["install_token"]
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("adopt after rotation = %d, want 200", c)
	}
}

// The other half of the same race: the fingerprint write fails. Nothing was
// spent, so the operator's retry must simply work — a failed pairing that
// quietly consumed the install token would cost a redeploy.
func TestAnAdoptionThatFailsWhileLearningTheFingerprintLeavesTheTokenSpendable(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{Gateways: gwpush.New(gatewayCA(t), nil, 5*time.Second)})
	g := newUnadoptedGateway(t, "placeholder")

	var created map[string]any
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"`+g.srv.URL+`"}`, &created); c != http.StatusCreated {
		t.Fatalf("create = %d", c)
	}
	id, _ := created["id"].(string)
	g.token, _ = created["install_token"].(string)

	faults.install(pgFault{
		Label: "learn_fingerprint", Table: "gateways", Op: "UPDATE",
		When:     "NEW.fingerprint <> OLD.fingerprint",
		SQLState: sqlStateConnectionFailure, Message: "injected: connection lost while learning the key",
	})
	faults.enable("learn_fingerprint")

	body, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "")
	if code != http.StatusInternalServerError {
		t.Fatalf("adopt with the fingerprint write failing = %d, want 500 (body %q)", code, body)
	}
	wantErrorEnvelope(t, "POST /api/v1/gateways/{id}/adopt", body, http.StatusInternalServerError)

	var after map[string]any
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+id, adminToken, "", &after); c != http.StatusOK {
		t.Fatalf("get = %d", c)
	}
	if after["fingerprint"] != "" || after["adopted_at"] != nil {
		t.Fatalf("a failed pairing left state behind: %+v", after)
	}

	faults.disable("learn_fingerprint")
	var adopted map[string]any
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/gateways/"+id+"/adopt", adminToken, "", &adopted); c != http.StatusOK {
		t.Fatalf("adopt after recovery = %d, want 200 — the install token should never have been spent", c)
	}
	if adopted["fingerprint"] == "" || adopted["adopted_at"] == nil {
		t.Fatalf("adoption after recovery did not complete: %+v", adopted)
	}
}

// The harness's own contract, because a fault that fires on the wrong call
// would make every test above lie about which arm it reached: calls before the
// Nth go through untouched, and the count survives the rollback the fault
// causes (it is a sequence, not a column).
func TestAFaultTargetedAtTheNthCallLetsTheEarlierCallsThrough(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{})
	faults.install(pgFault{
		Label: "second_runner", Table: "runners", Op: "INSERT", FailOn: 2,
		SQLState: sqlStateUniqueViolation, Message: "injected: the second registration fails",
	})
	faults.enable("second_runner")

	if s := registerRunner(t, srv.URL, "first"); s == "" {
		t.Fatal("the first registration was refused; the fault fired too early")
	}
	var tok struct{ Token string }
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok); c != http.StatusCreated {
		t.Fatalf("runner-token = %d", c)
	}
	body, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"second"}`)
	if code != http.StatusInternalServerError {
		t.Fatalf("the second registration = %d, want 500 (body %q)", code, body)
	}
	wantErrorEnvelope(t, "POST /api/v1/runners/register", body, http.StatusInternalServerError)
	if n := faults.fired("second_runner"); n != 2 {
		t.Fatalf("the fault counted %d firings, want 2", n)
	}

	// The third call still fails: the rolled-back second call did not rewind
	// the counter, so "from the 2nd onwards" still means what it said.
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"third"}`, nil); c != http.StatusInternalServerError {
		t.Fatalf("the third registration = %d, want 500", c)
	}
}

// A broken connection rather than a raised error code — a failover, a restart,
// an OOM kill. The pool must recover on its own; every request it loses on the
// way must fail loudly, and the caller's retries must not enqueue the work
// twice (ADR-0002: the idempotency key is what makes a retry safe).
//
// Driven from the ADMIN realm deliberately. The runner realm authenticates by
// a hashed lookup against the same database, so during an outage a runner is
// answered 401 rather than 500 — see the report accompanying TC-008; it is
// pre-existing behaviour, not something these tests should pin as correct.
func TestTheHubRecoversWhenPostgresDropsItsConnections(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{})
	deployPublish(t, srv.URL)

	faults.killConnections()

	const body = `{"idempotency_key":"reconnect-1"}`
	var accepted bool
	for attempt := 1; attempt <= 20 && !accepted; attempt++ {
		raw, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/flows/orders/execute", adminToken, body)
		switch code {
		case http.StatusAccepted, http.StatusOK:
			accepted = true
		case http.StatusInternalServerError:
			// Never a 404 or a 409: either would tell the caller the request
			// can never succeed, and they would stop retrying a hub that was
			// about to come back.
			wantErrorEnvelope(t, "POST /api/v1/flows/{name}/execute", raw, http.StatusInternalServerError)
			time.Sleep(50 * time.Millisecond)
		default:
			t.Fatalf("execute during a connection drop = %d, want 500 or 202 (body %q)", code, raw)
		}
	}
	if !accepted {
		t.Fatal("the hub never recovered its connections")
	}
	if n := faults.count(`SELECT count(*) FROM tasks WHERE idempotency_key = 'reconnect-1'`); n != 1 {
		t.Fatalf("tasks enqueued = %d, want exactly 1 — the retries through the outage duplicated the work", n)
	}
}

// leaseOneTask publishes work, claims it as the given runner and returns its
// id — the "task is leased by me" state every result-reporting arm starts in.
func leaseOneTask(t *testing.T, url, secret string) string {
	t.Helper()
	if c := call(t, http.MethodPost, url+"/api/v1/flows/orders/execute", adminToken, `{}`, nil); c != http.StatusAccepted {
		t.Fatalf("execute = %d", c)
	}
	var lease struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if c := call(t, http.MethodPost, url+"/api/v1/lease", secret, `{"wait_seconds":5}`, &lease); c != http.StatusOK {
		t.Fatalf("lease = %d", c)
	}
	if lease.Task.ID == "" {
		t.Fatal("lease returned no task")
	}
	return lease.Task.ID
}
