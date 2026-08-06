package gwclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/hubclient"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/task"
)

// fakeStatus stands in for the hub. It records what the runner asserts, which
// is the point of several of these tests: the runner must pass the GATEWAY's
// facts through unaltered rather than invent identity.
type fakeStatus struct {
	mu       sync.Mutex
	accepted []hubclient.AcceptedExecution
	finished map[string]hubclient.ExecutionOutcome
	read     []readArgs
	status   *hubclient.ExecutionStatus
	readErr  error
	acceptEr error
}

type readArgs struct{ id, route, principal, token string }

func newFakeStatus() *fakeStatus {
	return &fakeStatus{finished: map[string]hubclient.ExecutionOutcome{}}
}

func (f *fakeStatus) AcceptExecution(_ context.Context, e hubclient.AcceptedExecution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.acceptEr != nil {
		return f.acceptEr
	}
	f.accepted = append(f.accepted, e)
	return nil
}

func (f *fakeStatus) FinishExecution(_ context.Context, id string, out hubclient.ExecutionOutcome) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finished[id] = out
	return nil
}

func (f *fakeStatus) ExecutionStatusByID(_ context.Context, id, route, principal, token string) (*hubclient.ExecutionStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.read = append(f.read, readArgs{id, route, principal, token})
	return f.status, f.readErr
}

func (f *fakeStatus) lastAccept(t *testing.T) hubclient.AcceptedExecution {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.accepted) == 0 {
		t.Fatal("nothing was recorded at accept")
	}
	return f.accepted[len(f.accepted)-1]
}

// The 202 now carries a status URL under the caller's OWN route (ADR-0042 §3),
// and the row exists BEFORE the promise — so the URL resolves the instant the
// caller receives it.
func TestAcceptRecordsThenReturnsAStatusURL(t *testing.T) {
	fs := newFakeStatus()
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)

	code, body, _ := l.execute(t.Context(), inboundWith("orders", map[string]string{
		hdrRoute: "/orders", hdrPrincipal: "acme-erp", hdrPublicBase: "https://api.example.com",
	}, `{"order_id":"A"}`))

	if code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", code, body)
	}
	var got accepted
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	want := "https://api.example.com/orders/" + statusSegment + "/" + got.Task
	if got.StatusURL != want {
		t.Errorf("status_url = %q, want %q", got.StatusURL, want)
	}

	rec := fs.lastAccept(t)
	if rec.ID != got.Task {
		t.Errorf("recorded id %q but quoted %q — the caller would poll a row that does not exist", rec.ID, got.Task)
	}
	if rec.Route != "/orders" || rec.Principal != "acme-erp" {
		t.Errorf("recorded %+v; the gateway's facts must pass through unaltered", rec)
	}
	if rec.TokenSHA256 != "" {
		t.Error("an authenticated route was given a capability token it does not need")
	}
}

// Durable accept (ADR-0042 §6): the record comes before the promise. If it
// cannot be written, the caller is told so — an accepted task that vanishes
// without trace is the outcome this ordering exists to prevent.
func TestAcceptFailsWhenTheRecordCannotBeWritten(t *testing.T) {
	fs := newFakeStatus()
	fs.acceptEr = errFake
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)

	code, _, _ := l.execute(t.Context(), inboundWith("orders", map[string]string{
		hdrRoute: "/orders", hdrPrincipal: "acme-erp", hdrPublicBase: "https://api.example.com",
	}, `{}`))
	if code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 — the caller must not be promised a status URL that will 404", code)
	}
}

// An anonymous route stamps one principal for every caller, so comparing
// principals authorises nothing. The URL becomes a capability URL, and only the
// token's DIGEST reaches the hub (ADR-0042 §3b).
func TestAnonymousRouteGetsACapabilityURL(t *testing.T) {
	fs := newFakeStatus()
	l := statusLoop(t, `{"name":"hook","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)

	_, body, _ := l.execute(t.Context(), inboundWith("hook", map[string]string{
		hdrRoute: "/hooks/shopify", hdrPrincipal: anonymousPrincipal,
		hdrPublicBase: "https://api.example.com",
	}, `{}`))

	var got accepted
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(got.StatusURL)
	if err != nil {
		t.Fatal(err)
	}
	token := u.Query().Get(capabilityParam)
	if token == "" {
		t.Fatal("an anonymous route's status URL carries no capability token, so the id alone would authorise the read")
	}

	rec := fs.lastAccept(t)
	sum := sha256.Sum256([]byte(token))
	if rec.TokenSHA256 != hex.EncodeToString(sum[:]) {
		t.Error("the hub was sent something other than the token's digest")
	}
	if strings.Contains(rec.TokenSHA256, token) {
		t.Error("the token itself reached the hub")
	}
}

// A runner with no hub serves async requests but hands out no URL: there would
// be nowhere to read it from, and the field's absence is the honest signal.
func TestNoHubMeansNoStatusURL(t *testing.T) {
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, nil)

	code, body, _ := l.execute(t.Context(), inboundWith("orders", map[string]string{
		hdrRoute: "/orders", hdrPublicBase: "https://api.example.com",
	}, `{}`))
	if code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", code)
	}
	var got accepted
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.StatusURL != "" {
		t.Errorf("status_url = %q, want it omitted when there is no hub to read from", got.StatusURL)
	}
	if got.Task == "" {
		t.Error("the caller still needs a task id to correlate with")
	}
}

// A status read is ANSWERED, never executed: running the flow on a GET would
// be a side effect on a read.
func TestStatusReadIsAnsweredNotExecuted(t *testing.T) {
	fs := newFakeStatus()
	fs.status = &hubclient.ExecutionStatus{Task: "t-1", Flow: "orders", State: "running"}
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)
	l.opts.Lookup = func(string) (*flowdoc.Document, bool) {
		t.Error("a status read looked up a flow")
		return nil, false
	}

	in := inboundWith("orders", map[string]string{
		hdrOp: opStatus, hdrTask: "t-1", hdrRoute: "/orders", hdrPrincipal: "acme-erp",
	}, "")
	code, body, ctype := l.execute(t.Context(), in)

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", code, body)
	}
	if ctype != "application/json" {
		t.Errorf("content-type = %q", ctype)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["state"] != "running" {
		t.Errorf("state = %v, want running", got["state"])
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.read) != 1 {
		t.Fatalf("hub reads = %d, want 1", len(fs.read))
	}
	if fs.read[0].route != "/orders" || fs.read[0].principal != "acme-erp" {
		t.Errorf("read args = %+v; the gateway's facts must be passed through", fs.read[0])
	}
}

// The capability token travels in the caller's query and reaches the hub only
// as a digest.
func TestStatusReadForwardsTheCapabilityDigest(t *testing.T) {
	fs := newFakeStatus()
	fs.status = &hubclient.ExecutionStatus{Task: "t-1", State: "completed"}
	l := statusLoop(t, `{"name":"hook","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)

	in := inboundWith("hook", map[string]string{
		hdrOp: opStatus, hdrTask: "t-1", hdrRoute: "/hooks/shopify",
		hdrPrincipal: anonymousPrincipal, hdrQuery: capabilityParam + "=abc123",
	}, "")
	in.query, _ = url.ParseQuery(in.headers.Get(hdrQuery))
	if code, body, _ := l.execute(t.Context(), in); code != http.StatusOK {
		t.Fatalf("status = %d: %s", code, body)
	}

	sum := sha256.Sum256([]byte("abc123"))
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if got := fs.read[0].token; got != hex.EncodeToString(sum[:]) {
		t.Errorf("token forwarded as %q, want the digest", got)
	}
}

// Every refusal is the same 404 — unknown id, wrong route, wrong principal,
// wrong token. A distinguishable answer tells an attacker which ids exist.
func TestStatusRefusalIsAlways404(t *testing.T) {
	fs := newFakeStatus()
	fs.readErr = hubclient.ErrExecutionNotFound
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)

	code, _, _ := l.execute(t.Context(), inboundWith("orders", map[string]string{
		hdrOp: opStatus, hdrTask: "nope", hdrRoute: "/orders", hdrPrincipal: "acme-erp",
	}, ""))
	if code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", code)
	}
}

func TestAlreadyReadStatusIsGone(t *testing.T) {
	fs := newFakeStatus()
	fs.readErr = hubclient.ErrExecutionGone
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)

	code, _, _ := l.execute(t.Context(), inboundWith("orders", map[string]string{
		hdrOp: opStatus, hdrTask: "t-1", hdrRoute: "/orders", hdrPrincipal: "acme-erp",
	}, ""))
	if code != http.StatusGone {
		t.Errorf("status = %d, want 410", code)
	}
}

// A runner with no hub cannot answer a status read, and says so rather than
// erroring: this deployment never handed out a status URL.
func TestStatusReadWithoutAHubIs404(t *testing.T) {
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, nil)
	code, _, _ := l.execute(t.Context(), inboundWith("orders", map[string]string{
		hdrOp: opStatus, hdrTask: "t-1",
	}, ""))
	if code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", code)
	}
}

// The row is closed out when the task finishes, so a caller polling gets a
// terminal answer rather than "accepted" forever.
func TestCompletionFinalisesTheStatusRow(t *testing.T) {
	fs := newFakeStatus()
	l := statusLoop(t, `{"name":"orders","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`, fs)

	_, body, _ := l.execute(t.Context(), inboundWith("orders", map[string]string{
		hdrRoute: "/orders", hdrPrincipal: "acme-erp", hdrPublicBase: "https://api.example.com",
	}, `{"order_id":"A"}`))
	var got accepted
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		fs.mu.Lock()
		out, ok := fs.finished[got.Task]
		fs.mu.Unlock()
		if ok {
			if out.State != string(task.StateCompleted) {
				t.Errorf("final state = %q, want completed", out.State)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the status row was never finalised; a caller would poll `accepted` forever")
}

// --- helpers ---------------------------------------------------------------

var errFake = errStr("the hub said no")

type errStr string

func (e errStr) Error() string { return string(e) }

func statusLoop(t *testing.T, doc string, fs *fakeStatus) *Loop {
	t.Helper()
	d, err := flowdoc.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parsing the flow: %v", err)
	}
	svc := service.New(service.Options{})
	t.Cleanup(func() { _ = svc.Close(5 * time.Second) })

	opts := Options{
		Service: svc,
		Lookup:  func(name string) (*flowdoc.Document, bool) { return d, name == d.Name },
	}
	if fs != nil {
		opts.Status = fs
	}
	return &Loop{log: discardLog(), opts: opts}
}

func inboundWith(flow string, headers map[string]string, body string) *inbound {
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &inbound{id: "req-1", flow: flow, headers: h, body: []byte(body)}
}

// Fire-and-forget (ADR-0042 §3d): the accept path must not touch the hub at
// all. An event feed doing a million of these a day should not be writing a
// million rows nobody will read, and the round trip is the cost that made the
// shape worth having.
func TestFireAndForgetRecordsNothingAndPromisesNothing(t *testing.T) {
	fs := newFakeStatus()
	l := statusLoop(t, `{"name":"events","source":{"connector":"@webhook","action":"ndjson","ack":"none"},
		"sink":{"connector":"@discard"}}`, fs)

	code, body, _ := l.execute(t.Context(), inboundWith("events", map[string]string{
		hdrRoute: "/events", hdrPrincipal: "acme-erp", hdrPublicBase: "https://api.example.com",
	}, `{"event":"created"}`))

	if code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", code, body)
	}
	var got accepted
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Task != "" {
		t.Errorf("task = %q; an id nobody can look up is worse than no id", got.Task)
	}
	if got.StatusURL != "" {
		t.Errorf("status_url = %q; there is no row for it to resolve to", got.StatusURL)
	}
	if got.Status != "accepted" || got.Flow != "events" {
		t.Errorf("body = %+v; the envelope should lose only what does not exist", got)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.accepted) != 0 {
		t.Errorf("the hub was written to %d times on a fire-and-forget accept", len(fs.accepted))
	}
}

// A hub that is down must not stop a fire-and-forget route: it never needed the
// hub, so a failure there is not its failure.
func TestFireAndForgetAcceptsWhileTheHubIsUnreachable(t *testing.T) {
	fs := newFakeStatus()
	fs.acceptEr = errFake
	l := statusLoop(t, `{"name":"events","source":{"connector":"@webhook","action":"ndjson","ack":"none"},
		"sink":{"connector":"@discard"}}`, fs)

	code, body, _ := l.execute(t.Context(), inboundWith("events", map[string]string{
		hdrRoute: "/events", hdrPublicBase: "https://api.example.com",
	}, `{}`))
	if code != http.StatusAccepted {
		t.Errorf("status = %d, want 202: %s", code, body)
	}
}

// Verification is independent of acknowledgement, and matters MORE here: a 400
// is the only feedback a fire-and-forget caller can ever receive.
func TestFireAndForgetStillVerifiesItsInput(t *testing.T) {
	fs := newFakeStatus()
	l := statusLoop(t, `{"name":"events","source":{"connector":"@webhook","action":"ndjson","ack":"none",
		"input":{"scope":"body","schema":{"type":"object","required":["event"],
			"properties":{"event":{"type":"string"}}}}},
		"sink":{"connector":"@discard"}}`, fs)

	code, body, _ := l.execute(t.Context(), inboundWith("events", map[string]string{
		hdrRoute: "/events",
	}, `{"event": 7}`))
	if code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", code, body)
	}
	if !strings.Contains(string(body), "event") {
		t.Errorf("body %s does not name the offending field", body)
	}
}
