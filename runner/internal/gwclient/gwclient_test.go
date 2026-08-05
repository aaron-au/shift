package gwclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/service"
)

// A flow that reads the inbound body and returns it: @webhook in, @response
// out. The whole gateway path in one document.
const echoFlow = `{
  "name": "echo",
  "source": {"connector": "@webhook"},
  "sink": {"connector": "@response"}
}`

// The runner half of ADR-0038 §4, end to end against a stub gateway: poll,
// execute, deliver. Nothing here listens — the runner is a CLIENT of the
// gateway, which is what lets it sit behind a deny-all ingress policy.
func TestPollExecuteDeliver(t *testing.T) {
	doc, err := flowdoc.Parse([]byte(echoFlow))
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu        sync.Mutex
		delivered []byte
		status    string
		handedOut bool
	)
	done := make(chan struct{})

	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == pollPath:
			raw, _ := io.ReadAll(r.Body)
			var pr pollRequest
			if err := json.Unmarshal(raw, &pr); err != nil {
				t.Errorf("poll body: %v", err)
			}
			_ = pr
			// The poll body must NOT carry labels: a runner that could state
			// its own placement could promote itself (ADR-0041 §3).
			if bytes.Contains(raw, []byte("labels")) {
				t.Errorf("poll body carries labels: %s", raw)
			}
			mu.Lock()
			first := !handedOut
			handedOut = true
			mu.Unlock()
			if !first {
				// Subsequent polls park briefly and return empty, so the loop
				// keeps running without spinning.
				time.Sleep(20 * time.Millisecond)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set(hdrRequestID, "req-1")
			w.Header().Set(hdrFlow, "echo")
			w.Header().Set(fwdPrefix+"X-Shift-Principal", "acme-erp")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"order":1}`)

		case strings.HasPrefix(r.URL.Path, deliverPath):
			if got := strings.TrimPrefix(r.URL.Path, deliverPath); got != "req-1" {
				t.Errorf("deliver id = %q, want req-1", got)
			}
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			delivered, status = b, r.Header.Get(hdrStatus)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			close(done)

		default:
			t.Errorf("unexpected gateway call %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer gw.Close()

	svc := service.New(service.Options{})
	defer func() { _ = svc.Close(5 * time.Second) }()

	l := New(Options{
		Addrs:    []string{gw.URL},
		Service:  svc,
		Lookup:   func(name string) (*flowdoc.Document, bool) { return doc, name == "echo" },
		PollWait: time.Second,
	})
	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	go func() { l.Run(ctx); close(loopDone) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatal("no delivery within 15s")
	}
	cancel()
	<-loopDone

	mu.Lock()
	defer mu.Unlock()
	if status != "200" {
		t.Errorf("delivered status = %q, want 200", status)
	}
	if got := strings.TrimSpace(string(delivered)); got != `{"order":1}` {
		t.Errorf("delivered body = %q, want the flow's output", got)
	}
}

// A gateway naming a flow this runner does not hold is CONFIGURATION DRIFT,
// not a runner fault. It must answer 404 rather than hang — a caller blocked
// on the gateway would otherwise wait out the full delivery timeout.
func TestUnknownFlowDelivers404(t *testing.T) {
	var (
		mu     sync.Mutex
		status string
	)
	done := make(chan struct{})
	var once sync.Once

	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pollPath {
			sent := false
			once.Do(func() {
				w.Header().Set(hdrRequestID, "req-9")
				w.Header().Set(hdrFlow, "does-not-exist")
				w.WriteHeader(http.StatusOK)
				sent = true
			})
			if !sent {
				time.Sleep(20 * time.Millisecond)
				w.WriteHeader(http.StatusNoContent)
			}
			return
		}
		mu.Lock()
		status = r.Header.Get(hdrStatus)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		close(done)
	}))
	defer gw.Close()

	svc := service.New(service.Options{})
	defer func() { _ = svc.Close(5 * time.Second) }()

	l := New(Options{
		Addrs:    []string{gw.URL},
		Service:  svc,
		Lookup:   func(string) (*flowdoc.Document, bool) { return nil, false },
		PollWait: time.Second,
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go l.Run(ctx)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery for an unknown flow")
	}
	mu.Lock()
	defer mu.Unlock()
	if status != "404" {
		t.Errorf("status = %q, want 404", status)
	}
}

// A gateway that is down must not take the runner with it: the hub lease loop
// and every other gateway keep working, and the loop backs off rather than
// spinning on a refused connection.
func TestUnreachableGatewayBacksOffWithoutExiting(t *testing.T) {
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	url := gw.URL
	gw.Close() // refuse every connection from here on

	l := New(Options{
		Addrs:    []string{url},
		Service:  service.New(service.Options{}),
		Lookup:   func(string) (*flowdoc.Document, bool) { return nil, false },
		PollWait: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	stopped := make(chan struct{})
	go func() { l.Run(ctx); close(stopped) }()
	select {
	case <-stopped: // returned when the context ended: correct
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop with its context")
	}
}

// The gateway sends caller headers under a prefix so they cannot collide with
// the poll response's own. Restoring them is what lets the runner see the
// stamped principal at its real name.
func TestUnprefixRestoresCallerHeaders(t *testing.T) {
	in := http.Header{}
	in.Set(fwdPrefix+"Content-Type", "text/csv")
	in.Set(fwdPrefix+"X-Shift-Principal", "acme")
	in.Set("Content-Type", "application/octet-stream") // the POLL response's own
	in.Set(hdrFlow, "orders")                          // metadata, not the caller's

	got := unprefix(in)
	if v := got.Get("Content-Type"); v != "text/csv" {
		t.Errorf("Content-Type = %q, want the CALLER's, not the poll response's", v)
	}
	if v := got.Get("X-Shift-Principal"); v != "acme" {
		t.Errorf("principal = %q, want acme", v)
	}
	if v := got.Get(hdrFlow); v != "" {
		t.Errorf("poll metadata leaked into the caller's headers: %q", v)
	}
}
