package hubclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newClient wires a Client to a mux-backed test hub.
func newClient(t *testing.T, mux *http.ServeMux) *Client {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return New(srv.URL, "rs_secret")
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	c := New("https://hub.example/", "sec")
	if c.base != "https://hub.example" {
		t.Fatalf("base = %q, want no trailing slash", c.base)
	}
}

// Every control-plane call must carry the runner's bearer secret and, when
// there is a body, a JSON content type.
func TestDoSetsAuthAndContentType(t *testing.T) {
	var gotAuth, gotCT, gotBody string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusNoContent)
	})
	c := newClient(t, mux)
	if _, _, err := c.Lease(t.Context(), 7*time.Second); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer rs_secret" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q", gotCT)
	}
	if gotBody != `{"wait_seconds":7}` {
		t.Errorf("lease body = %q", gotBody)
	}
}

func TestRegister(t *testing.T) {
	var gotBody map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runners/register", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"runner_id": "run-1", "secret": "rs_new"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	id, c, err := Register(t.Context(), srv.URL, "reg-token", "runner-name")
	if err != nil {
		t.Fatal(err)
	}
	if id != "run-1" {
		t.Fatalf("runner id = %q", id)
	}
	if c.secret != "rs_new" {
		t.Fatalf("secret = %q", c.secret)
	}
	if gotBody["token"] != "reg-token" || gotBody["name"] != "runner-name" {
		t.Fatalf("register body = %+v", gotBody)
	}
}

func TestRegisterNon201(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runners/register", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"token spent"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, _, err := Register(t.Context(), srv.URL, "spent", "n")
	if err == nil || !strings.Contains(err.Error(), "token spent") {
		t.Fatalf("err = %v, want token-spent 403", err)
	}
}

func TestRegisterMalformedBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runners/register", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{not json`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if _, _, err := Register(t.Context(), srv.URL, "t", "n"); err == nil {
		t.Fatal("malformed register body accepted")
	}
}

func TestLease(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"task": map[string]any{
				"id":              "task-9",
				"flow_name":       "f",
				"document":        json.RawMessage(`{"name":"f"}`),
				"idempotency_key": "idem-1",
			},
			"lease_ttl_seconds": 30,
		})
	})
	c := newClient(t, mux)

	task, ttl, err := c.Lease(t.Context(), 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.ID != "task-9" || task.IdempotencyKey != "idem-1" {
		t.Fatalf("task = %+v", task)
	}
	if ttl != 30*time.Second {
		t.Fatalf("ttl = %s", ttl)
	}
	if string(task.Document) != `{"name":"f"}` {
		t.Fatalf("document = %s", task.Document)
	}
}

func TestLeaseNoContent(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	c := newClient(t, mux)

	task, ttl, err := c.Lease(t.Context(), time.Second)
	if err != nil || task != nil || ttl != 0 {
		t.Fatalf("empty long-poll: task=%v ttl=%v err=%v", task, ttl, err)
	}
}

func TestLeaseErrorStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"slow down"}`))
	})
	c := newClient(t, mux)

	_, _, err := c.Lease(t.Context(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "429") || !strings.Contains(err.Error(), "slow down") {
		t.Fatalf("err = %v, want 429 slow down", err)
	}
}

func TestLeaseMalformedBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"task":`)) // truncated
	})
	c := newClient(t, mux)

	if _, _, err := c.Lease(t.Context(), time.Second); err == nil {
		t.Fatal("malformed lease body accepted")
	}
}

func TestHeartbeat(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr error
		wantOK  bool
	}{
		{"ok", http.StatusNoContent, nil, true},
		{"lease lost", http.StatusConflict, ErrLeaseLost, false},
		{"server error", http.StatusInternalServerError, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("POST /api/v1/tasks/{id}/heartbeat", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			})
			c := newClient(t, mux)
			err := c.Heartbeat(t.Context(), "t1")
			switch {
			case tc.wantOK && err != nil:
				t.Fatalf("want nil, got %v", err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			case !tc.wantOK && tc.wantErr == nil && err == nil:
				t.Fatal("want error, got nil")
			}
		})
	}
}

func TestComplete(t *testing.T) {
	var got Result
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusNoContent)
	})
	c := newClient(t, mux)

	res := Result{RecordsIn: 5, RecordsOut: 4, SinkConfirmed: 4, RunnerTaskID: "local-1",
		Ops: []OpStat{{Name: "source", RecordsIn: 5, RecordsOut: 5, Seconds: 0.1}}}
	if err := c.Complete(t.Context(), "t1", res); err != nil {
		t.Fatal(err)
	}
	if got.RecordsIn != 5 || got.RecordsOut != 4 || got.RunnerTaskID != "local-1" || len(got.Ops) != 1 {
		t.Fatalf("hub received %+v", got)
	}
}

func TestCompleteLeaseLost(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks/{id}/complete", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})
	c := newClient(t, mux)
	if err := c.Complete(t.Context(), "t1", Result{}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost", err)
	}
}

func TestCompleteErrorStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks/{id}/complete", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c := newClient(t, mux)
	if err := c.Complete(t.Context(), "t1", Result{}); err == nil {
		t.Fatal("500 complete accepted")
	}
}

func TestFail(t *testing.T) {
	var got map[string]string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks/{id}/fail", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	})
	c := newClient(t, mux)
	if err := c.Fail(t.Context(), "t1", "boom"); err != nil {
		t.Fatal(err)
	}
	if got["error"] != "boom" {
		t.Fatalf("fail body = %+v", got)
	}
}

func TestFailLeaseLostAndError(t *testing.T) {
	for _, tc := range []struct {
		status  int
		wantErr error
	}{
		{http.StatusConflict, ErrLeaseLost},
		{http.StatusInternalServerError, nil},
	} {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /api/v1/tasks/{id}/fail", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
		})
		c := newClient(t, mux)
		err := c.Fail(t.Context(), "t1", "x")
		if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
			t.Fatalf("status %d: err = %v, want %v", tc.status, err, tc.wantErr)
		}
		if tc.wantErr == nil && err == nil {
			t.Fatalf("status %d: want error", tc.status)
		}
	}
}

func TestReportExecution(t *testing.T) {
	var got ExecutionReport
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/executions", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusCreated)
	})
	c := newClient(t, mux)
	if err := c.ReportExecution(t.Context(), ExecutionReport{FlowName: "f", Trigger: "webhook", State: "completed", RecordsIn: 3}); err != nil {
		t.Fatal(err)
	}
	if got.FlowName != "f" || got.Trigger != "webhook" || got.RecordsIn != 3 {
		t.Fatalf("report = %+v", got)
	}
}

func TestReportExecutionErrorStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/executions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	c := newClient(t, mux)
	if err := c.ReportExecution(t.Context(), ExecutionReport{}); err == nil {
		t.Fatal("non-201 execution report accepted")
	}
}

func TestResolveConnector(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/connectors/{name}/resolve", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_ = json.NewEncoder(w).Encode(ConnectorManifest{Name: "http", Version: "1.2.3", Digest: "abc"})
	})
	c := newClient(t, mux)

	m, err := c.ResolveConnector(t.Context(), "http", "1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "http" || m.Version != "1.2.3" || m.Digest != "abc" {
		t.Fatalf("manifest = %+v", m)
	}
	if !strings.Contains(gotQuery, "version=1.2.3") || !strings.Contains(gotQuery, "os=") || !strings.Contains(gotQuery, "arch=") {
		t.Fatalf("query = %q, want version/os/arch", gotQuery)
	}
}

func TestResolveConnectorErrorAndMalformed(t *testing.T) {
	t.Run("status", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v1/connectors/{name}/resolve", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no such connector"}`))
		})
		c := newClient(t, mux)
		if _, err := c.ResolveConnector(t.Context(), "nope", ""); err == nil || !strings.Contains(err.Error(), "no such connector") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v1/connectors/{name}/resolve", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{`))
		})
		c := newClient(t, mux)
		if _, err := c.ResolveConnector(t.Context(), "http", ""); err == nil {
			t.Fatal("malformed manifest accepted")
		}
	})
}

func TestFetchConnector(t *testing.T) {
	artifact := []byte("connector-bytes")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/connectors/{name}/versions/{ver}/artifact", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(artifact)
	})
	c := newClient(t, mux)

	var buf strings.Builder
	if err := c.FetchConnector(t.Context(), ConnectorManifest{Name: "http", Version: "1.0.0"}, &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != string(artifact) {
		t.Fatalf("fetched %q", buf.String())
	}
}

func TestFetchConnectorErrorStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/connectors/{name}/versions/{ver}/artifact", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	c := newClient(t, mux)
	if err := c.FetchConnector(t.Context(), ConnectorManifest{Name: "http", Version: "1.0.0"}, io.Discard); err == nil {
		t.Fatal("404 artifact accepted")
	}
}

func TestPublisherKeys(t *testing.T) {
	k1 := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/publisher-keys", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{
			{"public_key": base64.StdEncoding.EncodeToString(k1)},
		}})
	})
	c := newClient(t, mux)

	keys, err := c.PublisherKeys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || string(keys[0]) != string(k1) {
		t.Fatalf("keys = %v", keys)
	}
}

func TestPublisherKeysBadBase64(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/publisher-keys", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{
			{"public_key": "!!!not-base64!!!"},
		}})
	})
	c := newClient(t, mux)
	if _, err := c.PublisherKeys(t.Context()); err == nil || !strings.Contains(err.Error(), "base64") {
		t.Fatalf("err = %v, want base64 error", err)
	}
}

func TestPublisherKeysErrorStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/publisher-keys", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c := newClient(t, mux)
	if _, err := c.PublisherKeys(t.Context()); err == nil {
		t.Fatal("503 publisher-keys accepted")
	}
}

func TestResolveSecrets(t *testing.T) {
	var gotNames struct {
		Names []string `json:"names"`
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/secrets/resolve", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotNames)
		_ = json.NewEncoder(w).Encode(map[string]any{"secrets": map[string]string{"api_key": "s3cr3t"}})
	})
	c := newClient(t, mux)

	vals, err := c.ResolveSecrets(t.Context(), []string{"api_key"})
	if err != nil {
		t.Fatal(err)
	}
	if vals["api_key"] != "s3cr3t" {
		t.Fatalf("secrets = %+v", vals)
	}
	if len(gotNames.Names) != 1 || gotNames.Names[0] != "api_key" {
		t.Fatalf("names sent = %+v", gotNames.Names)
	}
}

func TestResolveSecretsErrorAndMalformed(t *testing.T) {
	t.Run("status", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /api/v1/secrets/resolve", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"bad token"}`))
		})
		c := newClient(t, mux)
		if _, err := c.ResolveSecrets(t.Context(), []string{"a"}); err == nil || !strings.Contains(err.Error(), "401") {
			t.Fatalf("err = %v, want 401", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /api/v1/secrets/resolve", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"secrets":`))
		})
		c := newClient(t, mux)
		if _, err := c.ResolveSecrets(t.Context(), []string{"a"}); err == nil {
			t.Fatal("malformed secrets accepted")
		}
	})
}

func TestSyncWebhooks(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/webhooks/sync", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"webhooks": []WebhookConfig{
			{Name: "ingest", TokenHash: "h", Document: json.RawMessage(`{"name":"f"}`)},
		}})
	})
	c := newClient(t, mux)

	hooks, err := c.SyncWebhooks(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) != 1 || hooks[0].Name != "ingest" || hooks[0].TokenHash != "h" {
		t.Fatalf("hooks = %+v", hooks)
	}
}

func TestSyncWebhooksErrorStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/webhooks/sync", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c := newClient(t, mux)
	if _, err := c.SyncWebhooks(t.Context()); err == nil {
		t.Fatal("500 sync accepted")
	}
}

// A cancelled context aborts the transport before any response — the loop
// relies on this to break out of a long-poll on shutdown. The handler has a
// bounded fallback so a missed cancellation can't wedge server shutdown.
func TestContextCancellation(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		w.WriteHeader(http.StatusNoContent)
	})
	c := newClient(t, mux)

	ctx, cancel := context.WithCancel(t.Context())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, _, err := c.Lease(ctx, 30*time.Second); err == nil {
		t.Fatal("cancelled long-poll returned nil error")
	}
}

// An already-cancelled context is rejected before the request is even sent.
func TestContextAlreadyCancelled(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	c := newClient(t, mux)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := c.Lease(ctx, time.Second); err == nil {
		t.Fatal("pre-cancelled context accepted")
	}
}

// readErr falls back to the raw body when there is no {"error":...} field.
func TestReadErrPlainBody(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/lease", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream unavailable"))
	})
	c := newClient(t, mux)
	_, _, err := c.Lease(t.Context(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), "upstream unavailable") {
		t.Fatalf("err = %v, want plain 502 body", err)
	}
}
