package hubclient_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// The wire contract for caller-facing status (ADR-0042 §3). These are separate
// modules with no compiler between them, so the paths, the query encoding and
// the status mapping are asserted rather than assumed.

func TestAcceptExecutionPostsTheRecord(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := hubclient.New(srv.URL, "secret")
	err := c.AcceptExecution(t.Context(), hubclient.AcceptedExecution{
		ID: "task-1", FlowName: "orders", Route: "/orders",
		Principal: "acme-erp", TokenSHA256: "digest",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v1/execution-status" {
		t.Errorf("path = %q", gotPath)
	}
	if gotBody["id"] != "task-1" || gotBody["route"] != "/orders" || gotBody["principal"] != "acme-erp" {
		t.Errorf("body = %v; the gateway's facts must travel unaltered", gotBody)
	}
}

// A collision must be distinguishable, because the runner's response to it is
// to mint a NEW id — not to give up and not to overwrite.
func TestAcceptExecutionReportsACollision(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	c := hubclient.New(srv.URL, "secret")
	err := c.AcceptExecution(t.Context(), hubclient.AcceptedExecution{ID: "dup", FlowName: "orders"})
	if !errors.Is(err, hubclient.ErrExecutionIDTaken) {
		t.Errorf("err = %v, want ErrExecutionIDTaken", err)
	}
}

func TestFinishExecutionPostsTheOutcome(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := hubclient.New(srv.URL, "secret")
	if err := c.FinishExecution(t.Context(), "task-1", hubclient.ExecutionOutcome{
		State: "failed", ErrorStep: "post-to-warehouse", ErrorCode: "connector_timeout",
	}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v1/execution-status/task-1/finish" {
		t.Errorf("path = %q", gotPath)
	}
	if gotBody["error_step"] != "post-to-warehouse" {
		t.Errorf("body = %v, want the canonical error shape", gotBody)
	}
}

func TestFinishExecutionOnAnUnknownID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := hubclient.New(srv.URL, "secret")
	if err := c.FinishExecution(t.Context(), "gone", hubclient.ExecutionOutcome{State: "completed"}); !errors.Is(err, hubclient.ErrExecutionNotFound) {
		t.Errorf("err = %v, want ErrExecutionNotFound", err)
	}
}

// The authorisation inputs are QUERY parameters, and they are what the hub
// decides on. Losing one in transit would turn a refusal into a disclosure.
func TestExecutionStatusSendsTheAuthorisationInputs(t *testing.T) {
	var gotQuery, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"task":"task-1","flow":"orders","state":"running"}`)
	}))
	defer srv.Close()

	c := hubclient.New(srv.URL, "secret")
	got, err := c.ExecutionStatusByID(t.Context(), "task-1", "/orders", "acme-erp", "digest")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v1/execution-status/task-1" {
		t.Errorf("path = %q", gotPath)
	}
	for _, want := range []string{"route=%2Forders", "principal=acme-erp", "token_sha256=digest"} {
		if !strings.Contains(gotQuery, want) {
			t.Errorf("query %q is missing %q", gotQuery, want)
		}
	}
	if got.State != "running" || got.Flow != "orders" {
		t.Errorf("decoded %+v", got)
	}
}

// A route with no capability token must not send an empty parameter: an empty
// token and no token are different questions at the hub.
func TestExecutionStatusOmitsAnAbsentToken(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = io.WriteString(w, `{"task":"t","state":"accepted"}`)
	}))
	defer srv.Close()

	c := hubclient.New(srv.URL, "secret")
	if _, err := c.ExecutionStatusByID(t.Context(), "t", "/orders", "acme-erp", ""); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gotQuery, "token_sha256") {
		t.Errorf("query %q carries a token parameter for a route that has none", gotQuery)
	}
}

// Both refusals map to their own sentinel, because the runner answers them
// differently — 404 versus 410 — and everything else is a gateway error.
func TestExecutionStatusRefusalsMap(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		want error
	}{
		{"not found", http.StatusNotFound, hubclient.ErrExecutionNotFound},
		{"already read", http.StatusGone, hubclient.ErrExecutionGone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.code)
			}))
			defer srv.Close()
			c := hubclient.New(srv.URL, "secret")
			if _, err := c.ExecutionStatusByID(t.Context(), "t", "/orders", "p", ""); !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestExecutionStatusServerErrorIsNotASentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := hubclient.New(srv.URL, "secret")
	_, err := c.ExecutionStatusByID(t.Context(), "t", "/orders", "p", "")
	if err == nil {
		t.Fatal("a 500 was not reported")
	}
	if errors.Is(err, hubclient.ErrExecutionNotFound) || errors.Is(err, hubclient.ErrExecutionGone) {
		t.Error("a hub failure was reported as a refusal, which would tell the caller their id is wrong")
	}
}
