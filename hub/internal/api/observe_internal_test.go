package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestObserveMiddleware pins the issue-#7 request middleware: every response
// carries an X-Request-Id, the same id is on the request context for handlers,
// and RecordHTTP sees the method/status (route is the matched pattern, "other"
// without a mux). Internal test — constructs *api directly, no store needed.
func TestObserveMiddleware(t *testing.T) {
	var got struct {
		method, route string
		status        int
		called        bool
	}
	var ctxID string
	a := &api{opts: Options{RecordHTTP: func(_ context.Context, m, rt string, st int, secs float64) {
		got.method, got.route, got.status, got.called = m, rt, st, true
		if secs < 0 {
			t.Errorf("negative duration %v", secs)
		}
	}}}
	h := a.observe(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctxID = RequestID(r.Context())
		w.WriteHeader(http.StatusTeapot)
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	rid := resp.Header.Get("X-Request-Id")
	if rid == "" {
		t.Fatal("missing X-Request-Id header")
	}
	if ctxID != rid {
		t.Errorf("context id %q != header id %q", ctxID, rid)
	}
	if !got.called {
		t.Fatal("RecordHTTP not called")
	}
	if got.method != http.MethodGet || got.status != http.StatusTeapot || got.route == "" {
		t.Errorf("recorded = %+v, want GET/418/non-empty route", got)
	}
}
