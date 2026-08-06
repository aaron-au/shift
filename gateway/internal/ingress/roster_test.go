package ingress_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// THE property of ADR-0041 §3: a runner proves WHO it is and never states WHAT
// it is.
//
// Before this, labels came from the poll body, so a runner that was
// compromised — or merely started with the wrong flag — could claim
// `environment: production` and be handed production traffic, with nothing in
// the system able to disagree and no audit trail saying it had.
func TestRunnerCannotPromoteItselfViaThePollBody(t *testing.T) {
	roster := &config.Config{Version: 1,
		Routes: []config.Route{{Path: "/x", Flow: "f"}},
		Runners: []config.Runner{
			{ID: "rnr-staging", Labels: map[string]string{"environment": "staging"}},
		},
	}

	reg := runners.New()
	d := ingress.NewDispatch(reg, nil, "").
		WithLabels(roster.LabelsFor).
		// Stand in for the client certificate: this runner has PROVEN it is
		// rnr-staging and cannot influence that from the request body.
		WithPeerID(func(*http.Request) string { return "rnr-staging" })

	mux := http.NewServeMux()
	d.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// The runner asks for production in its poll body, the old way. The field
	// no longer exists, but a hostile runner would send it anyway.
	go func() {
		body, _ := json.Marshal(map[string]any{
			"wait_seconds": 2,
			"labels":       map[string]string{"environment": "production"},
		})
		resp, err := post(t, srv.URL+"/api/v1/gw/poll", strings.NewReader(string(body)))
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	waitParked(t, reg)

	// A production route must NOT see it...
	if n := reg.Available(config.Selector{"environment": "production"}); n != 0 {
		t.Errorf("a staging runner is eligible for production work (n=%d) — it promoted itself", n)
	}
	// ...and its real, hub-asserted placement must still work.
	if n := reg.Available(config.Selector{"environment": "staging"}); n != 1 {
		t.Errorf("staging availability = %d, want 1", n)
	}
}

// A runner the hub has not vouched for is refused, not admitted with no
// labels. Label-less satisfies every EMPTY selector, so admitting it would
// hand an unvouched runner exactly the traffic nobody thought to restrict.
func TestRunnerAbsentFromTheRosterIsRefused(t *testing.T) {
	roster := &config.Config{Version: 1,
		Routes:  []config.Route{{Path: "/x", Flow: "f"}},
		Runners: []config.Runner{{ID: "rnr-known"}},
	}

	reg := runners.New()
	mux := http.NewServeMux()
	ingress.NewDispatch(reg, nil, "").
		WithLabels(roster.LabelsFor).
		WithPeerID(func(*http.Request) string { return "rnr-stranger" }).
		Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := post(t, srv.URL+"/api/v1/gw/poll", strings.NewReader(`{"wait_seconds":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for a runner absent from the roster", resp.StatusCode)
	}
	if n := reg.Parked(); n != 0 {
		t.Errorf("parked = %d after a refused poll, want 0", n)
	}
}

// A connection carrying no client certificate proves no identity, so it
// resolves to no runner and is refused by the same path.
func TestPollWithoutAProvenIdentityIsRefused(t *testing.T) {
	roster := &config.Config{Version: 1,
		Routes:  []config.Route{{Path: "/x", Flow: "f"}},
		Runners: []config.Runner{{ID: "rnr-known"}},
	}

	reg := runners.New()
	mux := http.NewServeMux()
	// The real tlsPeerID returns "" on a non-mTLS connection; httptest serves
	// plain HTTP, so the default extractor already models this.
	ingress.NewDispatch(reg, nil, "").WithLabels(roster.LabelsFor).Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := post(t, srv.URL+"/api/v1/gw/poll", strings.NewReader(`{"wait_seconds":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403 without a proven identity", resp.StatusCode)
	}
}
