package api_test

import (
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aaron-au/shift/hub/internal/api"
)

// apiCall matches a fully-static endpoint the studio fetches: api('/api/v1/…').
// Concatenated paths (api('/api/v1/flows/' + name)) end at the quote, so they
// arrive here with a trailing slash and are skipped — a prefix is not an
// endpoint, and the handlers behind them are covered by their own tests.
var apiCall = regexp.MustCompile(`api\('(/api/v1/[^']*)'`)

// The studio is a client of this hub, in the same repository, with no
// compile-time link between them. Renaming a route is therefore a silent
// break: the page still loads, the window still opens, and it is empty.
//
// This walks every static path the page fetches and asserts the hub routes it.
// It does not check the response shape — the point is narrower and the failure
// it catches is dumber than that: an endpoint the studio calls that nothing
// serves.
func TestTheStudioCallsNoEndpointTheHubDoesNotServe(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	seen := map[string]bool{}
	var paths []string
	for _, m := range apiCall.FindAllStringSubmatch(api.UIHTML(), -1) {
		p := m[1]
		if strings.HasSuffix(p, "/") || seen[p] {
			continue // a prefix for a concatenated id, or already checked
		}
		seen[p] = true
		paths = append(paths, p)
	}
	sort.Strings(paths)
	if len(paths) < 10 {
		t.Fatalf("found %d studio endpoints; the extraction is wrong, not the page", len(paths))
	}

	for _, p := range paths {
		t.Run(p, func(t *testing.T) {
			method := http.MethodGet
			if postOnly[strings.SplitN(p, "?", 2)[0]] {
				method = http.MethodPost
			}
			code := call(t, method, srv.URL+p, adminToken, "{}", nil)
			if code != http.StatusNotFound {
				return
			}
			// Some subsystems are legitimately absent — a hub with no KEK
			// serves no secrets — and the page already tolerates that. Anything
			// else answering 404 is a route the studio calls and the hub does
			// not have.
			if optional[strings.SplitN(p, "?", 2)[0]] {
				t.Skipf("%s is served only when its subsystem is configured", p)
			}
			t.Errorf("the studio fetches %s and the hub answers 404", p)
		})
	}
}

// optional names endpoints the hub registers only when the subsystem behind
// them is configured. The studio catches their absence rather than blanking
// the dashboard over it.
var optional = map[string]bool{
	"/api/v1/secrets": true, // requires a KEK (ADR-0010)
}

// postOnly names the studio endpoints that legitimately reject GET.
var postOnly = map[string]bool{
	"/api/v1/runner-tokens":      true,
	"/api/v1/flows/review":       true,
	"/api/v1/connectors/collect": true,
}

// The management windows are what make the hub administrable without curl.
// Their render targets are addressed by id from JS, so a renamed id is another
// silent break: the window opens and stays empty.
func TestTheManagementWindowsAreWired(t *testing.T) {
	ui := api.UIHTML()
	for _, w := range []struct{ win, target, render string }{
		{"win-connections", `id="connections"`, "renderConnections"},
		{"win-gateways", `id="gateways"`, "renderGateways"},
		{"win-routes", `id="routes"`, "renderRoutes"},
		{"win-runners", `id="runners"`, "renderRunners"},
	} {
		if !strings.Contains(ui, `id="`+w.win+`"`) {
			t.Errorf("no %s window", w.win)
		}
		if !strings.Contains(ui, w.target) {
			t.Errorf("%s has no render target %s", w.win, w.target)
		}
		// Rendered from refresh(), or the window shows nothing until something
		// else happens to call it.
		if !strings.Contains(ui, w.render+"(") {
			t.Errorf("%s is never rendered (%s missing)", w.win, w.render)
		}
	}
}

// A flow that has never been published must still be editable. The builder
// reads GET /api/v1/flows/{name}, which defaults to the PUBLISHED version —
// so a freshly deployed draft could not be opened at all, and the error said
// "flow has no published version" to somebody in the middle of creating the
// first one.
func TestADraftFlowCanBeOpenedForEditing(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	if c := call(t, http.MethodPut, srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, nil); c != http.StatusCreated {
		t.Fatalf("deploy = %d", c)
	}

	// The default is unchanged: "the flow" means the published version to
	// anything running it, and there is not one yet.
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/flows/orders", adminToken, "", nil); c != http.StatusNotFound {
		t.Fatalf("published version of an unpublished flow = %d, want 404", c)
	}

	var latest struct {
		Flow struct {
			LatestVersion int `json:"latest_version"`
		} `json:"flow"`
		Document map[string]any `json:"document"`
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/flows/orders?version=latest", adminToken, "", &latest); c != http.StatusOK {
		t.Fatalf("latest version of a draft = %d, want 200 — the builder cannot open it otherwise", c)
	}
	if latest.Flow.LatestVersion != 1 || latest.Document["name"] != "orders" {
		t.Fatalf("got %+v", latest)
	}

	// An explicit version is how the version picker shows what it would roll
	// back to.
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/flows/orders?version=1", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("explicit version = %d", c)
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/flows/orders?version=99", adminToken, "", nil); c != http.StatusNotFound {
		t.Fatalf("missing version = %d, want 404", c)
	}
	// A version that is not a version is a bad REQUEST, not a missing thing.
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/flows/orders?version=nope", adminToken, "", nil); c != http.StatusUnprocessableEntity {
		t.Fatalf("malformed version = %d, want 422", c)
	}
}
