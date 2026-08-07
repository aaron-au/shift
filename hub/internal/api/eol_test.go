package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func setEOL(t *testing.T, srv, name, version, body string) (int, map[string]any) {
	t.Helper()
	var out map[string]any
	code := call(t, http.MethodPost,
		fmt.Sprintf("%s/api/v1/connectors/%s/versions/%s/eol", srv, name, version),
		adminToken, body, &out)
	return code, out
}

// The shape of §7: announced, escalating, then loud. Before the deadline the
// version runs exactly as before while every flow pinning it carries a notice;
// after it, nothing resolves and the failure says what happened.
func TestEndOfLifeIsAnnouncedThenEnforced(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))
	publish("gen", "2.0.0", []byte("v2"))

	v := pinnedFlow(t, srv.URL, "orders", "gen", "1.0.0")
	if code, _ := publishFlow(t, srv.URL, "orders", v); code != http.StatusOK {
		t.Fatalf("publish = %d", code)
	}

	// Announced: a deadline three days out.
	soon := time.Now().Add(72 * time.Hour).UTC().Format(time.RFC3339)
	code, out := setEOL(t, srv.URL, "gen", "1.0.0",
		fmt.Sprintf(`{"eol_at":%q,"reason":"CVE-2026-1234 in a transitive dependency","target":"2.0.0"}`, soon))
	if code != http.StatusOK {
		t.Fatalf("set eol = %d (%v)", code, out["error"])
	}
	// The response names the flows rather than counting them: the next step is
	// telling those people.
	flows, _ := out["flows"].([]any)
	if len(flows) != 1 {
		t.Fatalf("flows = %v, want the one pinning it", out["flows"])
	}

	// It still RESOLVES — that is what "announced" means.
	if c := call(t, http.MethodGet,
		srv.URL+"/api/v1/connectors/gen/resolve?version=1.0.0&os=linux&arch=amd64", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("resolve before the deadline = %d, want 200", c)
	}

	// And it is loud where somebody is deciding.
	_, deployed := publishFlow(t, srv.URL, "orders", v)
	detail := noticeDetail(deployed, "connector-eol.scheduled")
	if detail == "" {
		t.Fatalf("no EOL notice: %v", deployed["notices"])
	}
	for _, want := range []string{"STOPS RUNNING", "CVE-2026-1234", "2.0.0"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("notice is missing %q: %q", want, detail)
		}
	}

	// Enforced: move the deadline into the past.
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	if c, o := setEOL(t, srv.URL, "gen", "1.0.0",
		fmt.Sprintf(`{"eol_at":%q,"reason":"CVE-2026-1234 in a transitive dependency","target":"2.0.0"}`, past)); c != http.StatusOK {
		t.Fatalf("set past eol = %d (%v)", c, o["error"])
	}

	// 410, not 404. The runner puts this message in the task result, and "not
	// found" would send somebody looking for a typo.
	body, c := call2t(t, http.MethodGet,
		srv.URL+"/api/v1/connectors/gen/resolve?version=1.0.0&os=linux&arch=amd64", adminToken, "")
	if c != http.StatusGone {
		t.Fatalf("resolve after the deadline = %d, want 410", c)
	}
	for _, want := range []string{"end of life", "CVE-2026-1234", "2.0.0"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the 410 does not carry %q: %s", want, body)
		}
	}

	// A manifest cached from before the deadline must not be a way to fetch
	// the artifact afterwards.
	if _, c := call2t(t, http.MethodGet,
		srv.URL+"/api/v1/connectors/gen/versions/1.0.0/artifact?os=linux&arch=amd64", adminToken, ""); c != http.StatusGone {
		t.Fatalf("artifact download after the deadline = %d, want 410", c)
	}

	// Newer versions are untouched — an EOL is about one release.
	if c := call(t, http.MethodGet,
		srv.URL+"/api/v1/connectors/gen/resolve?version=2.0.0&os=linux&arch=amd64", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("resolve of a live version = %d", c)
	}
}

// Publishing a dead pin produces a flow that cannot run, so publish refuses —
// in BOTH directions, unlike the currency gate. Rolling back to a connector
// that no longer resolves gives nobody a working flow.
func TestPublishRefusesADeadPinEvenOnRollback(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))
	publish("gen", "2.0.0", []byte("v2"))

	old := pinnedFlow(t, srv.URL, "orders", "gen", "1.0.0")
	if code, _ := publishFlow(t, srv.URL, "orders", old); code != http.StatusOK {
		t.Fatalf("initial publish = %d", code)
	}
	current := pinnedFlow(t, srv.URL, "orders", "gen", "2.0.0")
	if code, _ := publishFlow(t, srv.URL, "orders", current); code != http.StatusOK {
		t.Fatalf("publish forward = %d", code)
	}

	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	setEOL(t, srv.URL, "gen", "1.0.0", fmt.Sprintf(`{"eol_at":%q,"reason":"key disclosure","target":"2.0.0"}`, past))

	code, out := publishFlow(t, srv.URL, "orders", old)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("rollback onto a dead pin = %d, want 422", code)
	}
	msg := fmt.Sprint(out["error"])
	if !strings.Contains(msg, "end of life") || !strings.Contains(msg, "2.0.0") {
		t.Fatalf("the refusal does not say what happened or where to go: %q", msg)
	}
}

// Declaring an end-of-life is a human act on a live system, and humans mistype
// version numbers — so it can be withdrawn.
func TestAScheduledEndOfLifeCanBeWithdrawn(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))

	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	setEOL(t, srv.URL, "gen", "1.0.0", fmt.Sprintf(`{"eol_at":%q,"reason":"mistake","target":""}`, past))
	if c := call(t, http.MethodGet,
		srv.URL+"/api/v1/connectors/gen/resolve?version=1.0.0&os=linux&arch=amd64", adminToken, "", nil); c != http.StatusGone {
		t.Fatalf("resolve = %d, want 410", c)
	}

	if c, o := setEOL(t, srv.URL, "gen", "1.0.0", `{}`); c != http.StatusOK {
		t.Fatalf("clear = %d (%v)", c, o["error"])
	}
	if c := call(t, http.MethodGet,
		srv.URL+"/api/v1/connectors/gen/resolve?version=1.0.0&os=linux&arch=amd64", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("resolve after withdrawal = %d, want 200", c)
	}
}

// The reason is the whole output of this action: it is what a customer is told
// when their flow stops. An EOL with no reason is not worth declaring.
func TestAnEndOfLifeNeedsAReason(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))

	soon := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	if c, _ := setEOL(t, srv.URL, "gen", "1.0.0", fmt.Sprintf(`{"eol_at":%q}`, soon)); c != http.StatusUnprocessableEntity {
		t.Fatalf("eol with no reason = %d, want 422", c)
	}
	if c, _ := setEOL(t, srv.URL, "gen", "1.0.0", `{"eol_at":"next tuesday","reason":"x"}`); c != http.StatusUnprocessableEntity {
		t.Fatalf("malformed deadline = %d, want 422", c)
	}
	if c, _ := setEOL(t, srv.URL, "nosuch", "1.0.0", fmt.Sprintf(`{"eol_at":%q,"reason":"x"}`, soon)); c != http.StatusNotFound {
		t.Fatalf("eol on a missing connector = %d, want 404", c)
	}
}

// The list is what an operator works from: every deadline, soonest first, each
// with the flows it will take down.
func TestScheduledEndOfLivesAreListedWithTheirFlows(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))
	publish("gen", "2.0.0", []byte("v2"))

	v := pinnedFlow(t, srv.URL, "orders", "gen", "1.0.0")
	publishFlow(t, srv.URL, "orders", v)

	far := time.Now().Add(90 * 24 * time.Hour).UTC().Format(time.RFC3339)
	near := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	setEOL(t, srv.URL, "gen", "2.0.0", fmt.Sprintf(`{"eol_at":%q,"reason":"deprecated protocol"}`, far))
	setEOL(t, srv.URL, "gen", "1.0.0", fmt.Sprintf(`{"eol_at":%q,"reason":"CVE","target":"2.0.0"}`, near))

	var out struct {
		EOL []struct {
			Connector string `json:"connector"`
			Version   string `json:"version"`
			Flows     []struct {
				Flow string `json:"flow"`
			} `json:"flows"`
		} `json:"eol"`
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/connectors/eol", adminToken, "", &out); c != http.StatusOK {
		t.Fatalf("list = %d", c)
	}
	if len(out.EOL) != 2 {
		t.Fatalf("listed %d, want 2", len(out.EOL))
	}
	// Soonest first: the one that needs action today is the one to read first.
	if out.EOL[0].Version != "1.0.0" {
		t.Fatalf("order = %s then %s, want soonest first", out.EOL[0].Version, out.EOL[1].Version)
	}
	if len(out.EOL[0].Flows) != 1 || out.EOL[0].Flows[0].Flow != "orders" {
		t.Fatalf("flows = %+v, want the pinning flow named", out.EOL[0].Flows)
	}
	if len(out.EOL[1].Flows) != 0 {
		t.Fatalf("a version nothing pins listed flows: %+v", out.EOL[1].Flows)
	}
}

// EOL is admin-realm. A runner that could declare one could take the fleet's
// flows down.
func TestARunnerCannotDeclareAnEndOfLife(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))

	var tok struct{ Token string }
	call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok)
	var reg struct {
		Secret string `json:"secret"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); c != http.StatusCreated {
		t.Fatalf("register = %d", c)
	}
	soon := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/connectors/gen/versions/1.0.0/eol",
		reg.Secret, fmt.Sprintf(`{"eol_at":%q,"reason":"x"}`, soon), nil); c != http.StatusUnauthorized {
		t.Fatalf("eol as a runner = %d, want 401", c)
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/connectors/eol", reg.Secret, "", nil); c != http.StatusUnauthorized {
		t.Fatalf("list as a runner = %d, want 401", c)
	}
}
