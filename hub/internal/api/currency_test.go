package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// pinnedFlow deploys a flow pinning one connector version, returning the new
// flow version number.
func pinnedFlow(t *testing.T, srv, name, connector, version string) int {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q,
	  "source":{"connector":%q,"action":"records","version":%q},
	  "sink":{"connector":"@discard","action":""}}`, name, connector, version)
	var out struct {
		Version int `json:"version"`
	}
	if c := call(t, http.MethodPut, srv+"/api/v1/flows/"+name, adminToken, body, &out); c != http.StatusCreated {
		t.Fatalf("deploy = %d", c)
	}
	return out.Version
}

func publishFlow(t *testing.T, srv, name string, version int) (int, map[string]any) {
	t.Helper()
	var out map[string]any
	code := call(t, http.MethodPost,
		fmt.Sprintf("%s/api/v1/flows/%s/versions/%d/publish", srv, name, version), adminToken, "", &out)
	return code, out
}

// Age produces a NOTICE, never a refusal (ADR-0047 §5). A flow pinning an old
// build keeps executing — refusing to run on grounds of age would fire on a
// flow nobody had touched, which is the time bomb the whole ADR exists to
// avoid.
func TestAnOldPinIsAdvisedNotRefused(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))

	v := pinnedFlow(t, srv.URL, "orders", "gen", "1.0.0")
	if code, _ := publishFlow(t, srv.URL, "orders", v); code != http.StatusOK {
		t.Fatalf("publish a current pin = %d", code)
	}

	// Two more releases land, putting the published flow outside the window.
	publish("gen", "2.0.0", []byte("v2"))
	publish("gen", "3.0.0", []byte("v3"))

	// Re-publishing the SAME version is not moving forward, so it is not
	// gated — and the notice is what says the pin has aged.
	code, out := publishFlow(t, srv.URL, "orders", v)
	if code != http.StatusOK {
		t.Fatalf("republish of an aged pin = %d, want 200 — age advises, it does not refuse", code)
	}
	if !hasNotice(out, "connector-currency.behind") {
		t.Fatalf("no currency notice on a pin two releases behind: %v", out["notices"])
	}
	detail := noticeDetail(out, "connector-currency.behind")
	// The notice folds the whole span, not the last hop: somebody crossing
	// three releases wants to know what is in all of them.
	if !strings.Contains(detail, "3.0.0") || !strings.Contains(detail, "undeclared") {
		t.Fatalf("notice does not fold the span: %q", detail)
	}
	if !strings.Contains(detail, "still RUNS") {
		t.Fatalf("notice does not say the flow keeps running: %q", detail)
	}
}

// Publishing a flow FORWARD drags it forward (ADR-0047 §4). This is where the
// version limitation lives: bounded by the last time somebody edited the flow
// rather than by a calendar.
func TestPublishingForwardRefusesAnOutOfWindowPin(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		publish("gen", v, []byte("gen-"+v))
	}

	// A new flow pinned to the oldest build: two releases behind.
	v := pinnedFlow(t, srv.URL, "orders", "gen", "1.0.0")
	code, out := publishFlow(t, srv.URL, "orders", v)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("publish of an out-of-window pin = %d, want 422", code)
	}
	msg := fmt.Sprint(out["error"])
	if !strings.Contains(msg, "gen") || !strings.Contains(msg, "1.0.0") {
		t.Fatalf("the refusal does not name the step or the version: %q", msg)
	}

	// Inside the window publishes normally.
	v2 := pinnedFlow(t, srv.URL, "orders", "gen", "2.0.0")
	if code, _ := publishFlow(t, srv.URL, "orders", v2); code != http.StatusOK {
		t.Fatalf("publish of an in-window pin = %d", code)
	}
}

// The exemption that matters more than the rule. A rollback is an emergency
// action taken when the current version is misbehaving, and it deliberately
// keeps its original pins — so gating it on currency would deny somebody the
// one thing they need at the worst possible moment.
func TestARollbackIsNeverBlockedByTheSupportWindow(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("v1"))

	// v1 of the flow, published while 1.0.0 was current.
	old := pinnedFlow(t, srv.URL, "orders", "gen", "1.0.0")
	if code, _ := publishFlow(t, srv.URL, "orders", old); code != http.StatusOK {
		t.Fatalf("initial publish = %d", code)
	}

	// Time passes; two releases land and the flow is moved to the newest.
	publish("gen", "2.0.0", []byte("v2"))
	publish("gen", "3.0.0", []byte("v3"))
	current := pinnedFlow(t, srv.URL, "orders", "gen", "3.0.0")
	if code, _ := publishFlow(t, srv.URL, "orders", current); code != http.StatusOK {
		t.Fatalf("publish forward = %d", code)
	}

	// The new version misbehaves. Rolling back to the old one pins 1.0.0,
	// which is outside the window — and must still work.
	code, out := publishFlow(t, srv.URL, "orders", old)
	if code != http.StatusOK {
		t.Fatalf("rollback = %d, want 200 — a currency policy must never block a rollback", code)
	}
	// It still SAYS the pin is old. Advising and blocking are different things.
	if !hasNotice(out, "connector-currency.behind") {
		t.Fatalf("rollback lost the currency notice: %v", out["notices"])
	}
}

// A connector the registry cannot place — provisioned outside it, or a version
// since collected — must not read as "stale" and block a publish. Unknown is
// not old.
func TestAnUnplaceablePinDoesNotBlockPublish(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0"} {
		publish("gen", v, []byte("gen-"+v))
	}
	// A version the registry has never held.
	v := pinnedFlow(t, srv.URL, "orders", "gen", "0.9.0-local")
	if code, out := publishFlow(t, srv.URL, "orders", v); code != http.StatusOK {
		t.Fatalf("publish of an unplaceable pin = %d (%v), want 200", code, out["error"])
	}
	// And a connector with no registry presence at all.
	v2 := pinnedFlow(t, srv.URL, "local", "handrolled", "1.0.0")
	if code, out := publishFlow(t, srv.URL, "local", v2); code != http.StatusOK {
		t.Fatalf("publish of an unregistered connector = %d (%v), want 200", code, out["error"])
	}
}

// The compatibility class is what turns "3 versions behind" into a decision.
func TestTheCompatibilityClassFoldsIntoTheNotice(t *testing.T) {
	srv, publishWith := registryWithCompat(t)
	publishWith("gen", "1.0.0", "compatible", "")
	publishWith("gen", "2.0.0", "behaviour-change", "retry defaults changed")
	publishWith("gen", "3.0.0", "breaking", "config key renamed")

	v := pinnedFlow(t, srv.URL, "orders", "gen", "1.0.0")
	// Publishing forward is refused (§4), so the notice is read on deploy.
	var deployed map[string]any
	call(t, http.MethodPut, srv.URL+"/api/v1/flows/orders", adminToken, `{"name":"orders",
	  "source":{"connector":"gen","action":"records","version":"1.0.0"},
	  "sink":{"connector":"@discard","action":""}}`, &deployed)
	_ = v

	detail := noticeDetail(deployed, "connector-currency.behind")
	if detail == "" {
		t.Fatalf("no currency notice: %v", deployed["notices"])
	}
	for _, want := range []string{"BREAKING", "3.0.0", "behaviour change", "2.0.0"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("notice is missing %q: %q", want, detail)
		}
	}
	if sev := noticeSeverity(deployed, "connector-currency.behind"); sev != "warn" {
		t.Fatalf("severity = %q; a breaking version in the span is not an info", sev)
	}
}

// An undeclared version is not a safe one — it is one nobody said anything
// about, and the notice has to say so rather than implying "compatible".
func TestAnUndeclaredVersionIsNamedAsUndeclared(t *testing.T) {
	srv, publishWith := registryWithCompat(t)
	publishWith("gen", "1.0.0", "compatible", "")
	publishWith("gen", "2.0.0", "", "") // publisher said nothing
	publishWith("gen", "3.0.0", "compatible", "")

	var deployed map[string]any
	call(t, http.MethodPut, srv.URL+"/api/v1/flows/orders", adminToken, `{"name":"orders",
	  "source":{"connector":"gen","action":"records","version":"1.0.0"},
	  "sink":{"connector":"@discard","action":""}}`, &deployed)

	detail := noticeDetail(deployed, "connector-currency.behind")
	if !strings.Contains(detail, "undeclared") || !strings.Contains(detail, "2.0.0") {
		t.Fatalf("an undeclared version was not named as one: %q", detail)
	}
}

// A class the registry does not recognise is refused at publish rather than
// stored: the whole value of the field is that its four values mean something.
func TestAnUnknownCompatibilityClassIsRefused(t *testing.T) {
	srv, _ := registryWithConnectors(t)
	code := call(t, http.MethodPut,
		srv.URL+"/api/v1/connectors/gen/versions/1.0.0?os=linux&arch=amd64&compat=probably-fine",
		adminToken, "artifact", nil)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown compat class = %d, want 422", code)
	}
}

func hasNotice(out map[string]any, code string) bool {
	return noticeDetail(out, code) != "" || noticeSeverity(out, code) != ""
}

func noticeField(out map[string]any, code, field string) string {
	ns, _ := out["notices"].([]any)
	for _, n := range ns {
		m, _ := n.(map[string]any)
		if m["code"] == code {
			s, _ := m[field].(string)
			return s
		}
	}
	return ""
}

func noticeDetail(out map[string]any, code string) string {
	return noticeField(out, code, "detail")
}

func noticeSeverity(out map[string]any, code string) string {
	return noticeField(out, code, "severity")
}

// The tier is hub-asserted (ADR-0048 §1). A runner naming its own would be
// able to escape metering, or be handed work it should not see.
func TestTheTestTierIsSetByTheHubNotTheRunner(t *testing.T) {
	srv := newServer(t)

	var tok struct{ Token string }
	call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok)
	var reg struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); c != http.StatusCreated {
		t.Fatalf("register = %d", c)
	}

	// It registers as production; test capacity is granted, not arrived with.
	var list struct {
		Runners []struct {
			Tier string `json:"tier"`
		} `json:"runners"`
	}
	call(t, http.MethodGet, srv.URL+"/api/v1/runners", adminToken, "", &list)
	if len(list.Runners) != 1 || list.Runners[0].Tier != "production" {
		t.Fatalf("runners = %+v, want one production runner", list.Runners)
	}

	if c := call(t, http.MethodPut, srv.URL+"/api/v1/runners/"+reg.RunnerID+"/tier",
		adminToken, `{"tier":"test"}`, nil); c != http.StatusNoContent {
		t.Fatalf("set tier = %d", c)
	}
	call(t, http.MethodGet, srv.URL+"/api/v1/runners", adminToken, "", &list)
	if list.Runners[0].Tier != "test" {
		t.Fatalf("tier = %q, want test", list.Runners[0].Tier)
	}

	// A runner cannot set its own — the whole point of asserting it at the hub.
	if c := call(t, http.MethodPut, srv.URL+"/api/v1/runners/"+reg.RunnerID+"/tier",
		reg.Secret, `{"tier":"production"}`, nil); c != http.StatusUnauthorized {
		t.Fatalf("runner self-tiering = %d, want 401", c)
	}
	if c := call(t, http.MethodPut, srv.URL+"/api/v1/runners/"+reg.RunnerID+"/tier",
		adminToken, `{"tier":"staging"}`, nil); c != http.StatusUnprocessableEntity {
		t.Fatalf("unknown tier = %d, want 422", c)
	}
}

// An API execution is test-marked only when it says so. The default is
// production, because a default of "test" would quietly move billable work off
// the meter.
func TestAnExecutionIsProductionUnlessItSaysOtherwise(t *testing.T) {
	srv := newServer(t)
	if c := call(t, http.MethodPut, srv.URL+"/api/v1/flows/orders", adminToken, goodFlow, nil); c != http.StatusCreated {
		t.Fatalf("deploy = %d", c)
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("publish = %d", c)
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/flows/orders/execute", adminToken, `{}`, nil); c != http.StatusAccepted {
		t.Fatalf("execute = %d", c)
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/flows/orders/execute", adminToken, `{"test":true}`, nil); c != http.StatusAccepted {
		t.Fatalf("test execute = %d", c)
	}

	var tasks struct {
		Tasks []struct {
			Test bool `json:"test"`
		} `json:"tasks"`
	}
	call(t, http.MethodGet, srv.URL+"/api/v1/tasks?limit=10", adminToken, "", &tasks)
	if len(tasks.Tasks) != 2 {
		t.Fatalf("tasks = %d, want 2", len(tasks.Tasks))
	}
	marked := 0
	for _, tk := range tasks.Tasks {
		if tk.Test {
			marked++
		}
	}
	if marked != 1 {
		t.Fatalf("%d of 2 tasks are test-marked, want exactly the one that asked", marked)
	}
}
