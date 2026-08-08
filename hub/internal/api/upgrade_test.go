package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Bulk connector upgrade — three staged steps (ADR-0047 §9).
//
// The tests below are all about the STAGING. A bulk republish that just worked
// would be the one-button version the ADR rejects; what makes it safe is that
// the report shows the right flows, the gate refuses anything untested, and
// the document that was tested is byte-for-byte the one that ships.

type batchResp struct {
	ID    string `json:"id"`
	Flows []struct {
		Flow      string `json:"flow"`
		From      string `json:"from"`
		Draft     int    `json:"draft_version"`
		TaskID    string `json:"task_id"`
		TaskState string `json:"task_state"`
	} `json:"flows"`
	Published *string `json:"published_at"`
}

type locateResp struct {
	Target string `json:"target"`
	Flows  []struct {
		Flow     string   `json:"flow"`
		Pinned   string   `json:"pinned"`
		Behind   int      `json:"behind"`
		Summary  string   `json:"summary"`
		Steps    []string `json:"steps"`
		Breaking []string `json:"breaking"`
	} `json:"flows"`
}

// upgradeFixture: a registry with gen 1.0.0 published. `release` lands further
// gen versions with a declared compat class; `releaseOther` does the same for
// a second connector, which is how a flow gets a step a gen batch does not
// touch.
func upgradeFixture(t *testing.T) (string, func(version, compat string), func(name, version string)) {
	t.Helper()
	srv, publish := registryWithCompat(t)
	release := func(version, compat string) {
		publish("gen", version, compat, "")
	}
	release("1.0.0", "compatible")
	return srv.URL, release, func(name, version string) { publish(name, version, "compatible", "") }
}

// twoConnectorFlow deploys and publishes a flow whose source and sink are
// DIFFERENT connectors, each pinned. A bulk upgrade moves one of them; the
// other is what proves a batch touches only its own connector.
func twoConnectorFlow(t *testing.T, srv, name, genVersion, sinkConnector, sinkVersion string) {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q,
	  "source":{"connector":"gen","action":"records","version":%q},
	  "sink":{"connector":%q,"action":"put","version":%q}}`,
		name, genVersion, sinkConnector, sinkVersion)
	var out struct {
		Version int `json:"version"`
	}
	if c := call(t, http.MethodPut, srv+"/api/v1/flows/"+name, adminToken, body, &out); c != http.StatusCreated {
		t.Fatalf("deploy %s = %d", name, c)
	}
	if code, _ := publishFlow(t, srv, name, out.Version); code != http.StatusOK {
		t.Fatalf("publish %s = %d", name, code)
	}
}

// deployPublished deploys a flow pinned to a connector build and publishes it.
func deployPublished(t *testing.T, srv, name, version string) {
	t.Helper()
	v := pinnedFlow(t, srv, name, "gen", version)
	if code, _ := publishFlow(t, srv, name, v); code != http.StatusOK {
		t.Fatalf("publish %s = %d", name, code)
	}
}

func locate(t *testing.T, srv, target string) locateResp {
	t.Helper()
	var out locateResp
	url := srv + "/api/v1/connectors/gen/upgrade"
	if target != "" {
		url += "?to=" + target
	}
	if c := call(t, http.MethodGet, url, adminToken, "", &out); c != http.StatusOK {
		t.Fatalf("locate = %d", c)
	}
	return out
}

func stage(t *testing.T, srv, target string, flows ...string) (batchResp, int) {
	t.Helper()
	names, _ := json.Marshal(flows)
	body := fmt.Sprintf(`{"to":%q,"flows":%s}`, target, names)
	var out batchResp
	code := call(t, http.MethodPost, srv+"/api/v1/connectors/gen/upgrade/test", adminToken, body, &out)
	return out, code
}

// finishTask leases whatever is queued and reports the given outcome, which is
// how these tests drive step 2 to a conclusion.
func finishTask(t *testing.T, srv, secret string, ok bool) {
	t.Helper()
	var lease struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if c := call(t, http.MethodPost, srv+"/api/v1/lease", secret, `{"wait_seconds":5}`, &lease); c != http.StatusOK {
		t.Fatalf("lease = %d", c)
	}
	path, body, want := "/complete", `{"records_in":1,"records_out":1}`, http.StatusNoContent
	if !ok {
		path, body, want = "/fail", `{"error":"the upgrade broke it"}`, http.StatusOK
	}
	if c := call(t, http.MethodPost, srv+"/api/v1/tasks/"+lease.Task.ID+path, secret, body, nil); c != want {
		t.Fatalf("report %s = %d", path, c)
	}
}

// Step 1 is a REPORT, and a report that lists the wrong flows is worse than no
// report — somebody acts on it in bulk. Three things it must get right: only
// what is genuinely behind, never something already ahead, and the folded diff
// across the whole span rather than the last hop.
func TestBulkLocateReportsOnlyWhatIsBehindTheTarget(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")

	release("2.0.0", "breaking")
	release("3.0.0", "compatible")
	deployPublished(t, srv, "already-current", "3.0.0")

	got := locate(t, srv, "3.0.0")
	if len(got.Flows) != 1 || got.Flows[0].Flow != "orders" {
		t.Fatalf("locate = %+v, want only the flow behind the target", got.Flows)
	}
	f := got.Flows[0]
	if f.Behind != 2 || f.Pinned != "1.0.0" {
		t.Fatalf("orders is %d behind at %s, want 2 behind at 1.0.0", f.Behind, f.Pinned)
	}
	// The span is folded: crossing 1.0.0 → 3.0.0 means crossing the breaking
	// 2.0.0, which somebody upgrading in bulk will never read the notes for
	// unless the report says it (§6).
	if len(f.Breaking) != 1 || f.Breaking[0] != "2.0.0" {
		t.Fatalf("breaking = %v, want the intervening 2.0.0 named", f.Breaking)
	}
	if !strings.Contains(f.Summary, "BREAKING") {
		t.Fatalf("summary does not name the breaking hop: %q", f.Summary)
	}
	if len(f.Steps) == 0 {
		t.Fatalf("locate names no steps for %s: an operator cannot see what changes", f.Flow)
	}
}

// Defaulting to newest is the overwhelmingly common intent, and a bulk upgrade
// that made you type the version you can already see is friction of exactly
// the kind §9 exists to remove.
func TestBulkLocateDefaultsToNewest(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")

	got := locate(t, srv, "")
	if got.Target != "2.0.0" {
		t.Fatalf("default target = %q, want the newest release", got.Target)
	}
	if len(got.Flows) != 1 {
		t.Fatalf("flows = %+v, want orders", got.Flows)
	}
}

// A flow's PREDECESSOR version is retained so a rollback has somewhere to land
// (§2). Republishing it forward in a batch would destroy exactly that, and
// would do it inside a bulk action nobody is reading closely.
func TestBulkLocateIgnoresTheRollbackTarget(t *testing.T) {
	srv, release, _ := upgradeFixture(t)

	// Two published versions of the same flow, each published while its pin was
	// still current — §4 would otherwise refuse the second as a stale forward
	// publish, which is a different rule from the one under test here.
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")
	deployPublished(t, srv, "orders", "2.0.0")
	release("3.0.0", "compatible")

	// v1 (pinned 1.0.0) is retained as the rollback target; only the current
	// version is a candidate.
	got := locate(t, srv, "3.0.0")
	if len(got.Flows) != 1 {
		t.Fatalf("locate = %+v, want one entry: the current version only", got.Flows)
	}
	if got.Flows[0].Pinned != "2.0.0" {
		t.Fatalf("candidate pins %s, want the CURRENT published version's pin", got.Flows[0].Pinned)
	}
}

// The gate, which is the whole reason step 2 exists. A publish-all that ran
// regardless would make testing advice, and advice is what the one-button
// version amounts to.
func TestPublishAllRefusesWhileAFlowIsStillUntested(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")

	b, code := stage(t, srv, "2.0.0", "orders")
	if code != http.StatusAccepted {
		t.Fatalf("stage = %d", code)
	}
	if len(b.Flows) != 1 || b.Flows[0].TaskID == "" {
		t.Fatalf("staged batch = %+v, want one flow with a queued test task", b.Flows)
	}

	// Still queued: proven nothing yet.
	var refused struct {
		Untested []string `json:"untested"`
	}
	url := srv + "/api/v1/connector-upgrades/" + b.ID + "/publish"
	if c := call(t, http.MethodPost, url, adminToken, "", &refused); c != http.StatusConflict {
		t.Fatalf("publish of an untested batch = %d, want 409", c)
	}
	if len(refused.Untested) != 1 || refused.Untested[0] != "orders" {
		t.Fatalf("refusal = %v, want the untested flow named", refused.Untested)
	}

	secret := registerRunner(t, srv, "r1")
	finishTask(t, srv, secret, true)

	var out struct {
		Published []string `json:"published"`
	}
	if c := call(t, http.MethodPost, url, adminToken, "", &out); c != http.StatusOK {
		t.Fatalf("publish after a passing test = %d", c)
	}
	if len(out.Published) != 1 || out.Published[0] != "orders" {
		t.Fatalf("published = %v, want orders", out.Published)
	}
}

// A test that FAILED has proven the opposite of what the gate is looking for.
func TestAFailedTestBlocksThePublish(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "breaking")

	b, _ := stage(t, srv, "2.0.0", "orders")
	secret := registerRunner(t, srv, "r1")
	finishTask(t, srv, secret, false)

	var refused struct {
		Untested []string `json:"untested"`
	}
	c := call(t, http.MethodPost, srv+"/api/v1/connector-upgrades/"+b.ID+"/publish", adminToken, "", &refused)
	if c != http.StatusConflict {
		t.Fatalf("publish over a failed test = %d, want 409", c)
	}
	if len(refused.Untested) != 1 {
		t.Fatalf("refusal = %v, want the failed flow named", refused.Untested)
	}
}

// The property that makes the gate mean anything: the draft that was TESTED is
// the draft that ships. Rebuilding an equivalent document at publish time
// would test one artifact and deploy another — and the difference would only
// show up when the registry moved between the two steps.
func TestTheTestedDraftIsTheOneThatShips(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")

	b, _ := stage(t, srv, "2.0.0", "orders")
	draft := b.Flows[0].Draft
	if b.Flows[0].From != "1.0.0" {
		t.Fatalf("batch records from=%q, want the build it is moving off", b.Flows[0].From)
	}

	secret := registerRunner(t, srv, "r1")
	finishTask(t, srv, secret, true)
	if c := call(t, http.MethodPost, srv+"/api/v1/connector-upgrades/"+b.ID+"/publish", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("publish = %d", c)
	}

	// A release landing between the test and the publish must NOT retarget the
	// batch: the target was fixed at stage time.
	release("3.0.0", "compatible")

	var got struct {
		Flow struct {
			PublishedVersion int `json:"published_version"`
		} `json:"flow"`
		Document struct {
			Source struct {
				Version string `json:"version"`
			} `json:"source"`
		} `json:"document"`
	}
	if c := call(t, http.MethodGet, srv+"/api/v1/flows/orders?version=published", adminToken, "", &got); c != http.StatusOK {
		t.Fatalf("published flow = %d", c)
	}
	if got.Flow.PublishedVersion != draft {
		t.Fatalf("published version %d is not the tested draft %d", got.Flow.PublishedVersion, draft)
	}
	if got.Document.Source.Version != "2.0.0" {
		t.Fatalf("published pin = %q, want the target that was staged and tested", got.Document.Source.Version)
	}
}

// Two operators pressing the button together would both pass the gate. The
// batch is claimed before anything is published, so the loser gets 409 and no
// flow is republished twice.
func TestABatchPublishesOnce(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")

	b, _ := stage(t, srv, "2.0.0", "orders")
	secret := registerRunner(t, srv, "r1")
	finishTask(t, srv, secret, true)

	url := srv + "/api/v1/connector-upgrades/" + b.ID + "/publish"
	if c := call(t, http.MethodPost, url, adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("first publish = %d", c)
	}
	if c := call(t, http.MethodPost, url, adminToken, "", nil); c != http.StatusConflict {
		t.Fatalf("second publish = %d, want 409", c)
	}
}

// Staging is all-or-nothing. A batch quietly missing a flow would pass its own
// gate and report success having left that flow behind — the silent partial
// change this ADR removes.
func TestStagingRefusesTheWholeBatchWhenOneFlowCannotMove(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")

	if _, code := stage(t, srv, "2.0.0", "orders", "no-such-flow"); code != http.StatusNotFound {
		t.Fatalf("stage with an unknown flow = %d, want a refusal", code)
	}
	// Nothing was recorded, so there is no half-batch to publish by accident.
	var list struct {
		Batches []batchResp `json:"batches"`
	}
	if c := call(t, http.MethodGet, srv+"/api/v1/connector-upgrades", adminToken, "", &list); c != http.StatusOK {
		t.Fatalf("list = %d", c)
	}
	if len(list.Batches) != 0 {
		t.Fatalf("a refused stage left %d batches behind", len(list.Batches))
	}
}

// A target the registry does not have is a typo, and applying a typo in bulk
// is how a batch pins forty flows to a build nobody published.
func TestStagingRejectsAnUnknownTarget(t *testing.T) {
	srv, _, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")

	if _, code := stage(t, srv, "9.9.9", "orders"); code != http.StatusUnprocessableEntity {
		t.Fatalf("stage to an unpublished version = %d, want 422", code)
	}
	var out locateResp
	c := call(t, http.MethodGet, srv+"/api/v1/connectors/gen/upgrade?to=9.9.9", adminToken, "", &out)
	if c != http.StatusUnprocessableEntity {
		t.Fatalf("locate to an unpublished version = %d, want 422", c)
	}
}

// The batch view is how somebody watches step 2 finish. It joins each flow's
// task state LIVE rather than copying it in at stage time — a run that
// completes after staging has to show up, or the publish button never unlocks
// and the gate reads as broken.
func TestTheBatchViewTracksEachFlowsTestRun(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")

	b, _ := stage(t, srv, "2.0.0", "orders")
	url := srv + "/api/v1/connector-upgrades/" + b.ID

	var before batchResp
	if c := call(t, http.MethodGet, url, adminToken, "", &before); c != http.StatusOK {
		t.Fatalf("batch view = %d", c)
	}
	if len(before.Flows) != 1 || before.Flows[0].TaskState != "queued" {
		t.Fatalf("flows = %+v, want the test task showing as queued", before.Flows)
	}
	if before.Published != nil {
		t.Fatal("an unpublished batch reports a publish timestamp")
	}

	secret := registerRunner(t, srv, "r1")
	finishTask(t, srv, secret, true)

	var after batchResp
	if c := call(t, http.MethodGet, url, adminToken, "", &after); c != http.StatusOK {
		t.Fatalf("batch view = %d", c)
	}
	if after.Flows[0].TaskState != "completed" {
		t.Fatalf("task state = %q after the run finished, want completed", after.Flows[0].TaskState)
	}
}

// A bulk route that skipped the ordinary publish gates would be the way to
// publish what the ordinary route refuses — and the reason to reach for it
// would be that the ordinary route refused. The batch only moves ONE
// connector, so a refusal here is about a DIFFERENT step: here, a second
// connector whose end of life has already passed (§7).
//
// The rest of the batch still publishes. Aborting midway would leave the
// flows already published published, and unwinding them would be a second
// unreviewed mass change.
func TestABatchRefusesAFlowThatFailsTheOrdinaryPublishGate(t *testing.T) {
	srv, release, releaseOther := upgradeFixture(t)
	releaseOther("sink-conn", "1.0.0")

	// "clean" moves cleanly; "encumbered" also pins a build about to be killed.
	deployPublished(t, srv, "clean", "1.0.0")
	twoConnectorFlow(t, srv, "encumbered", "1.0.0", "sink-conn", "1.0.0")

	// A pin past its deadline cannot resolve, so publishing it would produce a
	// flow that fails at its first task. Backdated: this is about a build that
	// is already dead, not one that is dying.
	past := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	if c := call(t, http.MethodPost, srv+"/api/v1/connectors/sink-conn/versions/1.0.0/eol", adminToken,
		`{"eol_at":"`+past+`","reason":"CVE in a dependency","target":"2.0.0"}`, nil); c != http.StatusOK {
		t.Fatalf("schedule eol = %d", c)
	}
	release("2.0.0", "compatible")

	b, code := stage(t, srv, "2.0.0", "clean", "encumbered")
	if code != http.StatusAccepted {
		t.Fatalf("stage = %d", code)
	}
	secret := registerRunner(t, srv, "r1")
	finishTask(t, srv, secret, true)
	finishTask(t, srv, secret, true)

	var out struct {
		Published []string            `json:"published"`
		Failed    []map[string]string `json:"failed"`
	}
	got := call(t, http.MethodPost, srv+"/api/v1/connector-upgrades/"+b.ID+"/publish", adminToken, "", &out)
	if got != http.StatusMultiStatus {
		t.Fatalf("partial publish = %d, want 207 — some published, some refused", got)
	}
	if len(out.Published) != 1 || out.Published[0] != "clean" {
		t.Fatalf("published = %v, want the flow that passes the gate", out.Published)
	}
	if len(out.Failed) != 1 || out.Failed[0]["flow"] != "encumbered" {
		t.Fatalf("failed = %v, want the encumbered flow named", out.Failed)
	}
	if !strings.Contains(out.Failed[0]["error"], "end of life") {
		t.Fatalf("the refusal does not say why: %q", out.Failed[0]["error"])
	}
}

// The refusals. Each one is a way somebody can ask for a bulk change that
// would not mean what they think, and every one of them has to be answered
// before anything is staged — a batch is the wrong place to discover a typo.
func TestBulkUpgradeRefusalsAreAnsweredBeforeAnythingIsStaged(t *testing.T) {
	srv, release, _ := upgradeFixture(t)
	deployPublished(t, srv, "orders", "1.0.0")
	release("2.0.0", "compatible")

	// A connector the registry has never heard of. 404 rather than an empty
	// report: "no flows are behind" and "there is no such connector" are
	// different answers, and only one of them means you are done.
	if c := call(t, http.MethodGet, srv+"/api/v1/connectors/nope/upgrade", adminToken, "", nil); c != http.StatusNotFound {
		t.Fatalf("locate on an unknown connector = %d, want 404", c)
	}

	// A batch names the flows it moves; it does not select them for you.
	// Defaulting to "all of them" would turn a mistyped request into the
	// largest possible change.
	if c := call(t, http.MethodPost, srv+"/api/v1/connectors/gen/upgrade/test", adminToken,
		`{"to":"2.0.0","flows":[]}`, nil); c != http.StatusUnprocessableEntity {
		t.Fatalf("stage with no flows = %d, want 422", c)
	}
	if c := call(t, http.MethodPost, srv+"/api/v1/connectors/gen/upgrade/test", adminToken,
		`not json`, nil); c != http.StatusBadRequest {
		t.Fatalf("stage with a malformed body = %d, want 400", c)
	}

	// A flow already at the target has nothing to move, and staging it would
	// create a draft identical to what is published — then republish it, which
	// is a live change that achieves nothing.
	deployPublished(t, srv, "current", "2.0.0")
	if _, code := stage(t, srv, "2.0.0", "current"); code == http.StatusAccepted {
		t.Fatal("staged a flow that is already at the target")
	}

	// An unknown batch id.
	bad := srv + "/api/v1/connector-upgrades/00000000-0000-0000-0000-000000000000"
	if c := call(t, http.MethodGet, bad, adminToken, "", nil); c != http.StatusNotFound {
		t.Fatalf("unknown batch = %d, want 404", c)
	}
	if c := call(t, http.MethodPost, bad+"/publish", adminToken, "", nil); c != http.StatusNotFound {
		t.Fatalf("publish of an unknown batch = %d, want 404", c)
	}

	// The audit list is readable whether or not anything has been staged.
	var list struct {
		Batches []batchResp `json:"batches"`
	}
	if c := call(t, http.MethodGet, srv+"/api/v1/connector-upgrades?limit=5", adminToken, "", &list); c != http.StatusOK {
		t.Fatalf("batch list = %d", c)
	}
}
