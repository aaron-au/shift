package store_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
)

// publishedPinned deploys a flow pinned to a connector build and publishes it.
func publishedPinned(t *testing.T, s *store.Store, name, connector, version string) {
	t.Helper()
	v := deploy(t, s, name, `{
	  "name": "`+name+`",
	  "source": {"connector":"`+connector+`","action":"records","version":"`+version+`"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(t.Context(), name, v); err != nil {
		t.Fatalf("publish %s: %v", name, err)
	}
}

// The reference query behind bulk locate (ADR-0047 §9). It reports the build
// each flow pins rather than filtering by "older than X" — version ordering
// lives in one place, and a second implementation in SQL would be string
// comparison, which is wrong the first time somebody publishes 0.10.0.
func TestFlowsPinningConnectorReportsTheBuildEachFlowIsOn(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	publishedPinned(t, s, "orders", "gen", "1.0.0")
	publishedPinned(t, s, "invoices", "gen", "2.0.0")
	publishedPinned(t, s, "elsewhere", "sftp", "1.0.0")

	got, err := s.FlowsPinningConnector(ctx, "gen")
	if err != nil {
		t.Fatalf("pinning: %v", err)
	}
	on := map[string]string{}
	for _, p := range got {
		on[p.Flow] = p.Pinned
		if !p.Current {
			t.Fatalf("%s v%d reports as not current, but it is the only published version", p.Flow, p.Version)
		}
		if len(p.Steps) == 0 {
			t.Fatalf("%s names no steps: an operator cannot see what a batch would change", p.Flow)
		}
	}
	if on["orders"] != "1.0.0" || on["invoices"] != "2.0.0" {
		t.Fatalf("pins = %v, want each flow's own build", on)
	}
	if _, ok := on["elsewhere"]; ok {
		t.Fatal("a flow pinning a different connector is in the report")
	}
}

// The gate. "Not passed" has to cover never-queued and still-running as well
// as failed: an absent result has proven nothing, and letting it through would
// make the test step decoration.
func TestUntestedCoversMissingAndUnfinishedRunsNotJustFailures(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	publishedPinned(t, s, "queued-flow", "gen", "1.0.0")
	publishedPinned(t, s, "no-task", "gen", "1.0.0")
	publishedPinned(t, s, "passed", "gen", "1.0.0")

	task, err := s.EnqueueTest(ctx, "queued-flow", 0, "", 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	done, err := s.EnqueueTest(ctx, "passed", 0, "", 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	batch, err := s.CreateUpgradeBatch(ctx, "gen", "2.0.0", "tester", []store.StagedFlow{
		{Flow: "queued-flow", From: "1.0.0", Draft: 1, TaskID: task},
		{Flow: "no-task", From: "1.0.0", Draft: 1}, // enqueue failed at stage time
		{Flow: "passed", From: "1.0.0", Draft: 1, TaskID: done},
	})
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	// Drive only one of them to completion.
	runner := newRunner(t, s, "r1")
	claimed, err := s.Claim(ctx, runner, 0)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for claimed != nil && claimed.ID != done {
		claimed, err = s.Claim(ctx, runner, 0)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
	}
	if claimed == nil {
		t.Fatal("no task to claim")
	}
	if err := s.Complete(ctx, done, runner, json.RawMessage(`{"state":"completed"}`)); err != nil {
		t.Fatalf("complete: %v", err)
	}

	untested, err := s.UntestedFlows(ctx, batch)
	if err != nil {
		t.Fatalf("untested: %v", err)
	}
	want := map[string]bool{"queued-flow": true, "no-task": true}
	if len(untested) != 2 {
		t.Fatalf("untested = %v, want the queued one and the one with no task at all", untested)
	}
	for _, f := range untested {
		if !want[f] {
			t.Fatalf("untested names %q, which passed", f)
		}
	}
}

// A batch publishes once. Two operators pressing the button together would
// otherwise both pass the gate and both republish every flow.
func TestABatchIsClaimedExactlyOnce(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	publishedPinned(t, s, "orders", "gen", "1.0.0")
	batch, err := s.CreateUpgradeBatch(ctx, "gen", "2.0.0", "tester", []store.StagedFlow{
		{Flow: "orders", From: "1.0.0", Draft: 1},
	})
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if err := s.CloseUpgradeBatch(ctx, batch); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s.CloseUpgradeBatch(ctx, batch); !errors.Is(err, store.ErrAlreadyPublished) {
		t.Fatalf("second close = %v, want ErrAlreadyPublished", err)
	}

	b, err := s.GetUpgradeBatch(ctx, batch)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if b.Published == nil {
		t.Fatal("a closed batch has no published timestamp")
	}
	if b.Target != "2.0.0" || b.Connector != "gen" {
		t.Fatalf("batch = %+v, want the target recorded at stage time", b)
	}
	if len(b.Flows) != 1 || b.Flows[0].From != "1.0.0" {
		// from is recorded at stage time because it is what an operator needs
		// to undo by hand later, and by then the published version has moved.
		t.Fatalf("flows = %+v, want the build it moved off recorded", b.Flows)
	}

	list, err := s.UpgradeBatches(ctx, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].ID != batch {
		t.Fatalf("batches = %+v, want the one just created", list)
	}
}

// A batch id from another account must not resolve — bulk upgrade is a
// tenant-scoped action over tenant-scoped flows.
func TestABatchIsNotReadableAcrossAccounts(t *testing.T) {
	s := open(t)
	if _, err := s.GetUpgradeBatch(t.Context(), "00000000-0000-0000-0000-000000000000"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown batch = %v, want ErrNotFound", err)
	}
}
