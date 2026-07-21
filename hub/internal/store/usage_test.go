package store_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// wideRange brackets "now" generously so Postgres now() rows always fall in it.
func wideRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	return now.Add(-time.Hour), now.Add(time.Hour)
}

func TestUsageMeteringFromTasks(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "runner-a")
	deployPublished(t, s, "orders")

	// One completed task with record counts.
	id, err := s.Enqueue(ctx, "orders", 0, "ok", 2)
	if err != nil {
		t.Fatal(err)
	}
	lt, err := s.Claim(ctx, runnerID, 30*time.Second)
	if err != nil || lt == nil || lt.ID != id {
		t.Fatalf("claim = %+v, %v", lt, err)
	}
	if err := s.Complete(ctx, lt.ID, runnerID, json.RawMessage(`{"records_in":10,"records_out":8}`)); err != nil {
		t.Fatal(err)
	}

	// One task that fails to exhaustion (maxAttempts=1 → terminal on first fail).
	fid, err := s.Enqueue(ctx, "orders", 0, "boom", 1)
	if err != nil {
		t.Fatal(err)
	}
	flt, err := s.Claim(ctx, runnerID, 30*time.Second)
	if err != nil || flt == nil || flt.ID != fid {
		t.Fatalf("claim fail-task = %+v, %v", flt, err)
	}
	requeued, err := s.Fail(ctx, flt.ID, runnerID, "kaboom")
	if err != nil || requeued {
		t.Fatalf("terminal fail: requeued=%v err=%v", requeued, err)
	}

	since, until := wideRange()
	rep, err := s.Usage(ctx, since, until)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Totals.Executions != 2 || rep.Totals.Completed != 1 || rep.Totals.Failed != 1 {
		t.Fatalf("totals counts = %+v", rep.Totals)
	}
	if rep.Totals.RecordsIn != 10 || rep.Totals.RecordsOut != 8 {
		t.Fatalf("totals records = %+v", rep.Totals)
	}
	if len(rep.ByFlow) != 1 || rep.ByFlow[0].FlowName != "orders" || rep.ByFlow[0].Executions != 2 {
		t.Fatalf("by-flow = %+v", rep.ByFlow)
	}
	if len(rep.Series) < 1 {
		t.Fatalf("expected a daily series bucket, got %+v", rep.Series)
	}
}

func TestUsageMeteringFromDirectExecutions(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	start := time.Now().UTC().Add(-2 * time.Second)
	fin := start.Add(time.Second)
	if _, err := s.RecordDirectExecution(ctx, "", store.DirectExecution{
		FlowName: "hookflow", Trigger: "webhook", State: "completed",
		RecordsIn: 5, RecordsOut: 5, Started: &start, Finished: &fin,
	}); err != nil {
		t.Fatal(err)
	}

	since, until := wideRange()
	rep, err := s.Usage(ctx, since, until)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Totals.Executions != 1 || rep.Totals.RecordsIn != 5 {
		t.Fatalf("direct usage totals = %+v", rep.Totals)
	}
	// exec_seconds derives from started/finished (~1s).
	if rep.Totals.ExecSeconds < 0.9 || rep.Totals.ExecSeconds > 1.1 {
		t.Fatalf("exec_seconds = %v, want ~1", rep.Totals.ExecSeconds)
	}
}

func TestUsageEventsCursor(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "runner-a")
	deployPublished(t, s, "orders")
	for i := range 3 {
		id, err := s.Enqueue(ctx, "orders", 0, "k"+string(rune('a'+i)), 2)
		if err != nil {
			t.Fatal(err)
		}
		lt, _ := s.Claim(ctx, runnerID, 30*time.Second)
		if lt == nil || lt.ID != id {
			t.Fatalf("claim %d = %+v", i, lt)
		}
		if err := s.Complete(ctx, lt.ID, runnerID, json.RawMessage(`{"records_in":1}`)); err != nil {
			t.Fatal(err)
		}
	}

	// First page of 2 fills → next cursor set.
	page1, err := s.UsageEventsSince(ctx, 0, 2)
	if err != nil || len(page1) != 2 {
		t.Fatalf("page1 = %d, %v", len(page1), err)
	}
	if page1[0].ID >= page1[1].ID {
		t.Fatalf("events not id-ordered: %d, %d", page1[0].ID, page1[1].ID)
	}
	// Continue past the cursor: remaining row.
	page2, err := s.UsageEventsSince(ctx, page1[1].ID, 2)
	if err != nil || len(page2) != 1 {
		t.Fatalf("page2 = %d, %v", len(page2), err)
	}
	if page2[0].ID <= page1[1].ID {
		t.Fatalf("cursor did not advance: %d after %d", page2[0].ID, page1[1].ID)
	}
}

func TestUsageAccountIsolation(t *testing.T) {
	s := open(t)
	base := t.Context()
	acctB, err := s.CreateAccount(base, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	ctxB := store.WithAccount(base, acctB)

	// A direct execution under the default account only.
	now := time.Now().UTC()
	if _, err := s.RecordDirectExecution(base, "", store.DirectExecution{
		FlowName: "a-only", Trigger: "api", State: "completed",
		RecordsIn: 3, RecordsOut: 3, Started: &now, Finished: &now,
	}); err != nil {
		t.Fatal(err)
	}

	since, until := wideRange()
	repB, err := s.Usage(ctxB, since, until)
	if err != nil {
		t.Fatal(err)
	}
	if repB.Totals.Executions != 0 {
		t.Fatalf("account B saw account A usage: %+v", repB.Totals)
	}
	repA, err := s.Usage(base, since, until)
	if err != nil {
		t.Fatal(err)
	}
	if repA.Totals.Executions != 1 {
		t.Fatalf("account A usage = %+v", repA.Totals)
	}
}
