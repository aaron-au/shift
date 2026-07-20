package service

import (
	"testing"
)

func TestTieredBenchmark(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})

	rep, err := svc.RunTieredBenchmark(2000, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"simple", "standard", "complex", "extreme"}
	if len(rep.Tiers) != len(want) {
		t.Fatalf("tiers = %d, want %d", len(rep.Tiers), len(want))
	}
	for i, tr := range rep.Tiers {
		if tr.Tier != want[i] {
			t.Fatalf("tier[%d] = %q, want %q", i, tr.Tier, want[i])
		}
		if tr.SingleStreamRecS <= 0 || tr.AggregateRecS <= 0 {
			t.Errorf("tier %s: non-positive throughput single=%.1f aggregate=%.1f",
				tr.Tier, tr.SingleStreamRecS, tr.AggregateRecS)
		}
		if tr.Shape == "" {
			t.Errorf("tier %s: missing shape label", tr.Tier)
		}
	}

	// Reported through Status (the dashboard's source) and history.
	st := svc.Status()
	if st.Tiered == nil || len(st.Tiered.Tiers) != len(want) {
		t.Fatalf("status.tiered = %+v", st.Tiered)
	}
	if len(svc.TieredHistory()) != 1 {
		t.Fatalf("history = %d, want 1", len(svc.TieredHistory()))
	}

	// One at a time.
	svc.bench.mu.Lock()
	svc.bench.tieredRunning = true
	svc.bench.mu.Unlock()
	if _, err := svc.RunTieredBenchmark(2000, 1); err == nil {
		t.Fatal("expected rejection while a tiered benchmark runs")
	}
	svc.bench.mu.Lock()
	svc.bench.tieredRunning = false
	svc.bench.mu.Unlock()
}
