package mem

import (
	"sync"
	"testing"
)

func TestReserveRelease(t *testing.T) {
	g := New(100)
	if !g.TryReserve(60) {
		t.Fatal("first reserve failed")
	}
	if g.TryReserve(50) {
		t.Fatal("over-budget reserve succeeded")
	}
	if g.Used() != 60 {
		t.Fatalf("used = %d after failed reserve, want 60", g.Used())
	}
	g.Release(60)
	if g.Used() != 0 {
		t.Fatalf("used = %d, want 0", g.Used())
	}
	g.Reserve(150) // unconditional may overshoot
	if !g.OverBudget() {
		t.Fatal("expected over budget")
	}
	if g.Peak() != 150 {
		t.Fatalf("peak = %d, want 150", g.Peak())
	}
}

func TestDefaultBudget(t *testing.T) {
	if New(0).Budget() != DefaultBudget {
		t.Fatal("default budget not applied")
	}
}

func TestReleaseBelowZeroPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	New(10).Release(1)
}

func TestConcurrent(t *testing.T) {
	g := New(1 << 30)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10000 {
				if g.TryReserve(64) {
					g.Release(64)
				}
			}
		})
	}
	wg.Wait()
	if g.Used() != 0 {
		t.Fatalf("used = %d, want 0", g.Used())
	}
}
