package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentClaimExclusive is a control-plane load / correctness test
// (issue #10): under concurrent enqueue and concurrent claiming by several
// runners, FOR UPDATE SKIP LOCKED must hand each task to exactly one runner —
// no task lost, none double-claimed.
func TestConcurrentClaimExclusive(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deployPublished(t, s, "orders")

	const N = 30
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.Enqueue(ctx, "orders", 0, fmt.Sprintf("k-%d", i), 3); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("enqueue: %v", err)
	}

	// Register runners up front (registerRunner may t.Fatal — must run on the
	// test goroutine, not inside a claimer).
	const R = 4
	runners := make([]string, R)
	for r := 0; r < R; r++ {
		runners[r], _ = registerRunner(t, s, fmt.Sprintf("r%d", r))
	}

	var mu sync.Mutex
	seen := map[string]int{}
	claimErr := make(chan error, R)
	var cwg sync.WaitGroup
	for _, rid := range runners {
		cwg.Add(1)
		go func(rid string) {
			defer cwg.Done()
			for {
				lt, err := s.Claim(ctx, rid, 30*time.Second)
				if err != nil {
					claimErr <- err
					return
				}
				if lt == nil { // queue drained
					return
				}
				mu.Lock()
				seen[lt.ID]++
				mu.Unlock()
			}
		}(rid)
	}
	cwg.Wait()
	close(claimErr)
	for err := range claimErr {
		t.Fatalf("claim: %v", err)
	}

	if len(seen) != N {
		t.Fatalf("claimed %d distinct tasks, want %d", len(seen), N)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("task %s claimed %d times (want exactly once)", id, c)
		}
	}
}
