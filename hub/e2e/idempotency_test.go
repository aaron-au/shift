package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

// taskKey is the key the trigger supplies; the sink must present exactly
// these bytes (plus the http sink's batch ordinal) on every attempt.
const taskKey = "idem-e2e-2026-08-09T00:15:00Z"

// idemFlow runs ~6s: 60 batches x 100ms, each POSTed to the recorder below,
// which is where the sink-visible idempotency key becomes observable.
const idemFlow = `{"name":"idem",
  "source":{"connector":"gen","action":"gen","config":{"records":60000,"batch_records":1000,"delay_ms":100}},
  "sink":{"connector":"http","action":"post","config":{"url":%q,"allow_local":true}}}`

// TestTheSinkSeesTheSameIdempotencyKeyAfterACrashRedispatch closes the
// at-least-once safety claim (TC-010, ADR-0002) at the only level that
// actually proves it: a REAL receiver's view.
//
// TestCrashRecovery proves the queue re-dispatches after a kill -9; the store
// tests prove the queue row keeps its key; pkg/flowdoc proves the key is
// derived and injected. None of them looks at what arrives at the far end.
// This one does: a real runnerd leases the task, a real http connector POSTs
// to a real HTTP server, the runner is SIGKILLed mid-flow, a second runner
// replays the task, and the assertion is on the `Idempotency-Key` headers the
// destination actually received across the two attempts. If any hop between
// the queue row and the sink regenerated, dropped or mutated the key, the
// header sequence would differ and a receiver deduping on it would double-write.
func TestTheSinkSeesTheSameIdempotencyKeyAfterACrashRedispatch(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("needs postgres + real processes")
	}

	st, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, err := api.Handler(st, api.Options{
		AdminToken: adminToken,
		LeaseTTL:   2 * time.Second,
		LeasePoll:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub := httptest.NewServer(h)
	t.Cleanup(hub.Close)

	rec := &keyRecorder{}
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r.Header.Get("Idempotency-Key"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	bin := t.TempDir()
	build(t, bin, "runnerd", "github.com/aaron-au/shift/runner/cmd/runnerd")
	build(t, bin, "shift-connector-gen", "github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	build(t, bin, "shift-connector-http", "github.com/aaron-au/shift/connectors/cmd/shift-connector-http")

	doJSON(t, hub.URL, "PUT", "/api/v1/flows/idem", fmt.Sprintf(idemFlow, dest.URL), nil)
	doJSON(t, hub.URL, "POST", "/api/v1/flows/idem/versions/1/publish", "", nil)
	var acc struct {
		TaskID string `json:"task_id"`
	}
	doJSON(t, hub.URL, "POST", "/api/v1/flows/idem/execute",
		fmt.Sprintf(`{"idempotency_key":%q}`, taskKey), &acc)

	// Attempt 1: run until the sink has genuinely written, then kill without
	// warning. Waiting on the recorder rather than a fixed sleep is what makes
	// the "before" half non-empty deterministically.
	victim := startRunner(t, hub.URL, bin, "victim", "127.0.0.1:18351")
	waitFor(t, 60*time.Second, func() (bool, string) {
		n := len(rec.snapshot())
		return n >= 5, fmt.Sprintf("sink writes so far: %d", n)
	})
	if err := victim.Process.Kill(); err != nil { // SIGKILL: no drain, no goodbye
		t.Fatal(err)
	}
	_ = victim.Wait()
	// Let any request already in flight land before we split the record, so
	// the "after" half contains attempt 2 and nothing else.
	time.Sleep(500 * time.Millisecond)
	firstAttempt := rec.snapshot()
	t.Logf("runner killed after %d sink writes", len(firstAttempt))

	// Attempt 2: a different runner replays the whole task.
	startRunner(t, hub.URL, bin, "rescuer", "127.0.0.1:18352")
	waitFor(t, 120*time.Second, func() (bool, string) {
		tk := getTask(t, hub.URL, acc.TaskID)
		return tk.Task.State == "completed", "task " + tk.Task.State + " " + tk.Task.Error
	})

	tk := getTask(t, hub.URL, acc.TaskID)
	if tk.Task.Attempt != 2 {
		t.Fatalf("attempts = %d, want 2 (no re-dispatch happened, so this proves nothing)", tk.Task.Attempt)
	}
	if len(tk.Attempts) != 2 || tk.Attempts[0].RunnerID == tk.Attempts[1].RunnerID {
		t.Fatalf("attempt history = %+v, want two attempts on different runners", tk.Attempts)
	}

	all := rec.snapshot()
	secondAttempt := all[len(firstAttempt):]
	if len(firstAttempt) == 0 || len(secondAttempt) == 0 {
		t.Fatalf("sink writes: %d before the kill, %d after — need both halves",
			len(firstAttempt), len(secondAttempt))
	}

	// 1) Every header the destination ever saw carries the task's key
	// verbatim. The http sink appends `:<batch ordinal>` (its own dedup
	// granularity), so the assertion is on the exact prefix bytes plus a
	// numeric tail — nothing else is allowed to vary.
	for i, got := range all {
		ordinal, ok := strings.CutPrefix(got, taskKey+":")
		if !ok {
			t.Fatalf("sink write %d saw Idempotency-Key %q, want prefix %q", i, got, taskKey+":")
		}
		if _, err := strconv.Atoi(ordinal); err != nil {
			t.Fatalf("sink write %d: batch ordinal %q is not a number (header %q)", i, ordinal, got)
		}
	}

	// 2) The replay re-presents the SAME key sequence, starting from the same
	// first value. That identity is the entire dedup contract: a receiver that
	// remembers `taskKey:0` from attempt 1 recognises attempt 2's first write
	// and drops it. A key that were merely "non-empty and well-formed" on both
	// attempts would satisfy check 1 and still double-write.
	if secondAttempt[0] != firstAttempt[0] {
		t.Fatalf("replay started at key %q, attempt 1 started at %q — the receiver cannot dedup these",
			secondAttempt[0], firstAttempt[0])
	}
	overlap := min(len(firstAttempt), len(secondAttempt))
	for i := range overlap {
		if firstAttempt[i] != secondAttempt[i] {
			t.Fatalf("write %d: attempt 1 sent %q, attempt 2 sent %q", i, firstAttempt[i], secondAttempt[i])
		}
	}
	t.Logf("sink saw %d writes on attempt 1 and %d on attempt 2; keys identical over the %d-write overlap",
		len(firstAttempt), len(secondAttempt), overlap)
}

// keyRecorder collects the Idempotency-Key of every request the destination
// receives, in arrival order.
type keyRecorder struct {
	mu   sync.Mutex
	keys []string
}

func (r *keyRecorder) add(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
}

func (r *keyRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.keys...)
}
