package leaktest

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// The detector's two failure modes are opposite and both fatal to its
// usefulness: miss a real leak (silently useless) or invent one (a flaky gate
// that gets deleted within a week). Every test here pins one of the two.

func TestAParkedGoroutineIsReportedAsALeak(t *testing.T) {
	before := index(stacks())

	block := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		<-block // never sent to until the test ends: this is the leak
	}()
	<-started

	leaked := settle(before)
	if len(leaked) != 1 {
		t.Fatalf("want exactly 1 leaked goroutine, got %d", len(leaked))
	}
	if !strings.Contains(leaked[0].stack, "TestAParkedGoroutineIsReportedAsALeak") {
		t.Errorf("the report must name the code that leaked; got:\n%s", leaked[0].stack)
	}

	close(block)
	if remaining := settle(before); len(remaining) != 0 {
		t.Errorf("after releasing it, %d goroutine(s) still reported", len(remaining))
	}
}

// The whole reason settle retries: a correct shutdown is asynchronous. A
// detector that sampled once would fail every test that closes cleanly.
func TestAGoroutineStillWindingDownIsNotALeak(t *testing.T) {
	before := index(stacks())

	var wg sync.WaitGroup
	wg.Go(func() {
		time.Sleep(50 * time.Millisecond)
	})

	if leaked := settle(before); len(leaked) != 0 {
		t.Errorf("a goroutine that exits within the settle window is not a leak; got %d:\n%s",
			len(leaked), report(leaked))
	}
	wg.Wait()
}

// The identity diff is what lets this run without a denylist of every
// framework goroutine: whatever was already alive is not the test's problem.
func TestGoroutinesAliveBeforeTheSnapshotAreIgnored(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	started := make(chan struct{})
	go func() {
		close(started)
		<-block
	}()
	<-started

	// Snapshot AFTER starting it — so it is pre-existing.
	before := index(stacks())
	if leaked := diff(before); len(leaked) != 0 {
		t.Errorf("pre-existing goroutines must not count as leaks; got %d:\n%s",
			len(leaked), report(leaked))
	}
}

func TestRuntimeOwnedGoroutinesAreIgnored(t *testing.T) {
	for _, creator := range ignoredCreators {
		g := goroutine{id: 1, state: "running", stack: "goroutine 1 [running]:\ncreated by " + creator + " in goroutine 1"}
		if !ignored(g) {
			t.Errorf("%s should be ignored: no test can stop a runtime-owned goroutine", creator)
		}
	}
}

// The exclusions that would have been convenient and are deliberately absent —
// leaving an HTTP connection or a *sql.DB open IS the bug this package is for.
func TestConnectionGoroutinesAreNotExcused(t *testing.T) {
	for _, creator := range []string{
		"net/http.(*Transport).dialConnFor",
		"database/sql.OpenDB",
	} {
		g := goroutine{id: 1, state: "IO wait", stack: "goroutine 1 [IO wait]:\ncreated by " + creator}
		if ignored(g) {
			t.Errorf("%s must NOT be ignored — an unclosed client is a real finding", creator)
		}
	}
}

func TestTheDumpParsesIntoDistinctGoroutines(t *testing.T) {
	gs := stacks()
	if len(gs) == 0 {
		t.Fatal("parsed no goroutines out of a live dump")
	}
	seen := map[uint64]bool{}
	for _, g := range gs {
		if g.id == 0 {
			t.Errorf("goroutine parsed with id 0: %q", g.stack)
		}
		if seen[g.id] {
			t.Errorf("goroutine id %d parsed twice", g.id)
		}
		seen[g.id] = true
		if g.state == "" {
			t.Errorf("goroutine %d parsed with no state: %q", g.id, g.stack)
		}
		if strings.HasSuffix(g.state, "]:") || strings.HasPrefix(g.state, "[") {
			t.Errorf("goroutine %d state kept its brackets: %q", g.id, g.state)
		}
	}
	// A state carrying a duration ("chan receive, 3 minutes") must survive
	// intact rather than being truncated at the comma.
	if got := parseState("goroutine 7 [chan receive, 3 minutes]:"); got != "chan receive, 3 minutes" {
		t.Errorf("state = %q, want %q", got, "chan receive, 3 minutes")
	}
}

// parseState mirrors the head-line parsing in stacks() so the bracket/duration
// handling can be asserted on a fixed input rather than on whatever the runtime
// happens to be doing.
func parseState(head string) string {
	rest, _ := strings.CutPrefix(head, "goroutine ")
	_, stateText, _ := strings.Cut(rest, " ")
	return strings.TrimSuffix(strings.TrimPrefix(stateText, "["), "]:")
}

func TestTheReportIsCappedAndSaysHowManyWereHidden(t *testing.T) {
	var many []goroutine
	for i := range maxReported + 3 {
		many = append(many, goroutine{id: uint64(i + 1), state: "chan receive", stack: "stack"})
	}
	out := report(many)
	if !strings.Contains(out, "and 3 more stack(s)") {
		t.Errorf("report must say how many stacks it hid; got:\n%s", out)
	}
	if got := strings.Count(out, "--- goroutine "); got != maxReported {
		t.Errorf("printed %d stacks, want the %d cap", got, maxReported)
	}
}

// The cap is why the tally exists: with 5 stacks shown out of 40, the counts
// are the only place the shape of the leak survives.
func TestTheTallyCoversEveryLeakedGoroutineNotJustThePrintedOnes(t *testing.T) {
	stack := func(creator string) string {
		return "goroutine 1 [select]:\nsomepkg.fn()\ncreated by " + creator + " in goroutine 7"
	}
	var many []goroutine
	for i := range 20 {
		many = append(many, goroutine{id: uint64(i + 1), state: "select", stack: stack("pkg.Noisy")})
	}
	many = append(many, goroutine{id: 99, state: "select", stack: stack("pkg.Rare")})

	out := report(many)
	if !strings.Contains(out, " 20  pkg.Noisy") {
		t.Errorf("tally must count all 20 of the frequent creator; got:\n%s", out)
	}
	// pkg.Rare appears once and sorts last, so it is well outside the stack cap —
	// without the tally it would be invisible, which is the bug this closes.
	if !strings.Contains(out, "  1  pkg.Rare") {
		t.Errorf("a creator outside the printed cap must still appear in the tally; got:\n%s", out)
	}
	// Scoped to the tally section — the raw stacks below it legitimately keep
	// the creating goroutine number.
	tallySection, _, _ := strings.Cut(out, "--- goroutine ")
	if strings.Contains(tallySection, "in goroutine 7") {
		t.Errorf("the tally must strip the creating goroutine number, else identical\n"+
			"creators split into separate rows; got:\n%s", tallySection)
	}
}

func TestTallyOrdersByFrequencyThenName(t *testing.T) {
	g := func(creator string) goroutine {
		return goroutine{stack: "created by " + creator + " in goroutine 1"}
	}
	got := tally([]goroutine{g("b.One"), g("a.Two"), g("a.Two"), g("c.One")})
	want := []creatorCount{{"a.Two", 2}, {"b.One", 1}, {"c.One", 1}}
	if len(got) != len(want) {
		t.Fatalf("tally length %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("tally[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAGoroutineWithNoCreatorFrameStillTallies(t *testing.T) {
	got := tally([]goroutine{{stack: "goroutine 1 [running]:\nmain.main()"}})
	if len(got) != 1 || got[0].creator != "?" {
		t.Errorf("tally = %+v, want a single \"?\" row", got)
	}
}
