// Package leaktest fails a test package when goroutines it started are still
// running after the tests finish.
//
// THIS IS A VERBATIM COPY of engine/leaktest, and must stay one — everything
// from the package clause down is compared byte-for-byte by
// TestTheGatewayLeaktestCopyHasNotDrifted in pkg/shiftlog. Only this doc
// comment differs.
//
// The copy exists for the same reason the gateway keeps its own logging setup
// (ADR-0046 §2): gateway/go.mod has no `require` line and a test enforces that,
// because the gateway is the one component that sits in a DMZ and what it can
// import is a security property. Importing engine/leaktest — even for tests
// only — would put a module dependency in that go.mod.
//
// See engine/leaktest for the full rationale and usage.
package leaktest

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// settleTimeout bounds how long a goroutine gets to notice a closed channel or
// a cancelled context and exit. A leak check that reported the first sample
// would flag every correct-but-asynchronous shutdown, so the diff is retried
// until it comes back empty or this elapses.
const settleTimeout = 2 * time.Second

// maxReported caps the stacks printed. A single missing Close usually leaks one
// goroutine per test, so an unbounded report is thousands of near-identical
// stacks with the real information at the top.
const maxReported = 5

// ignoredCreators are goroutines owned by the Go runtime itself. They are
// listed by their `created by` frame, they can appear part-way through a run
// (GC workers scale with GOMAXPROCS; the signal goroutines start on first use),
// and no test can stop them — so they are not leaks in any actionable sense.
//
// Deliberately NOT listed: net/http persistConn read/write loops and
// database/sql connectionOpener. Those outlive a test only when something was
// left open, which is exactly the bug class this package exists to surface.
var ignoredCreators = []string{
	"runtime.gcBgMarkStartWorkers",
	"runtime.bgsweep",
	"runtime.bgscavenge",
	"runtime.init", // forcegchelper, runfinq
	"runtime.ensureSigM",
	"os/signal.Notify",
	"os/signal.init",
}

// A goroutine as parsed out of a runtime.Stack dump.
type goroutine struct {
	id    uint64
	state string
	stack string
}

// Main runs m, runs any cleanup funcs, then fails the package if goroutines
// started during the run are still alive. It calls os.Exit and does not return.
//
// The check is skipped when the tests themselves failed: a failing test often
// abandons a goroutine on purpose (t.Fatal from a helper), and reporting that
// as a leak buries the real failure under a stack dump.
func Main(m *testing.M, cleanup ...func()) {
	before := index(stacks())
	code := m.Run()
	for _, fn := range cleanup {
		fn()
	}
	if code == 0 {
		if leaked := settle(before); len(leaked) > 0 {
			fmt.Fprint(os.Stderr, report(leaked))
			code = 1
		}
	}
	os.Exit(code)
}

// Check snapshots the running goroutines and returns a func that fails t if any
// goroutine created in between is still alive. Intended as
//
//	defer leaktest.Check(t)()
//
// Not safe in a package that runs tests in parallel — use Main there.
func Check(t *testing.T) func() {
	t.Helper()
	before := index(stacks())
	return func() {
		if leaked := settle(before); len(leaked) > 0 {
			t.Errorf("%s", report(leaked))
		}
	}
}

// settle polls until no goroutine outside before remains, or settleTimeout
// elapses. Backs off so a fast, clean shutdown costs one millisecond rather
// than the whole budget.
func settle(before map[uint64]bool) []goroutine {
	deadline := time.Now().Add(settleTimeout)
	delay := time.Millisecond
	for {
		leaked := diff(before)
		if len(leaked) == 0 || time.Now().After(deadline) {
			return leaked
		}
		time.Sleep(delay)
		if delay < 50*time.Millisecond {
			delay *= 2
		}
	}
}

// diff returns the goroutines alive now that were neither alive at the snapshot
// nor owned by the runtime.
func diff(before map[uint64]bool) []goroutine {
	var leaked []goroutine
	for _, g := range stacks() {
		if before[g.id] || ignored(g) {
			continue
		}
		leaked = append(leaked, g)
	}
	return leaked
}

func ignored(g goroutine) bool {
	for _, creator := range ignoredCreators {
		if strings.Contains(g.stack, "created by "+creator) {
			return true
		}
	}
	return false
}

func index(gs []goroutine) map[uint64]bool {
	m := make(map[uint64]bool, len(gs))
	for _, g := range gs {
		m[g.id] = true
	}
	return m
}

// stacks dumps every goroutine and parses the blocks apart. The dump has no
// stable machine format, so this parses defensively: an unrecognised block is
// skipped rather than guessed at, since a mis-parse would either invent a leak
// or hide one.
func stacks() []goroutine {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	var out []goroutine
	for block := range strings.SplitSeq(string(buf), "\n\n") {
		block = strings.TrimSpace(block)
		head, _, ok := strings.Cut(block, "\n")
		if !ok {
			continue
		}
		// "goroutine 42 [chan receive, 3 minutes]:"
		rest, ok := strings.CutPrefix(head, "goroutine ")
		if !ok {
			continue
		}
		idText, stateText, ok := strings.Cut(rest, " ")
		if !ok {
			continue
		}
		id, err := strconv.ParseUint(idText, 10, 64)
		if err != nil {
			continue
		}
		state := strings.TrimSuffix(strings.TrimPrefix(stateText, "["), "]:")
		out = append(out, goroutine{id: id, state: state, stack: block})
	}
	return out
}

func report(leaked []goroutine) string {
	var b strings.Builder
	fmt.Fprintf(&b, "leaktest: %d goroutine(s) still running after the tests finished.\n"+
		"Each one was started during the run and never stopped — a flow, task, pipe or\n"+
		"client that was not closed or cancelled (ADR-0005/ADR-0029).\n\n", len(leaked))

	// Tally by creator BEFORE the capped stacks. One missing Close leaks the
	// same goroutine many times over, so the counts are the diagnosis and the
	// stacks are only the evidence — a report that capped the stacks without
	// the tally would hide how many distinct sources there are.
	fmt.Fprintf(&b, "by creator:\n")
	for _, c := range tally(leaked) {
		fmt.Fprintf(&b, "  %3d  %s\n", c.n, c.creator)
	}
	fmt.Fprintln(&b)

	for i, g := range leaked {
		if i == maxReported {
			fmt.Fprintf(&b, "... and %d more stack(s); the tally above covers all %d\n",
				len(leaked)-maxReported, len(leaked))
			break
		}
		fmt.Fprintf(&b, "--- goroutine %d [%s]:\n%s\n\n", g.id, g.state, g.stack)
	}
	return b.String()
}

type creatorCount struct {
	creator string
	n       int
}

// tally groups leaked goroutines by their `created by` frame, most frequent
// first. Goroutines with no creator frame (started by the runtime before any
// user code) group under "?".
func tally(leaked []goroutine) []creatorCount {
	counts := map[string]int{}
	for _, g := range leaked {
		counts[creatorOf(g)]++
	}
	out := make([]creatorCount, 0, len(counts))
	for c, n := range counts {
		out = append(out, creatorCount{creator: c, n: n})
	}
	slices.SortFunc(out, func(a, b creatorCount) int {
		if a.n != b.n {
			return b.n - a.n // most frequent first
		}
		return strings.Compare(a.creator, b.creator) // stable, so output diffs cleanly
	})
	return out
}

func creatorOf(g goroutine) string {
	_, after, ok := strings.Cut(g.stack, "created by ")
	if !ok {
		return "?"
	}
	line, _, _ := strings.Cut(after, "\n")
	// "pkg.Func in goroutine 41" — the goroutine number is noise in a tally.
	if name, _, ok := strings.Cut(line, " in goroutine "); ok {
		return name
	}
	return line
}
