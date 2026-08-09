package main

import (
	"runtime/debug"
	"testing"

	"github.com/aaron-au/shift/runner/internal/api"
	"github.com/aaron-au/shift/runner/internal/task"
)

// Regression: a hub-less runner served exactly ONE gateway request and then
// died. The completion hook wrapped a nil reporter in a non-nil closure, and
// because the hook runs on its own goroutine the nil call panicked THERE —
// which takes the whole process down rather than failing one request.
//
// Caught by running it in a cluster, not by the unit tests, because the bug
// lived in the wiring rather than in either component. Hence this test.
func TestGatewayOnDoneIsNilWithoutAHub(t *testing.T) {
	if got := gatewayOnDone(nil); got != nil {
		t.Fatal("gatewayOnDone(nil) returned a non-nil hook; calling it would panic on its goroutine")
	}
}

func TestGatewayOnDoneReportsWithAHub(t *testing.T) {
	var gotTask, gotTrigger string
	report := api.ExecReporter(func(tk task.Task, trigger string) {
		gotTask, gotTrigger = tk.ID, trigger
	})

	hook := gatewayOnDone(report)
	if hook == nil {
		t.Fatal("gatewayOnDone returned nil with a reporter present")
	}
	hook(task.Task{ID: "t-1"})

	if gotTask != "t-1" {
		t.Errorf("task id = %q, want t-1", gotTask)
	}
	if gotTrigger != "gateway" {
		t.Errorf("trigger = %q, want %q — the hub distinguishes intakes by it", gotTrigger, "gateway")
	}
}

// An empty gateway list must yield NO addresses, not one empty address: a
// trailing comma or an unset env var would otherwise start a poll loop against
// "" and spin on connection errors forever.
func TestSplitListDropsEmpties(t *testing.T) {
	for _, in := range []string{"", ",", " , ", ",,"} {
		if got := splitList(in); len(got) != 0 {
			t.Errorf("splitList(%q) = %v, want none", in, got)
		}
	}
	got := splitList("http://a:8444, http://b:8444,")
	if len(got) != 2 || got[0] != "http://a:8444" || got[1] != "http://b:8444" {
		t.Errorf("splitList = %v, want the two trimmed addresses", got)
	}
}

// Regression shape, the second time: a typed nil wrapped in a non-nil
// interface passes every `!= nil` check and panics on first use. The first
// occurrence (gatewayOnDone) killed a hub-less runner after ONE request,
// because the panic happened on its own goroutine.
func TestStatusReaderIsNilWithoutAHub(t *testing.T) {
	if got := statusReader(nil); got != nil {
		t.Fatal("statusReader(nil) returned a non-nil interface; using it would panic")
	}
}

// A runner that cannot authenticate the gateway it polls will hand its
// RESPONSE — real payload, for a real caller — to whatever answered on that
// address. A shared secret does not help: it proves the caller knows a
// string, not that the gateway is genuine. So an off-host gateway without an
// mTLS identity is refused at startup rather than run in a degraded mode
// (ADR-0041).
//
// Loopback is exempt because nothing else can reach it, and that is the dev
// and single-box path.
func TestLoopbackGateway(t *testing.T) {
	for addr, want := range map[string]bool{
		"http://127.0.0.1:8444":   true,
		"https://127.0.0.1:8444":  true,
		"http://localhost:8444":   true,
		"http://[::1]:8444":       true,
		"http://10.0.0.5:8444":    false,
		"http://gw.internal:8444": false,
		"http://shift-gateway-0.shift-gateway-control.shift-dmz.svc.cluster.local:8444": false,
		// Unparseable is NOT a reason to relax a security gate: an address
		// nobody can read is one nobody has verified is local.
		"://nonsense": false,
		"":            false,
	} {
		if got := loopbackGateway(addr); got != want {
			t.Errorf("loopbackGateway(%q) = %v, want %v", addr, got, want)
		}
	}
}

// TestTheHeapCeilingDefaultsToTheAdmissionBudget: everything else in the
// runner bounds memory by construction, but a `starlark` step's fuel counts
// steps rather than bytes (ADR-0052 §4), so the process keeps a soft ceiling.
// Soft matters: crossing it makes the collector work harder, where an OOM kill
// would lose every in-flight task rather than the one at fault.
func TestTheHeapCeilingDefaultsToTheAdmissionBudget(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	before := debug.SetMemoryLimit(-1) // read without setting
	t.Cleanup(func() { debug.SetMemoryLimit(before) })

	applyMemoryLimit("1GiB")
	got := debug.SetMemoryLimit(-1)
	// Budget plus half: the budget governs task state, while the process also
	// holds connector pools, HTTP buffers and the runtime itself.
	if want := int64(1<<30) + int64(1<<30)/2; got != want {
		t.Errorf("memory limit = %d, want %d", got, want)
	}
}

// An operator who sets GOMEMLIMIT means it.
func TestAnExplicitMemoryLimitWins(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "512MiB")
	before := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(before) })

	applyMemoryLimit("1GiB")
	if got := debug.SetMemoryLimit(-1); got != before {
		t.Errorf("memory limit = %d, want it untouched (%d)", got, before)
	}
}

// A malformed budget must not take the runner down: this is a backstop, and
// the budget itself is validated where it is actually used.
func TestAnUnparseableBudgetLeavesTheCeilingAlone(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	before := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(before) })

	for _, bad := range []string{"", "banana", "-5", "0"} {
		applyMemoryLimit(bad)
		if got := debug.SetMemoryLimit(-1); got != before {
			t.Errorf("budget %q changed the ceiling to %d", bad, got)
		}
	}
}
