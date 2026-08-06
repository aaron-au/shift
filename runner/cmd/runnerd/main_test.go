package main

import (
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
