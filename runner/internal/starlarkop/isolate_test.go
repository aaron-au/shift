package starlarkop

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.starlark.net/starlark"

	"github.com/aaron-au/shift/engine/record"
)

// panicker is a callable that panics, standing in for an interpreter or
// adapter bug — the residual risk that running in-process carries and that
// wazero would remove (ADR-0052 §1).
func panicker(_ *starlark.Thread, _ *starlark.Builtin, _ starlark.Tuple, _ []starlark.Tuple) (starlark.Value, error) {
	panic("simulated interpreter bug holding 4111111111111111")
}

// TestAPanicIsContainedNotFatal: without containment this would take the whole
// runner down, losing every in-flight task rather than the one at fault.
func TestAPanicIsContainedNotFatal(t *testing.T) {
	p := &Program{opts: Options{StepID: "s1"}, fuel: DefaultFuel}
	_, err := p.callIsolated(context.Background(), p.newThread(),
		starlark.NewBuiltin("boom", panicker), starlark.None)
	if err == nil {
		t.Fatal("a panic did not surface as an error")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Errorf("error = %v, want it to report a contained panic", err)
	}
	// The panic value can quote payload, and this error reaches the hub, so
	// only its text is carried — no stack, and nothing re-formatted around it.
	if strings.Contains(err.Error(), "\n") {
		t.Errorf("panic error is multi-line, so it carries a stack: %q", err)
	}
	// And the evaluator is still usable afterwards — containment, not just
	// survival of this one call.
	prog := compile(t, `def transform(rec): return {"ok": True}`)
	if _, keep, err := prog.Run(context.Background(), record.NewBatch(), record.Null()); err != nil || !keep {
		t.Errorf("evaluator unusable after a contained panic: keep=%v err=%v", keep, err)
	}
}

// TestTheCallerIsReleasedOnDeadline: the point of the goroutine is that a
// script the fuel budget cannot stop does not hold a worker forever.
func TestTheCallerIsReleasedOnDeadline(t *testing.T) {
	p := &Program{opts: Options{StepID: "s1"}, fuel: 1 << 62, deadline: 40 * time.Millisecond}
	slow := starlark.NewBuiltin("slow", func(_ *starlark.Thread, _ *starlark.Builtin, _ starlark.Tuple, _ []starlark.Tuple) (starlark.Value, error) {
		time.Sleep(3 * time.Second) // a host call that blocks
		return starlark.None, nil
	})
	start := time.Now()
	_, err := p.callIsolated(context.Background(), p.newThread(), slow, starlark.None)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a blocked script was not abandoned")
	}
	if !strings.Contains(err.Error(), "did not finish") {
		t.Errorf("error = %v, want it to name the deadline", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("caller blocked %s waiting on an abandoned script", elapsed)
	}
}

// TestCancellingTheContextReleasesTheCaller — the same property, driven by the
// task's own cancellation rather than by the deadline.
func TestCancellingTheContextReleasesTheCaller(t *testing.T) {
	p := &Program{opts: Options{StepID: "s1"}, fuel: 1 << 62}
	ctx, cancel := context.WithCancel(context.Background())
	slow := starlark.NewBuiltin("slow", func(_ *starlark.Thread, _ *starlark.Builtin, _ starlark.Tuple, _ []starlark.Tuple) (starlark.Value, error) {
		time.Sleep(3 * time.Second)
		return starlark.None, nil
	})
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()

	start := time.Now()
	if _, err := p.callIsolated(ctx, p.newThread(), slow, starlark.None); err == nil {
		t.Fatal("cancellation did not release the caller")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("caller blocked %s after cancellation", elapsed)
	}
}
