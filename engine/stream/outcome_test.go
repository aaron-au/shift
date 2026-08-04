package stream

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// The canonical-error rule exists because a fan-out cancels its shared context
// the moment any branch fails, so the innocent branches all report
// context.Canceled. Reporting one of those buries the real cause.
func TestCanonicalPrefersTheRealErrorOverTeardownNoise(t *testing.T) {
	boom := errors.New("sftp: connection refused")
	got := Canonical(context.Background(), context.Canceled, boom, context.Canceled)
	if !errors.Is(got, boom) {
		t.Fatalf("Canonical = %v, want the non-cancellation cause %v", got, boom)
	}
}

// Order is the caller's, so the same failure set always reports the same
// cause — otherwise the report would race between goroutines.
func TestCanonicalIsDeterministicAcrossMultipleRealErrors(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")
	for i := range 20 {
		if got := Canonical(context.Background(), first, second); !errors.Is(got, first) {
			t.Fatalf("iteration %d: Canonical = %v, want %v", i, got, first)
		}
	}
}

// A cancellation IS the cause when the caller aborted — a client disconnect or
// a deadline. The signal is that the PARENT context is done, which the
// executor's own teardown cancel (a derived context) never makes true.
func TestCanonicalReportsGenuineCallerCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	got := Canonical(parent, context.Canceled)
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("Canonical = %v, want the genuine cancellation", got)
	}
}

// The mirror image: the same error set with a live parent is teardown noise,
// but must still be surfaced rather than reported as a clean run — a
// cancellation with nothing to explain it is worth seeing (ADR-0005: never
// silently dropped).
func TestCanonicalSurfacesUnexplainedCancellation(t *testing.T) {
	if got := Canonical(context.Background(), context.Canceled); got == nil {
		t.Fatal("an unexplained cancellation was reported as a clean run")
	}
}

func TestCanonicalIsNilForACleanRun(t *testing.T) {
	if got := Canonical(context.Background(), nil, nil); got != nil {
		t.Fatalf("Canonical = %v, want nil", got)
	}
}

// A deliberate stop is a SUCCESS. If it were reported as an error the hub
// would retry a flow the author explicitly ended, and a default dead-letter
// handler would alert on a clean exit.
func TestCanonicalTreatsAStopAsSuccess(t *testing.T) {
	got := Canonical(context.Background(), ErrStopRequested, context.Canceled)
	if got != nil {
		t.Fatalf("Canonical = %v, want nil — a stop is not a failure", got)
	}
}

// A stop wins over a sibling's real error too: reaching @stop cancels the
// topology, and an error raised by that teardown must not convert a deliberate
// stop into a failed task.
func TestStopWinsOverAnErrorItCaused(t *testing.T) {
	out := Classify(context.Background(), errors.New("branch: write after close"), ErrStopRequested)
	if !out.Stopped || out.Err != nil {
		t.Fatalf("Classify = %+v, want stopped with no error", out)
	}
}

// The stopping step is recovered from the OpError tag Pipeline.Run attaches —
// never by parsing the message.
func TestClassifyNamesTheStoppingStep(t *testing.T) {
	tagged := &OpError{Op: "halt-on-empty", Err: ErrStopRequested}
	out := Classify(context.Background(), tagged)
	if !out.Stopped {
		t.Fatal("a tagged stop was not classified as a stop")
	}
	if out.StopStep != "halt-on-empty" {
		t.Fatalf("StopStep = %q, want the step that requested the stop", out.StopStep)
	}
}

// IsStop must see through wrapping, since the sentinel travels tagged by step
// and may be wrapped again by an operator.
func TestIsStopSeesThroughWrapping(t *testing.T) {
	wrapped := fmt.Errorf("sink: %w", &OpError{Op: "s", Err: ErrStopRequested})
	if !IsStop(wrapped) {
		t.Fatal("IsStop missed a wrapped sentinel; the stop would be reported as a failure")
	}
	if IsStop(errors.New("stop requested")) {
		t.Fatal("IsStop matched on message text rather than the sentinel")
	}
}

func TestClassifyReportsAPlainError(t *testing.T) {
	boom := errors.New("boom")
	out := Classify(context.Background(), boom)
	if out.Stopped {
		t.Fatal("a plain error was classified as a stop")
	}
	if !errors.Is(out.Err, boom) {
		t.Fatalf("Err = %v, want %v", out.Err, boom)
	}
}
