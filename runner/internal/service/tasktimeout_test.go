package service

import (
	"testing"
	"time"
)

// TC-029 fallout (docs/assurance/test-conformance.md §2f).
//
// TaskTimeout used to default to 0 meaning UNBOUNDED, on the reasoning that
// streaming workloads are legitimately long. The reasoning was right about
// duration and wrong about the failure mode: under ADR-0005 admission is the
// runner's only capacity control, so a task that never finishes shrinks the
// machine permanently, and nothing reports it — the task just stays `running`.
// The engine has a live instance (the tee→join enrichment topology deadlocks
// above ~10 batches), so this is not hypothetical.
//
// There is now no "off". A deployment that wants no practical ceiling sets a
// duration long enough to be one — which is a decision someone made, and that
// is the entire difference.
func TestEveryTaskGetsACeilingEvenWhenNobodyAsksForOne(t *testing.T) {
	var o Options
	o.defaults()
	if o.TaskTimeout <= 0 {
		t.Fatalf("TaskTimeout = %v with no option set; an unbounded task holds its "+
			"admission reservation for the life of the runner", o.TaskTimeout)
	}
	if o.TaskTimeout != DefaultTaskTimeout {
		t.Errorf("TaskTimeout = %v, want DefaultTaskTimeout (%v)", o.TaskTimeout, DefaultTaskTimeout)
	}
}

// A negative duration is a configuration mistake, not a request for infinity.
// Treating it as "off" would resurrect the exact hole this closed, via a typo.
func TestANonPositiveTaskTimeoutTakesTheDefaultRatherThanMeaningUnbounded(t *testing.T) {
	for _, d := range []time.Duration{0, -1, -time.Hour} {
		o := Options{TaskTimeout: d}
		o.defaults()
		if o.TaskTimeout != DefaultTaskTimeout {
			t.Errorf("TaskTimeout %v became %v, want the default %v", d, o.TaskTimeout, DefaultTaskTimeout)
		}
	}
}

// An explicit ceiling must survive defaults() untouched — including one long
// enough to be an opt-out. Silently clamping it would make the escape hatch a
// lie.
func TestAnExplicitTaskTimeoutIsKept(t *testing.T) {
	for _, d := range []time.Duration{time.Second, 90 * time.Minute, 1 << 62} {
		o := Options{TaskTimeout: d}
		o.defaults()
		if o.TaskTimeout != d {
			t.Errorf("explicit TaskTimeout %v became %v", d, o.TaskTimeout)
		}
	}
}
