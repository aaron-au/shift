package stream

import (
	"context"
	"errors"
)

// This file holds the flow error model's engine half (ADR-0031): the
// deliberate-stop sentinel and the rule that resolves the ONE error worth
// reporting from everything an execution's goroutines observed.
//
// Both exist because an execution is no longer a single chain. A fan-out
// (ADR-0029) cancels its shared context the moment any branch fails, so the
// innocent siblings and the driver all report context.Canceled. Reporting
// those would bury the actual cause under teardown noise; reporting the
// first error in goroutine-completion order would be non-deterministic.

// ErrStopRequested is the sentinel a deliberate early termination raises —
// the @stop terminal (ADR-0031 §3). It is NOT a failure: an execution that
// stops is a terminal SUCCESS, and Canonical resolves it to a nil error.
//
// It must not be modelled as an error, because an error would take one of two
// wrong paths: with retry enabled the hub would re-dispatch a flow the author
// deliberately ended, and with a default dead-letter handler enabled a clean
// exit would be dead-lettered — alerting on a success. Both corrupt the
// outcome, so a stop is classified before any error classification runs.
var ErrStopRequested = errors.New("stream: stop requested")

// IsStop reports whether err is (or wraps) the deliberate-stop sentinel.
func IsStop(err error) bool { return errors.Is(err, ErrStopRequested) }

// isCancel reports whether err is a bare context cancellation — the shape
// teardown produces in goroutines that did nothing wrong.
func isCancel(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Canonical resolves the single error an execution should report, given the
// caller's context and every error its goroutines returned (nils welcome, in
// any order). It returns nil for both a clean run and a deliberate stop —
// callers distinguish the two with IsStop over the same error set, or with
// the Outcome helper below.
//
// The rules, in order:
//
//  1. A deliberate stop wins outright. Reaching @stop cancels the topology
//     the same mechanical way a branch failure does, so its siblings report
//     context.Canceled; none of that is a failure.
//  2. The first NON-cancellation error is the cause. Order is the caller's
//     (the fan-out executor passes branches in declaration order), so the
//     report is deterministic rather than a race between goroutines.
//  3. Otherwise, a cancellation is reported only when cancellation is itself
//     the genuine cause — the PARENT context is done, meaning the caller
//     aborted, a client disconnected, or a deadline fired. The executor's own
//     teardown cancel never satisfies this, because that context is derived
//     from the parent, not the parent itself.
//  4. Failing all of that, any remaining error is returned rather than
//     swallowed. Reaching here means a goroutine returned a cancellation
//     error with no cancellation to explain it, which is worth surfacing
//     honestly rather than reporting a clean run (ADR-0005: never silently
//     dropped).
func Canonical(parent context.Context, errs ...error) error {
	for _, err := range errs {
		if err != nil && IsStop(err) {
			return nil
		}
	}
	for _, err := range errs {
		if err != nil && !isCancel(err) {
			return err
		}
	}
	if parent != nil && parent.Err() != nil {
		return parent.Err()
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Outcome is an execution's classified terminal result: the canonical error
// (nil on success), whether the run ended because a @stop terminal was
// reached, and which step requested that stop. A stopped run is a success, so
// Err is nil and Stopped is true.
type Outcome struct {
	Err      error
	Stopped  bool
	StopStep string
}

// Classify resolves every part of the outcome in one pass, so a caller cannot
// report a stop as an error (or an error as a stop) by checking only one half,
// and so exactly one place knows how to recover the stopping step's id.
func Classify(parent context.Context, errs ...error) Outcome {
	for _, err := range errs {
		if err != nil && IsStop(err) {
			out := Outcome{Stopped: true}
			// Pipeline.Run tags a sink error with its step id, which is how
			// the stopping step is named without parsing any string.
			if oe, ok := errors.AsType[*OpError](err); ok {
				out.StopStep = oe.Op
			}
			return out
		}
	}
	return Outcome{Err: Canonical(parent, errs...)}
}
