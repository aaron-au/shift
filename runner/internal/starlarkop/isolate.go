package starlarkop

import (
	"context"
	"fmt"
	"time"

	"go.starlark.net/starlark"
)

// Evaluation runs on its own goroutine, and this file is about being precise
// concerning what that does and does not buy — because the tempting summary
// ("we sandbox it in a goroutine") is wrong in a way that matters.
//
// What it DOES buy:
//
//   - Panic containment. An interpreter bug would otherwise take the runner
//     process down, losing every in-flight task rather than the one at fault.
//     This is the residual risk in-process carries that wazero would not
//     (ADR-0052 §1), so containing it is the point.
//   - Abandonment. Fuel bounds work and stops a runaway loop, but a script
//     that allocates enormously in FEW steps is not stopped by fuel — it is
//     stopped, if at all, by the machine. On its own goroutine the caller can
//     stop waiting, cancel the thread, fail the task and keep the runner
//     answering, instead of blocking a worker indefinitely.
//
// What it does NOT buy, and must never be claimed to:
//
//   - A memory limit. Go has no per-goroutine heap cap; an abandoned goroutine
//     keeps running until its next cancellation check and keeps its allocations
//     until it returns. Isolation here is about CONTAINMENT and RECOVERY, not
//     about a ceiling. A real per-script ceiling needs the wasm runtime, which
//     is exactly the trigger ADR-0052 §1 records for revisiting the choice.

// DefaultDeadline bounds one record's evaluation in wall-clock time.
//
// Fuel is the primary bound and is deterministic; this is the backstop for the
// case fuel cannot see — a small number of steps doing an enormous amount of
// allocation. It is deliberately generous, because tripping it on a legitimate
// script is worse than the case it guards.
const DefaultDeadline = 30 * time.Second

// result carries one evaluation off the worker goroutine.
type result struct {
	value starlark.Value
	err   error
}

// callIsolated runs the script's transform on its own goroutine, returning
// when it finishes, when ctx is done, or when the deadline expires.
// fn is a parameter rather than p.fn so the panic path can be exercised: a
// contained panic is a claim about behaviour, and a claim no test can reach is
// a claim nobody should believe.
func (p *Program) callIsolated(ctx context.Context, thread *starlark.Thread, fn starlark.Callable, arg starlark.Value) (starlark.Value, error) {
	deadline := p.deadline
	if deadline <= 0 {
		deadline = DefaultDeadline
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	// Buffered, so an abandoned goroutine can always complete its send and
	// exit rather than blocking forever on a receiver that has gone.
	done := make(chan result, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Deliberately does NOT include the panic value's detail
				// beyond its text: a panic inside the interpreter can carry
				// payload-derived values, and this error reaches the hub
				// (ADR-0052 §8).
				done <- result{err: fmt.Errorf("script panicked: %v", r)}
			}
		}()
		v, err := starlark.Call(thread, fn, starlark.Tuple{arg}, nil)
		done <- result{value: v, err: err}
	}()

	select {
	case r := <-done:
		return r.value, r.err
	case <-ctx.Done():
		// Ask the interpreter to stop at its next check. The goroutine may
		// still be inside a single enormous allocation and unable to notice
		// yet, which is precisely the limit documented above — but the task
		// fails now rather than waiting on it.
		thread.Cancel("deadline exceeded")
		return nil, fmt.Errorf("script did not finish within %s: it is doing too much work per "+
			"record, or allocating too much in too few steps for the fuel budget to catch "+
			"(fuel bounds steps, not bytes)", deadline)
	}
}
