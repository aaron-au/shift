// Package mem provides the watermark accounting that bounds engine memory
// (ADR-0003/0005): operators reserve against an explicit budget, and a
// failed reservation is the signal to spill — never an OOM, never an
// arbitrary task cap.
package mem

import (
	"fmt"
	"sync/atomic"
)

// DefaultBudget is the per-pipeline watermark applied when none is
// configured: 64 MiB.
const DefaultBudget = 64 << 20

// Governor tracks reserved bytes against a budget. It is safe for
// concurrent use. The zero value has no budget; use New.
type Governor struct {
	budget int64
	used   atomic.Int64
	// peak records the high-water mark for honest reporting.
	peak atomic.Int64
}

// New returns a Governor with the given budget in bytes; budget <= 0 uses
// DefaultBudget.
func New(budget int64) *Governor {
	if budget <= 0 {
		budget = DefaultBudget
	}
	return &Governor{budget: budget}
}

// TryReserve attempts to reserve n bytes. It returns false — without
// reserving — if the reservation would exceed the budget; the caller must
// spill or drain before retrying.
func (g *Governor) TryReserve(n int64) bool {
	for {
		cur := g.used.Load()
		next := cur + n
		if next > g.budget {
			return false
		}
		if g.used.CompareAndSwap(cur, next) {
			g.bumpPeak(next)
			return true
		}
	}
}

// Reserve reserves n bytes unconditionally (may exceed the budget). Used
// for allocations that already happened — accounting must stay honest even
// when the budget is temporarily overshot by one batch.
func (g *Governor) Reserve(n int64) {
	g.bumpPeak(g.used.Add(n))
}

// Release returns n bytes to the budget.
func (g *Governor) Release(n int64) {
	if v := g.used.Add(-n); v < 0 {
		panic(fmt.Sprintf("mem: governor released below zero (%d)", v))
	}
}

// OverBudget reports whether current usage exceeds the budget — the spill
// signal for stateful operators.
func (g *Governor) OverBudget() bool { return g.used.Load() > g.budget }

// Used returns currently reserved bytes.
func (g *Governor) Used() int64 { return g.used.Load() }

// Budget returns the configured budget in bytes.
func (g *Governor) Budget() int64 { return g.budget }

// Peak returns the high-water mark of reserved bytes.
func (g *Governor) Peak() int64 { return g.peak.Load() }

func (g *Governor) bumpPeak(v int64) {
	for {
		p := g.peak.Load()
		if v <= p || g.peak.CompareAndSwap(p, v) {
			return
		}
	}
}
