package flow

import "github.com/aaron-au/shift/pkg/flowdoc"

// Resume eligibility (ADR-0037). Whether a task may record a resume position
// is a property of the PLAN, decided before execution, and it is a
// correctness constraint rather than an optimisation.
//
// Whether the source can actually resume is discovered at run time — a source
// that does not implement the capability simply reports no position, and
// nothing is recorded. That half needs no plan inspection.

// blockingOps consume their entire input before emitting anything.
//
// They make resume WRONG, not merely inefficient, for two compounding
// reasons. The first confirmed sink write already reports end-of-input as the
// safe position, because the source was fully drained to build the operator's
// state — so a crash midway through emitting would record far more progress
// than was actually delivered. And resuming such a flow at all would rebuild
// the aggregate from the surviving suffix only, having lost the state for
// every record before the cursor: the output would be quietly wrong rather
// than merely incomplete.
//
// Joins (v3 merge, ADR-0029) block on their build side for the same reason.
var blockingOps = map[string]bool{
	"aggregate": true,
	"merge":     true,
}

// Resumable reports whether a plan may record resume positions. It is false
// when the plan contains any blocking operator, and false for a multi-path
// (v3 DAG) plan — several sources with independent positions have no single
// cursor to record, and pairing the right position with the right source is
// the fan-in design work, not something to guess at here.
func Resumable(plan *flowdoc.Plan) bool {
	if plan == nil {
		return false
	}
	if plan.Multi {
		for _, n := range plan.Nodes {
			if blockingOps[n.Type] {
				return false
			}
		}
		// A single-source fan-out is conceptually resumable, but the
		// confirm point is per-branch and the branches drain at different
		// rates, so "everything covered by this position has been written"
		// is no longer a single fact. Deferred deliberately rather than
		// approximated.
		return false
	}
	for _, s := range plan.Main {
		if blockingOps[s.Type] {
			return false
		}
	}
	return true
}
