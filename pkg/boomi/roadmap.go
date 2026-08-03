package boomi

import "strings"

// RoadmapStep is one feature in a build order, with what it actually buys.
type RoadmapStep struct {
	Feature string `json:"feature"`
	// CleanProcesses is how many processes import with no manual work once
	// this feature AND every feature before it in the order exists.
	CleanProcesses int `json:"clean_processes"`
	// Percent is CleanProcesses as a share of all processes.
	Percent float64 `json:"percent"`
}

// Roadmap returns a greedy build order: at each step, the feature that makes
// the most additional processes import cleanly.
//
// This is the report's most decision-useful output, and it exists because
// ranking features by shape count alone is misleading. A feature can account
// for the most shapes and still unblock almost nothing on its own, because the
// processes using it also use three other unbuilt features — one remaining gap
// blocks a whole process just as effectively as five. The cumulative view is
// the only honest answer to "what do we build first".
func (r *Report) buildRoadmap() []RoadmapStep {
	if len(r.Processes) == 0 {
		return nil
	}

	// Each process becomes the set of features it is waiting on.
	blockerOf := map[string]string{}
	for _, u := range r.ShapeDetail {
		if !u.Support.Importable() && u.Blocker != "" {
			blockerOf[u.Shape] = u.Blocker
		}
	}
	waiting := make([]map[string]bool, 0, len(r.Processes))
	for _, p := range r.Processes {
		need := map[string]bool{}
		for _, g := range p.Gaps {
			shape := g
			if i := strings.Index(g, " ("); i >= 0 {
				shape = g[:i]
			}
			if b := blockerOf[shape]; b != "" {
				need[b] = true
			}
		}
		waiting = append(waiting, need)
	}

	cleanWith := func(have map[string]bool) int {
		n := 0
		for _, need := range waiting {
			ok := true
			for f := range need {
				if !have[f] {
					ok = false
					break
				}
			}
			if ok {
				n++
			}
		}
		return n
	}

	remaining := make([]string, 0, len(r.Blockers))
	for _, b := range r.Blockers {
		remaining = append(remaining, b.Feature)
	}

	have := map[string]bool{}
	var steps []RoadmapStep
	for len(remaining) > 0 {
		bestIdx, bestClean := -1, -1
		for i, f := range remaining {
			have[f] = true
			c := cleanWith(have)
			delete(have, f)
			// Ties break on the shape-count order the blockers already carry,
			// which `remaining` preserves — so the result is deterministic.
			if c > bestClean {
				bestIdx, bestClean = i, c
			}
		}
		f := remaining[bestIdx]
		have[f] = true
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
		steps = append(steps, RoadmapStep{
			Feature: f, CleanProcesses: bestClean,
			Percent: 100 * float64(bestClean) / float64(len(r.Processes)),
		})
	}
	return steps
}
