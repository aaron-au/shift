package boomi

import (
	"sort"
)

// Report is the migration assessment for one export (ADR-0032 §5). It answers
// three questions an operator actually asks: how much comes across, what
// exactly does not, and what would we have to build to close the gap.
type Report struct {
	Root string `json:"root"`

	Components ComponentStats `json:"components"`
	Shapes     ShapeStats     `json:"shapes"`

	// ShapeDetail is every shape type found, most-used first.
	ShapeDetail []ShapeUsage `json:"shape_detail"`
	// Blockers ranks unbuilt SHIFT features by how many shapes each unblocks
	// — the evidence-driven build order.
	Blockers []Blocker `json:"blockers"`
	// Processes is the per-process verdict.
	Processes []ProcessVerdict `json:"processes"`
	// Secrets counts values that cannot cross an account boundary.
	Secrets SecretStats `json:"secrets"`
	// Roadmap is the greedy build order: which feature to build next, and how
	// many processes import cleanly once it exists.
	Roadmap []RoadmapStep `json:"roadmap,omitempty"`
	// Skipped lists files that could not be parsed.
	Skipped []SkippedFile `json:"skipped,omitempty"`
}

// ComponentStats counts components by Boomi type.
type ComponentStats struct {
	Total   int            `json:"total"`
	ByType  map[string]int `json:"by_type"`
	Deleted int            `json:"deleted"`
}

// ShapeStats counts shape instances by support status.
type ShapeStats struct {
	Total       int `json:"total"`
	Mapped      int `json:"mapped"`
	Divergent   int `json:"divergent"`
	NeedsManual int `json:"needs_manual"`
	Unsupported int `json:"unsupported"`
	// Distinct is how many distinct shape types appear.
	Distinct int `json:"distinct"`
}

// Coverage is the share of shape instances that land in a runnable flow
// without human intervention, as a percentage.
//
// It counts divergent shapes as covered because they DO import and run — the
// divergence is a behavioral warning, itemized separately. A reader who wants
// the stricter number can read Mapped alone; conflating the two in either
// direction would misrepresent the import.
func (s ShapeStats) Coverage() float64 {
	if s.Total == 0 {
		return 0
	}
	return 100 * float64(s.Mapped+s.Divergent) / float64(s.Total)
}

// ShapeUsage is one shape type's usage and verdict.
type ShapeUsage struct {
	Shape     string  `json:"shape"`
	Count     int     `json:"count"`
	Processes int     `json:"processes"`
	Support   Support `json:"support"`
	Construct string  `json:"construct,omitempty"`
	Blocker   string  `json:"blocker,omitempty"`
	Note      string  `json:"note,omitempty"`
}

// Blocker is one unbuilt SHIFT feature and the work it would unlock.
type Blocker struct {
	Feature string `json:"feature"`
	// Shapes is how many shape instances wait on this feature.
	Shapes int `json:"shapes"`
	// Processes is how many processes contain at least one such shape —
	// the number that matters, because one blocked shape blocks a whole
	// process from importing cleanly.
	Processes int `json:"processes"`
	// ShapeTypes are the Boomi shapes waiting on it.
	ShapeTypes []string `json:"shape_types"`
}

// ProcessVerdict is one process's importability.
type ProcessVerdict struct {
	Name  string `json:"name"`
	File  string `json:"file"`
	Total int    `json:"shapes"`
	// Blocked is the number of shapes that will not import.
	Blocked int `json:"blocked"`
	// Clean reports whether every shape imports (with or without divergence).
	Clean bool `json:"clean"`
	// Divergences are shapes that import but change behavior.
	Divergences []string `json:"divergences,omitempty"`
	// Gaps are the shapes that do not import, as "shape (label)".
	Gaps []string `json:"gaps,omitempty"`
}

// SecretStats counts what can never be imported: Boomi encrypts these at
// export against the source account, so the ciphertext is meaningless here.
// They are reported so a migration plan includes re-entering them.
type SecretStats struct {
	// Components is how many components carry encrypted values.
	Components int `json:"components"`
	// Values is the total number of encrypted values.
	Values int `json:"values"`
}

// Analyze produces the migration assessment for a parsed export.
func Analyze(ex *Export) *Report {
	r := &Report{
		Root:       ex.Root,
		Components: ComponentStats{ByType: map[string]int{}},
		Skipped:    ex.Skipped,
	}

	type agg struct {
		count     int
		processes map[string]bool
	}
	shapeAgg := map[string]*agg{}
	blockerAgg := map[string]*struct {
		shapes    int
		processes map[string]bool
		types     map[string]bool
	}{}

	for _, c := range ex.Components {
		r.Components.Total++
		typ := c.Type
		if typ == "" {
			typ = "(untyped)"
		}
		r.Components.ByType[typ]++
		if c.Deleted {
			r.Components.Deleted++
		}
		if c.EncryptedValues > 0 {
			r.Secrets.Components++
			r.Secrets.Values += c.EncryptedValues
		}
		if len(c.Shapes) == 0 {
			continue
		}

		v := ProcessVerdict{Name: c.Name, File: c.File, Total: len(c.Shapes)}
		for _, s := range c.Shapes {
			cap := Lookup(s.Type)
			r.Shapes.Total++
			switch cap.Support {
			case Mapped:
				r.Shapes.Mapped++
			case Divergent:
				r.Shapes.Divergent++
				v.Divergences = append(v.Divergences, s.Type+" ("+s.Display()+")")
			case NeedsManual:
				r.Shapes.NeedsManual++
			case Unsupported:
				r.Shapes.Unsupported++
			}
			if !cap.Support.Importable() {
				v.Blocked++
				v.Gaps = append(v.Gaps, s.Type+" ("+s.Display()+")")
			}

			a := shapeAgg[s.Type]
			if a == nil {
				a = &agg{processes: map[string]bool{}}
				shapeAgg[s.Type] = a
			}
			a.count++
			a.processes[c.File] = true

			// A divergence is a warning, not a gap, so it does not carry a
			// blocker into the ranking unless the capability names one.
			if cap.Blocker != "" && !cap.Support.Importable() {
				b := blockerAgg[cap.Blocker]
				if b == nil {
					b = &struct {
						shapes    int
						processes map[string]bool
						types     map[string]bool
					}{processes: map[string]bool{}, types: map[string]bool{}}
					blockerAgg[cap.Blocker] = b
				}
				b.shapes++
				b.processes[c.File] = true
				b.types[s.Type] = true
			}
		}
		v.Clean = v.Blocked == 0
		r.Processes = append(r.Processes, v)
	}

	r.Shapes.Distinct = len(shapeAgg)
	for shape, a := range shapeAgg {
		cap := Lookup(shape)
		r.ShapeDetail = append(r.ShapeDetail, ShapeUsage{
			Shape: shape, Count: a.count, Processes: len(a.processes),
			Support: cap.Support, Construct: cap.Construct,
			Blocker: cap.Blocker, Note: cap.Note,
		})
	}
	// Most-used first; ties by name so the report is deterministic.
	sort.Slice(r.ShapeDetail, func(i, j int) bool {
		if r.ShapeDetail[i].Count != r.ShapeDetail[j].Count {
			return r.ShapeDetail[i].Count > r.ShapeDetail[j].Count
		}
		return r.ShapeDetail[i].Shape < r.ShapeDetail[j].Shape
	})

	for feature, b := range blockerAgg {
		types := make([]string, 0, len(b.types))
		for t := range b.types {
			types = append(types, t)
		}
		sort.Strings(types)
		r.Blockers = append(r.Blockers, Blocker{
			Feature: feature, Shapes: b.shapes,
			Processes: len(b.processes), ShapeTypes: types,
		})
	}
	sort.Slice(r.Blockers, func(i, j int) bool {
		if r.Blockers[i].Shapes != r.Blockers[j].Shapes {
			return r.Blockers[i].Shapes > r.Blockers[j].Shapes
		}
		return r.Blockers[i].Feature < r.Blockers[j].Feature
	})
	sort.Slice(r.Processes, func(i, j int) bool { return r.Processes[i].File < r.Processes[j].File })
	r.Roadmap = r.buildRoadmap()

	return r
}

// CleanProcesses counts processes that import with no manual work.
func (r *Report) CleanProcesses() int {
	n := 0
	for _, p := range r.Processes {
		if p.Clean {
			n++
		}
	}
	return n
}
