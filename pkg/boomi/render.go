package boomi

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// RenderText writes the report as a plain-text migration assessment.
//
// The ordering is deliberate: the headline coverage number, then immediately
// what it excludes. A coverage figure quoted without its gaps is the exact
// dishonesty ADR-0032 §5 warns against.
func RenderText(w io.Writer, r *Report, verbose bool) error {
	p := func(format string, args ...any) error {
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}

	if err := p("Boomi export assessment\n%s\n\n", strings.Repeat("=", 23)); err != nil {
		return err
	}
	if err := p("export        %s\n", r.Root); err != nil {
		return err
	}
	if err := p("components    %d (%d types)\n", r.Components.Total, len(r.Components.ByType)); err != nil {
		return err
	}

	types := make([]string, 0, len(r.Components.ByType))
	for t := range r.Components.ByType {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool {
		if r.Components.ByType[types[i]] != r.Components.ByType[types[j]] {
			return r.Components.ByType[types[i]] > r.Components.ByType[types[j]]
		}
		return types[i] < types[j]
	})
	for _, t := range types {
		if err := p("                %4d  %s\n", r.Components.ByType[t], t); err != nil {
			return err
		}
	}

	s := r.Shapes
	if err := p("\nSHAPE COVERAGE\n"); err != nil {
		return err
	}
	if err := p("  %.1f%% of %d shapes import (%d distinct shape types across %d processes)\n",
		s.Coverage(), s.Total, s.Distinct, len(r.Processes)); err != nil {
		return err
	}
	if err := p("    %4d  mapped                 — faithful SHIFT construct\n", s.Mapped); err != nil {
		return err
	}
	if err := p("    %4d  mapped-with-divergence — imports, but behavior differs (read below)\n", s.Divergent); err != nil {
		return err
	}
	if err := p("    %4d  needs-manual           — designed, not built yet\n", s.NeedsManual); err != nil {
		return err
	}
	if err := p("    %4d  unsupported            — must be re-authored\n", s.Unsupported); err != nil {
		return err
	}
	if err := p("\n  %d of %d processes import with no manual work.\n", r.CleanProcesses(), len(r.Processes)); err != nil {
		return err
	}

	if len(r.Blockers) > 0 {
		if err := p("\nWHAT TO BUILD NEXT (ranked by shapes unblocked)\n"); err != nil {
			return err
		}
		for _, b := range r.Blockers {
			if err := p("  %4d shapes / %2d processes  %s\n", b.Shapes, b.Processes, b.Feature); err != nil {
				return err
			}
			if err := p("                              %s\n", strings.Join(b.ShapeTypes, ", ")); err != nil {
				return err
			}
		}
	}

	if len(r.Roadmap) > 0 {
		if err := p("\nBUILD ORDER — processes importing clean as each feature lands\n"); err != nil {
			return err
		}
		if err := p("  (greedy: one remaining gap blocks a whole process, so a feature that\n"); err != nil {
			return err
		}
		if err := p("   accounts for the most shapes may still unblock little on its own)\n"); err != nil {
			return err
		}
		for _, st := range r.Roadmap {
			if err := p("    + %-34s %3d/%d processes (%3.0f%%)\n",
				st.Feature, st.CleanProcesses, len(r.Processes), st.Percent); err != nil {
				return err
			}
		}
	}

	if err := p("\nSHAPE INVENTORY\n"); err != nil {
		return err
	}
	if err := p("  %-22s %5s %5s  %-22s %s\n", "SHAPE", "USES", "PROCS", "STATUS", "SHIFT CONSTRUCT"); err != nil {
		return err
	}
	for _, u := range r.ShapeDetail {
		construct := u.Construct
		if construct == "" {
			construct = "—"
		}
		if err := p("  %-22s %5d %5d  %-22s %s\n", u.Shape, u.Count, u.Processes, u.Support, construct); err != nil {
			return err
		}
	}

	// Divergences are the report's most important lines: they import silently
	// and change behavior, so they are called out rather than left in a table.
	var diverging []ShapeUsage
	for _, u := range r.ShapeDetail {
		if u.Support == Divergent {
			diverging = append(diverging, u)
		}
	}
	if len(diverging) > 0 {
		if err := p("\nDIVERGENCES — these import, but do NOT behave identically\n"); err != nil {
			return err
		}
		for _, u := range diverging {
			if err := p("  %s (%d uses)\n    %s\n", u.Shape, u.Count, u.Note); err != nil {
				return err
			}
		}
	}

	if r.Secrets.Values > 0 {
		if err := p("\nCANNOT BE IMPORTED — credentials\n"); err != nil {
			return err
		}
		if err := p("  %d encrypted values across %d components (passwords, OAuth tokens, private certificates).\n",
			r.Secrets.Values, r.Secrets.Components); err != nil {
			return err
		}
		if err := p("  Boomi encrypts these against the source account, so the ciphertext is meaningless\n"); err != nil {
			return err
		}
		if err := p("  outside it. They must be re-entered as SHIFT secrets after import.\n"); err != nil {
			return err
		}
	}

	if verbose && len(r.Processes) > 0 {
		if err := p("\nPER-PROCESS DETAIL\n"); err != nil {
			return err
		}
		for _, pr := range r.Processes {
			status := fmt.Sprintf("%d/%d shapes blocked", pr.Blocked, pr.Total)
			if pr.Clean {
				status = "imports clean"
			}
			if err := p("\n  %s\n    %s — %s\n", pr.Name, pr.File, status); err != nil {
				return err
			}
			for _, g := range pr.Gaps {
				if err := p("      gap:       %s\n", g); err != nil {
					return err
				}
			}
			for _, d := range pr.Divergences {
				if err := p("      divergence: %s\n", d); err != nil {
					return err
				}
			}
		}
	}

	if len(r.Skipped) > 0 {
		if err := p("\nUNREADABLE FILES (%d) — not counted above\n", len(r.Skipped)); err != nil {
			return err
		}
		for _, sk := range r.Skipped {
			if err := p("  %s: %s\n", sk.File, sk.Reason); err != nil {
				return err
			}
		}
	}
	return nil
}
