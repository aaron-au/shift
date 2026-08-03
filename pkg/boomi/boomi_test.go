package boomi_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/boomi"
)

// All fixtures under testdata/ are hand-authored — never copied from a
// customer export. See testdata/README.md.
const exportDir = "testdata/export"

func parse(t *testing.T) *boomi.Export {
	t.Helper()
	ex, err := boomi.ParseExport(exportDir)
	if err != nil {
		t.Fatal(err)
	}
	return ex
}

// TestParseExportReadsTheCanvas: a process component yields its shapes, their
// author labels, and the edges between them — the report is only as good as
// this.
func TestParseExportReadsTheCanvas(t *testing.T) {
	ex := parse(t)

	var orders *boomi.Component
	for _, c := range ex.Components {
		if c.ID == "proc-0001" {
			orders = c
		}
	}
	if orders == nil {
		t.Fatal("orders process not parsed")
	}
	if orders.Name != "Orders — sync" || orders.Type != "process" || orders.Version != 3 {
		t.Errorf("envelope = %q/%q/v%d", orders.Name, orders.Type, orders.Version)
	}
	if orders.Folder != "Demo/Orders" {
		t.Errorf("folder = %q", orders.Folder)
	}
	if len(orders.Shapes) != 5 {
		t.Fatalf("shapes = %d, want 5", len(orders.Shapes))
	}

	got := orders.Shapes[0]
	if got.Type != "start" || got.Label != "HTTP in" {
		t.Errorf("first shape = %q/%q", got.Type, got.Label)
	}
	if len(got.To) != 1 || got.To[0] != "shape2" {
		t.Errorf("edges from start = %v, want [shape2]", got.To)
	}
	// A decision fans out to two targets; losing one would silently drop a
	// branch of the flow.
	if to := orders.Shapes[2].To; len(to) != 2 {
		t.Errorf("decision edges = %v, want 2", to)
	}
	// Display falls back to the canvas id when the author set no label.
	if d := orders.Shapes[2].Display(); d != "shape3" {
		t.Errorf("Display() with no label = %q, want the canvas id", d)
	}
}

// TestParseExportSkipsRatherThanFails: an unreadable file must not abort the
// walk, and must be reported — a report that silently analyzed a subset would
// overstate coverage.
func TestParseExportSkipsRatherThanFails(t *testing.T) {
	ex := parse(t)
	if len(ex.Skipped) != 1 {
		t.Fatalf("skipped = %v, want the one broken fixture", ex.Skipped)
	}
	if !strings.Contains(ex.Skipped[0].File, "broken.xml") {
		t.Errorf("skipped the wrong file: %s", ex.Skipped[0].File)
	}
	if len(ex.Components) != 4 {
		t.Errorf("components = %d, want 4 (3 processes + 1 connection)", len(ex.Components))
	}
}

// TestParseExportIgnoresToolingDirs: .sync-state holds export bookkeeping, not
// live designs. Counting it would inflate every number in the report.
func TestParseExportIgnoresToolingDirs(t *testing.T) {
	for _, c := range parse(t).Components {
		if c.ID == "sync-0001" {
			t.Fatal(".sync-state component was analyzed; tooling dirs must be skipped")
		}
	}
}

// TestAnalyzeCoverage checks the headline arithmetic against fixtures whose
// expected classification is known by hand.
//
// Fixture totals: orders = start+map+decision+returndocuments (mapped) + stop
// (needs-manual); enrich = start+returndocuments (mapped) + branch (divergent)
// + dataprocess (unsupported) + wormhole (unknown ⇒ unsupported); clean =
// 3 mapped.
func TestAnalyzeCoverage(t *testing.T) {
	r := boomi.Analyze(parse(t))

	if got, want := r.Shapes.Total, 13; got != want {
		t.Fatalf("total shapes = %d, want %d", got, want)
	}
	for _, tc := range []struct {
		name string
		got  int
		want int
	}{
		{"mapped", r.Shapes.Mapped, 9},
		{"divergent", r.Shapes.Divergent, 1},
		{"needs-manual", r.Shapes.NeedsManual, 1},
		{"unsupported", r.Shapes.Unsupported, 2},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	// Coverage counts divergent as importable (it runs; the behavior change
	// is itemized separately), so 10 of 13.
	if got := r.Shapes.Coverage(); got < 76.9 || got > 77.0 {
		t.Errorf("coverage = %.2f%%, want ~76.92%% (10/13)", got)
	}
	// Only the all-mapped process imports without manual work.
	if got := r.CleanProcesses(); got != 1 {
		t.Errorf("clean processes = %d, want 1", got)
	}
}

// TestAnalyzeRanksBlockersByShapesUnblocked: the ranking is the report's
// actionable output — it turns a customer's export into a build order.
func TestAnalyzeRanksBlockersByShapesUnblocked(t *testing.T) {
	r := boomi.Analyze(parse(t))
	if len(r.Blockers) == 0 {
		t.Fatal("no blockers ranked")
	}
	for i := 1; i < len(r.Blockers); i++ {
		if r.Blockers[i-1].Shapes < r.Blockers[i].Shapes {
			t.Fatalf("blockers not ranked by shape count: %+v", r.Blockers)
		}
	}
	// A divergent shape imports, so it must NOT appear as a blocker — that
	// would double-count it as both covered and blocking.
	for _, b := range r.Blockers {
		for _, st := range b.ShapeTypes {
			if st == "branch" {
				t.Error("branch imports (with divergence) and must not be ranked as a blocker")
			}
		}
	}
}

// TestUnknownShapeIsReportedNotIgnored: a shape this analyzer has never seen
// is exactly what a migration estimate must not omit.
func TestUnknownShapeIsReportedNotIgnored(t *testing.T) {
	c := boomi.Lookup("wormhole")
	if c.Support != boomi.Unsupported {
		t.Errorf("unknown shape support = %q, want unsupported", c.Support)
	}
	if c.Support.Importable() {
		t.Error("unknown shape must not count as importable")
	}

	var found bool
	for _, u := range boomi.Analyze(parse(t)).ShapeDetail {
		if u.Shape == "wormhole" {
			found = true
		}
	}
	if !found {
		t.Error("unknown shape missing from the inventory")
	}
}

// TestEncryptedValuesCounted: credentials can never cross accounts, so the
// report must state how many need re-entering rather than dropping them.
func TestEncryptedValuesCounted(t *testing.T) {
	r := boomi.Analyze(parse(t))
	if r.Secrets.Values != 2 || r.Secrets.Components != 1 {
		t.Errorf("secrets = %d values / %d components, want 2/1", r.Secrets.Values, r.Secrets.Components)
	}
}

// TestPerProcessVerdictNamesTheGaps: an operator needs the specific shape that
// blocks a process, labeled as it appears in the Boomi UI.
func TestPerProcessVerdictNamesTheGaps(t *testing.T) {
	for _, p := range boomi.Analyze(parse(t)).Processes {
		switch p.Name {
		case "Passthrough":
			if !p.Clean || p.Blocked != 0 {
				t.Errorf("Passthrough should import clean, got %+v", p)
			}
		case "Orders — sync":
			if p.Clean || p.Blocked != 1 {
				t.Errorf("Orders should have exactly one gap, got %+v", p)
			}
			if len(p.Gaps) != 1 || !strings.Contains(p.Gaps[0], "Give up") {
				t.Errorf("gap should name the author's label: %v", p.Gaps)
			}
		case "Enrich":
			if len(p.Divergences) != 1 || !strings.Contains(p.Divergences[0], "branch") {
				t.Errorf("Enrich should record the branch divergence: %v", p.Divergences)
			}
		}
	}
}

// TestRenderTextStatesWhatItExcludes: a coverage number quoted without its
// gaps is the dishonesty ADR-0032 warns against, so the rendered report must
// carry the divergence and credential sections.
func TestRenderTextStatesWhatItExcludes(t *testing.T) {
	var buf bytes.Buffer
	if err := boomi.RenderText(&buf, boomi.Analyze(parse(t)), true); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"SHAPE COVERAGE",
		"WHAT TO BUILD NEXT",
		"DIVERGENCES",
		"SEQUENTIALLY",
		"CANNOT BE IMPORTED — credentials",
		"UNREADABLE FILES",
		"Give up",       // the gap, by the author's label
		"imports clean", // the per-process verdict
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered report missing %q", want)
		}
	}
}

// TestCapabilityTableIsWellFormed: every entry must key to itself and justify
// itself, and anything not importable must name what would unblock it —
// otherwise the build-order ranking silently loses work.
func TestCapabilityTableIsWellFormed(t *testing.T) {
	for _, c := range boomi.Capabilities() {
		if c.Shape == "" || c.Note == "" {
			t.Errorf("capability %+v: shape and note are required", c)
		}
		if c.Support.Importable() && c.Construct == "" {
			t.Errorf("%s: importable shapes must name their SHIFT construct", c.Shape)
		}
		if !c.Support.Importable() && c.Blocker == "" {
			t.Errorf("%s: non-importable shapes must name a blocker", c.Shape)
		}
		if got := boomi.Lookup(c.Shape); got.Shape != c.Shape {
			t.Errorf("%s: table key does not match entry (%q)", c.Shape, got.Shape)
		}
	}
}

// TestRoadmapIsCumulativeNotPerFeature guards the property that makes the
// build order worth computing at all: a process is clean only when EVERY
// feature it waits on exists, so the counts must rise monotonically and a
// feature must not be credited for a process that still has another gap.
func TestRoadmapIsCumulativeNotPerFeature(t *testing.T) {
	r := boomi.Analyze(parse(t))
	if len(r.Roadmap) == 0 {
		t.Fatal("no roadmap computed")
	}

	prev := -1
	for _, st := range r.Roadmap {
		if st.CleanProcesses < prev {
			t.Fatalf("roadmap not monotonic: %+v", r.Roadmap)
		}
		prev = st.CleanProcesses
	}
	// With every feature built, every process must import.
	last := r.Roadmap[len(r.Roadmap)-1]
	if last.CleanProcesses != len(r.Processes) {
		t.Errorf("final step leaves %d/%d processes blocked",
			len(r.Processes)-last.CleanProcesses, len(r.Processes))
	}
	if last.Percent < 99.9 {
		t.Errorf("final step = %.1f%%, want 100%%", last.Percent)
	}

	// Greedy must pick the feature that frees the most processes first. Orders
	// has exactly one gap (stop), so closing it yields Orders plus the
	// already-clean Passthrough.
	first := r.Roadmap[0]
	if first.Feature != "@stop terminal (ADR-0031)" {
		t.Errorf("first step = %q, want the single-gap process's blocker", first.Feature)
	}
	if first.CleanProcesses != 2 {
		t.Errorf("after the first feature, clean = %d, want 2 (Orders + the already-clean one)",
			first.CleanProcesses)
	}

	// Enrich waits on TWO features (custom code for dataprocess, assessment
	// for the unknown shape), so it must not be credited to either alone —
	// this is the property that makes the cumulative view necessary.
	for _, st := range r.Roadmap[:len(r.Roadmap)-1] {
		if st.CleanProcesses == len(r.Processes) {
			t.Errorf("step %q claims every process is clean before all features land", st.Feature)
		}
	}
}
