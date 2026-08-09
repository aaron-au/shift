package shiftlog_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Metric names are an operator contract in exactly the sense log `event` names
// are (ADR-0020 / ADR-0046): the dashboards, recording rules and alert
// expressions that consume them live outside this repo, so renaming one does
// not break a build — it silently stops matching, and an alert that was meant
// to page somebody never fires again. vocabulary_test.go pins the log side;
// this file pins the repo-wide metric side.
//
// The *exact* per-component metric surface is pinned where it can be asserted
// against a REAL scrape — hub/internal/telemetry/conformance_test.go and
// runner/internal/telemetry/conformance_test.go build the handler, collect
// once and parse the exposition. Those are stronger assertions and they are
// the authority on names, types and labels.
//
// What is left is what no single module can see, and that is why it lives
// here alongside the other cross-module conformance tests:
//
//   - which files in the repo register metrics at all. A third component that
//     starts exporting is invisible to the hub's and runner's own tests, and
//     would ship with no name pinning whatsoever.
//   - the naming convention every component must share, checked over the
//     source literals rather than a scrape — a source scan sees instruments
//     that a scrape cannot, because an OTel observable that no code path
//     happens to observe emits nothing at all.
//   - the high-cardinality label denylist, and that all three copies of it
//     agree.

// The complete set of files that may construct metric instruments. Metrics are
// a hub/runner concern by ADR-0020: the engine and pkg/ stay telemetry-free so
// they keep their dependency hygiene, and the gateway's go.mod must stay empty
// (ADR-0046 §2) because it is the one component that sits in a DMZ.
var metricRegistrationSites = []string{
	"hub/internal/telemetry/telemetry.go",
	"runner/internal/telemetry/telemetry.go",
}

// Per-component name prefix. An operator filters, groups and pages by prefix;
// a metric under the wrong one is invisible to every query written for its
// component.
var metricPrefixes = map[string]string{
	"hub/internal/telemetry/telemetry.go":    "shift_hub_",
	"runner/internal/telemetry/telemetry.go": "shift_runner_",
}

// The one metric that deliberately sits outside its component's prefix: a flow
// ending on a @stop terminal is a property of the flow, not of the runner that
// happened to execute it (ADR-0031 §3). Listed here so it stays a decision
// somebody made rather than a precedent anybody can follow.
var prefixExceptions = map[string]bool{"shift_flow_stops_total": true}

var (
	metricNameLit = regexp.MustCompile(`"(shift_[A-Za-z0-9_.]*)"`)
	attrKeyLit    = regexp.MustCompile(`attribute\.(?:String|Int|Int64|Float64|Bool)\("([^"]*)"`)
	metricImport  = regexp.MustCompile(`"go\.opentelemetry\.io/otel/metric"|prometheus\.NewRegistry\(`)
)

// A new component that starts exporting metrics needs its own name pinning,
// and nothing else in the repo would notice it appearing. This test is the
// tripwire: it fails on the commit that adds the exporter, not months later
// when an operator finds an unnamed metric on a dashboard.
func TestOnlyTheKnownComponentsRegisterMetrics(t *testing.T) {
	known := map[string]bool{}
	for _, f := range metricRegistrationSites {
		known[f] = true
	}
	root := filepath.Join("..", "..")
	var found []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == root {
				return nil
			}
			// _archive is the 2025 prototype, read-only reference (CLAUDE.md).
			if name := d.Name(); name == "_archive" || name == "node_modules" || strings.HasPrefix(name, ".") {
				return fs.SkipDir
			}
			return nil
		}
		// Test files are excluded deliberately: a test may exercise a metrics
		// library without registering anything an operator will ever scrape.
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		// #nosec G304,G122 -- a test walking this repository's own checkout; the
		// paths come from WalkDir, not from input, and nothing here is attacker-
		// reachable.
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if metricImport.Match(raw) {
			rel, _ := filepath.Rel(root, path)
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repo: %v", err)
	}
	sort.Strings(found)
	for _, f := range found {
		if !known[f] {
			t.Errorf("%s registers metrics but is not a known registration site.\n"+
				"Add it to metricRegistrationSites with its prefix, and give it a conformance test that pins "+
				"its metric names and labels against a real scrape — an unpinned metric is a promise nobody recorded.", f)
		}
	}
	for _, f := range metricRegistrationSites {
		if !slices.Contains(found, f) {
			t.Errorf("%s no longer registers metrics — if the component stopped exporting, say so here "+
				"(and in the operator docs); if it moved, this list is what keeps the new home pinned.", f)
		}
	}
}

// Prometheus naming is not decoration. `_total` tells a reader to reach for
// rate(); `_seconds` and `_bytes` tell them the unit without consulting a doc;
// `_count`/`_sum`/`_bucket` are reserved by the exposition format itself, so a
// metric that ends in one collides with a histogram's own series. camelCase
// is not expressible in PromQL selectors the way people expect and reads as a
// mistake on a dashboard.
func TestMetricNamesFollowTheConvention(t *testing.T) {
	valid := regexp.MustCompile(`^shift_[a-z0-9]+(_[a-z0-9]+)*$`)
	// Units Prometheus reserves for the base unit; a metric measuring
	// milliseconds must still be named _seconds and recorded in seconds, or
	// every dashboard axis is wrong by a factor of a thousand.
	nonBaseUnits := []string{"_ms", "_millis", "_milliseconds", "_us", "_micros", "_ns", "_nanos",
		"_nanoseconds", "_secs", "_kb", "_mb", "_gb", "_kib", "_mib", "_percent"}
	reserved := []string{"_count", "_sum", "_bucket", "_created", "_info"}

	for _, file := range metricRegistrationSites {
		for _, name := range metricNames(t, file) {
			if !valid.MatchString(name) {
				t.Errorf("%s: metric %q is not lower_snake_case under a shift_ prefix — "+
					"camelCase and dots read as a bug on every dashboard that shows them", file, name)
				continue
			}
			if prefix := metricPrefixes[file]; !strings.HasPrefix(name, prefix) && !prefixExceptions[name] {
				t.Errorf("%s: metric %q does not start with %q — operators select and group by component prefix, "+
					"so a metric outside it is invisible to every query written for that component.\n"+
					"If the exception is deliberate, add it to prefixExceptions with the reason.", file, name, prefix)
			}
			for _, u := range nonBaseUnits {
				if strings.HasSuffix(name, u) {
					t.Errorf("%s: metric %q uses a non-base unit; Prometheus convention is seconds and bytes, "+
						"and a mixed-unit dashboard is wrong by orders of magnitude without looking wrong", file, name)
				}
			}
			for _, r := range reserved {
				if strings.HasSuffix(name, r) {
					t.Errorf("%s: metric %q ends in %q, which the exposition format reserves for histogram and "+
						"summary series — the scrape would be ambiguous", file, name, r)
				}
			}
		}
	}
}

// The classic Prometheus outage: one label sourced from an unbounded domain —
// a task id, an account, a request id, a raw URL path, a caller IP —
// multiplies the series count by every distinct value ever seen. It does not
// degrade gracefully; it exhausts the exporting process's memory and then the
// Prometheus server's. ADR-0020 says cardinality is "bounded by construction";
// this is the construction.
//
// Checked over the source, not a scrape, on purpose: an observable instrument
// only appears in a scrape once something observes it, so a label added on a
// path the fixtures do not exercise would be invisible to the per-module
// tests and perfectly visible here.
func TestNoMetricLabelIsHighCardinality(t *testing.T) {
	for _, file := range metricRegistrationSites {
		src := repoFile(t, file)
		seen := map[string]bool{}
		for _, m := range attrKeyLit.FindAllStringSubmatch(src, -1) {
			key := m[1]
			if seen[key] {
				continue
			}
			seen[key] = true
			if why := unboundedLabel(key); why != "" {
				t.Errorf("%s: label %q: %s.\n"+
					"This is a production outage, not a style question — the dimension belongs on a span or a "+
					"log record, never on a metric (ADR-0020).", file, key, why)
			}
			if !regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(key) {
				t.Errorf("%s: label %q is not lower_snake_case; label keys are typed by hand into PromQL", file, key)
			}
		}
		if len(seen) == 0 {
			t.Errorf("%s: no metric labels found at all — the extraction regex has gone stale, "+
				"which would make this test pass by seeing nothing", file)
		}
	}
}

// The denylist is duplicated into the two telemetry packages so that a
// forbidden label fails in the module where somebody added it, with the
// failure next to the code. hub and runner are separate modules and neither
// may import the other (depguard, CLAUDE.md), so a copy is the only option —
// and a copy that drifts is worse than no copy, because two of the three would
// still pass. Same reasoning, same remedy as the gateway's leaktest copy.
func TestTheCardinalityDenylistHasNotDrifted(t *testing.T) {
	// The leading newline matters: this file mentions the signature in a string
	// literal above, and cutting on the first textual match would compare the
	// wrong thing (and pass).
	const marker = "\nfunc unboundedLabel(key string) string {"
	body := func(rel string) string {
		src := repoFile(t, rel)
		_, after, ok := strings.Cut(src, marker)
		if !ok {
			t.Fatalf("%s no longer defines unboundedLabel", rel)
		}
		before, _, ok := strings.Cut(after, "\n}\n")
		if !ok {
			t.Fatalf("%s: cannot find the end of unboundedLabel", rel)
		}
		return before
	}
	original := body("pkg/shiftlog/metrics_conformance_test.go")
	for _, copied := range []string{
		"hub/internal/telemetry/conformance_test.go",
		"runner/internal/telemetry/conformance_test.go",
	} {
		if body(copied) != original {
			t.Errorf("%s: its copy of unboundedLabel has drifted from the one in "+
				"pkg/shiftlog/metrics_conformance_test.go.\n"+
				"Re-copy the function verbatim; a denylist that differs per module is a denylist with holes.", copied)
		}
	}
}

// metricNames extracts every metric-name literal from a registration site.
func metricNames(tb testing.TB, rel string) []string {
	tb.Helper()
	src := repoFile(tb, rel)
	var out []string
	for _, m := range metricNameLit.FindAllStringSubmatch(src, -1) {
		out = append(out, m[1])
	}
	if len(out) == 0 {
		tb.Fatalf("%s: no metric names found — the extraction regex has gone stale, "+
			"which would make these tests pass by seeing nothing", rel)
	}
	return out
}

// unboundedLabel reports why a label key is unsafe as a Prometheus dimension,
// or "" if it is fine. Kept in step with the runner's copy and with the
// repo-wide source-level check in pkg/shiftlog.
func unboundedLabel(key string) string {
	// Identifiers are the whole problem: an id exists to be unique, which is
	// the one property a label must not have. `flow` and `connector` as NAMES
	// are fine and explicitly allowed (ADR-0020) — bounded by what is deployed.
	if key == "id" || strings.HasSuffix(key, "_id") {
		return "an id is unique by definition — that is exactly what a label must not be"
	}
	for _, bad := range []struct{ frag, why string }{
		{"task", "task ids are unbounded — one series per task executed"},
		{"trace", "trace ids are unbounded; they belong on spans"},
		{"account", "account/tenant ids grow with every customer and never shrink"},
		{"tenant", "account/tenant ids grow with every customer and never shrink"},
		{"request", "request ids are unbounded — one series per request"},
		{"path", "raw URL paths are attacker-controlled; use the matched route pattern"},
		{"url", "URLs are attacker-controlled and unbounded"},
		{"user", "user identities are unbounded and are personal data"},
		{"email", "personal data must never be a metric dimension"},
		{"ip", "caller IPs are unbounded and attacker-controlled"},
		{"host", "remote hostnames are attacker-controlled"},
		{"key", "keys and idempotency keys are unbounded (and may be sensitive)"},
		{"token", "credentials must never appear in a metric"},
		{"secret", "secret material must never appear in a metric"},
		{"payload", "payload never reaches the control plane (ADR-0016)"},
		{"execution", "per-execution ids are unbounded"},
	} {
		if strings.Contains(key, bad.frag) {
			return bad.why
		}
	}
	return ""
}
