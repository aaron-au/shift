package telemetry_test

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/aaron-au/shift/runner/internal/telemetry"
)

// Metric names and label keys are an operator contract exactly as log `event`
// names are (ADR-0020 / ADR-0046): dashboards, recording rules and alert
// expressions are written against them and live outside this repo, so a rename
// does not break a build — it silently stops matching, and the alert that was
// meant to page someone never fires again. pkg/shiftlog pins the log
// vocabulary; this file and its hub twin pin the metric one.
//
// These assertions run against a REAL scrape rather than the source text: the
// handler is built, driven through one collection cycle and its exposition
// output parsed. That is what an operator's Prometheus actually sees, so it
// survives any refactor that moves the registration code, and it also catches
// the layers between the instrument and the wire — the OTel Prometheus
// exporter's own name mangling (unit suffixes, `_total` handling) is part of
// the contract and is invisible to a source-level grep.
//
// The complementary source-level checks — that these names obey the platform
// naming convention, and that no NEW component starts registering metrics
// unnoticed — live in pkg/shiftlog/metrics_conformance_test.go, which is the
// only place in the repo that can see across module boundaries.

// metricSpec is the pinned shape of one metric family: its Prometheus type and
// the exact label keys it may carry.
type metricSpec struct {
	kind   string
	labels []string
}

// The runner's complete metric surface. Adding a metric MUST mean adding a
// line here: that is the point of the list, not an obstacle to it. A new
// metric is a new promise to operators, and this is where the promise is
// recorded.
var runnerMetrics = map[string]metricSpec{
	// Admission signals (ADR-0005): what the governor will and will not admit.
	"shift_runner_governor_budget_bytes": {kind: "gauge"},
	"shift_runner_governor_used_bytes":   {kind: "gauge"},
	"shift_runner_governor_peak_bytes":   {kind: "gauge"},
	"shift_runner_max_concurrent_by_mem": {kind: "gauge"},

	// Task lifecycle.
	"shift_runner_tasks_running":         {kind: "gauge"},
	"shift_runner_tasks_waiting":         {kind: "gauge"},
	"shift_runner_tasks_submitted_total": {kind: "counter"},
	"shift_runner_tasks_completed_total": {kind: "counter"},
	"shift_runner_tasks_failed_total":    {kind: "counter"},
	"shift_runner_records_in_total":      {kind: "counter"},

	// Deliberately NOT shift_runner_*: a flow ending on @stop is a property of
	// the flow, not of the runner that happened to execute it (ADR-0031 §3),
	// and it is a subset of tasks_completed rather than a peer of it. The
	// prefix exception is pinned here so it stays a decision rather than
	// becoming a precedent — see the convention test in pkg/shiftlog.
	"shift_flow_stops_total": {kind: "counter"},

	// `connector` is the connector NAME (http, sftp, ...) — bounded by what is
	// installed on the runner, not by traffic.
	"shift_runner_connector_in_use": {kind: "gauge", labels: []string{"connector"}},

	// `class` is a rate-limiter class name configured at startup ("webhook"),
	// never the bucket key — the key is the caller IP, and labelling by it
	// would grow the series count with every client that ever connects.
	"shift_runner_ratelimited_total": {kind: "counter", labels: []string{"class"}},
}

// scrapeRunner drives one full collection cycle and returns the parsed
// exposition. The snapshot is deliberately fully populated: an observable
// instrument that is never observed emits nothing at all, so a thin snapshot
// would let a metric silently vanish from the scrape while this test still
// passed.
func scrapeRunner(t *testing.T) exposition {
	t.Helper()
	h, err := telemetry.NewRunner(
		func() telemetry.Snapshot {
			return telemetry.Snapshot{
				GovBudget: 1 << 30, GovUsed: 1 << 20, GovPeak: 1 << 21, MaxByMem: 8,
				Submitted: 5, Completed: 4, Failed: 1, Stopped: 2, Waiting: 1, Running: 3,
				RecordsIn: 900,
				Conns:     []telemetry.ConnUse{{Name: "http", InUse: 2}},
			}
		},
		func() map[string]int64 { return map[string]int64{"webhook": 7} },
	)
	if err != nil {
		t.Fatalf("building the runner /metrics handler: %v", err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape returned %d, want 200", rec.Code)
	}
	return parseExposition(t, rec.Body.String())
}

// TestTheRunnerScrapeExposesExactlyThePinnedMetrics fails in both directions
// on purpose. A missing name means an operator's panel went blank; an
// unexpected one means a metric shipped without anybody deciding it was worth
// supporting forever.
func TestTheRunnerScrapeExposesExactlyThePinnedMetrics(t *testing.T) {
	got := scrapeRunner(t)
	for name := range runnerMetrics {
		if _, ok := got[name]; !ok {
			t.Errorf("metric %q is no longer exposed — every dashboard and alert referring to it is now silently dead.\n"+
				"If the rename was deliberate, update runnerMetrics here and the operator-facing docs together.", name)
		}
	}
	for name := range got {
		if _, ok := runnerMetrics[name]; !ok {
			t.Errorf("metric %q is exposed but not pinned.\n"+
				"Add it to runnerMetrics with its type and label keys — a metric is a permanent promise to operators.", name)
		}
	}
}

// A type change is as breaking as a rename: `rate()` over something that
// stopped being a counter, or a gauge alert on something that became a
// histogram, produces wrong numbers rather than an error.
func TestRunnerMetricTypesAreStable(t *testing.T) {
	got := scrapeRunner(t)
	for name, want := range runnerMetrics {
		f, ok := got[name]
		if !ok {
			continue // reported by the exact-set test
		}
		if f.kind != want.kind {
			t.Errorf("metric %q is a %s, pinned as %s — queries written for the old type now return wrong values, not errors",
				name, f.kind, want.kind)
		}
		// Prometheus convention, and what every operator assumes when they
		// reach for rate(): a `_total` suffix means a monotonic counter.
		if strings.HasSuffix(name, "_total") != (f.kind == "counter") {
			t.Errorf("metric %q is a %s: the `_total` suffix and the counter type must agree", name, f.kind)
		}
	}
}

// Label keys are pinned per metric because a renamed label breaks a dashboard
// exactly as thoroughly as a renamed metric — every `by (connector)` and every
// `{class="webhook"}` selector stops matching.
func TestRunnerMetricLabelsArePinned(t *testing.T) {
	got := scrapeRunner(t)
	for name, want := range runnerMetrics {
		f, ok := got[name]
		if !ok {
			continue
		}
		if g, w := strings.Join(f.labels(), ","), strings.Join(sorted(want.labels), ","); g != w {
			t.Errorf("metric %q carries labels [%s], pinned as [%s].\n"+
				"A label rename breaks every query that groups or selects on it.", name, g, w)
		}
	}
}

// The classic Prometheus outage: one label sourced from an unbounded domain —
// a task id, a flow run, an account, a request id, a raw URL path, a caller IP
// — multiplies the series count by every distinct value ever seen. It does not
// degrade gracefully; it exhausts the scrape target's memory and then the
// Prometheus server's. ADR-0020 forbids it by construction, and this is the
// assertion behind that word.
func TestNoRunnerMetricCarriesAnUnboundedLabel(t *testing.T) {
	got := scrapeRunner(t)
	for name, f := range got {
		for _, l := range f.labels() {
			if why := unboundedLabel(l); why != "" {
				t.Errorf("metric %q carries label %q: %s.\n"+
					"Unbounded labels are a production outage, not a style question — put it on a span or a log record instead (ADR-0020).",
					name, l, why)
			}
		}
	}
}

// --- exposition parsing -------------------------------------------------
//
// A hand-rolled parser rather than prometheus/common/expfmt: the format is
// three line shapes, and pulling a parsing library into the runner's go.mod
// for one test is a worse trade than twenty lines here.

type family struct {
	kind     string
	labelSet map[string]bool
}

func (f *family) labels() []string {
	out := make([]string, 0, len(f.labelSet))
	for k := range f.labelSet {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type exposition map[string]*family

// Series the OTel Prometheus exporter adds on its own behalf. They are the
// exporter's identity, not our contract, so they are excluded from the pins —
// but note they are NOT excluded from the cardinality check below, which runs
// over whatever the scrape actually carries.
var exporterOwned = map[string]bool{"target_info": true, "otel_scope_info": true}

var exporterLabels = map[string]bool{
	"otel_scope_name": true, "otel_scope_version": true, "otel_scope_schema_url": true,
	"le": true, "quantile": true, // bucket/quantile dimensions, part of the type
}

func parseExposition(tb testing.TB, body string) exposition {
	tb.Helper()
	out := exposition{}
	get := func(name string) *family {
		f, ok := out[name]
		if !ok {
			f = &family{labelSet: map[string]bool{}}
			out[name] = f
		}
		return f
	}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			if len(fields) != 4 || exporterOwned[fields[2]] {
				continue
			}
			get(fields[2]).kind = fields[3]
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, _ := strings.Cut(line, "{")
		name = strings.TrimSpace(name)
		if i := strings.IndexByte(name, ' '); i >= 0 {
			name = name[:i]
		}
		// A histogram is exposed as three series; they all belong to the one
		// family an operator names in a query.
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if base := strings.TrimSuffix(name, suffix); base != name && out[base] != nil {
				name = base
				break
			}
		}
		if exporterOwned[name] {
			continue
		}
		f := get(name)
		labels, _, _ := strings.Cut(rest, "} ")
		for _, kv := range strings.Split(labels, ",") {
			k, _, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			if k = strings.TrimSpace(k); k != "" && !exporterLabels[k] {
				f.labelSet[k] = true
			}
		}
	}
	return out
}

// unboundedLabel reports why a label key is unsafe as a Prometheus dimension,
// or "" if it is fine. Kept in step with the hub's copy and with the
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

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
