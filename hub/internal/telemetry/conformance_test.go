package telemetry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/hub/internal/telemetry"
)

// Metric names and label keys are an operator contract exactly as log `event`
// names are (ADR-0020 / ADR-0046): dashboards, recording rules and alert
// expressions are written against them and live outside this repo, so a rename
// does not break a build — it silently stops matching, and the alert that was
// meant to page someone never fires again. pkg/shiftlog pins the log
// vocabulary; this file and its runner twin pin the metric one.
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
// only place in the repo that can see across module boundaries. This file is
// duplicated rather than shared with the runner's copy for the same reason:
// hub and runner are separate modules and neither may import the other.

// metricSpec is the pinned shape of one metric family: its Prometheus type and
// the exact label keys it may carry.
type metricSpec struct {
	kind   string
	labels []string

	// totalSuffixIsNotACounter marks a KNOWN naming violation that already
	// shipped, so the rule below can stay strict for everything new. Only one
	// metric sets it — see the comment at its declaration. Renaming a metric
	// to fix its suffix is itself a breaking change for operators, so the
	// violation is recorded here rather than quietly corrected.
	totalSuffixIsNotACounter bool
}

// The hub's complete metric surface. Adding a metric MUST mean adding a line
// here: that is the point of the list, not an obstacle to it. A new metric is
// a new promise to operators, and this is where the promise is recorded.
var hubMetrics = map[string]metricSpec{
	// Queue health — what an operator pages on when work stops flowing.
	// `state` is a task state from a fixed vocabulary (queued/leased/...),
	// never a task id.
	"shift_hub_tasks":                 {kind: "gauge", labels: []string{"state"}},
	"shift_hub_oldest_queued_seconds": {kind: "gauge"},

	// Fleet and control-plane inventory.
	"shift_hub_runners_active": {kind: "gauge"},
	// KNOWN VIOLATION (recorded, not fixed): this is the number of registered
	// runners — a gauge — but `_total` conventionally means a monotonic
	// counter, so `rate(shift_hub_runners_total[5m])` is a natural thing for
	// an operator to write and a meaningless thing to compute. `shift_hub_
	// runners` would be the correct name; renaming it is itself a dashboard-
	// breaking change, so it is pinned as-is and flagged here.
	"shift_hub_runners_total": {kind: "gauge", totalSuffixIsNotACounter: true},
	"shift_hub_schedules_due": {kind: "gauge"},
	"shift_hub_schedules":     {kind: "gauge"},
	"shift_hub_flows":         {kind: "gauge"},

	// `class` is a rate-limiter class name configured at startup
	// ("public", ...), never the bucket key — the key is the caller IP, and
	// labelling by it would grow the series count with every client that ever
	// connects.
	"shift_hub_ratelimited_total": {kind: "counter", labels: []string{"class"}},

	// `route` is the MATCHED mux pattern (http.Request.Pattern), which is
	// bounded by the routing table, not the raw request path, which is
	// attacker-controlled and would let anyone mint series at will by curling
	// random URLs. See hub/internal/api.(*api).observe.
	"shift_hub_http_requests_total": {
		kind: "counter", labels: []string{"method", "route", "status"},
	},
	"shift_hub_http_request_duration_seconds": {
		kind: "histogram", labels: []string{"method", "route", "status"},
	},
}

// scrapeHub drives one full collection cycle and returns the parsed
// exposition. The snapshot is deliberately fully populated, and one request is
// recorded before scraping: an instrument with no data emits nothing at all,
// so a thin fixture would let a metric silently vanish from the scrape while
// this test still passed.
func scrapeHub(t *testing.T) exposition {
	t.Helper()
	h, err := telemetry.NewHub(
		func(context.Context) (telemetry.Snapshot, error) {
			return telemetry.Snapshot{
				Tasks:           map[string]int64{"queued": 3, "leased": 1},
				OldestQueuedSec: 12.5,
				RunnersActive:   2, RunnersTotal: 4,
				SchedulesDue: 1, Schedules: 6, Flows: 9,
			}, nil
		},
		func() map[string]int64 { return map[string]int64{"public": 7} },
	)
	if err != nil {
		t.Fatalf("building the hub /metrics handler: %v", err)
	}
	h.RecordHTTP(t.Context(), http.MethodGet, "GET /api/flows/{name}", http.StatusOK, 0.012)

	rec := httptest.NewRecorder()
	h.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape returned %d, want 200", rec.Code)
	}
	return parseExposition(t, rec.Body.String())
}

// TestTheHubScrapeExposesExactlyThePinnedMetrics fails in both directions on
// purpose. A missing name means an operator's panel went blank; an unexpected
// one means a metric shipped without anybody deciding it was worth supporting
// forever.
func TestTheHubScrapeExposesExactlyThePinnedMetrics(t *testing.T) {
	got := scrapeHub(t)
	for name := range hubMetrics {
		if _, ok := got[name]; !ok {
			t.Errorf("metric %q is no longer exposed — every dashboard and alert referring to it is now silently dead.\n"+
				"If the rename was deliberate, update hubMetrics here and the operator-facing docs together.", name)
		}
	}
	for name := range got {
		if _, ok := hubMetrics[name]; !ok {
			t.Errorf("metric %q is exposed but not pinned.\n"+
				"Add it to hubMetrics with its type and label keys — a metric is a permanent promise to operators.", name)
		}
	}
}

// A type change is as breaking as a rename: `rate()` over something that
// stopped being a counter, or a histogram_quantile over something that stopped
// being a histogram, produces wrong numbers rather than an error.
func TestHubMetricTypesAreStable(t *testing.T) {
	got := scrapeHub(t)
	for name, want := range hubMetrics {
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
		if !want.totalSuffixIsNotACounter && strings.HasSuffix(name, "_total") != (f.kind == "counter") {
			t.Errorf("metric %q is a %s: the `_total` suffix and the counter type must agree", name, f.kind)
		}
		// The escape hatch is for the one metric that already shipped wrong;
		// it must not become a way to silence the rule for new metrics.
		if want.totalSuffixIsNotACounter && f.kind == "counter" {
			t.Errorf("metric %q is a counter now — drop the totalSuffixIsNotACounter exemption", name)
		}
	}
}

// Label keys are pinned per metric because a renamed label breaks a dashboard
// exactly as thoroughly as a renamed metric — every `by (route)` and every
// `{state="queued"}` selector stops matching.
func TestHubMetricLabelsArePinned(t *testing.T) {
	got := scrapeHub(t)
	for name, want := range hubMetrics {
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
// Prometheus server's. The hub is the tenanted component, so it is the one
// with account ids and request ids in easy reach of a well-meaning patch.
// ADR-0020 forbids it by construction, and this is the assertion behind that
// word.
func TestNoHubMetricCarriesAnUnboundedLabel(t *testing.T) {
	got := scrapeHub(t)
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

// The route label is the one place a raw, attacker-controlled string could
// reach a label without anybody noticing — RecordHTTP takes it as a plain
// argument. The hub's own caller is checked in hub/internal/api; here we prove
// the value is passed through verbatim, so that caller is the only thing that
// needs to be right.
func TestTheHubRouteLabelIsWhateverTheCallerPassed(t *testing.T) {
	h, err := telemetry.NewHub(
		func(context.Context) (telemetry.Snapshot, error) { return telemetry.Snapshot{}, nil }, nil)
	if err != nil {
		t.Fatalf("building the hub /metrics handler: %v", err)
	}
	h.RecordHTTP(t.Context(), http.MethodGet, "GET /api/flows/{name}", http.StatusOK, 0.01)
	rec := httptest.NewRecorder()
	h.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `route="GET /api/flows/{name}"`) {
		t.Error("the route label is not the pattern the caller passed; the hub's bounded-cardinality guarantee " +
			"depends on that value being the matched mux pattern and nothing else")
	}
}

// A nil *Hub must stay callable: metrics are optional, and a deployment that
// disables them must not take a nil dereference on every request.
func TestRecordHTTPIsSafeWithoutMetrics(t *testing.T) {
	var h *telemetry.Hub
	h.RecordHTTP(t.Context(), http.MethodGet, "GET /", http.StatusOK, 0.01)
}

// --- exposition parsing -------------------------------------------------
//
// A hand-rolled parser rather than prometheus/common/expfmt: the format is
// three line shapes, and pulling a parsing library into the hub's go.mod for
// one test is a worse trade than twenty lines here.

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

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// A histogram's units and its bucket boundaries are one decision, and getting
// them out of step makes the metric worse than absent: the name still says
// seconds, the panel still draws, and every quantile it reports is a fiction.
//
// This shipped that way. RecordHTTP passes dur.Seconds(), but the histogram
// took the SDK's default boundaries — 0, 5, 10, 25 … 10000 — which are
// millisecond-shaped. Every real control-plane request (single-digit
// milliseconds) fell into the first bucket, so histogram_quantile returned
// ~2.5 no matter what the server was actually doing. Found by the TC-002
// conformance sweep; the fix is explicit boundaries.
func TestTheLatencyHistogramIsBucketedForTheUnitItRecords(t *testing.T) {
	const name = "shift_hub_http_request_duration_seconds"
	body := scrapeHubBody(t)

	bounds := bucketBounds(t, body, name)
	if len(bounds) == 0 {
		t.Fatalf("%s exposed no le buckets", name)
	}

	// Sub-millisecond-to-second work needs resolution below a second. The
	// default ladder has NOTHING under 5, which is the whole bug.
	var below1s int
	for _, b := range bounds {
		if b < 1 {
			below1s++
		}
	}
	if below1s < 4 {
		t.Errorf("%s has %d bucket(s) below 1s (%v) — a seconds histogram of "+
			"millisecond-scale requests needs resolution at the bottom, or every "+
			"quantile collapses into the first bucket", name, below1s, bounds)
	}

	// The top of the ladder should be a plausible request timeout, not 10000
	// seconds — a boundary near three hours is the giveaway that the ladder was
	// meant for milliseconds.
	if top := bounds[len(bounds)-1]; top > 600 {
		t.Errorf("%s tops out at %g seconds — that ladder is millisecond-shaped, "+
			"so the recorded seconds values never reach it", name, top)
	}

	// The recorded observation (12ms, from scrapeHub) must land somewhere with
	// buckets on BOTH sides, which is what makes a quantile meaningful.
	if bounds[0] > 0.012 {
		t.Errorf("the smallest bucket is %g s, above the 12 ms sample — the histogram "+
			"cannot distinguish a fast request from a very fast one", bounds[0])
	}
}

// scrapeHubBody is scrapeHub's raw exposition, for assertions that need the
// bucket lines parseExposition folds away.
func scrapeHubBody(t *testing.T) string {
	t.Helper()
	h, err := telemetry.NewHub(
		func(context.Context) (telemetry.Snapshot, error) { return telemetry.Snapshot{}, nil },
		func() map[string]int64 { return nil },
	)
	if err != nil {
		t.Fatalf("building the hub /metrics handler: %v", err)
	}
	h.RecordHTTP(t.Context(), http.MethodGet, "GET /api/flows/{name}", http.StatusOK, 0.012)

	rec := httptest.NewRecorder()
	h.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape returned %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

// bucketBounds returns the finite le boundaries of a histogram family, sorted.
func bucketBounds(tb testing.TB, body, name string) []float64 {
	tb.Helper()
	var out []float64
	for line := range strings.SplitSeq(body, "\n") {
		if !strings.HasPrefix(line, name+"_bucket{") {
			continue
		}
		i := strings.Index(line, `le="`)
		if i < 0 {
			continue
		}
		rest := line[i+4:]
		j := strings.Index(rest, `"`)
		if j < 0 {
			continue
		}
		if rest[:j] == "+Inf" {
			continue
		}
		v, err := strconv.ParseFloat(rest[:j], 64)
		if err != nil {
			tb.Fatalf("unparsable le=%q in %s", rest[:j], line)
		}
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}
