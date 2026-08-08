package service

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aaron-au/shift/runner/internal/flow"
)

// step is a small builder for a graph-form step (Type rides the embedded Op).
func step(id, typ string) flow.Step {
	s := flow.Step{ID: id}
	s.Type = typ
	return s
}

// TestFanOutTeeToTwoSinks: @webhook → tee → two @discard sinks. Every record
// reaches both sinks (built-ins, no connector subprocess).
func TestFanOutTeeToTwoSinks(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"a", "b"}
	a := step("a", "sink")
	a.Connector = "@discard"
	b := step("b", "sink")
	b.Connector = "@discard"
	doc := &flow.Document{Name: "tee", Steps: []flow.Step{in, tee, a, b}}

	body := []byte(`{"n":1}` + "\n" + `{"n":2}` + "\n" + `{"n":3}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	// Two branches × 3 records duplicated = 6 confirmed.
	if tk.SinkConfirmed != 6 {
		t.Fatalf("sink confirmed = %d, want 6 (3 to each branch)", tk.SinkConfirmed)
	}
}

// TestFanOutTeeWithBranchOp: a tee branch may carry its own operators (COW),
// while a sibling read-only branch is unaffected.
func TestFanOutTeeWithBranchOp(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"filt", "all"}
	// branch "filt": filter keep==true → sink
	filt := step("filt", "filter")
	filt.Path, filt.Cmp, filt.Value, filt.OnComplete = "$.keep", "eq", []byte("true"), "fa"
	fa := step("fa", "sink")
	fa.Connector = "@discard"
	// branch "all": straight to sink (read-only, shared)
	all := step("all", "sink")
	all.Connector = "@discard"
	doc := &flow.Document{Name: "teeop", Steps: []flow.Step{in, tee, filt, fa, all}}

	body := []byte(`{"keep":true}` + "\n" + `{"keep":false}` + "\n" + `{"keep":true}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	// filt branch keeps 2, all branch keeps 3 → 5 confirmed.
	if tk.SinkConfirmed != 5 {
		t.Fatalf("sink confirmed = %d, want 5 (2 filtered + 3 all)", tk.SinkConfirmed)
	}
}

// TestFanOutRouterPartitions: @webhook → router → two sinks by predicate; each
// record goes to exactly one branch (partition, not duplicate).
func TestFanOutRouterPartitions(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "r"
	r := step("r", "router")
	r.Routes = []flow.Route{{Path: "$.k", Cmp: "eq", Value: []byte(`"a"`), To: "sa"}}
	r.Default = "sb"
	sa := step("sa", "sink")
	sa.Connector = "@discard"
	sb := step("sb", "sink")
	sb.Connector = "@discard"
	doc := &flow.Document{Name: "route", Steps: []flow.Step{in, r, sa, sb}}

	body := []byte(`{"k":"a"}` + "\n" + `{"k":"b"}` + "\n" + `{"k":"a"}` + "\n")
	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: body})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	// Router partitions: 2 to "a", 1 to default = 3 total, no duplication.
	if tk.SinkConfirmed != 3 {
		t.Fatalf("sink confirmed = %d, want 3 (partition)", tk.SinkConfirmed)
	}
}

// TestFanInConcat: two @webhook sources → concat merge → @response. Both read
// the injected body, so the union emits every record twice.
func TestFanInConcat(t *testing.T) {
	svc := newTestService(t, Options{})

	s1 := step("s1", "source")
	s1.Connector, s1.Action, s1.OnSuccess = "@webhook", "ndjson", "m"
	s2 := step("s2", "source")
	s2.Connector, s2.Action, s2.OnSuccess = "@webhook", "ndjson", "m"
	m := step("m", "merge")
	m.Inputs, m.Mode, m.OnSuccess = []string{"s1", "s2"}, "concat", "out"
	out := step("out", "sink")
	out.Connector = "@response"
	doc := &flow.Document{Name: "concat", Steps: []flow.Step{s1, s2, m, out}}

	var resp bytes.Buffer
	body := []byte(`{"n":1}` + "\n" + `{"n":2}` + "\n")
	tk, err := svc.RunSync(doc, SubmitOpts{WebhookBody: body, Response: &resp})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if n := strings.Count(resp.String(), "\n"); n != 4 {
		t.Fatalf("response rows = %d, want 4 (2 sources × 2 records):\n%s", n, resp.String())
	}
}

// TestFanInJoin: two @webhook sources self-join on a key → @response. Each
// probe record is enriched with the matched build record under As.
func TestFanInJoin(t *testing.T) {
	svc := newTestService(t, Options{})

	probe := step("p", "source")
	probe.Connector, probe.Action, probe.OnSuccess = "@webhook", "ndjson", "j"
	build := step("b", "source")
	build.Connector, build.Action, build.OnSuccess = "@webhook", "ndjson", "j"
	j := step("j", "merge")
	j.Inputs, j.Mode, j.Build, j.As, j.OnSuccess = []string{"p", "b"}, "join", "b", "match", "out"
	j.JoinType = "inner"
	j.On = &flow.JoinOn{Left: "$.id", Right: "$.id"}
	out := step("out", "sink")
	out.Connector = "@response"
	doc := &flow.Document{Name: "join", Steps: []flow.Step{probe, build, j, out}}

	var resp bytes.Buffer
	body := []byte(`{"id":"A","v":1}` + "\n")
	tk, err := svc.RunSync(doc, SubmitOpts{WebhookBody: body, Response: &resp})
	if err != nil {
		t.Fatal(err)
	}
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if !strings.Contains(resp.String(), `"match":{"id":"A","v":1}`) {
		t.Fatalf("join output missing enriched match:\n%s", resp.String())
	}
}

// A mixed graph — fan-out AND fan-in in the same flow — used to be rejected
// at run time even though the hub validated it and the studio drew it. That
// gap between what the canvas accepts and what runs was the defect (#59).
//
// source → tee → [a, b] → merge(concat) → sink. The tee duplicates every
// record to both branches and the merge concatenates them, so two input
// records become four.
func TestAMixedFanOutAndFanInFlowRuns(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"a", "b"}
	a := step("a", "filter")
	a.Path, a.Cmp, a.OnComplete = "$.x", "exists", "m"
	b := step("b", "filter")
	b.Path, b.Cmp, b.OnComplete = "$.x", "exists", "m"
	m := step("m", "merge")
	m.Inputs, m.Mode, m.OnSuccess = []string{"a", "b"}, "concat", "out"
	out := step("out", "sink")
	out.Connector = "@discard"
	doc := &flow.Document{Name: "mixed", Steps: []flow.Step{in, tee, a, b, m, out}}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: []byte(`{"x":1}` + "\n" + `{"x":2}` + "\n")})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if tk.SinkConfirmed != 4 {
		t.Fatalf("sink confirmed %d, want 4 (2 records duplicated by the tee, concatenated by the merge)", tk.SinkConfirmed)
	}
}

// The ENRICHMENT shape, which ADR-0029 names as the natural home for
// request-reply enrichment: one source, teed so that one branch is enriched
// and the other carries the original, joined back together. This is arguably
// the most common real integration, and it was the headline casualty of the
// topology restriction.
func TestTheEnrichmentShapeRuns(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t"
	tee := step("t", "tee")
	tee.Branches = []string{"probe", "build"}
	// The probe side passes through untouched.
	probe := step("probe", "filter")
	probe.Path, probe.Cmp, probe.OnComplete = "$.id", "exists", "j"
	// The build side is the "enrichment": same records, joined back by id.
	build := step("build", "filter")
	build.Path, build.Cmp, build.OnComplete = "$.id", "exists", "j"
	j := step("j", "merge")
	j.Inputs, j.Mode, j.Build, j.OnSuccess = []string{"probe", "build"}, "join", "build", "out"
	j.On = &flow.JoinOn{Left: "$.id", Right: "$.id"}
	j.As, j.JoinType = "match", "inner"
	out := step("out", "sink")
	out.Connector, out.Action = "@response", ""
	doc := &flow.Document{Name: "enrich", Steps: []flow.Step{in, tee, probe, build, j, out}}

	var resp bytes.Buffer
	id, err := svc.SubmitWith(doc, SubmitOpts{
		WebhookBody: []byte(`{"id":"A","v":1}` + "\n"),
		Response:    &resp,
	})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if !strings.Contains(resp.String(), `"match":{"id":"A","v":1}`) {
		t.Fatalf("the teed branch was not joined back:\n%s", resp.String())
	}
}

// Two fan-outs in one graph: a tee whose branch tees again. Nesting was the
// other half of #59, and it is what proves the compiler recurses rather than
// handling one extra level.
func TestANestedFanOutRuns(t *testing.T) {
	svc := newTestService(t, Options{})

	in := step("in", "source")
	in.Connector, in.Action, in.OnSuccess = "@webhook", "ndjson", "t1"
	t1 := step("t1", "tee")
	t1.Branches = []string{"s1", "t2"}
	s1 := step("s1", "sink")
	s1.Connector = "@discard"
	t2 := step("t2", "tee")
	t2.Branches = []string{"s2", "s3"}
	s2 := step("s2", "sink")
	s2.Connector = "@discard"
	s3 := step("s3", "sink")
	s3.Connector = "@discard"
	doc := &flow.Document{Name: "nested", Steps: []flow.Step{in, t1, s1, t2, s2, s3}}

	id, err := svc.SubmitWith(doc, SubmitOpts{WebhookBody: []byte(`{"n":1}` + "\n" + `{"n":2}` + "\n")})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	// 2 records reach 3 sinks: the outer tee sends to s1 and the inner tee,
	// which sends to s2 and s3.
	if tk.SinkConfirmed != 6 {
		t.Fatalf("sink confirmed %d, want 6 (2 records × 3 terminal sinks)", tk.SinkConfirmed)
	}
}
