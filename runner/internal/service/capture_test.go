package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/runner/internal/flow"
)

// TestCaptureSamplerBoundsAndRedacts: the sampler keeps at most max records
// per step, flags More when the step produced beyond the sample, and masks
// secret values at the serialized-text layer.
func TestCaptureSamplerBoundsAndRedacts(t *testing.T) {
	cs := newCaptureSampler(2, newRedactor([]string{"secret-val"}))
	b := record.NewBatch()
	bld := b.Builder()
	for i := range 5 {
		bld.BeginMap()
		bld.KeyLiteral("id")
		bld.Int(int64(i))
		bld.KeyLiteral("tok")
		bld.StringLiteral("secret-val")
		bld.EndMap()
		b.Append(bld.Finish())
	}
	cs.Sample("s1", b) // 5 records, max 2 → keep 2, mark more
	cs.Sample("s1", b) // already full → still 2, more

	res := cs.result()
	if len(res) != 1 || res[0].StepID != "s1" {
		t.Fatalf("result = %+v", res)
	}
	if len(res[0].Records) != 2 || !res[0].More {
		t.Fatalf("bound/more wrong: %d records, more=%v", len(res[0].Records), res[0].More)
	}
	for _, r := range res[0].Records {
		s := string(r)
		if strings.Contains(s, "secret-val") {
			t.Fatalf("secret leaked into capture: %s", s)
		}
		if !strings.Contains(s, "***") {
			t.Fatalf("value not redacted: %s", s)
		}
	}
}

// TestCaptureEndToEnd: a capture-enabled run collects per-step samples
// (source + op stages) through the production path, bounded and redacted.
func TestCaptureEndToEnd(t *testing.T) {
	if testing.Short() || coverageRun() {
		t.Skip("spawns connector subprocesses")
	}
	svc := newTestService(t, Options{})

	cfg, _ := json.Marshal(map[string]any{"records": 100})
	doc := &flow.Document{
		Name:   "capture-flow",
		Source: flow.Endpoint{Connector: "gen", Action: "gen", Config: cfg},
		Ops: []flow.Op{
			{Type: "project", Fields: []flow.ProjectField{{Path: "$.id"}, {Path: "$.name"}}},
		},
		Sink: flow.Endpoint{Connector: "gen", Action: "discard"},
	}
	// gen's first record name is "customer-000000"; mark it secret.
	id, err := svc.SubmitWith(doc, SubmitOpts{Capture: true, CaptureMax: 3, SecretValues: []string{"customer-000000"}})
	if err != nil {
		t.Fatal(err)
	}
	tk := awaitTerminal(t, svc, id)
	if tk.State != "completed" {
		t.Fatalf("state = %s: %s", tk.State, tk.Error)
	}
	if len(tk.Captured) < 2 {
		t.Fatalf("want source + op capture, got %d steps: %+v", len(tk.Captured), tk.Captured)
	}
	byStep := map[string]int{}
	for _, c := range tk.Captured {
		byStep[c.StepID] = len(c.Records)
		if len(c.Records) > 3 {
			t.Fatalf("step %s exceeded max: %d", c.StepID, len(c.Records))
		}
		if !c.More {
			t.Errorf("step %s: expected More (100 records > 3)", c.StepID)
		}
	}
	if byStep["source"] == 0 {
		t.Fatalf("no source capture: %+v", tk.Captured)
	}
	// The source sample carries full gen records; the secret name is masked.
	src := tk.Captured[0]
	joined := ""
	var joinedSb94 strings.Builder
	for _, r := range src.Records {
		joinedSb94.WriteString(string(r))
	}
	joined += joinedSb94.String()
	if strings.Contains(joined, "customer-000000") {
		t.Fatalf("secret leaked into source capture: %s", joined)
	}
	if !strings.Contains(joined, "***") {
		t.Fatalf("source capture not redacted: %s", joined)
	}
}
