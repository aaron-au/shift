package compat_test

import (
	"strings"
	"testing"

	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/compat"
)

// desc builds a descriptor from a compact spec: action key "source/get"
// mapped to its config schema (empty string = no schema).
func desc(actions map[string]string) sdk.Descriptor {
	d := sdk.Descriptor{Name: "demo", Version: "1.0.0"}
	for key, schema := range actions {
		dir, name, _ := strings.Cut(key, "/")
		a := sdk.ActionDescriptor{Action: name, Direction: dir}
		if schema != "" {
			a.ConfigSchema = []byte(schema)
		}
		d.Actions = append(d.Actions, a)
	}
	return d
}

const twoFields = `{"type":"object",
  "properties":{"host":{"type":"string"},"port":{"type":"integer"}},
  "required":["host"]}`

// The whole gate rests on this: the direction of a change decides its class,
// and a diff engine that treated add and remove alike would be useless for
// the thing it exists to do.
func TestTheClassFollowsTheDirectionOfTheChange(t *testing.T) {
	cases := []struct {
		name       string
		old, fresh sdk.Descriptor
		want       compat.Class
		says       string
	}{{
		name:  "an action removed breaks every flow using it",
		old:   desc(map[string]string{"source/get": "", "sink/put": ""}),
		fresh: desc(map[string]string{"source/get": ""}),
		want:  compat.Breaking,
		says:  "action removed",
	}, {
		// A source that became a sink changes the step's ROLE in the graph.
		// No edit to config fixes that, so it is a removal to a flow.
		name:  "an action that changed direction reads as removed",
		old:   desc(map[string]string{"source/sync": ""}),
		fresh: desc(map[string]string{"sink/sync": ""}),
		want:  compat.Breaking,
		says:  "action removed",
	}, {
		name:  "a new action is purely additive",
		old:   desc(map[string]string{"source/get": ""}),
		fresh: desc(map[string]string{"source/get": "", "source/list": ""}),
		want:  compat.Compatible,
		says:  "new action",
	}, {
		name: "removing a config field breaks flows that still set it",
		old:  desc(map[string]string{"source/get": twoFields}),
		fresh: desc(map[string]string{"source/get": `{"type":"object",
		  "properties":{"host":{"type":"string"}},"required":["host"]}`}),
		want: compat.Breaking,
		says: `config field "port" removed`,
	}, {
		name: "a new REQUIRED field breaks every existing flow at once",
		old:  desc(map[string]string{"source/get": twoFields}),
		fresh: desc(map[string]string{"source/get": `{"type":"object",
		  "properties":{"host":{"type":"string"},"port":{"type":"integer"},"region":{"type":"string"}},
		  "required":["host","region"]}`}),
		want: compat.Breaking,
		says: "new REQUIRED config field",
	}, {
		name: "a new optional field is compatible",
		old:  desc(map[string]string{"source/get": twoFields}),
		fresh: desc(map[string]string{"source/get": `{"type":"object",
		  "properties":{"host":{"type":"string"},"port":{"type":"integer"},"region":{"type":"string"}},
		  "required":["host"]}`}),
		want: compat.Compatible,
		says: "new optional config field",
	}, {
		name: "tightening a field to required breaks flows that omit it",
		old:  desc(map[string]string{"source/get": twoFields}),
		fresh: desc(map[string]string{"source/get": `{"type":"object",
		  "properties":{"host":{"type":"string"},"port":{"type":"integer"}},
		  "required":["host","port"]}`}),
		want: compat.Breaking,
		says: "is now required",
	}, {
		name: "relaxing a field to optional is compatible",
		old:  desc(map[string]string{"source/get": twoFields}),
		fresh: desc(map[string]string{"source/get": `{"type":"object",
		  "properties":{"host":{"type":"string"},"port":{"type":"integer"}}}`}),
		want: compat.Compatible,
		says: "no longer required",
	}, {
		name: "a field that changed type breaks the config that fills it",
		old:  desc(map[string]string{"source/get": twoFields}),
		fresh: desc(map[string]string{"source/get": `{"type":"object",
		  "properties":{"host":{"type":"string"},"port":{"type":"string"}},"required":["host"]}`}),
		want: compat.Breaking,
		says: "changed type",
	}, {
		// Config schemas are STUDIO metadata (ADR-0018) — nothing enforces
		// them at run time — so a schema appearing cannot break a flow that
		// already ran.
		name:  "adding a schema where there was none is compatible",
		old:   desc(map[string]string{"source/get": ""}),
		fresh: desc(map[string]string{"source/get": twoFields}),
		want:  compat.Compatible,
		says:  "config schema added",
	}, {
		// Losing it costs an author their form, not their flow.
		name:  "removing a schema costs the builder its form",
		old:   desc(map[string]string{"source/get": twoFields}),
		fresh: desc(map[string]string{"source/get": ""}),
		want:  compat.Behaviour,
		says:  "config schema removed",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := compat.Compare(tc.old, tc.fresh)
			if got.Class != tc.want {
				t.Fatalf("class = %q, want %q\n%s", got.Class, tc.want, got)
			}
			if !strings.Contains(got.String(), tc.says) {
				t.Fatalf("report does not say %q:\n%s", tc.says, got)
			}
		})
	}
}

// x-shift-secret is not decoration: it drives the studio's secret picker, and
// it is the signal that a value must not be typed inline. A field that quietly
// stops being a secret is how a credential ends up in a flow document in plain
// text — which is worse than a config break, because nothing fails.
func TestLosingASecretMarkerIsBreaking(t *testing.T) {
	//nolint:gosec // G101 false positive: a JSON Schema DESCRIBING a password field, not a value
	secret := `{"type":"object","properties":{"password":{"type":"string","x-shift-secret":true}}}`
	plain := `{"type":"object","properties":{"password":{"type":"string"}}}`

	lost := compat.Compare(desc(map[string]string{"sink/put": secret}), desc(map[string]string{"sink/put": plain}))
	if lost.Class != compat.Breaking {
		t.Fatalf("dropping x-shift-secret = %q, want breaking\n%s", lost.Class, lost)
	}
	if !strings.Contains(lost.String(), "no longer marked as a secret") {
		t.Fatalf("the report does not name the risk:\n%s", lost)
	}

	gained := compat.Compare(desc(map[string]string{"sink/put": plain}), desc(map[string]string{"sink/put": secret}))
	if gained.Class != compat.Compatible {
		t.Fatalf("marking a field secret = %q, want compatible\n%s", gained.Class, gained)
	}
}

// A required field three levels down breaks a flow exactly as hard as one at
// the top. A diff that only read the first level would call that compatible,
// which is the failure mode that makes a gate worse than no gate.
func TestNestedFieldsAreCompared(t *testing.T) {
	before := `{"type":"object","properties":{
	  "tls":{"type":"object","properties":{"caFile":{"type":"string"}}}}}`
	after := `{"type":"object","properties":{
	  "tls":{"type":"object","properties":{"caFile":{"type":"string"},"serverName":{"type":"string"}},
	         "required":["serverName"]}}}`

	got := compat.Compare(desc(map[string]string{"source/get": before}), desc(map[string]string{"source/get": after}))
	if got.Class != compat.Breaking {
		t.Fatalf("nested required field = %q, want breaking\n%s", got.Class, got)
	}
	if !strings.Contains(got.String(), "tls.serverName") {
		t.Fatalf("the report does not use the nested path:\n%s", got)
	}
}

// An unparseable schema is not a licence to say "compatible". The gate cannot
// see the change, so it must not vouch for it.
func TestAnUncomparableSchemaIsNotWavedThrough(t *testing.T) {
	got := compat.Compare(
		desc(map[string]string{"source/get": `{"type":"string"}`}),
		desc(map[string]string{"source/get": `{"type":"number"}`}))
	if got.Class != compat.Behaviour {
		t.Fatalf("uncomparable schemas = %q, want behaviour-change\n%s", got.Class, got)
	}
	if !strings.Contains(got.String(), "by hand") {
		t.Fatalf("the report does not say a human must classify it:\n%s", got)
	}
}

// The worst change decides the class, and the report leads with it: a build
// that fails should read top-down from the thing that matters most.
func TestTheWorstChangeSetsTheClassAndLeadsTheReport(t *testing.T) {
	got := compat.Compare(
		desc(map[string]string{"source/get": twoFields}),
		desc(map[string]string{"source/get": `{"type":"object",
		  "properties":{"host":{"type":"string"}},"required":["host"]}`,
			"source/list": ""}))
	if got.Class != compat.Breaking {
		t.Fatalf("class = %q, want the worst change to decide\n%s", got.Class, got)
	}
	if len(got.Changes) != 2 || got.Changes[0].Class != compat.Breaking {
		t.Fatalf("report does not lead with the breaking change:\n%s", got)
	}
}

// An identical surface produces no changes at all — which is what lets the
// gate distinguish "nothing moved" from "moved compatibly", and refuse a
// class claimed over nothing.
func TestAnIdenticalSurfaceReportsNoChange(t *testing.T) {
	d := desc(map[string]string{"source/get": twoFields, "sink/put": ""})
	got := compat.Compare(d, d)
	if got.Changed() {
		t.Fatalf("an unchanged surface reported changes:\n%s", got)
	}
	if got.String() != "no surface change" {
		t.Fatalf("summary = %q", got.String())
	}
}

// Undeclared is weaker than compatible, deliberately — §6 shows the two
// separately because "nobody said" is not the same as "it is fine".
func TestDeclaringStrongerIsAlwaysAllowedAndUndeclaredIsWeakest(t *testing.T) {
	if !compat.AtLeast(compat.Breaking, compat.Compatible) {
		t.Error("a publisher may always declare something stronger than the diff requires")
	}
	if compat.AtLeast(compat.Compatible, compat.Breaking) {
		t.Error("compatible must not cover a breaking change")
	}
	if compat.AtLeast("", compat.Compatible) {
		t.Error("undeclared must not cover even a compatible change")
	}
	if !compat.AtLeast(compat.Behaviour, compat.Behaviour) {
		t.Error("a class covers itself")
	}
}

// The connection schema (ADR-0034) is compared on the same terms: it is where
// host and credentials live, so a break there takes out every node pointed at
// that system rather than one step.
func TestTheConnectionSchemaIsComparedToo(t *testing.T) {
	old := sdk.Descriptor{Name: "demo", ConnectionSchema: []byte(twoFields)}
	fresh := sdk.Descriptor{Name: "demo", ConnectionSchema: []byte(
		`{"type":"object","properties":{"host":{"type":"string"}},"required":["host"]}`)}

	got := compat.Compare(old, fresh)
	if got.Class != compat.Breaking {
		t.Fatalf("class = %q, want breaking\n%s", got.Class, got)
	}
	if got.Changes[0].Where != "connection" {
		t.Fatalf("change is located at %q, want connection", got.Changes[0].Where)
	}
}

// An enum is a contract, not decoration. Narrowing one makes every stored
// config that used the dropped value invalid, and the change is invisible to a
// field-level diff — no field is added, removed, or retyped — so without this
// the connector ships looking unchanged.
//
// This is a real escape: adding a format to a connector's enum passed the gate
// silently until the check existed.
func TestNarrowingAnEnumIsBreakingAndWideningIsNot(t *testing.T) {
	schema := func(vals string) []byte {
		return []byte(`{"type":"object","properties":{"format":{"type":"string","enum":` + vals + `}}}`)
	}

	widened := compat.Compare(
		descriptorWith(t, "get", "source", schema(`["ndjson","csv"]`)),
		descriptorWith(t, "get", "source", schema(`["ndjson","csv","xml"]`)),
	)
	if widened.Class != compat.Compatible {
		t.Errorf("widening an enum classified as %v, want compatible", widened.Class)
	}
	if !mentions(widened, `"xml"`) {
		t.Errorf("widening was not reported at all: %+v", widened)
	}

	narrowed := compat.Compare(
		descriptorWith(t, "get", "source", schema(`["ndjson","csv","xml"]`)),
		descriptorWith(t, "get", "source", schema(`["ndjson","csv"]`)),
	)
	if narrowed.Class != compat.Breaking {
		t.Errorf("narrowing an enum classified as %v, want breaking", narrowed.Class)
	}

	// Newly constraining a free-text field breaks anything already set to a
	// value outside the new set.
	constrained := compat.Compare(
		descriptorWith(t, "get", "source", []byte(`{"type":"object","properties":{"format":{"type":"string"}}}`)),
		descriptorWith(t, "get", "source", schema(`["ndjson"]`)),
	)
	if constrained.Class != compat.Breaking {
		t.Errorf("newly restricting a field to a fixed set classified as %v, want breaking", constrained.Class)
	}
}

// descriptorWith builds a one-action descriptor carrying a config schema.
func descriptorWith(t *testing.T, action, dir string, schema []byte) sdk.Descriptor {
	t.Helper()
	return sdk.Descriptor{
		Name: "demo", Version: "1.0.0",
		Actions: []sdk.ActionDescriptor{{Action: action, Direction: dir, ConfigSchema: schema}},
	}
}

func mentions(r compat.Report, want string) bool {
	for _, c := range r.Changes {
		if strings.Contains(c.Detail, want) {
			return true
		}
	}
	return false
}
