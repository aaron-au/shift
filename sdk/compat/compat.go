// Package compat turns connector backward compatibility from a discipline
// into a gate (ADR-0047 §8).
//
// ADR-0047 §6 has every connector version declare a compatibility class, and
// §5/§9 build real machinery on top of it: currency notices fold the class
// across a span, and a bulk upgrade shows an operator what crossing three
// releases will cost. All of that is only as good as the declaration — and a
// declaration made by a human at publish time, about a diff they wrote weeks
// earlier, is exactly the kind of promise that quietly rots.
//
// So the class is CHECKED. A connector records the action surface it shipped
// last release; the build compares the current surface against it, computes
// the smallest honest class for the difference, and fails if the connector
// declares something weaker. Declaring something STRONGER is always allowed —
// a publisher who knows a "compatible" change will surprise people can say
// breaking, and nothing here argues.
//
// What is compared is the PUBLIC shape: action names, their direction, and
// the config properties a flow author fills in. Not the implementation, and
// not behaviour — no static check can see that an HTTP source started
// following redirects. That is the honest boundary of this gate, and §6's
// `behaviour-change` class exists precisely for what it cannot see.
package compat

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/aaron-au/shift/sdk"
)

// Class is a compatibility class, ordered from harmless to hostile. The
// strings match ADR-0047 §6 and the hub's `compat=` values exactly, so a
// computed class can be handed to the registry without translation.
type Class string

const (
	// Compatible: a flow that ran before still runs, unchanged.
	Compatible Class = "compatible"
	// Behaviour: the same config still loads, but may do something different.
	Behaviour Class = "behaviour-change"
	// Breaking: config or output changed; the flow needs editing.
	Breaking Class = "breaking"
)

// rank orders the classes so "at least as strong as" is a comparison.
func rank(c Class) int {
	switch c {
	case Breaking:
		return 3
	case Behaviour:
		return 2
	case Compatible:
		return 1
	default:
		return 0 // undeclared is weaker than everything, including compatible
	}
}

// AtLeast reports whether declared covers computed.
//
// Undeclared is weaker than compatible, deliberately. §6 shows "undeclared"
// separately from "compatible" in every notice for the same reason: it does
// not mean the change is safe, it means nobody said.
func AtLeast(declared, computed Class) bool {
	return rank(declared) >= rank(computed)
}

// Change is one difference between two surfaces, with the class it forces.
type Change struct {
	Class  Class  `json:"class"`
	Where  string `json:"where"`  // "source/get", "connection", or "" for the connector
	Detail string `json:"detail"` // what changed, in the terms an author would use
}

func (c Change) String() string {
	if c.Where == "" {
		return string(c.Class) + ": " + c.Detail
	}
	return string(c.Class) + " in " + c.Where + ": " + c.Detail
}

// Report is the full diff and the smallest class that honestly covers it.
type Report struct {
	Class   Class    `json:"class"`
	Changes []Change `json:"changes"`
}

// Changed reports whether the surface moved at all.
func (r Report) Changed() bool { return len(r.Changes) > 0 }

// String renders the report the way a failing build should read it.
func (r Report) String() string {
	if !r.Changed() {
		return "no surface change"
	}
	lines := make([]string, 0, len(r.Changes))
	for _, c := range r.Changes {
		lines = append(lines, "  - "+c.String())
	}
	return string(r.Class) + " (" + strconv.Itoa(len(r.Changes)) + " change(s)):\n" + strings.Join(lines, "\n")
}

// Compare diffs two descriptors and reports the smallest honest class.
//
// Direction matters: `old` is what was released, `new` is what is about to be.
// Nothing here is symmetric — adding an optional field is compatible, removing
// one is breaking, and a diff engine that could not tell them apart would be
// useless for the thing this exists to do.
func Compare(old, latest sdk.Descriptor) Report {
	var r Report
	add := func(class Class, where, detail string) {
		r.Changes = append(r.Changes, Change{Class: class, Where: where, Detail: detail})
	}

	oldActions := indexActions(old)
	newActions := indexActions(latest)

	for key, o := range oldActions {
		n, ok := newActions[key]
		if !ok {
			// Direction is part of the key, so a source that became a sink
			// shows up here as a removal — which is what it is to a flow.
			// The step's role in the graph changes, and no edit to config
			// fixes that.
			add(Breaking, key, "action removed: every flow using it fails to resolve")
			continue
		}
		compareSchemas(add, key, o.ConfigSchema, n.ConfigSchema)
	}
	for key := range newActions {
		if _, ok := oldActions[key]; !ok {
			// Purely additive: nothing that ran before can notice.
			add(Compatible, key, "new action")
		}
	}

	compareSchemas(add, "connection", old.ConnectionSchema, latest.ConnectionSchema)

	// Stable output: worst first, then by location, so a failing build reads
	// top-down from the thing that matters most.
	sort.SliceStable(r.Changes, func(i, j int) bool {
		if r.Changes[i].Class != r.Changes[j].Class {
			return rank(r.Changes[i].Class) > rank(r.Changes[j].Class)
		}
		return r.Changes[i].Where < r.Changes[j].Where
	})
	for _, c := range r.Changes {
		if rank(c.Class) > rank(r.Class) {
			r.Class = c.Class
		}
	}
	return r
}

func indexActions(d sdk.Descriptor) map[string]sdk.ActionDescriptor {
	out := make(map[string]sdk.ActionDescriptor, len(d.Actions))
	for _, a := range d.Actions {
		out[a.Direction+"/"+a.Action] = a
	}
	return out
}

// compareSchemas diffs two config schemas for one location.
//
// Config schemas are STUDIO metadata: they render the builder's form
// (ADR-0018) and are never enforced by the runner or the hub. That is why
// adding one is compatible rather than breaking — no flow that ran before can
// start failing because a schema appeared. Removing one costs an author their
// form but not their flow, which is the shape of a behaviour change.
func compareSchemas(add func(Class, string, string), where string, oldRaw, newRaw json.RawMessage) {
	oldEmpty, newEmpty := len(oldRaw) == 0, len(newRaw) == 0
	switch {
	case oldEmpty && newEmpty:
		return
	case oldEmpty:
		add(Compatible, where, "config schema added (studio metadata; nothing enforces it at run time)")
		return
	case newEmpty:
		add(Behaviour, where, "config schema removed: the builder loses this action's form and falls back to raw JSON")
		return
	}

	oldFields, err1 := fieldsOf(oldRaw)
	newFields, err2 := fieldsOf(newRaw)
	if err1 != nil || err2 != nil {
		// An unparseable schema is not a licence to say "compatible". We
		// cannot see the change, so we cannot vouch for it.
		add(Behaviour, where, "config schema could not be compared (not an object schema); classify it by hand")
		return
	}

	for name, o := range oldFields {
		n, ok := newFields[name]
		if !ok {
			add(Breaking, where, fmt.Sprintf("config field %q removed: existing flows still set it", name))
			continue
		}
		if o.Type != n.Type && o.Type != "" && n.Type != "" {
			add(Breaking, where, fmt.Sprintf("config field %q changed type from %s to %s", name, o.Type, n.Type))
		}
		if !o.Required && n.Required {
			add(Breaking, where, fmt.Sprintf("config field %q is now required: flows that omit it stop working", name))
		}
		if o.Required && !n.Required {
			add(Compatible, where, fmt.Sprintf("config field %q is no longer required", name))
		}
		// An enum NARROWED is breaking and an enum WIDENED is compatible. This
		// is not a nicety: dropping a value from the list makes every stored
		// config that used it invalid, and without the check the connector
		// ships looking unchanged because no field was added or removed.
		for _, v := range o.Enum {
			if !slices.Contains(n.Enum, v) {
				add(Breaking, where, fmt.Sprintf("config field %q no longer accepts %q: flows set to it stop validating", name, v))
			}
		}
		for _, v := range n.Enum {
			if !slices.Contains(o.Enum, v) {
				add(Compatible, where, fmt.Sprintf("config field %q accepts a new value %q", name, v))
			}
		}
		if len(o.Enum) > 0 && len(n.Enum) == 0 {
			// From a closed set to anything at all. Compatible for existing
			// configs, but the studio loses its dropdown, so it is worth
			// surfacing rather than passing silently.
			add(Compatible, where, fmt.Sprintf("config field %q is no longer restricted to a fixed set of values", name))
		}
		if len(o.Enum) == 0 && len(n.Enum) > 0 {
			add(Breaking, where, fmt.Sprintf("config field %q is now restricted to a fixed set of values: flows setting anything else stop validating", name))
		}
		if o.Secret != n.Secret {
			// x-shift-secret drives the studio's secret picker AND signals
			// that a value must not be typed inline. Losing it is how a
			// credential ends up in a flow document in plain text.
			what := "no longer marked as a secret"
			class := Breaking
			if n.Secret {
				what, class = "is now marked as a secret", Compatible
			}
			add(class, where, fmt.Sprintf("config field %q %s", name, what))
		}
	}
	for name, n := range newFields {
		if _, ok := oldFields[name]; ok {
			continue
		}
		if n.Required {
			add(Breaking, where, fmt.Sprintf("new REQUIRED config field %q: every existing flow omits it", name))
		} else {
			add(Compatible, where, fmt.Sprintf("new optional config field %q", name))
		}
	}
}

// field is one config property, flattened to what a compatibility decision
// actually turns on.
type field struct {
	Type     string
	Required bool
	Secret   bool
	// Enum is the field's allowed values, if it constrains them. An enum is
	// part of the contract, not decoration: a value that used to validate and
	// no longer does breaks every flow that set it.
	Enum []string
}

// fieldsOf flattens an object schema to dotted paths. Nesting is walked
// because a required field three levels down breaks a flow exactly as hard as
// one at the top, and a diff that only read the first level would call that
// change compatible.
func fieldsOf(raw json.RawMessage) (map[string]field, error) {
	var s objectSchema
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	if s.Properties == nil {
		return nil, errors.New("compat: schema declares no properties")
	}
	out := map[string]field{}
	walk("", s, out)
	return out, nil
}

type objectSchema struct {
	Type       string                     `json:"type"`
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
	Secret     bool                       `json:"x-shift-secret"`
	Items      json.RawMessage            `json:"items"`
	Enum       []any                      `json:"enum"`
}

func walk(prefix string, s objectSchema, out map[string]field) {
	names := make([]string, 0, len(s.Properties))
	for name := range s.Properties {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		var child objectSchema
		if err := json.Unmarshal(s.Properties[name], &child); err != nil {
			continue
		}
		path := prefix + name
		out[path] = field{
			Type:     child.Type,
			Required: slices.Contains(s.Required, name),
			Secret:   child.Secret,
			Enum:     enumValues(child.Enum),
		}
		if child.Type == "object" && child.Properties != nil {
			walk(path+".", child, out)
		}
	}
}

// enumValues renders a schema enum as comparable strings. Non-string members
// (a numeric or boolean enum) are formatted rather than skipped: leaving them
// out would make narrowing such an enum invisible, which is the failure this
// comparison exists to catch.
func enumValues(vs []any) []string {
	if len(vs) == 0 {
		return nil
	}
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, fmt.Sprint(v))
	}
	slices.Sort(out)
	return out
}
