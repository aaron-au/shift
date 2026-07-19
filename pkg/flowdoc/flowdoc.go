// Package flowdoc defines the flow document — the declarative JSON that
// describes an integration. It is shared by the hub (which validates and
// stores documents; it never touches payload data) and the runner (which
// compiles them onto engine pipelines, see runner/internal/flow).
// Documents are deliberately plain data (developer- and AI-friendly:
// no DSL, no code), validated eagerly at deploy/submit time.
package flowdoc

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/aaron-au/shift/engine/record"
)

// Document is one executable flow definition. Two authoring forms lower
// to the same execution plan (see graph.go, Document.Plan):
//
//   - linear form (v1 sugar): Source + Ops + Sink — the ergonomic,
//     AI-friendly shape for a straight-through pipeline. Kept unchanged.
//   - graph form (v2): Steps + Start — nodes with typed outcome edges
//     (onSuccess / onComplete happy path, onFailure error handler). The
//     two forms are mutually exclusive within one document.
type Document struct {
	Name string `json:"name"`

	// Linear form.
	Source Endpoint `json:"source,omitzero"`
	Ops    []Op     `json:"ops,omitempty"`
	Sink   Endpoint `json:"sink,omitzero"`

	// Graph form.
	Steps []Step `json:"steps,omitempty"`
	Start string `json:"start,omitempty"` // entry step id ("" = the sole source step)
}

// Step is one node in the flow graph (v2). Every node — connector or
// transform — is a step. A Step embeds Op, so the transform types reuse
// the exact same fields and validation as the linear form and compile
// through the same applyOp path on the runner.
//
// Type namespace:
//   - connector: source | sink   (Connector/Action/Config apply)
//   - transform: filter | project | coerce | flatten | aggregate (Op fields)
//   - reserved:  wasm | python | subflow — parsed but rejected until M5b.
type Step struct {
	ID string `json:"id"`
	Op        // promotes Type + the transform option fields

	// Connector steps (source|sink).
	Connector string          `json:"connector,omitempty"`
	Action    string          `json:"action,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`

	// Outcome edges (step ids). A non-terminal step has exactly one happy
	// edge (OnSuccess XOR OnComplete); the two are structurally identical
	// (both name the next step on the happy path) — the distinction is
	// authoring intent. OnFailure names an error-handler step.
	OnSuccess  string `json:"onSuccess,omitempty"`
	OnComplete string `json:"onComplete,omitempty"`
	OnFailure  string `json:"onFailure,omitempty"`
}

// Endpoint views a connector step (source|sink) as an Endpoint, so the
// runner can bind it exactly like a linear-form endpoint.
func (s *Step) Endpoint() Endpoint {
	return Endpoint{Connector: s.Connector, Action: s.Action, Config: s.Config}
}

// happyEdge returns the step's single happy-path successor id (OnSuccess
// or OnComplete) and whether one is set.
func (s *Step) happyEdge() (string, bool) {
	if s.OnSuccess != "" {
		return s.OnSuccess, true
	}
	if s.OnComplete != "" {
		return s.OnComplete, true
	}
	return "", false
}

func isConnectorType(t string) bool { return t == "source" || t == "sink" }

func isTransformType(t string) bool {
	switch t {
	case "filter", "project", "coerce", "flatten", "aggregate":
		return true
	}
	return false
}

func isReservedType(t string) bool {
	switch t {
	case "wasm", "python", "subflow":
		return true
	}
	return false
}

// Endpoint names a connector action plus its opaque config document.
type Endpoint struct {
	Connector string          `json:"connector"`
	Action    string          `json:"action"`
	Config    json.RawMessage `json:"config,omitempty"`
}

// Op is one transform step. Type selects which of the option blocks apply.
type Op struct {
	Type string `json:"type"` // filter | project | coerce | flatten | aggregate

	// filter
	Path  string          `json:"path,omitempty"`
	Cmp   string          `json:"op,omitempty"` // eq | ne | gt | gte | lt | lte | exists
	Value json.RawMessage `json:"value,omitempty"`

	// project
	Fields []ProjectField `json:"fields,omitempty"`

	// coerce
	Rules []CoerceRule `json:"rules,omitempty"`

	// flatten
	Sep string `json:"sep,omitempty"`

	// aggregate
	Key  string `json:"key,omitempty"`
	Aggs []Agg  `json:"aggs,omitempty"`
}

// ProjectField mirrors stream.ProjectField in document form.
type ProjectField struct {
	Out  string `json:"out,omitempty"`
	Path string `json:"path"`
}

// CoerceRule converts a top-level field to a kind (int|float|bool|string).
type CoerceRule struct {
	Field string `json:"field"`
	To    string `json:"to"`
}

// Agg is one aggregate output column.
type Agg struct {
	Op   string `json:"op"` // count | sum | min | max
	Path string `json:"path,omitempty"`
	Out  string `json:"out"`
}

// CoerceKinds are the legal CoerceRule.To names.
var CoerceKinds = map[string]bool{"int": true, "float": true, "bool": true, "string": true}

// Parse decodes and validates a flow document.
func Parse(data []byte) (*Document, error) {
	var d Document
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("flow: invalid JSON: %w", err)
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// Validate checks the document without touching connectors. It routes to
// the graph validator when the document is in v2 (Steps) form, and keeps
// the linear-form checks otherwise.
func (d *Document) Validate() error {
	if d.Name == "" {
		return fmt.Errorf("flow: name is required")
	}
	if len(d.Steps) > 0 {
		if d.Source.Connector != "" || len(d.Ops) > 0 || d.Sink.Connector != "" {
			return fmt.Errorf("flow: use either the linear form (source/ops/sink) or the graph form (steps), not both")
		}
		_, err := d.buildPlan()
		return err
	}
	for label, ep := range map[string]Endpoint{"source": d.Source, "sink": d.Sink} {
		if ep.Connector == "" || ep.Action == "" {
			return fmt.Errorf("flow: %s needs connector and action", label)
		}
	}
	for i, op := range d.Ops {
		if err := op.validate(); err != nil {
			return fmt.Errorf("flow: op %d: %w", i, err)
		}
	}
	return nil
}

func (o *Op) validate() error {
	switch o.Type {
	case "filter":
		if _, err := record.ParsePath(o.Path); err != nil {
			return err
		}
		switch o.Cmp {
		case "eq", "ne", "gt", "gte", "lt", "lte":
			if len(o.Value) == 0 {
				return fmt.Errorf("filter %s needs a value", o.Cmp)
			}
			if _, err := ScalarValue(o.Value); err != nil {
				return err
			}
		case "exists":
		default:
			return fmt.Errorf("unknown filter op %q", o.Cmp)
		}
	case "project":
		if len(o.Fields) == 0 {
			return fmt.Errorf("project needs fields")
		}
		for _, f := range o.Fields {
			p, err := record.ParsePath(f.Path)
			if err != nil {
				return err
			}
			if f.Out == "" && p.LeafName() == "" {
				return fmt.Errorf("project field %s needs an out name", f.Path)
			}
		}
	case "coerce":
		if len(o.Rules) == 0 {
			return fmt.Errorf("coerce needs rules")
		}
		for _, r := range o.Rules {
			if !CoerceKinds[r.To] {
				return fmt.Errorf("unknown coerce kind %q", r.To)
			}
			if r.Field == "" {
				return fmt.Errorf("coerce rule needs field")
			}
		}
	case "flatten":
		if o.Sep == "" {
			return fmt.Errorf("flatten needs sep")
		}
	case "aggregate":
		if _, err := record.ParsePath(o.Key); err != nil {
			return err
		}
		if len(o.Aggs) == 0 {
			return fmt.Errorf("aggregate needs aggs")
		}
		for _, a := range o.Aggs {
			switch a.Op {
			case "count":
			case "sum", "min", "max":
				if _, err := record.ParsePath(a.Path); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown agg op %q", a.Op)
			}
			if a.Out == "" {
				return fmt.Errorf("agg needs out name")
			}
		}
	default:
		return fmt.Errorf("unknown op type %q", o.Type)
	}
	return nil
}

// ScalarValue converts a JSON scalar into a record scalar for comparison.
func ScalarValue(raw json.RawMessage) (record.Value, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return record.Value{}, fmt.Errorf("bad filter value: %w", err)
	}
	switch x := v.(type) {
	case nil:
		return record.Null(), nil
	case bool:
		return record.Bool(x), nil
	case float64:
		if x == float64(int64(x)) {
			return record.Int(int64(x)), nil
		}
		return record.Float(x), nil
	case string:
		return record.UnsafeString([]byte(x)), nil // retained: backed by the caller's compiled closure
	default:
		return record.Value{}, fmt.Errorf("filter value must be a scalar, got %T", v)
	}
}

// WithSinkConfig returns a copy of the document whose sink config has the
// given extra fields merged in (used by the runner to inject the task
// idempotency key before execution).
func (d *Document) WithSinkConfig(extra map[string]any) (*Document, error) {
	merged := map[string]any{}
	if len(d.Sink.Config) > 0 {
		if err := json.Unmarshal(d.Sink.Config, &merged); err != nil {
			return nil, fmt.Errorf("flow: sink config: %w", err)
		}
	}
	maps.Copy(merged, extra)
	raw, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}
	out := *d
	out.Sink.Config = raw
	return &out, nil
}
