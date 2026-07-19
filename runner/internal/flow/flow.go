// Package flow defines the flow document — the declarative JSON that
// describes an integration — and compiles it onto an engine pipeline.
// Documents are deliberately plain data (developer- and AI-friendly:
// no DSL, no code), validated eagerly at submit time.
package flow

import (
	"encoding/json"
	"fmt"

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
)

// Document is one executable flow definition (v1: linear source → ops →
// sink; DAG shapes arrive in M5).
type Document struct {
	Name   string   `json:"name"`
	Source Endpoint `json:"source"`
	Ops    []Op     `json:"ops,omitempty"`
	Sink   Endpoint `json:"sink"`
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

// Validate checks the document without touching connectors.
func (d *Document) Validate() error {
	if d.Name == "" {
		return fmt.Errorf("flow: name is required")
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
			if _, err := scalarValue(o.Value); err != nil {
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
			if _, err := kindOf(r.To); err != nil {
				return err
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

// CompileOptions supply per-task execution resources.
type CompileOptions struct {
	// Gov bounds stateful operator memory for this task (spill beyond).
	Gov *mem.Governor
	// SpillDir hosts scratch ("" = OS temp).
	SpillDir string
}

// Apply compiles the document's ops onto a pipeline (source and sink are
// bound by the caller, which owns connector processes).
func (d *Document) Apply(p *stream.Pipeline, opts CompileOptions) (*stream.Pipeline, error) {
	for i, op := range d.Ops {
		var err error
		p, err = op.apply(p, opts)
		if err != nil {
			return nil, fmt.Errorf("flow: op %d: %w", i, err)
		}
	}
	return p, nil
}

func (o *Op) apply(p *stream.Pipeline, opts CompileOptions) (*stream.Pipeline, error) {
	switch o.Type {
	case "filter":
		pred, err := compileFilter(o)
		if err != nil {
			return nil, err
		}
		return p.Filter("filter:"+o.Cmp, pred), nil
	case "project":
		fields := make([]stream.ProjectField, len(o.Fields))
		for i, f := range o.Fields {
			fields[i] = stream.ProjectField{Out: f.Out, From: record.MustParsePath(f.Path)}
		}
		return p.Project(fields...), nil
	case "coerce":
		rules := make([]stream.CoerceRule, len(o.Rules))
		for i, r := range o.Rules {
			k, err := kindOf(r.To)
			if err != nil {
				return nil, err
			}
			rules[i] = stream.CoerceRule{Field: r.Field, To: k}
		}
		return p.Coerce(rules...), nil
	case "flatten":
		return p.Flatten(o.Sep), nil
	case "aggregate":
		aggs := make([]stream.Agg, len(o.Aggs))
		for i, a := range o.Aggs {
			spec := stream.Agg{Out: a.Out}
			switch a.Op {
			case "count":
				spec.Op = stream.AggCount
			case "sum":
				spec.Op = stream.AggSum
			case "min":
				spec.Op = stream.AggMin
			case "max":
				spec.Op = stream.AggMax
			}
			if a.Path != "" {
				spec.From = record.MustParsePath(a.Path)
			}
			aggs[i] = spec
		}
		return p.Aggregate(stream.AggregateSpec{
			Key:      record.MustParsePath(o.Key),
			Aggs:     aggs,
			Gov:      opts.Gov,
			SpillDir: opts.SpillDir,
		}), nil
	default:
		return nil, fmt.Errorf("unknown op type %q", o.Type)
	}
}

func compileFilter(o *Op) (func(record.Value) bool, error) {
	path := record.MustParsePath(o.Path)
	if o.Cmp == "exists" {
		return func(v record.Value) bool {
			got, ok := path.Get(v)
			return ok && !got.IsNull()
		}, nil
	}
	want, err := scalarValue(o.Value)
	if err != nil {
		return nil, err
	}
	cmp := o.Cmp
	return func(v record.Value) bool {
		got, ok := path.Get(v)
		if !ok {
			return false
		}
		switch cmp {
		case "eq":
			return got.EqualScalar(want)
		case "ne":
			return !got.EqualScalar(want)
		default: // ordered comparisons: numeric only
			if !isNumeric(got) || !isNumeric(want) {
				return false
			}
			g, w := got.Float(), want.Float()
			switch cmp {
			case "gt":
				return g > w
			case "gte":
				return g >= w
			case "lt":
				return g < w
			case "lte":
				return g <= w
			}
			return false
		}
	}, nil
}

func isNumeric(v record.Value) bool {
	return v.Kind() == record.KindInt || v.Kind() == record.KindFloat
}

// scalarValue converts a JSON scalar into a record scalar for comparison.
func scalarValue(raw json.RawMessage) (record.Value, error) {
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
		return record.UnsafeString([]byte(x)), nil // retained: backed by this compiled filter's closure
	default:
		return record.Value{}, fmt.Errorf("filter value must be a scalar, got %T", v)
	}
}

func kindOf(name string) (record.Kind, error) {
	switch name {
	case "int":
		return record.KindInt, nil
	case "float":
		return record.KindFloat, nil
	case "bool":
		return record.KindBool, nil
	case "string":
		return record.KindString, nil
	default:
		return 0, fmt.Errorf("unknown coerce kind %q", name)
	}
}
