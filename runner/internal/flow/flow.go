// Package flow compiles flow documents (pkg/flowdoc — the declarative
// JSON shared with the hub) onto engine pipelines. The document model
// lives in flowdoc so the hub can validate at deploy time; only the
// runner compiles and executes.
package flow

import (
	"fmt"

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Aliases so runner code has one import for the document model.
type (
	// Document is one executable flow definition.
	Document = flowdoc.Document
	// Endpoint names a connector action plus its config.
	Endpoint = flowdoc.Endpoint
	// Op is one transform step.
	Op = flowdoc.Op
	// ProjectField mirrors stream.ProjectField in document form.
	ProjectField = flowdoc.ProjectField
	// CoerceRule converts a top-level field to a kind.
	CoerceRule = flowdoc.CoerceRule
	// Agg is one aggregate output column.
	Agg = flowdoc.Agg
)

// Parse decodes and validates a flow document.
func Parse(data []byte) (*Document, error) { return flowdoc.Parse(data) }

// CompileOptions supply per-task execution resources.
type CompileOptions struct {
	// Gov bounds stateful operator memory for this task (spill beyond).
	Gov *mem.Governor
	// SpillDir hosts scratch ("" = OS temp).
	SpillDir string
}

// Apply compiles the document's ops onto a pipeline (source and sink are
// bound by the caller, which owns connector processes).
func Apply(d *Document, p *stream.Pipeline, opts CompileOptions) (*stream.Pipeline, error) {
	for i := range d.Ops {
		var err error
		p, err = applyOp(&d.Ops[i], p, opts)
		if err != nil {
			return nil, fmt.Errorf("flow: op %d: %w", i, err)
		}
	}
	return p, nil
}

func applyOp(o *Op, p *stream.Pipeline, opts CompileOptions) (*stream.Pipeline, error) {
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
	want, err := flowdoc.ScalarValue(o.Value)
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
