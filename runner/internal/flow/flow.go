// Package flow compiles flow documents (pkg/flowdoc — the declarative
// JSON shared with the hub) onto engine pipelines. The document model
// lives in flowdoc so the hub can validate at deploy time; only the
// runner compiles and executes.
package flow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
	// Step is one node in the flow graph (v2).
	Step = flowdoc.Step
	// Plan is a document's normalized, validated execution plan.
	Plan = flowdoc.Plan
	// ProjectField mirrors stream.ProjectField in document form.
	ProjectField = flowdoc.ProjectField
	// CoerceRule converts a top-level field to a kind.
	CoerceRule = flowdoc.CoerceRule
	// Agg is one aggregate output column.
	Agg = flowdoc.Agg
	// Route is one ordered branch of a v3 router node.
	Route = flowdoc.Route
	// JoinOn names a v3 merge/join's linked element (left/right paths).
	JoinOn = flowdoc.JoinOn
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

// Apply compiles the document's transform steps onto a pipeline (source
// and sink are bound by the caller, which owns connector processes). It
// lowers through the document's Plan, so both authoring forms compile the
// same way, and stamps each operator with its step id (the telemetry key
// and the OpError tag used for error routing).
func Apply(d *Document, p *stream.Pipeline, opts CompileOptions) (*stream.Pipeline, error) {
	plan, err := d.Plan()
	if err != nil {
		return nil, err
	}
	if plan.Multi {
		return nil, errors.New("flow: multi-path flows (fan-out/fan-in, ADR-0029) are not yet executable on this runner")
	}
	return applyTransforms(plan.Main, p, opts)
}

// ApplyOps folds a slice of transform steps onto p, stamping each operator
// with its step id (telemetry + OpError routing key). Unlike Apply, the steps
// are exactly the operators to apply — no source/sink endpoints — so the
// multi-path compiler can build the linear segments of a v3 DAG (ADR-0029).
func ApplyOps(steps []*flowdoc.Step, p *stream.Pipeline, opts CompileOptions) (*stream.Pipeline, error) {
	for _, s := range steps {
		np, err := applyOp(&s.Op, p, opts)
		if err != nil {
			return nil, fmt.Errorf("flow: step %q: %w", s.ID, err)
		}
		p = np.RenameLastOp(s.ID)
	}
	return p, nil
}

// ApplyOpsFold is ApplyOps for callers that cannot return a build error (a
// fan-out branch builder, whose signature yields only a *Pipeline): any error
// is folded into the pipeline and surfaced at Run.
func ApplyOpsFold(steps []*flowdoc.Step, p *stream.Pipeline, opts CompileOptions) *stream.Pipeline {
	out, err := ApplyOps(steps, p, opts)
	if err != nil {
		return p.Fail(err)
	}
	return out
}

// CompilePredicate compiles a filter-grammar predicate (path/op/value) into a
// record test — used to compile router route predicates (ADR-0029). The path
// and value were already validated by flowdoc.
func CompilePredicate(path, cmp string, value json.RawMessage) (func(record.Value) bool, error) {
	return compileFilter(&Op{Path: path, Cmp: cmp, Value: value})
}

// applyTransforms folds the middle (non-endpoint) steps of a plan onto p.
func applyTransforms(main []*flowdoc.Step, p *stream.Pipeline, opts CompileOptions) (*stream.Pipeline, error) {
	if len(main) < 2 {
		return p, nil
	}
	for _, s := range main[1 : len(main)-1] {
		np, err := applyOp(&s.Op, p, opts)
		if err != nil {
			return nil, fmt.Errorf("flow: step %q: %w", s.ID, err)
		}
		p = np.RenameLastOp(s.ID)
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
	case "map":
		fields, err := compileMap(o.Maps)
		if err != nil {
			return nil, err
		}
		return p.Map(fields), nil
	default:
		return nil, fmt.Errorf("unknown op type %q", o.Type)
	}
}

// compileMap builds the engine mapper fields from the document form, parsing
// paths and constants (all already validated by flowdoc).
func compileMap(maps []flowdoc.MapField) ([]stream.MapField, error) {
	out := make([]stream.MapField, len(maps))
	for i := range maps {
		m := &maps[i]
		mf := stream.MapField{Out: flowdoc.MapOutSegments(m.Out)}
		switch {
		case len(m.Concat) > 0:
			for _, part := range m.Concat {
				if strings.HasPrefix(part, "$") {
					mf.Concat = append(mf.Concat, stream.MapPart{Path: record.MustParsePath(part), IsPath: true})
				} else {
					mf.Concat = append(mf.Concat, stream.MapPart{Lit: part})
				}
			}
		case m.From != "":
			mf.From, mf.FromSet = record.MustParsePath(m.From), true
		case len(m.Const) > 0:
			v, err := flowdoc.ScalarValue(m.Const)
			if err != nil {
				return nil, err
			}
			mf.Const, mf.ConstSet = v, true
		}
		if len(m.Default) > 0 {
			v, err := flowdoc.ScalarValue(m.Default)
			if err != nil {
				return nil, err
			}
			mf.Default, mf.DefaultSet = v, true
		}
		if m.To != "" {
			k, err := kindOf(m.To)
			if err != nil {
				return nil, err
			}
			mf.To, mf.ToSet = k, true
		}
		out[i] = mf
	}
	return out, nil
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
