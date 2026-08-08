package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/flow"
)

// The general v3 DAG compiler (ADR-0029 §2, issue #59).
//
// executeMulti used to case-match on node counts: exactly one fan-out and no
// merge, or exactly one merge and no fan-out. Anything else — a second tee, or
// a tee and a merge in the same graph — was rejected at run time even though
// the hub validated it and the studio drew it. `source → tee → enrich → merge`
// is the ENRICHMENT shape, arguably the most common real integration, and the
// gap between what the canvas accepts and what runs was the defect.
//
// This compiles the whole adjacency instead, which is what ADR-0029 §2
// describes: a v3 plan is a set of linear SEGMENTS joined at tee/router/merge
// nodes. The engine primitives already supported it; only the dispatcher was
// specialised.
//
// The shape of the compilation:
//
//   - streamInto(node) returns the record stream ARRIVING at a node, built
//     recursively from its predecessors. The plan is a validated DAG, so the
//     recursion terminates.
//   - Every fan-out node becomes one driver (RunTee/RunRouter) running in its
//     own goroutine. A branch that runs straight to a sink keeps its real Sink
//     — that is the existing, common case and it must not get slower. A branch
//     that feeds a merge or another fan-out ends at a stream.Pipe instead, and
//     the node downstream reads the pipe's Source.
//   - Every sink not already consumed as a direct fan-out branch becomes its
//     own driver goroutine.
//
// Concurrency is per stage, bounded by each pipe's queue depth — flow control
// inside one task, never a gate between tasks (ADR-0005).

// stageResult is one stage's outcome. A @stop is carried here rather than as
// an error because the fan-out executor already RESOLVED it: only that
// executor can tell its own teardown cancel from the caller's, so it reports
// a deliberate stop with a nil error (ADR-0031 §3). Re-deriving it downstream
// would risk promoting a racing client timeout into a genuine cancellation
// and failing a task the author deliberately ended.
type stageResult struct {
	rep      stream.Report
	stopped  bool
	stopStep string
}

// dagRun is the compilation and execution state for one multi-path task.
type dagRun struct {
	svc     *Service
	plan    *flowdoc.Plan
	opts    flow.CompileOptions
	sampler *captureSampler
	sub     SubmitOpts

	preds map[string][]string // node id → predecessors, derived from plan.Data

	// arriving memoises the stream reaching a node, so a node consumed by two
	// downstream readers is compiled once. Fan-out nodes are absent: they have
	// N outputs, and each successor edge gets its own pipe in edgeIn.
	arriving map[string]stream.Source
	edgeIn   map[string]stream.Source // "from→to" → the pipe feeding that edge
	builtFO  map[string]bool          // fan-out nodes whose driver is built

	stages     []func(context.Context) (stageResult, error)
	confirmers []func() int64
	cleanups   []func()

	// consumed marks sinks driven by a fan-out branch, so they are not also
	// given their own stage.
	consumed map[string]bool
}

// executeDAG compiles and runs any validated v3 topology.
func (s *Service) executeDAG(ctx context.Context, doc *flow.Document, plan *flowdoc.Plan, redact func(string) string, sampler *captureSampler, o SubmitOpts) (execResult, error) {
	r := &dagRun{
		svc:     s,
		plan:    plan,
		opts:    flow.CompileOptions{Gov: mem.New(s.opts.TaskWatermark), SpillDir: s.opts.SpillDir, Test: o.Test},
		sampler: sampler,
		sub:     o,

		preds:    predecessors(plan),
		arriving: map[string]stream.Source{},
		edgeIn:   map[string]stream.Source{},
		builtFO:  map[string]bool{},
		consumed: map[string]bool{},
	}
	defer func() {
		for _, c := range r.cleanups {
			c()
		}
	}()

	if err := r.compile(); err != nil {
		return execResult{}, err
	}
	res, runErr := r.run(ctx)
	if sampler != nil {
		res.captured = sampler.result()
	}
	return s.routeMultiError(ctx, plan, doc, redact, res, runErr)
}

// compile walks the plan and builds every stage.
func (r *dagRun) compile() error {
	// Fan-out drivers first. Building them registers the pipes their branches
	// feed, which the sink stages below then read — and it marks the sinks a
	// branch drives directly, so those do not get a second stage.
	for id, n := range r.plan.Nodes {
		if isFanOut(r.plan, n, id) {
			if err := r.buildFanOut(id); err != nil {
				return err
			}
		}
	}
	// Then every remaining sink.
	for id, n := range r.plan.Nodes {
		if n.Type != "sink" || r.consumed[id] || !r.isDataNode(id) {
			continue
		}
		if err := r.buildSinkStage(id); err != nil {
			return err
		}
	}
	if len(r.stages) == 0 {
		return errors.New("service: flow has no runnable terminal")
	}
	return nil
}

// isDataNode reports whether a node is on the data path rather than an error
// handler. Handlers are run by routeMultiError on failure, never as a stage.
func (r *dagRun) isDataNode(id string) bool {
	_, ok := r.plan.Nodes[id]
	if !ok {
		return false
	}
	// A handler sink is unreachable from any data edge: it has no data
	// predecessor and is not a source.
	if len(r.preds[id]) > 0 {
		return true
	}
	for _, s := range r.plan.Sources {
		if s == id {
			return true
		}
	}
	return false
}

// buildFanOut compiles one tee/router node into a driver stage.
func (r *dagRun) buildFanOut(id string) error {
	if r.builtFO[id] {
		return nil
	}
	r.builtFO[id] = true

	fo := r.plan.Nodes[id]
	succ := r.plan.Data[id]
	isRouter := fo.Type == "router"

	branches := make([]stream.Branch, 0, len(succ))
	for _, sid := range succ {
		ops, endID, err := segmentFrom(r.plan, sid)
		if err != nil {
			return err
		}
		end := r.plan.Nodes[endID]
		if end == nil {
			return fmt.Errorf("service: unknown node %q", endID)
		}

		var sink stream.Sink
		if end.Type == "sink" {
			// The common case, and the fast one: straight to a real sink, no
			// pipe and no extra copy.
			s, confirmed, cleanup, err := r.svc.bindSink(end, r.sub)
			if err != nil {
				return err
			}
			r.cleanups = append(r.cleanups, cleanup)
			r.confirmers = append(r.confirmers, confirmed)
			r.consumed[endID] = true
			sink = s
		} else {
			// The branch feeds a merge or another fan-out: it ends at a pipe,
			// and the node downstream reads the other half.
			pipe := stream.NewPipe(0)
			sink = pipe.Sink()
			last := sid
			if len(ops) > 0 {
				last = ops[len(ops)-1].ID
			}
			r.edgeIn[edgeKey(last, endID)] = pipe.Source()
		}

		name := endID
		segOps := ops
		branches = append(branches, stream.Branch{
			Name: name,
			Sink: sink,
			// Shared (no copy) is safe for a router branch (it owns its
			// partition batch) and for an op-less branch straight to a sink.
			// A branch with ops, or one whose sink is a pipe (which copies
			// anyway), leaves it false.
			Shared: isRouter || (len(segOps) == 0 && end.Type == "sink"),
			Build: func(bs stream.Source) *stream.Pipeline {
				// Each branch samples too: on a fan-out, "which branch did
				// this record take" is unanswerable from the upstream sample
				// alone (ADR-0014).
				return flow.ApplyOpsFold(segOps, sampled(stream.New(bs, name), r.sampler), r.opts)
			},
		})
	}

	upstream, err := r.streamInto(id)
	if err != nil {
		return err
	}
	var match func(record.Value) int
	if isRouter {
		match, err = compileRouter(fo, succ)
		if err != nil {
			return err
		}
	}
	r.stages = append(r.stages, func(ctx context.Context) (stageResult, error) {
		var frep stream.FanoutReport
		var runErr error
		if isRouter {
			frep, runErr = stream.RunRouter(ctx, upstream, branches, match, stream.FanoutOptions{})
		} else {
			frep, runErr = stream.RunTee(ctx, upstream, branches, stream.FanoutOptions{})
		}
		out := stageResult{stopped: frep.Stopped, stopStep: frep.StopStep}
		for _, br := range frep.Branches {
			out.rep.Ops = append(out.rep.Ops, br.Ops...)
			out.rep.RecordsOut += br.RecordsOut
		}
		return out, runErr
	})
	return nil
}

// buildSinkStage compiles the segment terminating at a sink into a stage.
//
// The segment is built here rather than via streamInto so the pipeline has
// exactly one measured origin stage: wrapping an already-built source in a
// second stream.New would put a duplicate entry in every Ops report.
func (r *dagRun) buildSinkStage(sinkID string) error {
	origin, originID, ops, err := r.segmentTo(sinkID)
	if err != nil {
		return err
	}
	sink, confirmed, cleanup, err := r.svc.bindSink(r.plan.Nodes[sinkID], r.sub)
	if err != nil {
		return err
	}
	r.cleanups = append(r.cleanups, cleanup)
	r.confirmers = append(r.confirmers, confirmed)

	p, err := flow.ApplyOps(ops, sampled(stream.New(origin, originID), r.sampler), r.opts)
	if err != nil {
		return err
	}
	r.stages = append(r.stages, func(ctx context.Context) (stageResult, error) {
		rep, err := p.Run(ctx, sink, sinkID)
		return stageResult{rep: rep}, err
	})
	return nil
}

// run executes every stage concurrently and folds the results.
//
// Stages are goroutines because they are genuinely concurrent producers and
// consumers joined by bounded pipes: a merge cannot read until the tee branch
// upstream of it is running, and running them in sequence would deadlock on
// the first full queue. Cancellation on first error is what stops the rest
// promptly rather than leaving them to drain into nothing.
func (r *dagRun) run(ctx context.Context) (execResult, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	reps := make([]stageResult, len(r.stages))
	errs := make([]error, len(r.stages))
	var wg sync.WaitGroup
	for i, stage := range r.stages {
		wg.Go(func() {
			rep, err := stage(runCtx)
			reps[i], errs[i] = rep, err
			if err != nil {
				cancel() // a failed stage tears the topology down
			}
		})
	}
	wg.Wait()

	res := execResult{}
	for _, sr := range reps {
		res.rep.Ops = append(res.rep.Ops, sr.rep.Ops...)
		res.rep.RecordsOut += sr.rep.RecordsOut
		if sr.stopped {
			res.stopped, res.stopStep = true, sr.stopStep
		}
	}
	for _, c := range r.confirmers {
		res.confirmed += c()
	}
	err := firstError(errs)
	// A deliberate stop cancels the run context to tear the rest down, so the
	// other stages report cancellation. That is the teardown, not the outcome:
	// the stop already resolved this execution to a success (ADR-0031 §3).
	if res.stopped && (errors.Is(err, context.Canceled) || errors.Is(err, stream.ErrPipeClosed)) {
		err = nil
	}
	return res, err
}

// firstError picks the error to report from a set of concurrent stages.
//
// A stage that failed on its own is always preferred over one that merely saw
// the teardown: cancelling the run context is how the topology stops, so
// every other stage reports context.Canceled, and reporting THAT would name
// the wrong cause (ADR-0031 §1 — one canonical error per execution).
func firstError(errs []error) error {
	var fallback error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, stream.ErrPipeClosed) {
			if fallback == nil {
				fallback = err
			}
			continue
		}
		return err
	}
	return fallback
}

// streamInto builds the record stream ARRIVING at a node, memoised.
func (r *dagRun) streamInto(id string) (stream.Source, error) {
	if src, ok := r.arriving[id]; ok {
		return src, nil
	}
	n := r.plan.Nodes[id]
	if n == nil {
		return nil, fmt.Errorf("service: unknown node %q", id)
	}

	var src stream.Source
	var err error
	switch {
	case n.Type == "merge":
		src, err = r.buildMerge(n)
	case len(r.preds[id]) == 0:
		return nil, fmt.Errorf("service: node %q has no input", id)
	default:
		src, err = r.streamFromSingle(id)
	}
	if err != nil {
		return nil, err
	}
	r.arriving[id] = src
	return src, nil
}

// streamFromSingle builds the stream arriving at a node with one predecessor
// chain — walking back to the segment's origin (a source, a pipe from a
// fan-out branch, or a merge) and applying the transforms between.
func (r *dagRun) streamFromSingle(id string) (stream.Source, error) {
	origin, originID, ops, err := r.segmentTo(id)
	if err != nil {
		return nil, err
	}
	p, err := flow.ApplyOps(ops, sampled(stream.New(origin, originID), r.sampler), r.opts)
	if err != nil {
		return nil, err
	}
	return p.AsSource()
}

// segmentTo walks back from a node to the start of its linear segment,
// returning the origin stream, the ORIGIN NODE ID, and the transforms between
// it and id (exclusive of id itself).
//
// The origin id is what names the segment's first stage in reports, OpErrors
// and capture samples. It has to be the origin rather than the destination:
// telemetry keys on step ids (ADR-0013), and "what did the source emit"
// is a question about the source, so labelling the sample with the node the
// segment happens to lead to would file it under the wrong step.
func (r *dagRun) segmentTo(id string) (stream.Source, string, []*flowdoc.Step, error) {
	var chain []*flowdoc.Step
	cur := id
	for {
		ps := r.preds[cur]
		if len(ps) != 1 {
			return nil, "", nil, fmt.Errorf("service: node %q has %d inputs; only a merge may have more than one", cur, len(ps))
		}
		p := ps[0]
		// A fan-out predecessor means this edge is fed by a pipe.
		if pn := r.plan.Nodes[p]; pn != nil && isFanOut(r.plan, pn, p) {
			if err := r.buildFanOut(p); err != nil {
				return nil, "", nil, err
			}
			src, ok := r.edgeIn[edgeKey(cur, id)]
			if !ok {
				// The branch ran straight to a sink, so there is no pipe: the
				// caller is asking for a stream that is already consumed.
				return nil, "", nil, fmt.Errorf("service: node %q reads a fan-out branch that terminates elsewhere", id)
			}
			return src, cur, reverse(chain), nil
		}
		pn := r.plan.Nodes[p]
		if pn == nil {
			return nil, "", nil, fmt.Errorf("service: unknown node %q", p)
		}
		if isTransformStep(pn) {
			chain = append(chain, pn)
			cur = p
			continue
		}
		// A source, or a merge: the segment's origin.
		src, err := r.originOf(pn)
		if err != nil {
			return nil, "", nil, err
		}
		return src, pn.ID, reverse(chain), nil
	}
}

// originOf resolves a segment origin: a bound source, or a merge's output.
func (r *dagRun) originOf(n *flowdoc.Step) (stream.Source, error) {
	if n.Type == "merge" {
		return r.streamInto(n.ID)
	}
	if n.Type != "source" {
		return nil, fmt.Errorf("service: node %q (%s) cannot start a segment", n.ID, n.Type)
	}
	src, cleanup, err := r.svc.bindSource(n, r.sub)
	if err != nil {
		return nil, err
	}
	r.cleanups = append(r.cleanups, cleanup)
	return src, nil
}

// buildMerge assembles a fan-in node's output from its inputs.
func (r *dagRun) buildMerge(mg *flowdoc.Step) (stream.Source, error) {
	ps := r.preds[mg.ID]
	if len(ps) < 2 {
		return nil, fmt.Errorf("service: merge %q needs at least 2 inputs (found %d)", mg.ID, len(ps))
	}
	type input struct {
		producerID string
		src        stream.Source
	}
	inputs := make([]input, 0, len(ps))
	for _, p := range ps {
		src, err := r.inputEdge(p, mg.ID)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, input{p, src})
	}

	switch mg.Mode {
	case flowdoc.MergeConcat:
		srcs := make([]stream.Source, len(inputs))
		for i := range inputs {
			srcs[i] = inputs[i].src
		}
		return stream.Concat(srcs...), nil
	case flowdoc.MergeJoin:
		if len(inputs) != 2 {
			return nil, fmt.Errorf("service: join %q needs exactly 2 inputs (found %d)", mg.ID, len(inputs))
		}
		var build, probe stream.Source
		for _, in := range inputs {
			if in.producerID == mg.Build {
				build = in.src
			} else {
				probe = in.src
			}
		}
		if build == nil || probe == nil {
			return nil, fmt.Errorf("service: join build input %q not found among merge inputs", mg.Build)
		}
		return stream.Join(probe, build, stream.JoinSpec{
			LeftKey:  record.MustParsePath(mg.On.Left),
			RightKey: record.MustParsePath(mg.On.Right),
			As:       mg.As,
			Type:     joinType(mg.JoinType),
			Gov:      r.opts.Gov,
		}), nil
	default:
		return nil, fmt.Errorf("service: unknown merge mode %q", mg.Mode)
	}
}

// inputEdge resolves the stream flowing along one edge into a consumer.
func (r *dagRun) inputEdge(from, to string) (stream.Source, error) {
	fn := r.plan.Nodes[from]
	if fn == nil {
		return nil, fmt.Errorf("service: unknown node %q", from)
	}
	if isFanOut(r.plan, fn, from) {
		if err := r.buildFanOut(from); err != nil {
			return nil, err
		}
		src, ok := r.edgeIn[edgeKey(from, to)]
		if !ok {
			return nil, fmt.Errorf("service: fan-out %q has no branch feeding %q", from, to)
		}
		return src, nil
	}
	if src, ok := r.edgeIn[edgeKey(from, to)]; ok {
		return src, nil
	}
	// A plain segment: build the stream leaving `from`.
	return r.streamLeaving(from)
}

// streamLeaving builds the stream produced BY a node (as opposed to arriving
// at it): its own transform applied, for a transform; its bound stream, for a
// source; its merged output, for a merge.
func (r *dagRun) streamLeaving(id string) (stream.Source, error) {
	n := r.plan.Nodes[id]
	if n == nil {
		return nil, fmt.Errorf("service: unknown node %q", id)
	}
	switch {
	case n.Type == "merge":
		return r.streamInto(id)
	case n.Type == "source":
		// Wrapped, not raw: a merge input is a segment like any other, and on
		// a fan-in "what did THIS side contribute" is the question capture
		// exists to answer (ADR-0014). An unwrapped source would also be
		// missing from the Ops report entirely.
		src, err := r.originOf(n)
		if err != nil {
			return nil, err
		}
		return sampled(stream.New(src, n.ID), r.sampler).AsSource()
	case isTransformStep(n):
		origin, originID, ops, err := r.segmentTo(id)
		if err != nil {
			return nil, err
		}
		ops = append(ops, n) // include this node's own transform
		p, err := flow.ApplyOps(ops, sampled(stream.New(origin, originID), r.sampler), r.opts)
		if err != nil {
			return nil, err
		}
		return p.AsSource()
	default:
		return nil, fmt.Errorf("service: node %q (%s) cannot produce a stream", id, n.Type)
	}
}

// predecessors inverts plan.Data.
func predecessors(plan *flowdoc.Plan) map[string][]string {
	preds := map[string][]string{}
	for from, tos := range plan.Data {
		for _, to := range tos {
			preds[to] = append(preds[to], from)
		}
	}
	return preds
}

// isFanOut reports whether a node has more than one data successor.
func isFanOut(plan *flowdoc.Plan, n *flowdoc.Step, id string) bool {
	return len(plan.Data[id]) > 1 || n.Type == "tee" || n.Type == "router"
}

// segmentFrom follows the single-successor chain from a node, collecting the
// transforms it passes, and returns the first structural node or sink it
// reaches — the far end of the segment.
func segmentFrom(plan *flowdoc.Plan, fromID string) ([]*flowdoc.Step, string, error) {
	var ops []*flowdoc.Step
	cur := fromID
	for {
		n := plan.Nodes[cur]
		if n == nil {
			return nil, "", fmt.Errorf("service: unknown node %q", cur)
		}
		if !isTransformStep(n) {
			return ops, cur, nil // sink, merge, or a nested fan-out
		}
		ops = append(ops, n)
		succ := plan.Data[cur]
		if len(succ) != 1 {
			return nil, "", fmt.Errorf("service: node %q has %d successors inside a segment", cur, len(succ))
		}
		cur = succ[0]
	}
}

func edgeKey(from, to string) string { return from + "\x00" + to }

func reverse(s []*flowdoc.Step) []*flowdoc.Step {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}
