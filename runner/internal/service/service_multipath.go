package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/sdk/host"
)

// This file compiles a v3 multi-path Plan (ADR-0029) onto the engine's
// fan-out / fan-in executors. It supports the two high-value topology
// families end-to-end:
//
//   - fan-out to sinks:  source → ops → (tee|router) → { ops → sink } × N
//   - fan-in from sources: { source → ops } × N → merge(concat|join) → ops → sink
//
// Nested or mixed graphs (a tee branch feeding a merge, fan-out after fan-in)
// are validated by the hub but not yet executable here; they fail with a clear
// error rather than silently mis-running. Linear/v2 flows never reach here.

// bindSource binds a source step to a stream.Source: the built-in @webhook
// body, or a pooled connector subprocess. The returned cleanup releases the
// pooled process (a no-op for @webhook). Pool lifetime is the pool's ctx, not
// the task's (a task's cancel must not kill a process a sibling shares).
func (s *Service) bindSource(step *flow.Step, o SubmitOpts) (stream.Source, func(), error) {
	src, cleanup, _, err := s.bindSourceInfo(step, o)
	return src, cleanup, err
}

// bindSourceInfo is bindSource plus the identity of the connector build behind
// the source. A resume cursor is only meaningful to the build that produced it
// (ADR-0037), so the caller pins both to the checkpoint it records — a
// replacement runner may hold a different version, and an older cursor read
// under a newer one could resolve to a different position, silently.
func (s *Service) bindSourceInfo(step *flow.Step, o SubmitOpts) (stream.Source, func(), host.Info, error) {
	if step.Connector == flowdoc.WebhookSource {
		if o.WebhookBody == nil {
			return nil, nil, host.Info{}, errors.New("service: @webhook flow requires a request body")
		}
		return ndjson.NewReader(bytes.NewReader(o.WebhookBody), ndjson.ReaderOptions{}), func() {}, host.Info{}, nil
	}
	proc, err := s.pool.Get(s.baseCtx, step.Connector, step.Version)
	if err != nil {
		return nil, nil, host.Info{}, err
	}
	src := proc.Source(step.Action, step.Config)
	// A resume cursor is opaque and belongs to the connector that produced it
	// (ADR-0037). Passing an empty one is a no-op, so the caller forwards
	// whatever the hub returned without inspecting it. A cursor handed to a
	// source that cannot honour it fails the stream rather than silently
	// starting over — that failure is deliberate, and the runner falls back to
	// a full replay.
	if len(o.ResumeFrom) > 0 {
		if resumeAllowed(o, proc.Info()) {
			src.ResumeFrom(o.ResumeFrom)
		} else {
			slog.Warn("ignoring a resume cursor from another connector build; replaying from the start",
				"event", "task.checkpoint.ignored",
				"cursor_connector", o.ResumeConnector, "cursor_version", o.ResumeVersion,
				"connector", proc.Info().Name, "version", proc.Info().Version)
		}
	}
	return src, func() { s.pool.Put(step.Connector, step.Version) }, proc.Info(), nil
}

// bindSink binds a sink step: a built-in terminal (@discard / @response) or a
// pooled connector subprocess. Returns the sink, a confirmed-record reporter,
// and a cleanup.
func (s *Service) bindSink(step *flow.Step, o SubmitOpts) (stream.Sink, func() int64, func(), error) {
	switch step.Connector {
	case flowdoc.DiscardSink:
		ds := &discardSink{}
		return ds, func() int64 { return ds.n }, func() {}, nil
	case flowdoc.ResponseSink:
		rs := newResponseSink(o.Response)
		return rs, func() int64 { return rs.n }, func() {}, nil
	case flowdoc.StopSink:
		return &stopSink{}, func() int64 { return 0 }, func() {}, nil
	default:
		proc, err := s.pool.Get(s.baseCtx, step.Connector, step.Version)
		if err != nil {
			return nil, nil, nil, err
		}
		ss := proc.Sink(step.Action, step.Config)
		return ss, func() int64 { return ss.Records }, func() { s.pool.Put(step.Connector, step.Version) }, nil
	}
}

// executeMulti dispatches a v3 DAG plan to the fan-out or fan-in executor.
func (s *Service) executeMulti(ctx context.Context, doc *flow.Document, plan *flowdoc.Plan, redact func(string) string, _ *captureSampler, o SubmitOpts) (execResult, error) {
	var fanouts, merges []*flowdoc.Step
	for id, n := range plan.Nodes {
		if len(plan.Data[id]) > 1 {
			fanouts = append(fanouts, n)
		}
		if n.Type == "merge" {
			merges = append(merges, n)
		}
	}
	switch {
	case len(fanouts) == 1 && len(merges) == 0:
		return s.executeFanOut(ctx, doc, plan, fanouts[0], redact, o)
	case len(merges) == 1 && len(fanouts) == 0:
		return s.executeFanIn(ctx, doc, plan, merges[0], redact, o)
	default:
		return execResult{}, fmt.Errorf("service: flow topology not yet executable on this runner (%d fan-out, %d fan-in node(s)); nested or mixed graphs are a later change", len(fanouts), len(merges))
	}
}

// executeFanOut runs source → ops → (tee|router) → { ops → sink } × N.
func (s *Service) executeFanOut(ctx context.Context, doc *flow.Document, plan *flowdoc.Plan, fo *flowdoc.Step, redact func(string) string, o SubmitOpts) (execResult, error) {
	if len(plan.Sources) != 1 {
		return execResult{}, fmt.Errorf("service: fan-out needs a single source (found %d)", len(plan.Sources))
	}
	opts := flow.CompileOptions{Gov: mem.New(s.opts.TaskWatermark), SpillDir: s.opts.SpillDir}

	// Upstream: the single source through its ops, up to the fan-out node.
	srcStep := plan.Nodes[plan.Sources[0]]
	upOps, endID, err := linearOps(plan, srcStep.ID)
	if err != nil {
		return execResult{}, err
	}
	if endID != fo.ID {
		return execResult{}, fmt.Errorf("service: source %q does not lead directly to the fan-out", srcStep.ID)
	}
	src, srcCleanup, err := s.bindSource(srcStep, o)
	if err != nil {
		return execResult{}, err
	}
	defer srcCleanup()
	upPipe, err := flow.ApplyOps(upOps, stream.New(src, srcStep.ID), opts)
	if err != nil {
		return execResult{}, err
	}
	upstream, err := upPipe.AsSource()
	if err != nil {
		return execResult{}, err
	}

	// Branches: each fan-out successor through its ops to a sink.
	succ := plan.Data[fo.ID]
	isRouter := fo.Type == "router"
	branches := make([]stream.Branch, 0, len(succ))
	confirmers := make([]func() int64, 0, len(succ))
	for _, bid := range succ {
		bOps, sinkID, err := branchOps(plan, bid)
		if err != nil {
			return execResult{}, err
		}
		sink, confirmed, cleanup, err := s.bindSink(plan.Nodes[sinkID], o)
		if err != nil {
			return execResult{}, err
		}
		defer cleanup()
		confirmers = append(confirmers, confirmed)
		ops := bOps
		// Shared (no copy) is safe for a router branch (it owns its partition
		// batch) and for an op-less tee branch (read-only straight to a sink);
		// a tee branch with ops must copy-on-write (Shared=false).
		branches = append(branches, stream.Branch{
			Name:   sinkID,
			Sink:   sink,
			Shared: isRouter || len(ops) == 0,
			Build: func(bs stream.Source) *stream.Pipeline {
				return flow.ApplyOpsFold(ops, stream.New(bs, sinkID), opts)
			},
		})
	}

	var frep stream.FanoutReport
	var runErr error
	if isRouter {
		match, err := compileRouter(fo, succ)
		if err != nil {
			return execResult{}, err
		}
		frep, runErr = stream.RunRouter(ctx, upstream, branches, match, stream.FanoutOptions{})
	} else {
		frep, runErr = stream.RunTee(ctx, upstream, branches, stream.FanoutOptions{})
	}

	res := aggregateFanout(frep, confirmers)
	return s.routeMultiError(ctx, plan, doc, redact, res, runErr)
}

// executeFanIn runs { source → ops } × N → merge(concat|join) → ops → sink.
func (s *Service) executeFanIn(ctx context.Context, doc *flow.Document, plan *flowdoc.Plan, mg *flowdoc.Step, redact func(string) string, o SubmitOpts) (execResult, error) {
	gov := mem.New(s.opts.TaskWatermark)
	opts := flow.CompileOptions{Gov: gov, SpillDir: s.opts.SpillDir}

	type input struct {
		producerID string // the immediate predecessor of the merge (names the join build side)
		src        stream.Source
	}
	var inputs []input
	for _, srcID := range plan.Sources {
		srcStep := plan.Nodes[srcID]
		ops, endID, err := linearOps(plan, srcID)
		if err != nil {
			return execResult{}, err
		}
		if endID != mg.ID {
			return execResult{}, fmt.Errorf("service: source %q does not lead directly to the merge", srcID)
		}
		src, cleanup, err := s.bindSource(srcStep, o)
		if err != nil {
			return execResult{}, err
		}
		defer cleanup()
		pipe, err := flow.ApplyOps(ops, stream.New(src, srcID), opts)
		if err != nil {
			return execResult{}, err
		}
		isrc, err := pipe.AsSource()
		if err != nil {
			return execResult{}, err
		}
		producerID := srcID
		if len(ops) > 0 {
			producerID = ops[len(ops)-1].ID
		}
		inputs = append(inputs, input{producerID, isrc})
	}
	if len(inputs) < 2 {
		return execResult{}, fmt.Errorf("service: merge needs at least 2 source inputs (found %d)", len(inputs))
	}

	var merged stream.Source
	switch mg.Mode {
	case flowdoc.MergeConcat:
		srcs := make([]stream.Source, len(inputs))
		for i := range inputs {
			srcs[i] = inputs[i].src
		}
		merged = stream.Concat(srcs...)
	case flowdoc.MergeJoin:
		if len(inputs) != 2 {
			return execResult{}, fmt.Errorf("service: join needs exactly 2 inputs (found %d)", len(inputs))
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
			return execResult{}, fmt.Errorf("service: join build input %q not found among merge inputs", mg.Build)
		}
		merged = stream.Join(probe, build, stream.JoinSpec{
			LeftKey:  record.MustParsePath(mg.On.Left),
			RightKey: record.MustParsePath(mg.On.Right),
			As:       mg.As,
			Type:     joinType(mg.JoinType),
			Gov:      gov,
		})
	default:
		return execResult{}, fmt.Errorf("service: unknown merge mode %q", mg.Mode)
	}

	// Downstream: merge → ops → sink.
	downOps, sinkID, err := branchOps(plan, plan.Data[mg.ID][0])
	if err != nil {
		return execResult{}, err
	}
	sink, confirmed, cleanup, err := s.bindSink(plan.Nodes[sinkID], o)
	if err != nil {
		return execResult{}, err
	}
	defer cleanup()

	p, err := flow.ApplyOps(downOps, stream.New(merged, mg.ID), opts)
	if err != nil {
		return execResult{}, err
	}
	rep, runErr := p.Run(ctx, sink, sinkID)
	res := execResult{rep: rep, confirmed: confirmed()}
	return s.routeMultiError(ctx, plan, doc, redact, res, runErr)
}

// aggregateFanout folds per-branch reports and confirmed counts into one
// execResult.
func aggregateFanout(frep stream.FanoutReport, confirmers []func() int64) execResult {
	res := execResult{stopped: frep.Stopped, stopStep: frep.StopStep}
	for _, br := range frep.Branches {
		res.rep.Ops = append(res.rep.Ops, br.Ops...)
		res.rep.RecordsOut += br.RecordsOut
	}
	for _, c := range confirmers {
		res.confirmed += c()
	}
	return res
}

// routeMultiError applies the shared onFailure routing (ADR-0013) to a
// multi-path run's error, tagging by the failing step id.
func (s *Service) routeMultiError(ctx context.Context, plan *flowdoc.Plan, doc *flow.Document, redact func(string) string, res execResult, runErr error) (execResult, error) {
	// The fan-out executor already classified its own branch errors (it has to
	// — only it can tell its teardown cancel from the caller's). Running the
	// rule again here is harmless and covers the fan-in path, which runs a
	// single pipeline and arrives with a raw error.
	runErr = classify(ctx, &res, runErr)
	if runErr == nil {
		return res, nil
	}
	failStep := failingStep(runErr)
	if h := plan.HandlerFor(failStep); h != nil {
		res.handled = true
		res.handlerStep = h.ID
		if herr := s.runHandler(ctx, h, doc.Name, failStep, redact(runErr.Error())); herr != nil {
			res.handlerErr = redact(herr.Error())
		}
	}
	return res, runErr
}

// compileRouter builds the router's record→branch-index match function. succ
// is the fan-out's ordered successors: the routes in order, then the default
// (if any). A record takes the first matching route, else the default, else is
// dropped (-1).
func compileRouter(fo *flowdoc.Step, succ []string) (func(record.Value) int, error) {
	type pred struct {
		fn     func(record.Value) bool
		branch int
	}
	preds := make([]pred, 0, len(fo.Routes))
	for i, r := range fo.Routes {
		fn, err := flow.CompilePredicate(r.Path, r.Cmp, r.Value)
		if err != nil {
			return nil, fmt.Errorf("service: router route %d: %w", i, err)
		}
		preds = append(preds, pred{fn, i}) // route i maps to succ[i]
	}
	defaultIdx := -1
	if fo.Default != "" {
		defaultIdx = len(fo.Routes) // succ[len(Routes)] is the default target
	}
	return func(v record.Value) int {
		for _, p := range preds {
			if p.fn(v) {
				return p.branch
			}
		}
		return defaultIdx
	}, nil
}

// linearOps follows the single-successor chain from fromID, collecting the
// transform steps it passes, and returns the first non-linear node reached (a
// fan-out, a merge, or a sink) — the boundary of the linear segment.
func linearOps(plan *flowdoc.Plan, fromID string) ([]*flowdoc.Step, string, error) {
	var ops []*flowdoc.Step
	cur := fromID
	for {
		succ := plan.Data[cur]
		if len(succ) != 1 {
			return ops, cur, nil // fan-out (>1) or terminal (0)
		}
		n := plan.Nodes[succ[0]]
		if n == nil {
			return nil, "", fmt.Errorf("service: unknown node %q", succ[0])
		}
		if !isTransformStep(n) {
			return ops, n.ID, nil // reached a structural node or a sink
		}
		ops = append(ops, n)
		cur = n.ID
	}
}

// branchOps collects the transform steps from startID (inclusive) to the sink
// that terminates the branch. A non-transform, non-sink node (a nested
// fan-out/merge) is rejected — those topologies are not yet executable.
func branchOps(plan *flowdoc.Plan, startID string) ([]*flowdoc.Step, string, error) {
	var ops []*flowdoc.Step
	cur := startID
	for {
		n := plan.Nodes[cur]
		if n == nil {
			return nil, "", fmt.Errorf("service: unknown node %q", cur)
		}
		if n.Type == "sink" {
			return ops, cur, nil
		}
		if !isTransformStep(n) {
			return nil, "", fmt.Errorf("service: node %q (%s) inside a branch is not yet executable (nested fan-out/fan-in)", cur, n.Type)
		}
		ops = append(ops, n)
		succ := plan.Data[cur]
		if len(succ) != 1 {
			return nil, "", fmt.Errorf("service: unsupported branch shape at %q", cur)
		}
		cur = succ[0]
	}
}

func isTransformStep(n *flowdoc.Step) bool {
	switch n.Type {
	case "filter", "project", "coerce", "flatten", "aggregate":
		return true
	}
	return false
}

func joinType(t string) stream.JoinType {
	if t == flowdoc.JoinLeft {
		return stream.JoinTypeLeft
	}
	return stream.JoinTypeInner
}

// resumeAllowed reports whether a stored cursor may be handed to the connector
// build now bound (ADR-0037).
//
// A cursor carries no self-description — it is opaque bytes only its producer
// understands — so the only defence against reading a v0.3 cursor under v0.4
// is to refuse. That matters because runners are replaceable by design: the
// build that recorded a position is frequently NOT the build that resumes it.
// An empty recorded identity means the cursor predates this pinning and is
// treated as untrusted for the same reason.
func resumeAllowed(o SubmitOpts, info host.Info) bool {
	if o.ResumeConnector == "" || o.ResumeVersion == "" {
		return false
	}
	return o.ResumeConnector == info.Name && o.ResumeVersion == info.Version
}
