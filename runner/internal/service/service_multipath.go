package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/sdk/host"
)

// This file holds the connector binding and shared helpers for v3 multi-path
// execution (ADR-0029). The topology compiler itself lives in dag.go, which
// compiles ANY validated DAG — including nested and mixed graphs — into linear
// segments joined at tee/router/merge nodes. Linear/v2 flows never reach here.

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
	// Test input diverts a real source in a TEST execution (ADR-0048 §5). The
	// connector below is untouched and still runs in production — the option
	// is additive, never a substitution, which is why nothing has to be
	// removed before publishing.
	if o.Test && step.TestInput != nil && step.TestInput.Enabled {
		return testInputSource(step), func() {}, host.Info{}, nil
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
		// A mock diverts a real sink in a TEST execution (ADR-0048 §5): it
		// records what would have been written rather than writing it. The
		// connector is left entirely alone — not launched, not configured, not
		// removed from the document — so the deployed flow is unchanged.
		if o.Test && step.Mock != nil && step.Mock.Enabled {
			ms := &discardSink{}
			return ms, func() int64 { return ms.n }, func() {}, nil
		}
		proc, err := s.pool.Get(s.baseCtx, step.Connector, step.Version)
		if err != nil {
			return nil, nil, nil, err
		}
		ss := proc.Sink(step.Action, step.Config)
		return ss, func() int64 { return ss.Records }, func() { s.pool.Put(step.Connector, step.Version) }, nil
	}
}

// executeMulti dispatches a v3 DAG plan. Every validated topology compiles
// through the general segment compiler in dag.go (ADR-0029 §2, issue #59);
// this indirection is kept so the call site reads the same as the linear one.
func (s *Service) executeMulti(ctx context.Context, doc *flow.Document, plan *flowdoc.Plan, redact func(string) string, sampler *captureSampler, o SubmitOpts) (execResult, error) {
	return s.executeDAG(ctx, doc, plan, redact, sampler, o)
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

// testInputSource emits a step's configured test records instead of calling
// its connector (ADR-0048 §5).
//
// Only ever reached inside a TEST execution: the caller checks, so this cannot
// be the thing that accidentally diverts production. Records were validated as
// JSON at parse time, so there is nothing to fail on here.
func testInputSource(step *flow.Step) stream.Source {
	var buf bytes.Buffer
	for _, r := range step.TestInput.Records {
		buf.Write(r)
		buf.WriteByte('\n')
	}
	return ndjson.NewReader(bytes.NewReader(buf.Bytes()), ndjson.ReaderOptions{})
}
