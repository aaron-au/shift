// Package service is the runner's task service: resource-governed
// admission (ADR-0005), connector pooling, engine pipeline execution, and
// result recording. Intakes (HTTP now, hub lease loop later) are thin
// layers over Submit (ADR-0008).
package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
	"github.com/aaron-au/shift/runner/internal/connpool"
	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// Options configure the service.
type Options struct {
	// ConnectorDir holds shift-connector-<name> binaries.
	ConnectorDir string
	// MemBudget is the runner-wide admission budget in bytes (default 1 GiB).
	MemBudget int64
	// TaskWatermark is each task's stateful-operator budget (default 64 MiB);
	// aggregates spill beyond it.
	TaskWatermark int64
	// TaskOverhead approximates fixed per-task cost: batches in flight,
	// frames, connector buffers (default 16 MiB).
	TaskOverhead int64
	// SpillDir hosts task scratch ("" = OS temp).
	SpillDir string
	// LocateConnector resolves connectors from the hub registry
	// (verified) when ConnectorDir doesn't provide them. Optional.
	LocateConnector func(ctx context.Context, name string) (string, error)
	// RequireSigned disables the ConnectorDir fallback: everything must
	// come verified through LocateConnector.
	RequireSigned bool
	// TaskHistory bounds the result ring (default 500).
	TaskHistory int
	// PoolIdleTTL reaps idle connectors (default 5m).
	PoolIdleTTL time.Duration
}

func (o *Options) defaults() {
	if o.MemBudget <= 0 {
		o.MemBudget = 1 << 30
	}
	if o.TaskWatermark <= 0 {
		o.TaskWatermark = 64 << 20
	}
	if o.TaskOverhead <= 0 {
		o.TaskOverhead = 16 << 20
	}
}

// Service executes flows. Create with New, stop with Close.
type Service struct {
	opts  Options
	gov   *mem.Governor
	pool  *connpool.Pool
	store *task.Store

	mu       sync.Mutex
	released chan struct{} // closed+swapped on every capacity release
	draining bool
	wg       sync.WaitGroup

	bench *benchState
}

// New builds a service.
func New(opts Options) *Service {
	opts.defaults()
	return &Service{
		opts: opts,
		gov:  mem.New(opts.MemBudget),
		pool: connpool.New(connpool.Options{
			Dir:           opts.ConnectorDir,
			Locate:        opts.LocateConnector,
			RequireSigned: opts.RequireSigned,
			IdleTTL:       opts.PoolIdleTTL,
		}),
		store:    task.NewStore(opts.TaskHistory),
		released: make(chan struct{}),
		bench:    &benchState{},
	}
}

// taskCost is the admission reservation for one task.
func (s *Service) taskCost() int64 { return s.opts.TaskWatermark + s.opts.TaskOverhead }

// Submit validates the flow, registers a task, and runs it asynchronously.
// It returns the task id immediately; admission may hold the task in
// "waiting" until capacity frees (resource-based, never a count cap —
// ADR-0005).
func (s *Service) Submit(doc *flow.Document, benchmark bool) (string, error) {
	return s.SubmitWith(doc, SubmitOpts{Benchmark: benchmark})
}

// SubmitOpts carries per-task execution options that the lease path
// supplies but the simple (dashboard/benchmark) path does not.
type SubmitOpts struct {
	// Benchmark marks a synthetic capacity-benchmark task.
	Benchmark bool
	// SecretValues are the resolved secret plaintexts for this task. They
	// are used only to redact any value that leaks into an error string or
	// error-handler record (ADR-0010: secrets never in logs). Never stored.
	SecretValues []string
	// Capture turns on per-step INPUT/OUTPUT data capture (test mode). The
	// sample stays runner-side, redacted, and ephemeral.
	Capture bool
	// CaptureMax bounds the sampled records per step (default 20).
	CaptureMax int
}

// SubmitWith registers and runs a task with explicit options.
func (s *Service) SubmitWith(doc *flow.Document, o SubmitOpts) (string, error) {
	if err := doc.Validate(); err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return "", fmt.Errorf("service: runner is draining")
	}
	s.wg.Add(1)
	s.mu.Unlock()

	t := &task.Task{
		ID:        task.NewID(),
		Flow:      doc.Name,
		Benchmark: o.Benchmark,
		State:     task.StateWaiting,
		Submitted: time.Now(),
	}
	s.store.Add(t)
	go s.run(t.ID, doc, o)
	return t.ID, nil
}

func (s *Service) run(id string, doc *flow.Document, o SubmitOpts) {
	defer s.wg.Done()
	ctx := context.Background()

	// Admission: reserve or wait for a release. The wait is unbounded by
	// design — capacity, not a count, is the limit (ADR-0005).
	cost := s.taskCost()
	for !s.gov.TryReserve(cost) {
		s.mu.Lock()
		ch := s.released
		s.mu.Unlock()
		<-ch
	}
	defer func() {
		s.gov.Release(cost)
		s.mu.Lock()
		close(s.released)
		s.released = make(chan struct{})
		s.mu.Unlock()
	}()

	now := time.Now()
	s.store.Update(id, func(t *task.Task) {
		t.State = task.StateRunning
		t.Started = &now
	})

	redact := newRedactor(o.SecretValues)
	var sampler *captureSampler
	if o.Capture {
		sampler = newCaptureSampler(o.CaptureMax, redact)
	}
	res, err := s.execute(ctx, doc, redact, sampler)
	end := time.Now()
	s.store.Update(id, func(t *task.Task) {
		t.Finished = &end
		t.Captured = res.captured // useful on success and failure alike
		if err != nil {
			t.State = task.StateFailed
			t.Error = redact(err.Error())
			t.Handled = res.handled
			t.HandlerStep = res.handlerStep
			t.HandlerError = res.handlerErr
			return
		}
		t.State = task.StateCompleted
		t.SinkConfirmed = res.confirmed
		t.RecordsOut = res.rep.RecordsOut
		if len(res.rep.Ops) > 0 {
			t.RecordsIn = res.rep.Ops[0].RecordsIn
		}
		for _, op := range res.rep.Ops {
			t.Ops = append(t.Ops, task.OpStat{
				Name:       op.Name,
				StepID:     op.Name, // Apply/New/Run label every op by its step id
				RecordsIn:  op.RecordsIn,
				RecordsOut: op.RecordsOut,
				Seconds:    float64(op.Nanos) / 1e9,
			})
		}
	})
}

// execResult carries an execution's outcome, including whether an error
// was routed to an onFailure handler.
type execResult struct {
	rep         stream.Report
	confirmed   int64
	handled     bool
	handlerStep string
	handlerErr  string
	captured    []task.StepCapture
}

// execute binds connectors, runs the compiled pipeline, and on failure
// routes to the failing step's error handler (v2 onFailure), if any. When
// sampler is non-nil, per-step INPUT/OUTPUT samples are collected.
func (s *Service) execute(ctx context.Context, doc *flow.Document, redact func(string) string, sampler *captureSampler) (execResult, error) {
	plan, err := doc.Plan()
	if err != nil {
		return execResult{}, err
	}
	srcStep := plan.Main[0]
	sinkStep := plan.Main[len(plan.Main)-1]

	srcProc, err := s.pool.Get(ctx, srcStep.Connector)
	if err != nil {
		return execResult{}, err
	}
	defer s.pool.Put(srcStep.Connector)
	sinkProc, err := s.pool.Get(ctx, sinkStep.Connector)
	if err != nil {
		return execResult{}, err
	}
	defer s.pool.Put(sinkStep.Connector)

	src := srcProc.Source(srcStep.Action, srcStep.Config)
	sink := sinkProc.Sink(sinkStep.Action, sinkStep.Config)

	taskGov := mem.New(s.opts.TaskWatermark)
	base := stream.New(src, srcStep.ID)
	if sampler != nil {
		base = base.WithSampler(sampler) // wire before ops are appended
	}
	p, err := flow.Apply(doc, base, flow.CompileOptions{Gov: taskGov, SpillDir: s.opts.SpillDir})
	if err != nil {
		return execResult{}, err
	}
	rep, runErr := p.Run(ctx, sink, sinkStep.ID)
	res := execResult{rep: rep, confirmed: sink.Records}
	if sampler != nil {
		res.captured = sampler.result()
	}
	if runErr == nil {
		return res, nil
	}

	// Error routing: identify the failing step (the OpError tag) and, if the
	// flow declares an onFailure handler covering it, run the handler with a
	// payload-free, redacted error record. The task still fails.
	failStep := ""
	if oe, ok := errors.AsType[*stream.OpError](runErr); ok {
		failStep = oe.Op
	}
	if h := plan.HandlerFor(failStep); h != nil {
		res.handled = true
		res.handlerStep = h.ID
		if herr := s.runHandler(ctx, h, doc.Name, failStep, redact(runErr.Error())); herr != nil {
			res.handlerErr = redact(herr.Error())
		}
	}
	return res, runErr
}

// runHandler delivers a single error record to a v2 onFailure handler (a
// sink action). The record is metadata only — flow, failing step, error,
// timestamp — never the payload the hub must not see (doctrine).
func (s *Service) runHandler(ctx context.Context, h *flow.Step, flowName, failStep, errMsg string) error {
	proc, err := s.pool.Get(ctx, h.Connector)
	if err != nil {
		return err
	}
	defer s.pool.Put(h.Connector)
	sink := proc.Sink(h.Action, h.Config)

	b := errorRecord(flowName, failStep, errMsg, time.Now().UTC().Format(time.RFC3339))
	if err := sink.Write(ctx, b); err != nil {
		_ = sink.Close()
		return err
	}
	return sink.Close()
}

// errorRecord builds the single, payload-free record handed to an
// onFailure handler: flow, failing step, (already-redacted) error, and a
// timestamp. It never contains any of the flow's payload data.
func errorRecord(flowName, failStep, errMsg, at string) *record.Batch {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("flow")
	bld.StringLiteral(flowName)
	bld.KeyLiteral("step")
	bld.StringLiteral(failStep)
	bld.KeyLiteral("error")
	bld.StringLiteral(errMsg)
	bld.KeyLiteral("at")
	bld.StringLiteral(at)
	bld.EndMap()
	b.Append(bld.Finish())
	return b
}

// newRedactor returns a function that masks each non-empty secret value in
// a string. Zero secrets yields the identity function.
func newRedactor(values []string) func(string) string {
	pairs := make([]string, 0, len(values)*2)
	for _, v := range values {
		if v != "" {
			pairs = append(pairs, v, "***")
		}
	}
	if len(pairs) == 0 {
		return func(s string) string { return s }
	}
	return strings.NewReplacer(pairs...).Replace
}

// Status is the runner-wide snapshot for the API/dashboard.
type Status struct {
	Governor struct {
		Budget int64 `json:"budget"`
		Used   int64 `json:"used"`
		Peak   int64 `json:"peak"`
	} `json:"governor"`
	TaskCost   int64             `json:"task_cost"`
	MaxByMem   int64             `json:"max_concurrent_by_mem"`
	Totals     task.Totals       `json:"totals"`
	Connectors []connpool.Status `json:"connectors"`
	Benchmark  *CapacityReport   `json:"benchmark,omitempty"`
	BenchBusy  bool              `json:"benchmark_running"`
	Tiered     *TieredReport     `json:"tiered,omitempty"`
	TieredBusy bool              `json:"tiered_running"`
}

// Status snapshots the service.
func (s *Service) Status() Status {
	var st Status
	st.Governor.Budget = s.gov.Budget()
	st.Governor.Used = s.gov.Used()
	st.Governor.Peak = s.gov.Peak()
	st.TaskCost = s.taskCost()
	st.MaxByMem = s.gov.Budget() / s.taskCost()
	st.Totals = s.store.Totals()
	st.Connectors = s.pool.Snapshot()
	st.Benchmark, st.BenchBusy = s.bench.snapshot()
	st.Tiered, st.TieredBusy = s.bench.snapshotTiered()
	return st
}

// Tasks lists recent tasks, newest first.
func (s *Service) Tasks(n int) []task.Task { return s.store.Recent(n) }

// Task fetches one task.
func (s *Service) Task(id string) (task.Task, bool) { return s.store.Get(id) }

// Close drains: no new submissions, waits for running tasks (bounded by
// timeout), then shuts the connector pool down.
func (s *Service) Close(timeout time.Duration) error {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
	}
	return s.pool.Close()
}
