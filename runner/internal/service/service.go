// Package service is the runner's task service: resource-governed
// admission (ADR-0005), connector pooling, engine pipeline execution, and
// result recording. Intakes (HTTP now, hub lease loop later) are thin
// layers over Submit (ADR-0008).
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aaron-au/shift/engine/mem"
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
		Benchmark: benchmark,
		State:     task.StateWaiting,
		Submitted: time.Now(),
	}
	s.store.Add(t)
	go s.run(t.ID, doc)
	return t.ID, nil
}

func (s *Service) run(id string, doc *flow.Document) {
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

	rep, confirmed, err := s.execute(ctx, doc)
	end := time.Now()
	s.store.Update(id, func(t *task.Task) {
		t.Finished = &end
		if err != nil {
			t.State = task.StateFailed
			t.Error = err.Error()
			return
		}
		t.State = task.StateCompleted
		t.SinkConfirmed = confirmed
		t.RecordsOut = rep.RecordsOut
		if len(rep.Ops) > 0 {
			t.RecordsIn = rep.Ops[0].RecordsIn
		}
		for _, op := range rep.Ops {
			t.Ops = append(t.Ops, task.OpStat{
				Name:       op.Name,
				RecordsIn:  op.RecordsIn,
				RecordsOut: op.RecordsOut,
				Seconds:    float64(op.Nanos) / 1e9,
			})
		}
	})
}

// execute binds connectors and runs the compiled pipeline.
func (s *Service) execute(ctx context.Context, doc *flow.Document) (stream.Report, int64, error) {
	srcProc, err := s.pool.Get(ctx, doc.Source.Connector)
	if err != nil {
		return stream.Report{}, 0, err
	}
	defer s.pool.Put(doc.Source.Connector)
	sinkProc, err := s.pool.Get(ctx, doc.Sink.Connector)
	if err != nil {
		return stream.Report{}, 0, err
	}
	defer s.pool.Put(doc.Sink.Connector)

	src := srcProc.Source(doc.Source.Action, doc.Source.Config)
	sink := sinkProc.Sink(doc.Sink.Action, doc.Sink.Config)

	taskGov := mem.New(s.opts.TaskWatermark)
	p, err := flow.Apply(doc,
		stream.New(src, "source:"+doc.Source.Connector+"/"+doc.Source.Action),
		flow.CompileOptions{Gov: taskGov, SpillDir: s.opts.SpillDir},
	)
	if err != nil {
		return stream.Report{}, 0, err
	}
	rep, err := p.Run(ctx, sink, "sink:"+doc.Sink.Connector+"/"+doc.Sink.Action)
	return rep, sink.Records, err
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
