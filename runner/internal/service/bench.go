package service

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/task"
)

// CapacityReport is the runner's measured execution capacity (ADR-0008):
// the admin's add/subtract-compute signal, and later the hub's placement
// input. Produced by RunBenchmark through the production execution path.
type CapacityReport struct {
	At      time.Time `json:"at"`
	Records int64     `json:"records_per_stream"`

	SingleStreamRecS float64 `json:"single_stream_rec_s"`
	Streams          int     `json:"concurrent_streams"`
	AggregateRecS    float64 `json:"aggregate_rec_s"`
	// ScalingEfficiency = aggregate / (single × streams); 1.0 is perfect.
	ScalingEfficiency float64 `json:"scaling_efficiency"`

	// MaxConcurrentByMem = admission budget ÷ per-task cost: how many
	// tasks this runner admits before work waits (ADR-0005).
	MaxConcurrentByMem int64 `json:"max_concurrent_by_mem"`
	// EstimatedCapacityRecS extrapolates aggregate throughput to the
	// memory-admission ceiling (conservative: capped at measured
	// aggregate when the ceiling is below the measured stream count).
	EstimatedCapacityRecS float64 `json:"estimated_capacity_rec_s"`

	DurationS float64  `json:"duration_s"`
	TaskIDs   []string `json:"task_ids"`
}

type benchState struct {
	mu      sync.Mutex
	running bool
	latest  *CapacityReport
	history []CapacityReport // newest first, bounded
}

func (b *benchState) snapshot() (*CapacityReport, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.latest, b.running
}

// History returns past capacity reports, newest first.
func (s *Service) BenchHistory() []CapacityReport {
	s.bench.mu.Lock()
	defer s.bench.mu.Unlock()
	out := make([]CapacityReport, len(s.bench.history))
	copy(out, s.bench.history)
	return out
}

// benchmarkFlow is the calibration pipeline: real gen connector source,
// representative ops, real discard sink — the production path end to end.
func benchmarkFlow(records int64) *flow.Document {
	cfg, _ := json.Marshal(map[string]any{"records": records})
	return &flow.Document{
		Name:   "capacity-benchmark",
		Source: flow.Endpoint{Connector: "gen", Action: "gen", Config: cfg},
		Ops: []flow.Op{
			{Type: "filter", Path: "$.active", Cmp: "eq", Value: json.RawMessage("true")},
			{Type: "flatten", Sep: "_"},
			{Type: "project", Fields: []flow.ProjectField{
				{Path: "$.id"}, {Path: "$.name"},
				{Out: "city", Path: "$.address_city"}, {Path: "$.amount"},
			}},
		},
		Sink: flow.Endpoint{Connector: "gen", Action: "discard"},
	}
}

// RunBenchmark measures capacity: one stream, then `streams` concurrent
// (default GOMAXPROCS, clamped to the memory-admission ceiling). Runs are
// ordinary visible tasks and respect admission like all work. Only one
// benchmark runs at a time.
func (s *Service) RunBenchmark(records int64, streams int) (*CapacityReport, error) {
	if records <= 0 {
		records = 1_000_000
	}
	if streams <= 0 {
		streams = runtime.GOMAXPROCS(0)
	}
	if ceiling := int(s.gov.Budget() / s.taskCost()); streams > ceiling {
		streams = max(ceiling, 1)
	}
	s.bench.mu.Lock()
	if s.bench.running {
		s.bench.mu.Unlock()
		return nil, fmt.Errorf("service: a benchmark is already running")
	}
	s.bench.running = true
	s.bench.mu.Unlock()
	defer func() {
		s.bench.mu.Lock()
		s.bench.running = false
		s.bench.mu.Unlock()
	}()

	start := time.Now()
	rep := &CapacityReport{At: start, Records: records, Streams: streams}

	// Phase 1: single stream.
	single, err := s.runBenchTask(records, rep)
	if err != nil {
		return nil, err
	}
	rep.SingleStreamRecS = single

	// Phase 2: concurrent streams.
	ids := make([]string, streams)
	doc := benchmarkFlow(records)
	wallStart := time.Now()
	for i := range streams {
		id, err := s.Submit(doc, true)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	rep.TaskIDs = append(rep.TaskIDs, ids...)
	var totalIn int64
	for _, id := range ids {
		t, err := s.awaitTask(id, 10*time.Minute)
		if err != nil {
			return nil, err
		}
		totalIn += t.RecordsIn
	}
	wall := time.Since(wallStart).Seconds()
	rep.AggregateRecS = float64(totalIn) / wall
	if rep.SingleStreamRecS > 0 && streams > 0 {
		rep.ScalingEfficiency = rep.AggregateRecS / (rep.SingleStreamRecS * float64(streams))
	}
	rep.MaxConcurrentByMem = s.gov.Budget() / s.taskCost()
	// Never extrapolate beyond evidence: capacity is the measured
	// aggregate. Memory headroom above the measured stream count is
	// reported separately (MaxConcurrentByMem) for the admin to reason
	// about I/O-bound (non-CPU-saturated) workloads.
	rep.EstimatedCapacityRecS = rep.AggregateRecS
	rep.DurationS = time.Since(start).Seconds()

	s.bench.mu.Lock()
	s.bench.latest = rep
	s.bench.history = append([]CapacityReport{*rep}, s.bench.history...)
	if len(s.bench.history) > 20 {
		s.bench.history = s.bench.history[:20]
	}
	s.bench.mu.Unlock()
	return rep, nil
}

func (s *Service) runBenchTask(records int64, rep *CapacityReport) (recS float64, err error) {
	id, err := s.Submit(benchmarkFlow(records), true)
	if err != nil {
		return 0, err
	}
	rep.TaskIDs = append(rep.TaskIDs, id)
	t, err := s.awaitTask(id, 10*time.Minute)
	if err != nil {
		return 0, err
	}
	secs := t.Finished.Sub(*t.Started).Seconds()
	if secs <= 0 {
		return 0, fmt.Errorf("service: benchmark task finished instantly; increase records")
	}
	return float64(t.RecordsIn) / secs, nil
}

// awaitTask polls the store until the task reaches a terminal state.
func (s *Service) awaitTask(id string, timeout time.Duration) (task.Task, error) {
	deadline := time.Now().Add(timeout)
	for {
		t, ok := s.store.Get(id)
		if !ok {
			return task.Task{}, fmt.Errorf("service: task %s evicted mid-benchmark", id)
		}
		switch t.State {
		case task.StateCompleted:
			return t, nil
		case task.StateFailed:
			return task.Task{}, fmt.Errorf("service: benchmark task failed: %s", t.Error)
		default:
		}
		if time.Now().After(deadline) {
			return task.Task{}, fmt.Errorf("service: benchmark task %s timed out", id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
