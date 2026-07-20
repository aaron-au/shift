package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/aaron-au/shift/runner/internal/flow"
)

// The tiered benchmark (M5e) grades throughput by process shape, not just
// raw capacity: the four tiers span passthrough to a multi-stage,
// high-cardinality pipeline, so the per-tier rec/s figures are the honest
// basis for incumbent comparison (never one number hiding the shape it was
// measured on). Every tier runs the production path — real gen source, real
// discard sink, the same admission and connector pooling as user work — and
// lowers through the v2 flow Plan like any flow.
//
// The sink is the in-process discard connector for every tier so the
// benchmark is reproducible on any runner with no external target. An
// http-sink "extreme" profile (a live endpoint under load) is a separate,
// connector-equipped benchmark and is deliberately not folded in here — it
// would make the numbers depend on an unrelated network target.

// TierResult is one graded shape's measured throughput.
type TierResult struct {
	Tier              string  `json:"tier"`
	Shape             string  `json:"shape"`
	RecordsPerStream  int64   `json:"records_per_stream"`
	SingleStreamRecS  float64 `json:"single_stream_rec_s"`
	ConcurrentStreams int     `json:"concurrent_streams"`
	AggregateRecS     float64 `json:"aggregate_rec_s"`
	ScalingEfficiency float64 `json:"scaling_efficiency"`
}

// TieredReport is a full sweep across the graded tiers.
type TieredReport struct {
	At        time.Time    `json:"at"`
	Tiers     []TierResult `json:"tiers"`
	DurationS float64      `json:"duration_s"`
}

// tier defines one graded shape and how to build its flow at a given size.
type tier struct {
	name  string
	shape string
	flow  func(records int64) *flow.Document
}

// benchTiers are ordered simplest → hardest. Each is an ordinary flow; the
// gradient is transform depth and aggregate cardinality (spill pressure).
var benchTiers = []tier{
	{"simple", "passthrough (source → sink)", func(records int64) *flow.Document {
		return tierFlow("bench-simple", records, 1000, nil)
	}},
	{"standard", "filter + coerce + project", func(records int64) *flow.Document {
		return tierFlow("bench-standard", records, 1000, []flow.Op{
			{Type: "filter", Path: "$.active", Cmp: "eq", Value: json.RawMessage("true")},
			{Type: "coerce", Rules: []flow.CoerceRule{{Field: "amount", To: "int"}}},
			{Type: "project", Fields: []flow.ProjectField{
				{Path: "$.id"}, {Path: "$.name"}, {Path: "$.amount"}, {Path: "$.region"},
			}},
		})
	}},
	{"complex", "flatten + aggregate (high-cardinality, spill-capable)", func(records int64) *flow.Document {
		return tierFlow("bench-complex", records, 50_000, []flow.Op{
			{Type: "flatten", Sep: "_"},
			{Type: "aggregate", Key: "$.region", Aggs: []flow.Agg{
				{Op: "count", Out: "n"}, {Op: "sum", Path: "$.amount", Out: "total"},
			}},
		})
	}},
	{"extreme", "filter + flatten + project + aggregate (very high cardinality)", func(records int64) *flow.Document {
		return tierFlow("bench-extreme", records, 200_000, []flow.Op{
			{Type: "filter", Path: "$.active", Cmp: "eq", Value: json.RawMessage("true")},
			{Type: "flatten", Sep: "_"},
			{Type: "project", Fields: []flow.ProjectField{
				{Path: "$.id"}, {Path: "$.name"}, {Path: "$.amount"},
				{Path: "$.region"}, {Out: "city", Path: "$.address_city"},
			}},
			{Type: "aggregate", Key: "$.region", Aggs: []flow.Agg{
				{Op: "count", Out: "n"}, {Op: "sum", Path: "$.amount", Out: "total"},
			}},
		})
	}},
}

// tierFlow builds a gen→ops→discard document with the given group
// cardinality (drives aggregate state size / spill pressure).
func tierFlow(name string, records, groups int64, ops []flow.Op) *flow.Document {
	cfg, _ := json.Marshal(map[string]any{"records": records, "groups": groups})
	return &flow.Document{
		Name:   name,
		Source: flow.Endpoint{Connector: "gen", Action: "gen", Config: cfg},
		Ops:    ops,
		Sink:   flow.Endpoint{Connector: "gen", Action: "discard"},
	}
}

// TieredHistory returns past tiered reports, newest first.
func (s *Service) TieredHistory() []TieredReport {
	s.bench.mu.Lock()
	defer s.bench.mu.Unlock()
	out := make([]TieredReport, len(s.bench.tieredHistory))
	copy(out, s.bench.tieredHistory)
	return out
}

// RunTieredBenchmark sweeps every graded tier, measuring single-stream and
// concurrent throughput per tier through the production execution path.
// Only one tiered benchmark runs at a time.
func (s *Service) RunTieredBenchmark(records int64, streams int) (*TieredReport, error) {
	if records <= 0 {
		records = 200_000
	}
	if streams <= 0 {
		streams = runtime.GOMAXPROCS(0)
	}
	if ceiling := int(s.gov.Budget() / s.taskCost()); streams > ceiling {
		streams = max(ceiling, 1)
	}

	s.bench.mu.Lock()
	if s.bench.tieredRunning {
		s.bench.mu.Unlock()
		return nil, errors.New("service: a tiered benchmark is already running")
	}
	s.bench.tieredRunning = true
	s.bench.mu.Unlock()
	defer func() {
		s.bench.mu.Lock()
		s.bench.tieredRunning = false
		s.bench.mu.Unlock()
	}()

	start := time.Now()
	rep := &TieredReport{At: start}
	for _, t := range benchTiers {
		res, err := s.measureTier(t, records, streams)
		if err != nil {
			return nil, fmt.Errorf("tier %s: %w", t.name, err)
		}
		rep.Tiers = append(rep.Tiers, res)
	}
	rep.DurationS = time.Since(start).Seconds()

	s.bench.mu.Lock()
	s.bench.tieredLatest = rep
	s.bench.tieredHistory = append([]TieredReport{*rep}, s.bench.tieredHistory...)
	if len(s.bench.tieredHistory) > 20 {
		s.bench.tieredHistory = s.bench.tieredHistory[:20]
	}
	s.bench.mu.Unlock()
	return rep, nil
}

// measureTier runs one tier: a single stream (isolated throughput) then
// `streams` concurrent (aggregate throughput + scaling efficiency).
func (s *Service) measureTier(t tier, records int64, streams int) (TierResult, error) {
	res := TierResult{Tier: t.name, Shape: t.shape, RecordsPerStream: records, ConcurrentStreams: streams}

	// Single stream.
	id, err := s.Submit(t.flow(records), true)
	if err != nil {
		return res, err
	}
	tk, err := s.awaitTask(id, 10*time.Minute)
	if err != nil {
		return res, err
	}
	secs := tk.Finished.Sub(*tk.Started).Seconds()
	if secs <= 0 {
		return res, errors.New("tier finished instantly; increase records")
	}
	res.SingleStreamRecS = float64(tk.RecordsIn) / secs

	// Concurrent streams.
	ids := make([]string, streams)
	wallStart := time.Now()
	for i := range streams {
		id, err := s.Submit(t.flow(records), true)
		if err != nil {
			return res, err
		}
		ids[i] = id
	}
	var totalIn int64
	for _, id := range ids {
		tk, err := s.awaitTask(id, 10*time.Minute)
		if err != nil {
			return res, err
		}
		totalIn += tk.RecordsIn
	}
	wall := time.Since(wallStart).Seconds()
	if wall > 0 {
		res.AggregateRecS = float64(totalIn) / wall
	}
	if res.SingleStreamRecS > 0 && streams > 0 {
		res.ScalingEfficiency = res.AggregateRecS / (res.SingleStreamRecS * float64(streams))
	}
	return res, nil
}
