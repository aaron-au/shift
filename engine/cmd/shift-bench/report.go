package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

// metrics is the machine-readable view of one run: derived, unit-tagged
// numbers suitable for JSON output, benchstat-style comparison, and the
// rendered results table. Raw counters live on result; this is the report.
type metrics struct {
	Scenario     string  `json:"scenario"`
	InputBytes   int64   `json:"input_bytes"`
	RecordsIn    int64   `json:"records_in"`
	RecordsOut   int64   `json:"records_out"`
	WallNanos    int64   `json:"wall_nanos"`
	WallSeconds  float64 `json:"wall_seconds"`
	MiBPerSec    float64 `json:"mib_per_sec"`
	RecordsPerS  float64 `json:"records_per_sec"`
	PeakRSSBytes int64   `json:"peak_rss_bytes"`
	HeapTotal    uint64  `json:"heap_total_bytes"`
	BytesPerRec  uint64  `json:"bytes_per_record"`
	AllocsPerRec float64 `json:"allocs_per_record"`
	GCCycles     uint32  `json:"gc_cycles"`
	SpillBytes   int64   `json:"spill_bytes"`
}

func (res result) metrics() metrics {
	secs := float64(res.report.WallNanos) / 1e9
	var recs int64
	if len(res.report.Ops) > 0 {
		recs = res.report.Ops[0].RecordsIn
	}
	m := metrics{
		Scenario:     res.scenario,
		InputBytes:   res.inputBytes,
		RecordsIn:    recs,
		RecordsOut:   res.report.RecordsOut,
		WallNanos:    res.report.WallNanos,
		WallSeconds:  secs,
		PeakRSSBytes: res.peakRSS,
		HeapTotal:    res.totalAlloc,
		GCCycles:     res.gcCycles,
		SpillBytes:   res.spillBytes,
	}
	if secs > 0 {
		m.MiBPerSec = float64(res.inputBytes) / (1 << 20) / secs
		m.RecordsPerS = float64(recs) / secs
	}
	if recs > 0 {
		m.BytesPerRec = res.totalAlloc / uint64(recs)
		m.AllocsPerRec = float64(res.mallocs) / float64(recs)
	}
	return m
}

// benchReport is the top-level JSON document (-json). Runs holds every
// measured run; Summary reports the median wall / throughput and the worst
// (highest) peak RSS — the number the RSS gate checks.
type benchReport struct {
	Scenario   string    `json:"scenario"`
	Runs       []metrics `json:"runs"`
	Summary    summary   `json:"summary"`
	RSSLimit   int64     `json:"rss_limit_bytes,omitempty"`
	RSSLimited bool      `json:"rss_limited"`
}

type summary struct {
	Count           int     `json:"count"`
	MedianWallNanos int64   `json:"median_wall_nanos"`
	MedianMiBPerSec float64 `json:"median_mib_per_sec"`
	WorstPeakRSS    int64   `json:"worst_peak_rss_bytes"`
}

func summarize(results []result) summary {
	walls := make([]int64, len(results))
	mibs := make([]float64, len(results))
	worst := int64(0)
	for i, res := range results {
		m := res.metrics()
		walls[i] = m.WallNanos
		mibs[i] = m.MiBPerSec
		if res.peakRSS > worst {
			worst = res.peakRSS
		}
	}
	slices.Sort(walls)
	slices.Sort(mibs)
	return summary{
		Count:           len(results),
		MedianWallNanos: medianInt64(walls),
		MedianMiBPerSec: medianFloat(mibs),
		WorstPeakRSS:    worst,
	}
}

func medianInt64(sorted []int64) int64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func medianFloat(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func emitJSON(results []result, rssLimit int64) {
	runs := make([]metrics, len(results))
	for i, res := range results {
		runs[i] = res.metrics()
	}
	rep := benchReport{
		Scenario:   results[0].scenario,
		Runs:       runs,
		Summary:    summarize(results),
		RSSLimited: rssLimit >= 0,
	}
	if rssLimit >= 0 {
		rep.RSSLimit = rssLimit
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		fatal(err)
	}
}

func printSummary(results []result) {
	s := summarize(results)
	fmt.Printf("=== summary (%d runs) ===\n", s.Count)
	fmt.Printf("median wall %.2fs   %.1f MiB/s   worst peak rss %s\n",
		float64(s.MedianWallNanos)/1e9, s.MedianMiBPerSec, fmtBytes(s.WorstPeakRSS))
}
