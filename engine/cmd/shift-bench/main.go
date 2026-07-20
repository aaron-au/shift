// Command shift-bench measures the streaming engine against the M1 exit
// criteria (ADR-0003): bounded RSS regardless of payload size, zero disk
// below the watermark, honest CPU/allocation metrics. The baseline scenario
// is the naive buffered implementation the engine exists to beat.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/aaron-au/shift/engine/format/csvf"
	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/mem"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
)

func main() {
	var (
		scenario  = flag.String("scenario", "transform", "transform | csv | aggregate | baseline")
		sizeStr   = flag.String("bytes", "256MiB", "input size (e.g. 64MiB, 1GiB)")
		watermark = flag.String("watermark", "64MiB", "memory watermark for stateful operators")
		groups    = flag.Int64("groups", 500_000, "distinct group keys (aggregate scenario)")
		maxRSS    = flag.String("max-rss", "", "fail if peak RSS exceeds this (e.g. 100MiB)")
		spillDir  = flag.String("spill-dir", "", "scratch store directory (default: OS temp)")
		runs      = flag.Int("runs", 1, "repeat the scenario N times (reports each + a summary)")
		warmup    = flag.Bool("warmup", false, "run one untimed warmup before measuring (steady-state, warm caches)")
		jsonOut   = flag.Bool("json", false, "emit machine-readable JSON instead of the human report")
	)
	flag.Parse()

	size, err := parseSize(*sizeStr)
	if err != nil {
		fatal(err)
	}
	if *groups < 1 {
		fatal(fmt.Errorf("-groups must be >= 1"))
	}
	if *runs < 1 {
		fatal(fmt.Errorf("-runs must be >= 1"))
	}
	wm, err := parseSize(*watermark)
	if err != nil {
		fatal(err)
	}
	var rssLimit int64 = -1
	if *maxRSS != "" {
		rssLimit, err = parseSize(*maxRSS)
		if err != nil {
			fatal(err)
		}
	}

	r := runner{size: size, watermark: wm, groups: *groups, spillDir: *spillDir}

	if *warmup {
		if _, err := r.run(*scenario); err != nil {
			fatal(err)
		}
	}

	results := make([]result, 0, *runs)
	for i := 0; i < *runs; i++ {
		res, err := r.run(*scenario)
		if err != nil {
			fatal(err)
		}
		results = append(results, res)
	}

	if *jsonOut {
		emitJSON(results, rssLimit)
	} else {
		for i, res := range results {
			if *runs > 1 {
				fmt.Printf("=== run %d/%d ===\n", i+1, *runs)
			}
			res.print()
		}
		if *runs > 1 {
			printSummary(results)
		}
	}

	// Gate on the worst (highest) peak RSS across all runs.
	if rssLimit >= 0 {
		worst := int64(0)
		for _, res := range results {
			if res.peakRSS > worst {
				worst = res.peakRSS
			}
		}
		if worst > rssLimit {
			fmt.Fprintf(os.Stderr, "FAIL: peak RSS %s exceeds limit %s\n", fmtBytes(worst), fmtBytes(rssLimit))
			os.Exit(1)
		}
		// Gate status goes to stderr so -json stdout stays pure JSON.
		fmt.Fprintf(os.Stderr, "PASS: peak RSS %s within limit %s\n", fmtBytes(worst), fmtBytes(rssLimit))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "shift-bench:", err)
	os.Exit(1)
}

type runner struct {
	size      int64
	watermark int64
	groups    int64
	spillDir  string
}

type result struct {
	scenario   string
	inputBytes int64
	report     stream.Report
	spillBytes int64
	peakRSS    int64
	userCPU    string
	sysCPU     string
	totalAlloc uint64
	mallocs    uint64
	gcCycles   uint32
}

func (r runner) run(scenario string) (result, error) {
	ctx := context.Background()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var rep stream.Report
	var spillBytes int64
	var err error
	switch scenario {
	case "transform":
		rep, err = r.transform(ctx)
	case "csv":
		rep, err = r.csv(ctx)
	case "aggregate":
		rep, spillBytes, err = r.aggregate(ctx)
	case "baseline":
		rep, err = r.baseline(ctx)
	default:
		return result{}, fmt.Errorf("unknown scenario %q", scenario)
	}
	if err != nil {
		return result{}, err
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	rss, user, sys := procUsage()
	return result{
		scenario:   scenario,
		inputBytes: r.size,
		report:     rep,
		spillBytes: spillBytes,
		peakRSS:    rss,
		userCPU:    user.String(),
		sysCPU:     sys.String(),
		totalAlloc: after.TotalAlloc - before.TotalAlloc,
		mallocs:    after.Mallocs - before.Mallocs,
		gcCycles:   after.NumGC - before.NumGC,
	}, nil
}

// transform: NDJSON in -> flatten -> filter -> project -> NDJSON out.
func (r runner) transform(ctx context.Context) (stream.Report, error) {
	src := ndjson.NewReader(newGenerator("ndjson", r.size, r.groups), ndjson.ReaderOptions{})
	p := stream.New(src, "read-ndjson").
		Flatten("_").
		Filter("active-only", func(v record.Value) bool {
			a, _ := v.Field("active")
			return a.Bool()
		}).
		Project(
			stream.ProjectField{From: record.MustParsePath("$.id")},
			stream.ProjectField{From: record.MustParsePath("$.name")},
			stream.ProjectField{Out: "city", From: record.MustParsePath("$.address_city")},
			stream.ProjectField{From: record.MustParsePath("$.amount")},
			stream.ProjectField{From: record.MustParsePath("$.region")},
		)
	return p.Run(ctx, ndjson.NewWriter(io.Discard), "write-ndjson")
}

// csv: typed CSV in -> filter -> NDJSON out.
func (r runner) csv(ctx context.Context) (stream.Report, error) {
	src := csvf.NewReader(newGenerator("csv", r.size, r.groups), csvf.ReaderOptions{
		Types: map[string]csvf.ColumnType{
			"id":     csvf.TypeInt,
			"amount": csvf.TypeFloat,
			"active": csvf.TypeBool,
		},
	})
	p := stream.New(src, "read-csv").
		Filter("active-only", func(v record.Value) bool {
			a, _ := v.Field("active")
			return a.Bool()
		})
	return p.Run(ctx, ndjson.NewWriter(io.Discard), "write-ndjson")
}

// aggregate: NDJSON in -> group by region -> NDJSON out. High group
// cardinality plus the watermark exercises spill.
func (r runner) aggregate(ctx context.Context) (stream.Report, int64, error) {
	gov := mem.New(r.watermark)
	src := ndjson.NewReader(newGenerator("ndjson", r.size, r.groups), ndjson.ReaderOptions{})
	p := stream.New(src, "read-ndjson")
	p = p.Aggregate(stream.AggregateSpec{
		Key:      record.MustParsePath("$.region"),
		Gov:      gov,
		SpillDir: r.spillDir,
		Aggs: []stream.Agg{
			{Op: stream.AggCount, Out: "n"},
			{Op: stream.AggSum, From: record.MustParsePath("$.amount"), Out: "total"},
			{Op: stream.AggMax, From: record.MustParsePath("$.amount"), Out: "hi"},
		},
	})
	spill := stream.SpillBytes(p)
	rep, err := p.Run(ctx, ndjson.NewWriter(io.Discard), "write-ndjson")
	return rep, spill(), err
}

// baseline: the disk-light streaming engine's antithesis — read everything
// into []map[string]any, transform, marshal back out. Exists to quantify
// what the engine saves. Refuses very large inputs.
func (r runner) baseline(ctx context.Context) (stream.Report, error) {
	if r.size > 2<<30 {
		return stream.Report{}, fmt.Errorf("baseline refuses inputs over 2GiB (it buffers everything)")
	}
	return runBaseline(ctx, newGenerator("ndjson", r.size, r.groups))
}

func (res result) print() {
	rep := res.report
	secs := float64(rep.WallNanos) / 1e9
	var recs int64
	if len(rep.Ops) > 0 {
		recs = rep.Ops[0].RecordsIn // records entering the pipeline
	}
	fmt.Printf("scenario    %s\n", res.scenario)
	fmt.Printf("input       %s (%s records)\n", fmtBytes(res.inputBytes), fmtCount(recs))
	fmt.Printf("wall        %.2fs   %s/s   %s rec/s\n",
		secs, fmtBytes(int64(float64(res.inputBytes)/secs)), fmtCount(int64(float64(recs)/secs)))
	fmt.Printf("cpu         user %s   sys %s\n", res.userCPU, res.sysCPU)
	fmt.Printf("peak rss    %s\n", fmtBytes(res.peakRSS))
	perRec := uint64(0)
	allocsPerRec := float64(0)
	if recs > 0 {
		perRec = res.totalAlloc / uint64(recs)
		allocsPerRec = float64(res.mallocs) / float64(recs)
	}
	fmt.Printf("heap        %s total   %d B/rec   %.2f allocs/rec   %d gc cycles\n",
		fmtBytes(int64(res.totalAlloc)), perRec, allocsPerRec, res.gcCycles) //nolint:gosec // display only; heap totals fit int64
	fmt.Printf("spill       %s\n", fmtBytes(res.spillBytes))
	fmt.Printf("records out %s\n", fmtCount(rep.RecordsOut))
	for _, op := range rep.Ops {
		fmt.Printf("  op %-14s %8.2fs  in %-12s out %-12s\n",
			op.Name, float64(op.Nanos)/1e9, fmtCount(op.RecordsIn), fmtCount(op.RecordsOut))
	}
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	// Ordered longest-first so "MiB" wins over "B".
	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1},
	}
	for _, sf := range suffixes {
		if strings.HasSuffix(s, sf.suffix) {
			mult = sf.mult
			s = strings.TrimSuffix(s, sf.suffix)
			break
		}
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return int64(n * float64(mult)), nil
}

func fmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func fmtCount(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	default:
		return strconv.FormatInt(n, 10)
	}
}
