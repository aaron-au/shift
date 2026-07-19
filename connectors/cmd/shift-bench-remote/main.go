// Command shift-bench-remote quantifies the out-of-process connector
// overhead (M2 exit criterion, ADR-0007): the same pipeline runs once with
// in-process source/sink and once through a spawned connector subprocess;
// the delta is the transport cost.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aaron-au/shift/connectors/internal/genconn"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
	"github.com/aaron-au/shift/sdk/host"
)

func main() {
	var (
		records   = flag.Int64("records", 2_000_000, "records to stream")
		connector = flag.String("connector", "bin/shift-connector-gen", "gen connector binary (remote mode)")
		maxRatio  = flag.Float64("max-ratio", 0, "fail if remote wall time exceeds local x ratio (0 = report only)")
	)
	flag.Parse()

	local, err := run("local  (in-process)", localSource(*records), &countSink{})
	if err != nil {
		fatal(err)
	}
	remote, err := runRemote(*connector, *records)
	if err != nil {
		fatal(err)
	}

	ratio := remote.Seconds() / local.Seconds()
	fmt.Printf("\noverhead    remote/local wall ratio %.2fx\n", ratio)
	if *maxRatio > 0 {
		if ratio > *maxRatio {
			fmt.Fprintf(os.Stderr, "FAIL: ratio %.2fx exceeds limit %.2fx\n", ratio, *maxRatio)
			os.Exit(1)
		}
		fmt.Printf("PASS: ratio %.2fx within limit %.2fx\n", ratio, *maxRatio)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "shift-bench-remote:", err)
	os.Exit(1)
}

// pipeline applies the same ops in both modes.
func buildPipeline(src stream.Source, name string) *stream.Pipeline {
	return stream.New(src, name).
		Filter("active-only", func(v record.Value) bool {
			a, _ := v.Field("active")
			return a.Bool()
		}).
		Project(
			stream.ProjectField{From: record.MustParsePath("$.id")},
			stream.ProjectField{From: record.MustParsePath("$.name")},
			stream.ProjectField{Out: "city", From: record.MustParsePath("$.address.city")},
			stream.ProjectField{From: record.MustParsePath("$.amount")},
		)
}

func run(label string, src stream.Source, sink stream.Sink) (time.Duration, error) {
	start := time.Now()
	rep, err := buildPipeline(src, "source").Run(context.Background(), sink, "sink")
	if err != nil {
		return 0, fmt.Errorf("%s: %w", label, err)
	}
	wall := time.Since(start)
	recsIn := rep.Ops[0].RecordsIn
	fmt.Printf("%-22s %8.2fs   %10.0f rec/s   in %d out %d\n",
		label, wall.Seconds(), float64(recsIn)/wall.Seconds(), recsIn, rep.RecordsOut)
	return wall, nil
}

func runRemote(binary string, records int64) (time.Duration, error) {
	ctx := context.Background()
	p, err := host.Launch(ctx, binary, host.LaunchOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = p.Close() }()
	src := p.Source("gen", fmt.Appendf(nil, `{"records":%d}`, records))
	sink := p.Sink("discard", nil)
	return run("remote (subprocess)", src, sink)
}

// localSource adapts genconn's source for in-process use.
func localSource(records int64) stream.Source {
	src, err := genconn.OpenLocal(fmt.Appendf(nil, `{"records":%d}`, records))
	if err != nil {
		fatal(err)
	}
	return src
}

// countSink is the in-process analogue of the discard sink.
type countSink struct{ n int64 }

func (c *countSink) Write(_ context.Context, b *record.Batch) error {
	c.n += int64(b.Len())
	return nil
}
func (c *countSink) Close() error { return nil }
