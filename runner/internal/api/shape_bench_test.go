package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// Does trigger throughput change with integration complexity?
//
// BenchmarkSyncRunThroughput answers only for the floor case — one record,
// source straight to sink — which is deliberately the cheapest flow that can
// exist, so it measures per-invocation OVERHEAD and nothing else. That is the
// right number for "how many triggers per second", and the wrong number for
// "how many of MY integrations per second".
//
// Per invocation the runner pays two separable costs:
//
//	fixed     HTTP, admission (ADR-0005), connector pool checkout, plan build,
//	          task record — independent of what the flow does
//	variable  records x operators — the engine's actual work
//
// These benchmarks sweep each axis with the other pinned, so the two can be
// read apart instead of guessed at:
//
//	GOMAXPROCS=4 go test ./internal/api/ -run '^$' -bench Shape -benchtime 3s
//
// Shapes mirror the tiers in internal/service/bench_tiers.go, so a figure here
// (invocations/sec) sits alongside the tiered capacity report (records/sec)
// for the same workload rather than describing a different one.

type shape struct {
	name string
	ops  string // JSON array body for "ops", "" = passthrough
}

var shapes = []shape{
	{"simple", ""},
	{"standard", `[
	  {"type":"filter","path":"$.active","op":"eq","value":true},
	  {"type":"coerce","rules":[{"field":"amount","to":"int"}]},
	  {"type":"project","fields":[{"path":"$.id"},{"path":"$.name"},
	    {"path":"$.amount"},{"path":"$.region"}]}
	]`},
	{"complex", `[
	  {"type":"flatten","sep":"_"},
	  {"type":"aggregate","key":"$.region","aggs":[
	    {"op":"count","out":"n"},{"op":"sum","path":"$.amount","out":"total"}]}
	]`},
	{"extreme", `[
	  {"type":"filter","path":"$.active","op":"eq","value":true},
	  {"type":"flatten","sep":"_"},
	  {"type":"project","fields":[{"path":"$.id"},{"path":"$.name"},
	    {"path":"$.amount"},{"path":"$.region"},{"out":"city","path":"$.address_city"}]},
	  {"type":"aggregate","key":"$.region","aggs":[
	    {"op":"count","out":"n"},{"op":"sum","path":"$.amount","out":"total"}]}
	]`},
}

// shapeFlow builds a gen -> ops -> discard document with the given record
// count. groups tracks records so aggregate cardinality does not silently
// become the variable under test.
func shapeFlow(b *testing.B, s shape, records int) string {
	b.Helper()
	groups := records
	if groups > 50_000 {
		groups = 50_000
	}
	if groups < 1 {
		groups = 1
	}
	doc := `{"name":"shape-` + s.name + `",` +
		`"source":{"connector":"gen","action":"gen","config":{"records":` +
		strconv.Itoa(records) + `,"groups":` + strconv.Itoa(groups) + `}},`
	if s.ops != "" {
		doc += `"ops":` + s.ops + `,`
	}
	doc += `"sink":{"connector":"gen","action":"discard"}}`
	if !json.Valid([]byte(doc)) {
		b.Fatalf("shape %s produced invalid JSON", s.name)
	}
	return doc
}

// BenchmarkShapeComplexity holds records at 1 and varies transform depth.
// Whatever moves here is NOT the engine doing more work on more data — it is
// the cost of a deeper plan on a single record. Flat results mean fixed cost
// dominates and "tps" is a property of the trigger path, not of the flow.
func BenchmarkShapeComplexity(b *testing.B) {
	for _, s := range shapes {
		b.Run(s.name, func(b *testing.B) {
			runShape(b, shapeFlow(b, s, 1))
		})
	}
}

// BenchmarkShapeRecords varies the payload, for a realistic
// filter/coerce/project integration and for the heaviest shape there is. This
// is where tps falls away, and where a single invocations/sec figure stops
// meaning anything without the payload size attached.
//
// Both series run because complexity and payload are not independent: at one
// record a deeper plan is nearly free, and the difference only appears once
// there is data for the extra operators to touch.
func BenchmarkShapeRecords(b *testing.B) {
	for _, s := range []shape{shapes[1], shapes[3]} { // standard, extreme
		for _, records := range []int{1, 100, 1000, 10_000} {
			b.Run(s.name+"/"+strconv.Itoa(records), func(b *testing.B) {
				runShape(b, shapeFlow(b, s, records))
			})
		}
	}
}

// runShape drives the synchronous path, which is self-limiting: each request
// holds its goroutine until the flow finishes, so RunParallel yields sustained
// throughput without a separate completion signal.
func runShape(b *testing.B, doc string) {
	b.Helper()
	srv, _ := benchRunner(b, nil)
	client := benchClient()
	url := srv.URL + "/api/flows/run"

	// One untimed call proves the shape actually runs: a flow that 422s would
	// otherwise benchmark the error path and look impressively fast.
	if code := post(b, client, url, doc); code != http.StatusOK {
		b.Fatalf("flow did not run: %d", code)
	}

	start := time.Now()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if code := post(b, client, url, doc); code != http.StatusOK {
				b.Fatalf("run = %d, want 200", code)
			}
		}
	})
	b.StopTimer()
	reportTPS(b, start)
}
