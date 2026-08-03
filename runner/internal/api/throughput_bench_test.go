package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/secretref"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/task"
	"github.com/aaron-au/shift/runner/internal/webhook"
)

// Trigger-path throughput (ADR-0008 capacity, ADR-0035 §3).
//
// The engine's RECORD throughput is benchmarked elsewhere (shift-bench,
// docs/bench-M1.md). What was never measured is the TRIGGER path: how many
// flow invocations per second a runner sustains through its public HTTP
// surface. That is the number webhook and request-reply workloads live on,
// and the one any "N tps" claim has to rest on.
//
// These run over a real httptest server and a real connector subprocess, so
// they include the HTTP stack, admission (ADR-0005), connector pool
// checkout, and the engine. They deliberately exclude a hub: the point is
// what a runner does on its own, and every hub round trip taken off these
// paths shows up here as throughput.
//
// Pin the core count to a licence tier (ADR-0033):
//
//	GOMAXPROCS=4 go test ./internal/api/ -run '^$' -bench Throughput -benchtime 5s
//
// b.ReportMetric emits tps directly, so a result needs no arithmetic.
//
// Two measurement traps this file avoids, both of which produced badly
// wrong numbers first time round:
//
//   - Cold start. Spawning the first connector subprocess costs ~1s. Without
//     a warm-up the sync benchmarks settle at b.N=1 and report ~1 tps, which
//     is the subprocess launch, not the path.
//   - Async overload. The webhook path answers 202 and executes later, so an
//     open-loop b.N accept race cannot measure throughput: Go scales b.N from
//     the accept time, accepts outrun execution, and the run ends with
//     thousands of tasks still queued (6064 waiting, on the attempt that made
//     this comment necessary). The webhook benchmark is CLOSED-LOOP — a
//     bounded number of invocations in flight — so accept rate cannot exceed
//     completion rate and b.N measures real service time.
//   - Measuring the instrument. Completion is read from the service's lifetime
//     totals, NOT from the execution-report callback: reportWhenDone polls at
//     200ms, so gating on it capped the result at inFlight/200ms (140 tps for
//     a path that does ~4500) — the benchmark was measuring its own poller.

// A minimal flow: one record in, one out. This measures per-invocation
// OVERHEAD; a larger payload would just re-measure the engine.
const benchFlow = `{"name":"bench",
  "source":{"connector":"gen","action":"gen","config":{"records":1}},
  "sink":{"connector":"gen","action":"discard"}}`

// benchRunner builds the gen connector once and returns a live server with
// its connector pool already warm.
func benchRunner(b *testing.B, report ExecReporter) (*httptest.Server, *service.Service) {
	b.Helper()
	dir := b.TempDir()
	cmd := exec.CommandContext(context.Background(), "go", "build", //nolint:gosec // G204: builds our own package
		"-o", filepath.Join(dir, "shift-connector-gen"),
		"github.com/aaron-au/shift/connectors/cmd/shift-connector-gen")
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("build gen connector: %v\n%s", err, out)
	}
	svc := service.New(service.Options{ConnectorDir: dir})
	b.Cleanup(func() { _ = svc.Close(30 * time.Second) })

	h := Handler(svc, Options{
		RunnerName: "bench", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: webhook.NewRegistry(),
		Report: report, Secrets: secretref.New(nil),
	})
	srv := httptest.NewServer(h)
	b.Cleanup(srv.Close)

	// Warm the pool: the first invocation pays connector process launch.
	// Leaving it inside the timed region is what pinned b.N at 1.
	client := benchClient()
	for range runtime.GOMAXPROCS(0) {
		if code := post(b, client, srv.URL+"/api/flows/run", benchFlow); code != http.StatusOK {
			b.Fatalf("warm-up run = %d, want 200", code)
		}
	}
	return srv, svc
}

func benchClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// Keep-alive headroom, else the benchmark measures TCP setup.
	tr.MaxIdleConnsPerHost = 512
	return &http.Client{Transport: tr, Timeout: 60 * time.Second}
}

// post issues one request and drains the body so the connection is reused.
func post(b *testing.B, client *http.Client, url, body string) int {
	b.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		b.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		b.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// reportTPS emits transactions/sec alongside the core count they were
// achieved on, so a result compares directly against a licence tier.
func reportTPS(b *testing.B, start time.Time) {
	b.Helper()
	if elapsed := time.Since(start).Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "tps")
		b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cores")
	}
}

// BenchmarkSyncRunThroughput is the request-reply path (ADR-0024): the
// caller waits for the flow's output in the same response. Self-limiting —
// each request occupies its goroutine until the flow finishes — so
// RunParallel gives sustained throughput directly.
func BenchmarkSyncRunThroughput(b *testing.B) {
	srv, _ := benchRunner(b, nil)
	client := benchClient()
	url := srv.URL + "/api/flows/run"

	start := time.Now()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if code := post(b, client, url, benchFlow); code != http.StatusOK {
				b.Fatalf("run = %d, want 200", code)
			}
		}
	})
	b.StopTimer()
	reportTPS(b, start)
}

// BenchmarkWebhookThroughput is the push-trigger path (ADR-0016): accept,
// admit, answer 202, execute asynchronously.
//
// Closed-loop: at most inFlightCap invocations are outstanding at once, so
// accepts cannot outrun executions and the figure is sustained END-TO-END
// throughput. An open-loop version measures only how fast a queue fills.
func BenchmarkWebhookThroughput(b *testing.B) {
	srv, svc := benchRunner(b, nil)
	client := benchClient()

	hookDoc := `{"document":{"name":"hook",
	  "source":{"connector":"@webhook","action":"ndjson"},
	  "sink":{"connector":"gen","action":"discard"}}}`
	if code := putJSON(b, client, srv.URL+"/api/webhooks/bench", hookDoc); code != http.StatusOK {
		b.Fatalf("register hook = %d", code)
	}

	// Deep enough to keep every core busy, shallow enough that the backlog
	// stays bounded and the run ends when the accepts do.
	inFlightCap := 8 * runtime.GOMAXPROCS(0)
	slots := make(chan struct{}, inFlightCap)
	for range inFlightCap {
		slots <- struct{}{}
	}
	base := finished(svc)
	stop := make(chan struct{})
	defer close(stop)
	// One releaser returns a slot per completed execution. Lifetime totals
	// are the signal: unlike the result ring they never evict, and unlike
	// the report callback they carry no polling interval of their own.
	go func() {
		var released int64
		for {
			for done := finished(svc) - base; released < done; released++ {
				select {
				case slots <- struct{}{}:
				case <-stop:
					return
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()

	url := srv.URL + "/hooks/bench"
	start := time.Now()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			<-slots // wait for an earlier invocation to finish
			if code := post(b, client, url, `{"id":1}`); code != http.StatusAccepted {
				b.Fatalf("hook = %d, want 202", code)
			}
		}
	})
	// Include the tail: the last in-flight invocations are real work.
	deadline := time.Now().Add(time.Minute)
	for finished(svc)-base < int64(b.N) {
		if time.Now().After(deadline) {
			b.Fatalf("only %d/%d executions completed", finished(svc)-base, b.N)
		}
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()
	reportTPS(b, start)
}

// finished counts terminal executions over the service's lifetime.
func finished(svc *service.Service) int64 {
	t := svc.Status().Totals
	return t.Completed + t.Failed
}

// BenchmarkSyncRunReportOverhead quantifies what inline reporting cost. The
// stand-in hub is one WAN round trip away; with the report off the response
// path (ADR-0035 §3) this must track BenchmarkSyncRunThroughput instead of
// collapsing toward 1/latency per in-flight request.
func BenchmarkSyncRunReportOverhead(b *testing.B) {
	const hubLatency = 5 * time.Millisecond
	srv, _ := benchRunner(b, func(task.Task, string) { time.Sleep(hubLatency) })
	client := benchClient()
	url := srv.URL + "/api/flows/run"

	start := time.Now()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if code := post(b, client, url, benchFlow); code != http.StatusOK {
				b.Fatalf("run = %d, want 200", code)
			}
		}
	})
	b.StopTimer()
	reportTPS(b, start)
}

func putJSON(b *testing.B, client *http.Client, url, body string) int {
	b.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewReader([]byte(body)))
	if err != nil {
		b.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		b.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}
