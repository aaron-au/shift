package ingress_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// End-to-end request-reply benchmark: caller → gateway → parked runner →
// (simulated connector work) → back, over real HTTP on both hops.
//
// The runner here is a stub rather than the real engine, ON PURPOSE. The
// engine's own cost is already measured (docs/bench-M1.md) and the runner's
// trigger path separately (docs/dev/04-runner.md); what is unmeasured, and
// what this measures, is the GATEWAY's contribution — the dispatch hand-over,
// the two-request exchange, and how they behave under concurrency.
//
// Run:
//
//	go test ./gateway/internal/ingress -run xxx -bench Gateway -benchtime 3s
//	SHIFT_BENCH_PROFILE=1 go test ./gateway/internal/ingress -run TestGatewayLatencyProfile -v
//
// Backends model what a connector actually does — talk to something else and
// wait — because a zero-latency stub measures Go's scheduler, not this system.

// backend is one simulated downstream: a service time distribution.
type backend struct {
	name string
	// base is the floor; jitter is added uniformly on top. p99 spikes model
	// the tail every real integration has (GC pause, connection re-establish,
	// a cold index on the far side).
	base, jitter time.Duration
	spikeRate    float64 // fraction of calls that hit the slow path
	spike        time.Duration
}

// median is the backend's expected p50 service time: the floor plus half the
// uniform jitter. Spikes are deliberately excluded — at 1-3% they move the
// tail, not the median.
//
// Subtracting the FLOOR instead (the obvious mistake) overstates the
// platform's share by half the jitter, which for the rest-20ms shape is 5ms
// against a real cost under 1ms.
func (b backend) median() time.Duration { return b.base + b.jitter/2 }

func (b backend) work(rnd *rand.Rand) time.Duration {
	d := b.base
	if b.jitter > 0 {
		d += time.Duration(rnd.Int64N(int64(b.jitter)))
	}
	if b.spikeRate > 0 && rnd.Float64() < b.spikeRate {
		d += b.spike
	}
	return d
}

var backends = []backend{
	// The gateway's own overhead, with nothing underneath it. Any number here
	// is pure framework cost — this is the figure to defend.
	{name: "instant", base: 0},
	// A local cache or an in-memory lookup.
	{name: "fast-1ms", base: 800 * time.Microsecond, jitter: 400 * time.Microsecond},
	// A same-region REST API: the shape most request-reply flows actually are.
	{name: "rest-20ms", base: 15 * time.Millisecond, jitter: 10 * time.Millisecond,
		spikeRate: 0.01, spike: 100 * time.Millisecond},
	// A database round trip with a contended pool.
	{name: "db-50ms", base: 40 * time.Millisecond, jitter: 25 * time.Millisecond,
		spikeRate: 0.02, spike: 250 * time.Millisecond},
	// A slow SOAP/legacy endpoint — the case where the gateway must simply not
	// be the problem.
	{name: "legacy-200ms", base: 150 * time.Millisecond, jitter: 120 * time.Millisecond,
		spikeRate: 0.03, spike: 800 * time.Millisecond},
}

// rig is a gateway plus N stub runners, wired over real HTTP.
type rig struct {
	public  *httptest.Server
	control *httptest.Server
	reg     *runners.Registry
	stop    func()
	served  atomic.Int64
}

func newRig(tb testing.TB, nRunners int, b backend, payload int) *rig {
	tb.Helper()

	reg := runners.New()
	reg.DeliveryTimeout = 30 * time.Second
	pub := ingress.New(reg, discardLogger())
	if err := pub.SetConfig(&config.Config{Version: 1, Routes: []config.Route{{
		Path: "/bench", Method: http.MethodPost, Flow: "bench",
		Selector: config.Selector{"environment": "production"},
	}}}); err != nil {
		tb.Fatal(err)
	}

	mux := http.NewServeMux()
	ingress.NewDispatch(reg, discardLogger(), "").Routes(mux)
	r := &rig{
		public:  httptest.NewServer(pub),
		control: httptest.NewServer(mux),
		reg:     reg,
	}

	body := bytes.Repeat([]byte("x"), payload)
	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := range nRunners {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			// Deterministic per-runner stream: reproducible without every
			// runner walking the same sequence. Reproducibility is the point —
			// a benchmark whose service times vary run to run cannot be
			// compared against itself.
			rnd := rand.New(rand.NewPCG(uint64(seed)+1, 0x5eed)) //nolint:gosec // G404: simulated service times, not a credential
			cl := &http.Client{Transport: &http.Transport{
				MaxIdleConnsPerHost: 64, // one runner keeps its connections warm
			}}
			for {
				select {
				case <-done:
					return
				default:
				}
				if !r.serveOnce(cl, rnd, b, body) {
					return
				}
			}
		}(i)
	}

	r.stop = func() {
		close(done)
		r.public.Close()
		r.control.Close()
		wg.Wait()
	}
	return r
}

// serveOnce is one poll→execute→deliver cycle. It returns false when the rig
// is shutting down.
func (r *rig) serveOnce(cl *http.Client, rnd *rand.Rand, b backend, out []byte) bool {
	pollBody, _ := json.Marshal(map[string]any{
		"labels":       map[string]string{"environment": "production", "workload": "api"},
		"wait_seconds": 1,
	})
	req, err := http.NewRequest(http.MethodPost, r.control.URL+"/api/v1/gw/poll", bytes.NewReader(pollBody)) //nolint:noctx // bench rig; the poll window bounds it
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cl.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode == http.StatusNoContent {
		_ = resp.Body.Close()
		return true // empty window
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return false
	}
	id := resp.Header.Get("X-Shift-Request-Id")
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	// The connector: go somewhere else and wait.
	if d := b.work(rnd); d > 0 {
		time.Sleep(d)
	}

	dreq, err := http.NewRequest(http.MethodPost, r.control.URL+"/api/v1/gw/deliver/"+id, bytes.NewReader(out)) //nolint:noctx // bench rig
	if err != nil {
		return false
	}
	dreq.Header.Set("Content-Type", "application/x-ndjson")
	dresp, err := cl.Do(dreq)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, dresp.Body)
	_ = dresp.Body.Close()
	r.served.Add(1)
	return true
}

// discardLogger silences the gateway during a benchmark. The default logger
// writes a line per 503 and per delivery timeout, and at the request rates
// below that is both noise and measurable I/O in the hot path.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// BenchmarkGatewayRequestReply measures the full caller-visible round trip
// against each backend shape, with a runner pool deep enough not to be the
// bottleneck.
func BenchmarkGatewayRequestReply(bch *testing.B) {
	for _, b := range backends {
		bch.Run(b.name, func(bch *testing.B) {
			r := newRig(bch, 32, b, 256)
			defer r.stop()
			waitParkedN(bch, r.reg, 1)

			cl := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
			body := []byte(`{"order":1}`)

			bch.ResetTimer()
			bch.ReportAllocs()
			bch.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req, err := http.NewRequest(http.MethodPost, r.public.URL+"/bench", bytes.NewReader(body)) //nolint:noctx // bench
					if err != nil {
						bch.Error(err)
						return
					}
					resp, err := cl.Do(req)
					if err != nil {
						bch.Error(err)
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						bch.Errorf("status %d", resp.StatusCode)
						return
					}
				}
			})
		})
	}
}

// TestGatewayLatencyProfile reports percentiles rather than a mean, because a
// mean hides exactly the tail an integration platform is judged on. Skipped
// unless SHIFT_BENCH_PROFILE=1 — it takes tens of seconds and is a measurement,
// not an assertion.
func TestGatewayLatencyProfile(t *testing.T) {
	if os.Getenv("SHIFT_BENCH_PROFILE") != "1" {
		t.Skip("set SHIFT_BENCH_PROFILE=1 to run the latency profile")
	}

	type row struct {
		backend           string
		conc, runners     int
		p50, p95, p99     time.Duration
		max               time.Duration
		rps               float64
		errs              int64
		gatewayOverheadP5 time.Duration
	}
	var rows []row

	for _, b := range backends {
		for _, conc := range []int{1, 8, 64} {
			nRunners := max(conc*2, 8)
			r := newRig(t, nRunners, b, 256)
			waitParkedN(t, r.reg, 1)

			const perWorker = 60
			lat := make([][]time.Duration, conc)
			var errs atomic.Int64
			var wg sync.WaitGroup
			start := time.Now()
			for w := range conc {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					cl := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 8}}
					body := []byte(`{"order":1}`)
					for range perWorker {
						t0 := time.Now()
						req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
							r.public.URL+"/bench", bytes.NewReader(body))
						if err != nil {
							errs.Add(1)
							continue
						}
						resp, err := cl.Do(req)
						if err != nil {
							errs.Add(1)
							continue
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if resp.StatusCode != http.StatusOK {
							errs.Add(1)
							continue
						}
						lat[w] = append(lat[w], time.Since(t0))
					}
				}(w)
			}
			wg.Wait()
			elapsed := time.Since(start)
			r.stop()

			var all []time.Duration
			for _, s := range lat {
				all = append(all, s...)
			}
			sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
			if len(all) == 0 {
				t.Errorf("%s c=%d: no successful requests", b.name, conc)
				continue
			}
			rows = append(rows, row{
				backend: b.name, conc: conc, runners: nRunners,
				p50: pct(all, 0.50), p95: pct(all, 0.95), p99: pct(all, 0.99),
				max: all[len(all)-1],
				rps: float64(len(all)) / elapsed.Seconds(),
				// The backend's expected median subtracted out: what is left is
				// the platform's contribution.
				gatewayOverheadP5: pct(all, 0.50) - b.median(),
				errs:              errs.Load(),
			})
		}
	}

	t.Log("")
	t.Logf("%-14s %5s %8s %9s %9s %9s %9s %10s %6s",
		"backend", "conc", "runners", "p50", "p95", "p99", "max", "req/s", "errs")
	for _, r := range rows {
		t.Logf("%-14s %5d %8d %9s %9s %9s %9s %10.0f %6d",
			r.backend, r.conc, r.runners,
			ms(r.p50), ms(r.p95), ms(r.p99), ms(r.max), r.rps, r.errs)
	}
	t.Log("")
	t.Log("platform cost = p50 minus the backend's expected median.")
	t.Log("NOTE: time.Sleep granularity (~1ms on darwin) is a floor on the error")
	t.Log("here, so anything under ~1ms is at the limit of what this rig resolves.")
	t.Log("The `instant` row is the honest overhead figure: no sleep, no subtraction.")
	for _, r := range rows {
		t.Logf("  %-14s conc=%-3d %s", r.backend, r.conc, ms(r.gatewayOverheadP5))
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
}

func waitParkedN(tb testing.TB, reg *runners.Registry, n int) {
	tb.Helper()
	for range 400 {
		if reg.Parked() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	tb.Fatalf("fewer than %d runners parked", n)
}
