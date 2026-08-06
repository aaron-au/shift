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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crypto/tls"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/identity"
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

// wire is how the runner reaches the control listener. mTLS is what ships
// (ADR-0041); the plaintext row exists only so the security work has a
// measured price rather than an assumed one.
type wire struct {
	name   string
	secure bool // mutual TLS on the control listener
	h2     bool // let the runner negotiate HTTP/2 (mTLS only)
}

var (
	wirePlain  = wire{name: "plaintext"}
	wireMTLSH1 = wire{name: "mtls-http1", secure: true}
	wireMTLSH2 = wire{name: "mtls-http2", secure: true, h2: true}
)

// rig is a gateway plus N stub runners, wired over real HTTP.
type rig struct {
	public  *httptest.Server
	control *httptest.Server
	reg     *runners.Registry
	stop    func()
	served  atomic.Int64
}

func newRig(tb testing.TB, nRunners int, b backend, payload int, w wire) *rig {
	tb.Helper()

	reg := runners.New()
	reg.DeliveryTimeout = 30 * time.Second
	// Every stub runner is rostered, exactly as the hub would place it
	// (ADR-0041 §3) — so the measured path includes the roster lookup and the
	// identity resolution the real gateway does on every single poll.
	roster := make([]config.Runner, nRunners)
	for i := range roster {
		roster[i] = config.Runner{
			ID:     runnerID(i),
			Labels: map[string]string{"environment": "production", "workload": "api"},
		}
	}
	cfg := &config.Config{Version: 1,
		Routes: []config.Route{{
			Path: "/bench", Method: http.MethodPost, Flow: "bench",
			Selector: config.Selector{"environment": "production"},
		}},
		Runners: roster,
	}
	pub := ingress.New(reg, discardLogger())
	if err := pub.SetConfig(cfg); err != nil {
		tb.Fatal(err)
	}

	dispatch := ingress.NewDispatch(reg, discardLogger(), "").WithLabels(cfg.LabelsFor)
	if !w.secure {
		// No TLS means no proven identity, so the plaintext rig substitutes a
		// header. It is a stand-in for the certificate ONLY here: it keeps both
		// rows doing identical roster work, so the difference between them is
		// the transport and nothing else.
		dispatch = dispatch.WithPeerID(func(r *http.Request) string {
			return r.Header.Get("X-Bench-Runner")
		})
	}
	mux := http.NewServeMux()
	dispatch.Routes(mux)

	r := &rig{public: httptest.NewServer(pub), reg: reg}

	var ca *testCA
	if w.secure {
		ca = newTestCA(tb)
		dir := tb.TempDir()
		writeGatewayBundle(tb, dir, ca, "gw-bench")
		bundle, err := identity.Load(dir)
		if err != nil {
			tb.Fatal(err)
		}
		ctrl := httptest.NewUnstartedServer(mux)
		ctrl.TLS = bundle.ServerTLS()
		ctrl.EnableHTTP2 = w.h2
		ctrl.StartTLS()
		r.control = ctrl
	} else {
		r.control = httptest.NewServer(mux)
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
			id := runnerID(seed)
			cl := runnerTransport(tb, ca, id, w)
			for {
				select {
				case <-done:
					return
				default:
				}
				if !r.serveOnce(cl, id, rnd, b, body) {
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

// runnerID names one stub runner. It is the certificate subject in the mTLS
// rows, so it is also what the roster is keyed by.
func runnerID(i int) string { return fmt.Sprintf("rnr-%d", i) }

// runnerTransport builds the client one stub runner uses, matching what the
// real runner does in that mode: a pool deep enough not to exhaust ephemeral
// ports, and — for mTLS — this runner's own certificate.
func runnerTransport(tb testing.TB, ca *testCA, id string, w wire) *http.Client {
	tb.Helper()
	tr := &http.Transport{MaxIdleConnsPerHost: 64} // one runner keeps its connections warm
	if w.secure {
		cert := ca.clientCert(tb, id)
		tr.TLSClientConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      ca.pool(),
			MinVersion:   tls.VersionTLS13,
		}
		if w.h2 {
			tr.ForceAttemptHTTP2 = true
		} else {
			// Pin ALPN to HTTP/1.1 so this row isolates the COST OF TLS. The
			// h2 row then shows what the shipped configuration actually does,
			// which is a different question and has a different answer.
			tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
		}
	}
	return &http.Client{Transport: tr}
}

// serveOnce is one poll→execute→deliver cycle. It returns false when the rig
// is shutting down.
func (r *rig) serveOnce(cl *http.Client, id string, rnd *rand.Rand, b backend, out []byte) bool {
	// No labels: a runner states nothing about itself (ADR-0041 §3).
	pollBody, _ := json.Marshal(map[string]any{"wait_seconds": 1})
	req, err := http.NewRequest(http.MethodPost, r.control.URL+"/api/v1/gw/poll", bytes.NewReader(pollBody)) //nolint:noctx // bench rig; the poll window bounds it
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bench-Runner", id) // ignored under mTLS; the certificate wins
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
	reqID := resp.Header.Get("X-Shift-Request-Id")
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	// The connector: go somewhere else and wait.
	if d := b.work(rnd); d > 0 {
		time.Sleep(d)
	}

	dreq, err := http.NewRequest(http.MethodPost, r.control.URL+"/api/v1/gw/deliver/"+reqID, bytes.NewReader(out)) //nolint:noctx // bench rig
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
			r := newRig(bch, 32, b, 256, wireMTLSH2)
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
		errs              *failures
		gatewayOverheadP5 time.Duration
	}
	var rows []row

	for _, b := range backends {
		for _, conc := range []int{1, 8, 64} {
			nRunners := max(conc*2, 8)
			r := newRig(t, nRunners, b, 256, wireMTLSH2)
			waitParkedN(t, r.reg, 1)

			all, errs, elapsed := drive(t, r, conc, 60)
			r.stop()

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
				errs:              errs,
			})
		}
	}

	t.Log("")
	t.Logf("%-14s %5s %8s %9s %9s %9s %9s %10s %6s  %s",
		"backend", "conc", "runners", "p50", "p95", "p99", "max", "req/s", "errs", "detail")
	for _, r := range rows {
		t.Logf("%-14s %5d %8d %9s %9s %9s %9s %10.0f %6d  %s",
			r.backend, r.conc, r.runners,
			ms(r.p50), ms(r.p95), ms(r.p99), ms(r.max), r.rps, r.errs.total(), r.errs)
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

// The benchmark rig is a fixture, and fixtures rot silently. ADR-0041 moved
// labels out of the poll body; the rig kept sending them and stopped matching
// any route selector, so every dispatch answered 503 — and nothing failed,
// because benchmarks do not run under `make check`. The measurement was simply
// unavailable until someone next ran it by hand.
//
// This test runs in the normal suite and serves one request over each wire, so
// the rig cannot rot that way again.
func TestBenchRigServesOverEveryWire(t *testing.T) {
	for _, w := range []wire{wirePlain, wireMTLSH1, wireMTLSH2} {
		t.Run(w.name, func(t *testing.T) {
			r := newRig(t, 2, backends[0], 64, w)
			defer r.stop()
			waitParkedN(t, r.reg, 1)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				r.public.URL+"/bench", strings.NewReader(`{"order":1}`))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 — the benchmark rig is not serving", resp.StatusCode)
			}
		})
	}
}

// TestMutualTLSCost answers one question: what did ADR-0041 cost at runtime?
//
// Three transports, identical in every other respect — same roster lookup,
// same identity resolution, same backends, same runner count — so the only
// variable is the wire:
//
//	plaintext   what shipped before mTLS: HTTP/1.1, a shared secret
//	mtls-http1  mutual TLS, ALPN pinned to HTTP/1.1 — the COST OF TLS alone
//	mtls-http2  mutual TLS with h2, which is what actually ships
//
// The third row is the one that matters operationally and the second is the
// one that answers the question honestly, because h2 changes connection
// behaviour independently of the encryption.
//
// Skipped unless SHIFT_BENCH_PROFILE=1: it is a measurement, not an assertion.
func TestMutualTLSCost(t *testing.T) {
	if os.Getenv("SHIFT_BENCH_PROFILE") != "1" {
		t.Skip("set SHIFT_BENCH_PROFILE=1 to run the mTLS cost profile")
	}

	type key struct {
		backend string
		conc    int
	}
	type result struct {
		p50, p95, p99 time.Duration
		rps           float64
		errs          *failures
	}
	got := map[key]map[string]result{}

	wires := []wire{wirePlain, wireMTLSH1, wireMTLSH2}
	// Two backends: the framework-cost case, where any TLS overhead is at its
	// most visible, and the realistic case, where it should disappear under
	// the backend.
	for _, b := range []backend{backends[0], backends[2]} {
		for _, conc := range []int{1, 8, 64} {
			for _, w := range wires {
				r := newRig(t, max(conc*2, 8), b, 256, w)
				waitParkedN(t, r.reg, 1)
				lat, errs, elapsed := drive(t, r, conc, 60)
				r.stop()

				if len(lat) == 0 {
					t.Errorf("%s %s c=%d: no successful requests", b.name, w.name, conc)
					continue
				}
				k := key{b.name, conc}
				if got[k] == nil {
					got[k] = map[string]result{}
				}
				got[k][w.name] = result{
					p50: pct(lat, 0.50), p95: pct(lat, 0.95), p99: pct(lat, 0.99),
					rps: float64(len(lat)) / elapsed.Seconds(), errs: errs,
				}
			}
		}
	}

	t.Log("")
	t.Logf("%-12s %5s %-12s %9s %9s %9s %10s %6s %10s",
		"backend", "conc", "wire", "p50", "p95", "p99", "req/s", "errs", "vs plain")
	for _, b := range []backend{backends[0], backends[2]} {
		for _, conc := range []int{1, 8, 64} {
			row := got[key{b.name, conc}]
			base := row[wirePlain.name]
			for _, w := range wires {
				res, ok := row[w.name]
				if !ok {
					continue
				}
				delta := "—"
				if w.name != wirePlain.name && base.p50 > 0 {
					delta = fmt.Sprintf("%+.2fms", float64(res.p50-base.p50)/1e6)
				}
				t.Logf("%-12s %5d %-12s %9s %9s %9s %10.0f %6d %10s  %s",
					b.name, conc, w.name, ms(res.p50), ms(res.p95), ms(res.p99),
					res.rps, res.errs.total(), delta, res.errs)
			}
		}
	}
}

// failures records WHY requests failed rather than only how many. A benchmark
// that reports a bare error count invites the reader to assume noise; the two
// causes here mean opposite things. A 503 means the gateway had no eligible
// runner parked at that instant — capacity, and it sheds by design. A transport
// error means the connection itself failed, which is a defect.
type failures struct {
	mu        sync.Mutex
	statuses  map[int]int
	transport int
	sample    string
}

func (f *failures) status(code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statuses == nil {
		f.statuses = map[int]int{}
	}
	f.statuses[code]++
}

func (f *failures) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transport++
	if f.sample == "" {
		f.sample = err.Error()
	}
}

func (f *failures) total() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.transport
	for _, c := range f.statuses {
		n += c
	}
	return int64(n)
}

func (f *failures) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.transport == 0 && len(f.statuses) == 0 {
		return ""
	}
	parts := make([]string, 0, len(f.statuses)+1)
	for code, n := range f.statuses {
		parts = append(parts, fmt.Sprintf("%d×%d", n, code))
	}
	sort.Strings(parts)
	if f.transport > 0 {
		parts = append(parts, fmt.Sprintf("%d×transport (%s)", f.transport, f.sample))
	}
	return strings.Join(parts, " ")
}

// drive runs conc workers issuing perWorker public requests each, returning
// sorted latencies, a breakdown of what failed, and wall time.
func drive(t *testing.T, r *rig, conc, perWorker int) ([]time.Duration, *failures, time.Duration) {
	t.Helper()
	lat := make([][]time.Duration, conc)
	fails := &failures{}
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
					fails.fail(err)
					continue
				}
				resp, err := cl.Do(req)
				if err != nil {
					fails.fail(err)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					fails.status(resp.StatusCode)
					continue
				}
				lat[w] = append(lat[w], time.Since(t0))
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	for _, s := range lat {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all, fails, elapsed
}

// BenchmarkControlHandshake isolates the TLS handshake itself: a runner that
// reconnects on every poll versus one that keeps its connection parked.
//
// This is the number people expect mTLS to cost, and it is real — but a runner
// handshakes once and then holds that connection for its whole life, so the
// per-request share of it is the "warm" row, not the "cold" one. Getting this
// wrong in either direction is how a security decision ends up argued about
// with the wrong figure.
func BenchmarkControlHandshake(bch *testing.B) {
	ca := newTestCA(bch)
	dir := bch.TempDir()
	writeGatewayBundle(bch, dir, ca, "gw-bench")
	bundle, err := identity.Load(dir)
	if err != nil {
		bch.Fatal(err)
	}
	cfg := &config.Config{Version: 1,
		Routes:  []config.Route{{Path: "/bench", Flow: "bench"}},
		Runners: []config.Runner{{ID: "rnr-0", Labels: map[string]string{"environment": "production"}}},
	}
	mux := http.NewServeMux()
	ingress.NewDispatch(runners.New(), discardLogger(), "").WithLabels(cfg.LabelsFor).Routes(mux)
	ctrl := httptest.NewUnstartedServer(mux)
	ctrl.TLS = bundle.ServerTLS()
	ctrl.EnableHTTP2 = true
	ctrl.StartTLS()
	defer ctrl.Close()

	// Both rows park for the same tiny window and return 204, so the DIFFERENCE
	// between them is the handshake and nothing else. Neither row on its own is
	// a handshake measurement — each also contains a poll.
	poll := func(cl *http.Client) {
		body, _ := json.Marshal(map[string]any{"wait_seconds": 0.0001})
		req, _ := http.NewRequest(http.MethodPost, ctrl.URL+"/api/v1/gw/poll", bytes.NewReader(body)) //nolint:noctx // bench
		resp, err := cl.Do(req)
		if err != nil {
			bch.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	bch.Run("cold", func(bch *testing.B) {
		for range bch.N {
			cl := runnerTransport(bch, ca, "rnr-0", wireMTLSH2)
			poll(cl)
			cl.CloseIdleConnections()
		}
	})
	bch.Run("warm", func(bch *testing.B) {
		cl := runnerTransport(bch, ca, "rnr-0", wireMTLSH2)
		poll(cl) // handshake once, outside the loop
		bch.ResetTimer()
		for range bch.N {
			poll(cl)
		}
	})
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
