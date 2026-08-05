// Command runnerd is the SHIFT runner: a stateless worker that executes
// integration flows through the streaming engine and connector
// subprocesses, governed by resource-based admission (ADR-0005). Two
// intakes over one task service (ADR-0008): the local HTTP API +
// dashboard, and — when a hub is configured — the hub lease loop (M3b).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/api"
	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/bind"
	"github.com/aaron-au/shift/runner/internal/connstore"
	"github.com/aaron-au/shift/runner/internal/gwclient"
	"github.com/aaron-au/shift/runner/internal/hubclient"
	"github.com/aaron-au/shift/runner/internal/leaseloop"
	"github.com/aaron-au/shift/runner/internal/ratelimit"
	"github.com/aaron-au/shift/runner/internal/service"
	"github.com/aaron-au/shift/runner/internal/task"
	"github.com/aaron-au/shift/runner/internal/telemetry"
	"github.com/aaron-au/shift/runner/internal/webhook"
)

// version is stamped via -ldflags at release build time.
var version = "dev"

func main() {
	var (
		listen        = flag.String("listen", envOr("SHIFT_LISTEN", "127.0.0.1:8340"), "API/dashboard address (loopback by default; auth arrives with hub identity in M4)")
		connectorDir  = flag.String("connector-dir", envOr("SHIFT_CONNECTOR_DIR", "bin"), "directory of shift-connector-* binaries")
		memBudget     = flag.String("mem-budget", envOr("SHIFT_MEM_BUDGET", "1GiB"), "admission budget (ADR-0005)")
		taskWatermark = flag.String("task-watermark", envOr("SHIFT_TASK_WATERMARK", "64MiB"), "per-task stateful-operator budget; spill beyond")
		spillDir      = flag.String("spill-dir", os.Getenv("SHIFT_SPILL_DIR"), "scratch dir (default: OS temp)")
		name          = flag.String("name", envOr("SHIFT_RUNNER_NAME", hostname()), "runner display name")
		hubURL        = flag.String("hub", os.Getenv("SHIFT_HUB_URL"), "hub base URL; enables the lease intake (M3b)")
		gatewayAddrs  = flag.String("gateways", os.Getenv("SHIFT_GATEWAYS"), "comma-separated gateway control-listener URLs to poll for inbound work (ADR-0038); empty = no gateway intake")
		flowsDir      = flag.String("flows-dir", os.Getenv("SHIFT_FLOWS_DIR"), "directory of {\"document\":<flow>} JSON files to register as webhooks at start-up (the hub is authoritative when attached)")
		gatewayPolls  = flag.Int("gateway-polls", envInt("SHIFT_GATEWAY_POLLS", 16), "parked polls per gateway; bounds concurrent inbound requests from one gateway (the resource governor is still the real ceiling)")
		hubCA         = flag.String("hub-ca", os.Getenv("SHIFT_HUB_CA_FILE"), "extra CA certificate for the hub (self-signed bundles)")
		credFile      = flag.String("cred-file", os.Getenv("SHIFT_HUB_CRED_FILE"), "persist/reuse the runner's hub identity here (reg tokens are single-use)")
		connCache     = flag.String("connector-cache", envOr("SHIFT_CONNECTOR_CACHE", ""), "cache dir for registry-fetched connectors (default <spill-dir or temp>/shift-connectors)")
		requireSigned = flag.Bool("require-signed", os.Getenv("SHIFT_REQUIRE_SIGNED") == "1", "refuse local connector binaries; registry-verified artifacts only")
		users         = flag.String("users", os.Getenv("SHIFT_RUNNER_USERS"), "control-surface users \"user:bcrypt-hash:role;...\" (role: admin|operator|viewer); empty = open (loopback only)")
		webhookRPS    = flag.Float64("rl-webhook-rps", envFloat("SHIFT_RUNNER_RL_WEBHOOK_RPS", 0), "per-{hook,IP} webhook ingress request/sec limit (0=off; M6c)")
		taskTimeout   = flag.Duration("task-timeout", envDuration("SHIFT_RUNNER_TASK_TIMEOUT", 0), "max execution time per task (0=off; streaming workloads are legitimately long)")
	)
	flag.Parse()
	// Env only — a flag would leak the token into process listings. The
	// token is single-use: each runner instance gets its own (ADR-0009).
	hubRegToken := os.Getenv("SHIFT_HUB_REG_TOKEN")
	if hubRegToken == "" {
		// Compose bundles hand the token over as a file.
		if p := os.Getenv("SHIFT_HUB_REG_TOKEN_FILE"); p != "" {
			raw, err := os.ReadFile(p) //nolint:gosec // G304: operator-configured token file (env)
			if err != nil {
				log.Fatalf("runnerd: SHIFT_HUB_REG_TOKEN_FILE: %v", err)
			}
			hubRegToken = strings.TrimSpace(string(raw))
		}
	}

	budget, err := parseSize(*memBudget)
	if err != nil {
		log.Fatalf("runnerd: -mem-budget: %v", err)
	}
	watermark, err := parseSize(*taskWatermark)
	if err != nil {
		log.Fatalf("runnerd: -task-watermark: %v", err)
	}

	// Hub connection first (when configured): the connector locator and
	// the lease intake both hang off the registered client.
	var client *hubclient.Client
	var locate func(ctx context.Context, name string) (string, error)
	if *hubURL != "" {
		hc, err := hubclient.HTTPClient(*hubCA)
		if err != nil {
			log.Fatalf("runnerd: %v", err)
		}
		regCtx, regCancel := context.WithTimeout(context.Background(), 90*time.Second)
		runnerID, cl, err := hubclient.Connect(regCtx, hc, *hubURL, *credFile, hubRegToken, *name)
		regCancel()
		if err != nil {
			log.Fatalf("runnerd: hub registration: %v", err)
		}
		client = cl
		log.Printf("runnerd: registered with hub %q as %q", *hubURL, runnerID) //nolint:gosec // G706: operator-supplied flag + hub-issued id, %q-escaped

		cache := *connCache
		if cache == "" {
			base := *spillDir
			if base == "" {
				base = os.TempDir()
			}
			cache = base + "/shift-connectors"
		}
		var pinned [][]byte
		if raw := os.Getenv("SHIFT_TRUSTED_KEYS"); raw != "" {
			for k := range strings.SplitSeq(raw, ",") {
				key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(k))
				if err != nil {
					log.Fatalf("runnerd: SHIFT_TRUSTED_KEYS: %v", err)
				}
				pinned = append(pinned, key)
			}
		}
		cs, err := connstore.New(connstore.Options{Dir: cache, Client: client, PinnedKeys: pinned})
		if err != nil {
			log.Fatalf("runnerd: %v", err)
		}
		locate = cs.Ensure
	} else if *requireSigned {
		log.Fatal("runnerd: -require-signed needs -hub (the registry is the only source of signed artifacts)")
	}

	svc := service.New(service.Options{
		ConnectorDir:    *connectorDir,
		MemBudget:       budget,
		TaskWatermark:   watermark,
		SpillDir:        *spillDir,
		LocateConnector: locate,
		RequireSigned:   *requireSigned,
		TaskTimeout:     *taskTimeout,
	})

	// Hub lease intake (M3b): lease work alongside the local API.
	var loop *leaseloop.Loop
	var hubStatus func() any
	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()
	loopDone := make(chan struct{})
	if client != nil {
		loop = leaseloop.New(leaseloop.Options{Client: client, Service: svc})
		hubStatus = func() any { return loop.Status() }
		go func() { loop.Run(loopCtx); close(loopDone) }()
	} else {
		close(loopDone)
	}

	// Control-surface auth (ADR-0016). Configured users → enforce; none →
	// open (loopback dev). A non-loopback bind with no users is a foot-gun,
	// so warn loudly.
	guard := auth.NewGuard(nil)
	if *users != "" {
		basic, err := auth.NewBasic(*users)
		if err != nil {
			log.Fatalf("runnerd: %v", err) //nolint:gocritic // exitAfterDefer: startup-fatal; process exits and the OS reclaims resources — deferred loopCancel() is moot
		}
		guard = auth.NewGuard(basic)
	} else if !strings.HasPrefix(*listen, "127.0.0.1:") && !strings.HasPrefix(*listen, "localhost:") {
		log.Printf("runnerd: WARNING: control API is UNAUTHENTICATED on a non-loopback address %s — set SHIFT_RUNNER_USERS", *listen)
	}

	// Direct (push) executions never enter the hub queue; report their
	// metadata so the hub still sees fleet load + history (ADR-0016).
	// Best-effort, and only when attached to a hub.
	var report api.ExecReporter
	if client != nil {
		report = func(t task.Task, trigger string) {
			rep := hubclient.ExecutionReport{
				FlowName: t.Flow, Trigger: trigger, State: string(t.State),
				RecordsIn: t.RecordsIn, RecordsOut: t.RecordsOut,
				Error: t.Error, Started: t.Started, Finished: t.Finished,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := client.ReportExecution(ctx, rep); err != nil {
				log.Printf("runnerd: report execution: %v", err)
			}
		}
	}

	// Secret resolution for the runner-direct paths (webhook, execute, run).
	// A standalone runner gets a resolver with no fetch: a document that
	// references a secret then fails with a clear error instead of handing
	// the connector a reference object (ADR-0010, ADR-0035).
	var taskFetch bind.Fetch
	if client != nil {
		taskFetch = bind.FetchFrom(client.ResolveTaskConfig, func(hc hubclient.Connection) bind.Connection {
			return bind.Connection{Connector: hc.Connector, Config: hc.Config}
		})
	}
	binder := bind.New(taskFetch)

	// Webhook registry (ADR-0016). Hub-attached runners have it filled by a
	// periodic sync (the hub is authoritative for config); standalone
	// runners populate it via the local PUT /api/webhooks endpoint.
	hooks := webhook.NewRegistry()
	if *flowsDir != "" {
		// Seed from a local directory. Useful for a standalone runner and for
		// container deployments where the flow ships as a mounted file, and
		// harmless alongside a hub: the sync below REPLACES the registry, so
		// the hub stays authoritative the moment it says anything.
		n, err := loadFlowDir(*flowsDir, hooks)
		if err != nil {
			log.Fatalf("runnerd: -flows-dir: %v", err)
		}
		log.Printf("runnerd: loaded %d flow(s) from %q", n, *flowsDir) //nolint:gosec // G706: operator-supplied flag, %q-escaped
	}
	if client != nil {
		go syncWebhooks(loopCtx, client, hooks)
	}

	// Gateway intake (ADR-0038). Optional and absent by default: a deployment
	// whose flows are all scheduled or polled never runs a gateway, and
	// carries zero inbound attack surface as a result. The runner reaches OUT
	// to each gateway, so it needs no inbound reachability of its own — which
	// is what lets it sit behind a deny-all ingress policy.
	if addrs := splitList(*gatewayAddrs); len(addrs) > 0 {
		gw := gwclient.New(gwclient.Options{
			Addrs:   addrs,
			Service: svc,
			Lookup: func(name string) (*flowdoc.Document, bool) {
				h, ok := hooks.Get(name)
				if !ok {
					return nil, false
				}
				return h.Parsed, true
			},
			Bind: func(ctx context.Context, doc *flowdoc.Document) (*flowdoc.Document, []string, error) {
				return binder.Apply(ctx, doc)
			},
			PollConcurrency: *gatewayPolls,
			Token:           os.Getenv("SHIFT_GATEWAY_TOKEN"),
			Log:             slog.Default(),
			OnDone:          gatewayOnDone(report),
		})
		go gw.Run(loopCtx)
		log.Printf("runnerd: gateway intake polling %d gateway(s) (placement labels come from the hub, ADR-0041)", len(addrs))
	}

	// Webhook ingress rate limit (M6c, ADR-0021), keyed {hook, source IP}.
	// 0 = off (loopback/dev). Burst ~2x rps (min 1).
	wlBurst := int(*webhookRPS * 2)
	if wlBurst < 1 {
		wlBurst = 1
	}
	webhookLimit := ratelimit.New(map[string]ratelimit.Cfg{"webhook": {RPS: *webhookRPS, Burst: wlBurst}})
	defer webhookLimit.Stop()

	// Prometheus /metrics (M6a, ADR-0020) — sourced from the in-memory
	// service snapshot (governor, task totals, connector pool).
	metricsH, err := telemetry.NewRunner(func() telemetry.Snapshot {
		st := svc.Status()
		snap := telemetry.Snapshot{
			GovBudget: st.Governor.Budget, GovUsed: st.Governor.Used, GovPeak: st.Governor.Peak,
			MaxByMem:  st.MaxByMem,
			Submitted: st.Totals.Submitted, Completed: st.Totals.Completed, Failed: st.Totals.Failed,
			Stopped: st.Totals.Stopped,
			Waiting: st.Totals.Waiting, Running: st.Totals.Running, RecordsIn: st.Totals.RecordsIn,
		}
		for _, c := range st.Connectors {
			snap.Conns = append(snap.Conns, telemetry.ConnUse{Name: c.Name, InUse: int64(c.InUse)})
		}
		return snap
	}, func() map[string]int64 {
		return map[string]int64{"webhook": webhookLimit.Rejected("webhook")}
	})
	if err != nil {
		log.Fatalf("runnerd: metrics: %v", err)
	}

	srv := &http.Server{
		Addr: *listen,
		Handler: api.Handler(svc, api.Options{
			RunnerName: *name, Version: version, Started: time.Now(),
			HubStatus: hubStatus, Guard: guard, Report: report, Hooks: hooks,
			MetricsHandler: metricsH, WebhookLimit: webhookLimit, Binder: binder,
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("runnerd %s: dashboard on http://%s (connectors: %s, budget: %s)",
			version, *listen, *connectorDir, *memBudget)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("runnerd: serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	log.Print("runnerd: draining (SIGTERM)")
	loopCancel() // stop leasing; in-flight leased tasks report before Run returns
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	select {
	case <-loopDone:
	case <-time.After(25 * time.Second):
		log.Print("runnerd: lease loop drain timed out")
	}
	if err := svc.Close(25 * time.Second); err != nil {
		log.Printf("runnerd: close: %v", err)
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "runner"
	}
	return h
}

// loadFlowDir registers every *.json file in dir as a webhook, named after
// the file. It returns how many it registered.
//
// The hub remains authoritative: syncWebhooks REPLACES the registry wholesale,
// so anything seeded here survives only until the hub first speaks. That
// ordering is deliberate — a locally-mounted file must never be able to
// override, or silently resurrect, a flow the hub has retired.
func loadFlowDir(dir string, hooks *webhook.Registry) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // G304: operator-configured directory
		if err != nil {
			return n, err
		}
		var req struct {
			Document json.RawMessage `json:"document"`
			Token    string          `json:"token"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return n, fmt.Errorf("%s: %w", e.Name(), err)
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		// Validate at load, not at first request: a malformed flow mounted
		// into a container should fail the pod, not the caller who happens to
		// trigger it first.
		h, err := webhook.NewHook(name, req.Document, hashToken(req.Token))
		if err != nil {
			return n, fmt.Errorf("%s: %w", e.Name(), err)
		}
		hooks.Put(h)
		n++
	}
	return n, nil
}

// hashToken mirrors the API's per-hook token hashing: the plaintext is never
// stored, only its digest.
func hashToken(tok string) string {
	if tok == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// gatewayOnDone adapts the hub reporter to the gateway intake's completion
// hook, and returns NIL when there is nothing to report to.
//
// The nil matters more than it looks. OnDone runs on its own goroutine, so a
// closure that called a nil reporter would panic THERE — and a panic in a
// goroutine takes the whole process down, on the first request a caller makes.
// A hub-less runner would serve exactly one gateway request and die.
func gatewayOnDone(report api.ExecReporter) func(task.Task) {
	if report == nil {
		return nil
	}
	return func(t task.Task) { report(t, "gateway") }
}

// splitList parses a comma-separated list, dropping empties so a trailing
// comma or a blank env var does not become an address of "".
func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
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
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return int64(n * float64(mult)), nil
}

// syncWebhooks periodically pulls the runner's webhook configs from the hub
// and replaces the local registry (the hub is authoritative for attached
// runners). Best-effort: errors are logged and retried next tick.
func syncWebhooks(ctx context.Context, client *hubclient.Client, hooks *webhook.Registry) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		cfgs, err := client.SyncWebhooks(sctx)
		cancel()
		if err != nil {
			log.Printf("runnerd: webhook sync: %v", err)
		} else {
			hs := make([]webhook.Hook, 0, len(cfgs))
			for _, c := range cfgs {
				// Parse once per sync, not once per inbound request. One bad
				// document must not cost the runner every other hook, so it
				// is skipped and logged rather than aborting the replace.
				h, err := webhook.NewHook(c.Name, c.Document, c.TokenHash)
				if err != nil {
					log.Printf("runnerd: webhook sync: hook %q has an invalid document, skipping: %v", c.Name, err)
					continue
				}
				hs = append(hs, h)
			}
			hooks.Replace(hs)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
