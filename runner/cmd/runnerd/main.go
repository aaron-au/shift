// Command runnerd is the SHIFT runner: a stateless worker that executes
// integration flows through the streaming engine and connector
// subprocesses, governed by resource-based admission (ADR-0005). Two
// intakes over one task service (ADR-0008): the local HTTP API +
// dashboard, and — when a hub is configured — the hub lease loop (M3b).
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aaron-au/shift/runner/internal/api"
	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/connstore"
	"github.com/aaron-au/shift/runner/internal/hubclient"
	"github.com/aaron-au/shift/runner/internal/leaseloop"
	"github.com/aaron-au/shift/runner/internal/service"
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
		hubCA         = flag.String("hub-ca", os.Getenv("SHIFT_HUB_CA_FILE"), "extra CA certificate for the hub (self-signed bundles)")
		credFile      = flag.String("cred-file", os.Getenv("SHIFT_HUB_CRED_FILE"), "persist/reuse the runner's hub identity here (reg tokens are single-use)")
		connCache     = flag.String("connector-cache", envOr("SHIFT_CONNECTOR_CACHE", ""), "cache dir for registry-fetched connectors (default <spill-dir or temp>/shift-connectors)")
		requireSigned = flag.Bool("require-signed", os.Getenv("SHIFT_REQUIRE_SIGNED") == "1", "refuse local connector binaries; registry-verified artifacts only")
		users         = flag.String("users", os.Getenv("SHIFT_RUNNER_USERS"), "control-surface users \"user:bcrypt-hash:role;...\" (role: admin|operator|viewer); empty = open (loopback only)")
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
			log.Fatalf("runnerd: %v", err)
		}
		guard = auth.NewGuard(basic)
	} else if !strings.HasPrefix(*listen, "127.0.0.1:") && !strings.HasPrefix(*listen, "localhost:") {
		log.Printf("runnerd: WARNING: control API is UNAUTHENTICATED on a non-loopback address %s — set SHIFT_RUNNER_USERS", *listen)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.Handler(svc, *name, version, time.Now(), hubStatus, guard),
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
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
