// Command runnerd is the SHIFT runner: a stateless worker that executes
// integration flows through the streaming engine and connector
// subprocesses, governed by resource-based admission (ADR-0005). Two
// intakes over one task service (ADR-0008): the local HTTP API +
// dashboard, and — when a hub is configured — the hub lease loop (M3b).
package main

import (
	"context"
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
	)
	flag.Parse()
	// Env only — a flag would leak the token into process listings. The
	// token is single-use: each runner instance gets its own (ADR-0009).
	hubRegToken := os.Getenv("SHIFT_HUB_REG_TOKEN")

	budget, err := parseSize(*memBudget)
	if err != nil {
		log.Fatalf("runnerd: -mem-budget: %v", err)
	}
	watermark, err := parseSize(*taskWatermark)
	if err != nil {
		log.Fatalf("runnerd: -task-watermark: %v", err)
	}

	svc := service.New(service.Options{
		ConnectorDir:  *connectorDir,
		MemBudget:     budget,
		TaskWatermark: watermark,
		SpillDir:      *spillDir,
	})

	// Hub lease intake (M3b): register with the single-use token, then
	// lease work alongside the local API.
	var loop *leaseloop.Loop
	var hubStatus func() any
	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()
	loopDone := make(chan struct{})
	if *hubURL != "" {
		if hubRegToken == "" {
			log.Fatal("runnerd: -hub set but SHIFT_HUB_REG_TOKEN is empty")
		}
		regCtx, regCancel := context.WithTimeout(context.Background(), 30*time.Second)
		runnerID, client, err := hubclient.Register(regCtx, *hubURL, hubRegToken, *name)
		regCancel()
		if err != nil {
			log.Fatalf("runnerd: hub registration: %v", err)
		}
		log.Printf("runnerd: registered with hub %q as %q", *hubURL, runnerID) //nolint:gosec // G706: operator-supplied flag + hub-issued id, %q-escaped
		loop = leaseloop.New(leaseloop.Options{Client: client, Service: svc})
		hubStatus = func() any { return loop.Status() }
		go func() { loop.Run(loopCtx); close(loopDone) }()
	} else {
		close(loopDone)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           api.Handler(svc, *name, version, time.Now(), hubStatus),
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
