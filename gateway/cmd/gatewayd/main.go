// Command gatewayd is SHIFT's inbound gateway: the sole publicly reachable
// component, deployable in a DMZ (ADR-0038).
//
// It is optional. A deployment whose flows are all scheduled or polled never
// runs it and carries zero inbound attack surface — which is why this is a
// separate binary rather than a feature inside the hub, where every customer
// would carry it forever.
//
// Local configuration is limited to facts about THIS HOST: listen addresses,
// the identity bundle path, log level. Everything about what we serve —
// routes, allowlists, rate limits, TLS mode, certificates — arrives pushed
// from the hub and lives only in memory. The gateway never opens a connection
// into the internal network; the hub dials it, and runners poll it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

func main() {
	if err := run(); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

// run holds the whole lifecycle so its defers actually execute: os.Exit in
// main would skip them, which would leave the signal handler installed and the
// listener unclosed on the way out.
func run() error {
	var (
		publicAddr  = flag.String("public", ":8443", "public listener (inbound requests)")
		controlAddr = flag.String("control", "127.0.0.1:8444", "control listener (runner poll/deliver, hub config push)")
		configFile  = flag.String("config", "", "bootstrap configuration file (development only; the hub is the source of truth)")
		debug       = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	lvl := slog.LevelInfo
	if *debug {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	reg := runners.New()
	h := ingress.New(reg, log)

	if *configFile != "" {
		// Development bootstrap only. In a real deployment the hub pushes
		// configuration over the mutually-authenticated control listener and
		// this flag stays unset — a route defined locally is a second source
		// of truth, and the failure mode is serving stale policy instead of a
		// clean 503.
		cfg, err := loadConfig(*configFile)
		if err != nil {
			return fmt.Errorf("config %s: %w", *configFile, err)
		}
		if err := h.SetConfig(cfg); err != nil {
			return fmt.Errorf("config %s: %w", *configFile, err)
		}
		log.Warn("configuration loaded from a local file; the hub is the source of truth in a real deployment",
			"file", *configFile, "version", cfg.Version, "routes", len(cfg.Routes))
	}

	// Two listeners with different exposure. The public one faces the
	// internet; the control one carries runner poll/deliver and (later) the
	// hub's config push, and must NEVER be published — an unauthenticated
	// caller able to reach /poll could intercept inbound payloads by
	// impersonating a runner. It defaults to loopback for that reason.
	ctrlMux := http.NewServeMux()
	ingress.NewDispatch(reg, log).Routes(ctrlMux)
	ctrlMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"configured":     h.Configured(),
			"config_version": h.ConfigVersion(),
			"runners_parked": reg.Parked(),
		})
	})

	srv := &http.Server{
		Addr:    *publicAddr,
		Handler: h,
		// The public edge must never let a slow client hold a connection open
		// indefinitely; the header timeout is the cheapest defence against the
		// classic slowloris shape.
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctrl := &http.Server{
		Addr:              *controlAddr,
		Handler:           ctrlMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		_ = ctrl.Shutdown(sctx)
	}()

	errc := make(chan error, 2)
	go func() { errc <- serve(ctrl) }()
	go func() { errc <- serve(srv) }()

	// Unconfigured is the correct starting state, not a fault: the gateway
	// serves 503 until the hub pushes a configuration, and its health endpoint
	// says so, so an ungreeted gateway is visible from the hub.
	log.Info("gateway listening",
		"public", *publicAddr, "control", *controlAddr, "configured", h.Configured())

	// Either listener failing is fatal: a gateway serving the public port with
	// no control listener can never be handed a runner, and would answer 503
	// to everything while looking healthy.
	if err := <-errc; err != nil {
		return err
	}
	return <-errc
}

func serve(s *http.Server) error {
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func loadConfig(path string) (*config.Config, error) {
	b, err := os.ReadFile(path) // #nosec G304 -- operator-supplied path, by design
	if err != nil {
		return nil, err
	}
	var c config.Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
