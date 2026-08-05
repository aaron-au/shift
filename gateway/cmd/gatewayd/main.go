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
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
		publicAddr = flag.String("public", ":8443", "public listener (inbound requests)")
		debug      = flag.Bool("debug", false, "verbose logging")
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

	srv := &http.Server{
		Addr:    *publicAddr,
		Handler: h,
		// The public edge must never let a slow client hold a connection open
		// indefinitely; the header timeout is the cheapest defence against the
		// classic slowloris shape.
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	// Unconfigured is the correct starting state, not a fault: the gateway
	// serves 503 until the hub pushes a configuration, and its health endpoint
	// says so, so an ungreeted gateway is visible from the hub.
	log.Info("gateway listening", "public", *publicAddr, "configured", h.Configured())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
