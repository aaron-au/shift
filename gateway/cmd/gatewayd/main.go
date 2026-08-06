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
	stdlog "log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/identity"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// buildVersion is stamped at link time (-ldflags -X main.buildVersion=…),
// matching hubd/runnerd. It rides every log record so a mixed-version fleet is
// legible (ADR-0046 §3).
var buildVersion = "dev"

// logBridge re-emits stdlib log output as slog records, so a third-party
// library writing through the global logger cannot put prose into a JSON
// stream. Mirrors pkg/shiftlog; see the note in main about why it is copied.
type logBridge struct{ l *slog.Logger }

func (b logBridge) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	if before, rest, found := strings.Cut(msg, ": "); found && !strings.Contains(before, " ") {
		msg = rest
	}
	b.l.LogAttrs(context.Background(), slog.LevelInfo, msg)
	return len(p), nil
}

// logText reports whether out is a terminal (text) rather than a pipe (JSON).
func logText(out *os.File) bool {
	if f := strings.ToLower(strings.TrimSpace(os.Getenv("SHIFT_LOG_FORMAT"))); f == "text" {
		return true
	} else if f == "json" {
		return false
	}
	info, err := out.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func main() {
	if err := run(); err != nil {
		slog.Error("gateway stopped", "event", "gateway.stopped", "error", err)
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
		identityDir = flag.String("identity", os.Getenv("SHIFT_GATEWAY_IDENTITY"), "directory holding the hub-issued identity bundle (ADR-0041); enables mTLS on the control listener")
		configFile  = flag.String("config", "", "bootstrap configuration file (development only; the hub is the source of truth)")
		debug       = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	// Env, never a flag: a flag would put the shared secret into every process
	// listing on the host (same rule as the runner's hub registration token).
	controlToken := os.Getenv("SHIFT_GATEWAY_CONTROL_TOKEN")
	if controlToken == "" {
		if p := os.Getenv("SHIFT_GATEWAY_CONTROL_TOKEN_FILE"); p != "" {
			raw, err := os.ReadFile(p) //nolint:gosec // G304: operator-configured token file (env)
			if err != nil {
				return fmt.Errorf("SHIFT_GATEWAY_CONTROL_TOKEN_FILE: %w", err)
			}
			controlToken = strings.TrimSpace(string(raw))
		}
	}
	// Load the identity bundle first: it decides whether the control listener
	// can be mutually authenticated, which decides whether the weaker
	// alternatives below are even considered.
	var bundle *identity.Bundle
	if *identityDir != "" {
		b, err := identity.Load(*identityDir)
		if err != nil {
			return err
		}
		bundle = b
	}

	// FAIL CLOSED. An unauthenticated /poll reachable off-host lets anyone park
	// a fake runner: they receive real inbound payloads and can deliver forged
	// responses to real callers. That is interception plus response forgery
	// from one open port, so the combination is refused outright rather than
	// warned about — a warning is something a deployment scrolls past.
	//
	// mTLS satisfies this outright: it authenticates every runner individually
	// AND lets the runner verify the gateway, which a shared secret cannot.
	if bundle == nil && controlToken == "" && !loopbackOnly(*controlAddr) {
		return fmt.Errorf("control listener %q is not loopback and has no identity bundle: "+
			"set -identity (ADR-0041), or export SHIFT_GATEWAY_CONTROL_TOKEN, or bind -control to 127.0.0.1", *controlAddr)
	}

	// Logging setup is duplicated from pkg/shiftlog on purpose (ADR-0046 §2):
	// this module's go.mod has ZERO dependencies, which is an auditable
	// security property of the one component that may sit in a DMZ. The
	// contract with the other binaries is the output SCHEMA — asserted by
	// TestLogSchemaMatchesTheOtherBinaries — not shared code.
	lvl := slog.LevelInfo
	if err := lvl.UnmarshalText([]byte(strings.TrimSpace(os.Getenv("SHIFT_LOG_LEVEL")))); err != nil {
		lvl = slog.LevelInfo // a typo in a log level must not stop the gateway
	}
	if *debug {
		lvl = slog.LevelDebug // the explicit flag wins over the env
	}
	// stdout, and text only on a terminal — same rule as the other two.
	var lh slog.Handler
	lopts := &slog.HandlerOptions{Level: lvl}
	if logText(os.Stdout) {
		lh = slog.NewTextHandler(os.Stdout, lopts)
	} else {
		lh = slog.NewJSONHandler(os.Stdout, lopts)
	}
	log := slog.New(lh.WithAttrs([]slog.Attr{
		slog.String("component", "gateway"),
		slog.String("version", buildVersion),
	}))
	slog.SetDefault(log)
	stdlog.SetFlags(0)
	stdlog.SetOutput(logBridge{log})

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
			"event", "gateway.config.local_file",
			"file", *configFile, "version", cfg.Version, "routes", len(cfg.Routes))
	}

	// Two listeners with different exposure. The public one faces the
	// internet; the control one carries runner poll/deliver and (later) the
	// hub's config push, and must NEVER be published — an unauthenticated
	// caller able to reach /poll could intercept inbound payloads by
	// impersonating a runner. It defaults to loopback for that reason.
	ctrlMux := http.NewServeMux()
	dispatch := ingress.NewDispatch(reg, log, controlToken)
	if bundle != nil {
		// Placement is asserted by the hub, keyed by the identity each runner
		// proves with its client certificate (ADR-0041 §3). Without a bundle
		// there is no proven identity, so the roster cannot be consulted and
		// runners park labelled only by whatever the (absent) roster says —
		// which is why the roster is wired ONLY in the mTLS case.
		dispatch = dispatch.WithLabels(func(id string) (map[string]string, bool) {
			c := h.Config()
			if c == nil {
				return nil, false
			}
			return c.LabelsFor(id)
		})
	}
	dispatch.Routes(ctrlMux)
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
	if bundle != nil {
		ctrl.TLSConfig = bundle.ServerTLS()
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
	go func() { errc <- serveControl(ctrl, bundle != nil) }()
	go func() { errc <- serve(srv) }()

	// Unconfigured is the correct starting state, not a fault: the gateway
	// serves 503 until the hub pushes a configuration, and its health endpoint
	// says so, so an ungreeted gateway is visible from the hub.
	// A FIXED field set, computed above rather than appended conditionally. Two
	// reasons: a query should not have to handle a field that is sometimes
	// absent, and keys listed literally at the call site are keys the
	// vocabulary gate can actually check (ADR-0046 §3) — a `args...` splat is
	// invisible to it.
	//
	// cert_expires is operationally load-bearing: a lapsed certificate strands
	// this gateway permanently, because renewing it would mean dialling the
	// hub. Renewal is the hub's job to PUSH (ADR-0041 §4).
	identityID, certExpires := "", ""
	if bundle != nil {
		identityID = bundle.ID
		certExpires = bundle.NotAfter.UTC().Format(time.RFC3339)
	}
	log.Info("gateway listening",
		"event", "gateway.started",
		"public", *publicAddr,
		"control", *controlAddr,
		"configured", h.Configured(),
		"identity", identityID,
		"control_mtls", bundle != nil,
		"cert_expires", certExpires,
		// Whether the control listener authenticates at all — never anything
		// about the credential itself.
		"control_authenticated", dispatch.Authenticated(),
	)

	// Either listener failing is fatal: a gateway serving the public port with
	// no control listener can never be handed a runner, and would answer 503
	// to everything while looking healthy.
	if err := <-errc; err != nil {
		return err
	}
	return <-errc
}

// loopbackOnly reports whether addr binds only the loopback interface.
//
// A bare port (":8444") or an empty host binds EVERY interface, so those are
// deliberately not loopback — that is the shape a container ships with, and
// the shape that must carry a secret.
func loopbackOnly(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false // unparseable: assume the worst
	}
	if host == "" {
		return false // ":8444" — all interfaces
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// serveControl starts the control listener, with TLS when an identity bundle
// is present. ListenAndServeTLS with empty paths uses the certificates already
// on TLSConfig.
func serveControl(s *http.Server, tls bool) error {
	var err error
	if tls {
		err = s.ListenAndServeTLS("", "")
	} else {
		err = s.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
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
