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

	"github.com/aaron-au/shift/gateway/internal/adopt"
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
		stateDir    = flag.String("state", os.Getenv("SHIFT_GATEWAY_STATE"), "adoption state directory (ADR-0049); the gateway generates its own key here and waits to be adopted")
		identityDir = flag.String("identity", os.Getenv("SHIFT_GATEWAY_IDENTITY"), "DEPRECATED (superseded by -state, ADR-0049): directory holding a hand-placed identity bundle (ADR-0041)")
		configFile  = flag.String("config", "", "bootstrap configuration file (development only; the hub is the source of truth)")
		debug       = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	// Env, never a flag: a flag would put the shared secret into every process
	// listing on the host (same rule as the runner's hub registration token).
	// Env only — a flag would leak the token into process listings. Single-use
	// and burned at adoption, so it is a bootstrap credential rather than a
	// standing one (ADR-0049 §1a).
	installToken := os.Getenv("SHIFT_GATEWAY_INSTALL_TOKEN")
	if installToken == "" {
		// Compose and k8s hand secrets over as files.
		if p := os.Getenv("SHIFT_GATEWAY_INSTALL_TOKEN_FILE"); p != "" {
			raw, err := os.ReadFile(p) //nolint:gosec // G304: operator-configured token file (env)
			if err != nil {
				return fmt.Errorf("SHIFT_GATEWAY_INSTALL_TOKEN_FILE: %w", err)
			}
			installToken = strings.TrimSpace(string(raw))
		}
	}

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
	// Adoption state (ADR-0049) is the modern path: the gateway generates its
	// own anchor key and waits for the hub to dial it. The hand-placed bundle
	// below is the superseded one, kept until the hub push side is proven end
	// to end in a deployment.
	var st *adopt.State
	if *stateDir != "" {
		s, err := adopt.Open(*stateDir, installToken)
		if err != nil {
			return err
		}
		st = s
	}
	if st != nil && *identityDir != "" {
		// Two sources of identity is one too many; refusing beats picking.
		return errors.New("-state and -identity are alternatives: -state adopts (ADR-0049), " +
			"-identity loads a hand-placed bundle (ADR-0041). Set one")
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
	if st == nil && bundle == nil && controlToken == "" && !loopbackOnly(*controlAddr) {
		return fmt.Errorf("control listener %q is not loopback and has no identity: "+
			"set -state (ADR-0049, recommended), or -identity (ADR-0041), "+
			"or export SHIFT_GATEWAY_CONTROL_TOKEN, or bind -control to 127.0.0.1", *controlAddr)
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
	if bundle != nil || st != nil {
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
	if st != nil {
		// Adoption, renewal and the configuration push (ADR-0049). Applying a
		// pushed configuration goes through the SAME validation as a local
		// file: the hub is authoritative, but it is not exempt.
		adopt.Handler(ctrlMux, st, buildVersion, func(raw []byte) error {
			cfg, err := parseConfig(raw)
			if err != nil {
				return err
			}
			return h.SetConfig(cfg)
		}, log)
	}
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
	switch {
	case st != nil:
		ctrl.TLSConfig = st.ControlTLS()
	case bundle != nil:
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
	go func() { errc <- serveControl(ctrl, st != nil || bundle != nil) }()
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
	switch {
	case st != nil:
		identityID = st.GatewayID()
		if na := st.NotAfter(); !na.IsZero() {
			certExpires = na.UTC().Format(time.RFC3339)
		}
	case bundle != nil:
		identityID = bundle.ID
		certExpires = bundle.NotAfter.UTC().Format(time.RFC3339)
	}
	if st != nil && !st.Adopted() {
		// Nothing for a human to copy: the hub already holds the install token
		// and will dial. The fingerprint is logged because it is public, and
		// because it is what an operator compares when a pairing is refused.
		//
		// A gateway with no install token can never be adopted, so that is a
		// warning with a fix in it rather than a state to sit in quietly.
		//
		// The field is named "adoptable" rather than anything with "token" in
		// it: the vocabulary gate refuses key names that read as credentials
		// (ADR-0046 §7), and it is right to — a reader scanning logs should not
		// have to work out whether a field holds a secret or merely mentions
		// one.
		if installToken == "" {
			log.Warn("waiting to be adopted, but no install token was supplied — the hub's pairing will be refused",
				"event", "gateway.awaiting_adoption",
				"control", *controlAddr,
				"fingerprint", st.Fingerprint(),
				"adoptable", false)
		} else {
			log.Info("waiting to be adopted",
				"event", "gateway.awaiting_adoption",
				"control", *controlAddr,
				"fingerprint", st.Fingerprint(),
				"adoptable", true)
		}
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
	return parseConfig(b)
}

// parseConfig decodes a configuration document, whoever supplied it.
//
// Shared by the local file and the hub's push on purpose: the hub is
// authoritative, but authoritative is not the same as exempt. A malformed push
// must be rejected exactly as loudly as a malformed file.
func parseConfig(b []byte) (*config.Config, error) {
	var c config.Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
