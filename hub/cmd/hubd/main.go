// Command hubd is the SHIFT hub: the HA control plane owning identity,
// flow versions, and the durable task queue (ADR-0002, ADR-0009).
// Stateless over Postgres — run as many replicas as you like; the queue
// coordinates through SKIP LOCKED and leases, migrations through an
// advisory lock.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/connpolicy"
	"github.com/aaron-au/shift/hub/internal/gwpush"
	"github.com/aaron-au/shift/hub/internal/gwsync"
	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/pki"
	"github.com/aaron-au/shift/hub/internal/ratelimit"
	"github.com/aaron-au/shift/hub/internal/scheduler"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/hub/internal/telemetry"
	"github.com/aaron-au/shift/pkg/shiftlog"
)

// version is stamped via -ldflags at release build time.
var version = "dev"

func main() {
	var (
		listen   = flag.String("listen", envOr("SHIFT_HUB_LISTEN", "127.0.0.1:8400"), "API address (loopback default; use -tls-cert/-tls-key for non-local binds)")
		dsn      = flag.String("db", os.Getenv("SHIFT_HUB_DB"), "Postgres DSN (required; e.g. postgres://shift:...@localhost:5432/shift)")
		leaseTTL = flag.Duration("lease-ttl", envDuration("SHIFT_HUB_LEASE_TTL", 30*time.Second), "task lease duration between heartbeats")
		tlsCert  = flag.String("tls-cert", os.Getenv("SHIFT_HUB_TLS_CERT"), "TLS certificate file (serve HTTPS)")
		tlsKey   = flag.String("tls-key", os.Getenv("SHIFT_HUB_TLS_KEY"), "TLS key file")

		runnerCACert = flag.String("runner-ca-cert", os.Getenv("SHIFT_HUB_RUNNER_CA_CERT"),
			"control-plane CA certificate — issues runner client certificates (ADR-0044)")
		runnerCAKey   = flag.String("runner-ca-key", os.Getenv("SHIFT_HUB_RUNNER_CA_KEY"), "control-plane CA key")
		gatewayCACert = flag.String("gateway-ca-cert", os.Getenv("SHIFT_HUB_GATEWAY_CA_CERT"),
			"PEM certificate of the gateway CA (ADR-0049); enables gateway adoption")
		gatewayCAKey = flag.String("gateway-ca-key", os.Getenv("SHIFT_HUB_GATEWAY_CA_KEY"),
			"PEM private key of the gateway CA")
		gatewayCertTTL = flag.Duration("gateway-cert-ttl", envDuration("SHIFT_HUB_GATEWAY_CERT_TTL", pki.DefaultTTL),
			"lifetime of an issued gateway identity")
		runnerCertTTL = flag.Duration("runner-cert-ttl", envDuration("SHIFT_HUB_RUNNER_CERT_TTL", pki.DefaultTTL),
			"lifetime of an issued runner certificate (runners renew at half of it)")
		runnerAuth = flag.String("runner-auth", envOr("SHIFT_HUB_RUNNER_AUTH", "both"),
			`runner credentials accepted: "mtls", "bearer" or "both"`)

		oidcIssuer   = flag.String("oidc-issuer", os.Getenv("SHIFT_HUB_OIDC_ISSUER"), "OIDC issuer URL (enables the OIDC admin realm)")
		oidcClientID = flag.String("oidc-client-id", os.Getenv("SHIFT_HUB_OIDC_CLIENT_ID"), "OIDC client id")
		oidcRedirect = flag.String("oidc-redirect-url", os.Getenv("SHIFT_HUB_OIDC_REDIRECT_URL"), "dashboard login callback URL, e.g. https://hub.example:8400/auth/callback (enables browser login)")
		kekFile      = flag.String("kek-file", os.Getenv("SHIFT_HUB_KEK_FILE"), "active KEK file, 32 raw bytes (enables the secrets store)")
		kekFilesOld  = flag.String("kek-files-old", os.Getenv("SHIFT_HUB_KEK_FILES_OLD"), "comma-separated retired KEK files still needed to unwrap")

		schedInterval = flag.Duration("sched-interval", envDuration("SHIFT_HUB_SCHED_INTERVAL", 5*time.Second), "scheduler poll interval")
		statusSweep   = flag.Duration("status-sweep", envDuration("SHIFT_HUB_STATUS_SWEEP", 10*time.Minute),
			"how often to prune read/expired async execution status (ADR-0042)")

		connAllow = flag.String("connector-allow", os.Getenv("SHIFT_HUB_CONNECTOR_ALLOW"), "comma-separated connector allowlist (empty = all); cloud hubs restrict")
		connDeny  = flag.String("connector-deny", os.Getenv("SHIFT_HUB_CONNECTOR_DENY"), "comma-separated connector denylist (hidden + blocked at deploy)")

		// Rate limits per class, requests/sec (M6c, ADR-0021). 0 = disabled
		// (the default) — loopback/dev/self-hosted stay frictionless; cloud
		// deployments set real numbers. Burst defaults to ~2x rps.
		rlAdminRPS  = flag.Float64("rl-admin-rps", envFloat("SHIFT_HUB_RL_ADMIN_RPS", 0), "per-admin-identity request/sec limit (0=off)")
		rlRunnerRPS = flag.Float64("rl-runner-rps", envFloat("SHIFT_HUB_RL_RUNNER_RPS", 0), "per-runner request/sec limit (0=off)")
		rlPublicRPS = flag.Float64("rl-public-rps", envFloat("SHIFT_HUB_RL_PUBLIC_RPS", 0), "per-client-IP request/sec limit on unauthenticated routes (0=off)")
	)
	flag.Parse()

	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("hubd", version)
		return
	}

	// Structured logs on STDOUT (ADR-0046). SHIFT_LOG_LEVEL is the
	// platform-wide knob; SHIFT_HUB_LOG_LEVEL still works for anyone who
	// already set it.
	shiftlog.Setup(shiftlog.Options{
		Component: shiftlog.ComponentHub,
		Version:   version,
		Level:     envOr("SHIFT_HUB_LOG_LEVEL", os.Getenv("SHIFT_LOG_LEVEL")),
		Format:    os.Getenv("SHIFT_LOG_FORMAT"),
	})
	if *dsn == "" {
		shiftlog.Fatalf("hubd: -db (or SHIFT_HUB_DB) is required")
	}
	// Env only — a flag would leak the token into process listings.
	adminToken := os.Getenv("SHIFT_HUB_ADMIN_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	st, err := store.Open(ctx, *dsn)
	cancel()
	if err != nil {
		shiftlog.Fatalf("hubd: %v", err)
	}
	defer st.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	err = st.Migrate(ctx)
	cancel()
	if err != nil {
		shiftlog.Fatalf("hubd: migrate: %v", err) //nolint:gocritic // exitAfterDefer: startup-fatal; process exits and the OS reclaims the pool/fds — deferred st.Close() is moot
	}

	opts := api.Options{AdminToken: adminToken, LeaseTTL: *leaseTTL, RunnerAuth: api.RunnerAuthMode(*runnerAuth)}
	if *runnerCACert != "" || *runnerCAKey != "" {
		ca, err := pki.Load("runner", *runnerCACert, *runnerCAKey, *runnerCertTTL)
		if err != nil {
			shiftlog.Fatalf("hubd: %v", err) //nolint:gocritic // exitAfterDefer: startup-fatal, the OS reclaims the pool
		}
		opts.RunnerCA = ca
		//nolint:gosec // G706: both values are our own — a parsed duration flag and the CA's own NotAfter
		slog.Info("runner mTLS enabled",
			shiftlog.KeyEvent, "hub.runner_mtls.enabled",
			"cert_ttl", runnerCertTTL.String(), "ca_not_after", ca.NotAfter().UTC().Format(time.RFC3339))
	}
	var gwLoop *gwsync.Loop
	if *gatewayCACert != "" || *gatewayCAKey != "" {
		gwCA, err := pki.Load("gateway", *gatewayCACert, *gatewayCAKey, *gatewayCertTTL)
		if err != nil {
			shiftlog.Fatalf("hubd: %v", err) //nolint:gocritic // exitAfterDefer: startup-fatal, the OS reclaims the pool
		}
		// The hub mints its OWN client certificate from this CA at start-up. It
		// already holds the CA key, so a second file to manage would buy
		// nothing; and the identity is short-lived and in-memory, so a hub
		// restart rotates it for free.
		hubCert, err := selfIssue(gwCA)
		if err != nil {
			shiftlog.Fatalf("hubd: %v", err) //nolint:gocritic // exitAfterDefer: startup-fatal, the OS reclaims the pool
		}
		client := gwpush.New(gwCA, hubCert, 30*time.Second)
		opts.Gateways = client
		gwLoop = gwsync.New(gwsync.Options{
			Store: st, Client: client,
			RunnerCA: func() []byte {
				if opts.RunnerCA == nil {
					return nil
				}
				return opts.RunnerCA.CAPEM()
			},
		})
		//nolint:gosec // G706: both values are our own — a parsed duration flag and the CA's own NotAfter
		slog.Info("gateway adoption enabled",
			shiftlog.KeyEvent, "hub.gateway_ca.enabled",
			"cert_ttl", gatewayCertTTL.String(), "ca_not_after", gwCA.NotAfter().UTC().Format(time.RFC3339))
	}

	if policy := connpolicy.Parse(*connAllow, *connDeny); policy.Restricted() {
		opts.ConnectorPolicy = policy
		slog.Info("connector capability policy active", shiftlog.KeyEvent, "hub.connector_policy.active")
	}

	if *oidcIssuer != "" {
		// Client secret is env-only, like the admin token.
		clientSecret := os.Getenv("SHIFT_HUB_OIDC_CLIENT_SECRET")
		opts.OIDC, opts.OIDCFlow = mustOIDC(*oidcIssuer, *oidcClientID, clientSecret, *oidcRedirect)
		if adminToken != "" {
			slog.Warn("break-glass admin token is set alongside OIDC — unset SHIFT_HUB_ADMIN_TOKEN once OIDC login works",
				shiftlog.KeyEvent, "hub.breakglass.enabled")
		}
	}
	if *kekFile != "" {
		var old []string
		if *kekFilesOld != "" {
			old = strings.Split(*kekFilesOld, ",")
		}
		provider, err := kek.NewLocalFiles(*kekFile, old...)
		if err != nil {
			shiftlog.Fatalf("hubd: %v", err)
		}
		opts.Secrets = secrets.New(st, provider)
	}

	// Every replica runs the scheduler loop; the store's advisory lock
	// elects one worker per pass (ADR-0012).
	sched := scheduler.New(st, scheduler.Options{Interval: *schedInterval})
	schedCtx, stopSched := context.WithCancel(context.Background())
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		sched.Run(schedCtx)
	}()
	opts.SchedStatus = sched.Status

	// Gateway reconcile (ADR-0049 §6). A gateway never dials inward, so
	// renewal and configuration are things the HUB has to notice on a timer —
	// this loop is the gateway's whole lifeline.
	gwCtx, stopGW := context.WithCancel(context.Background())
	gwDone := make(chan struct{})
	if gwLoop != nil {
		go func() {
			defer close(gwDone)
			gwLoop.Run(gwCtx)
		}()
	} else {
		close(gwDone)
	}
	defer func() { stopGW(); <-gwDone }()

	// Prune caller-facing execution status (ADR-0042 §3c). Unsynchronised
	// across replicas on purpose: this only deletes rows already past their
	// grace or TTL, so concurrent passes are wasted work rather than a
	// correctness problem — unlike the scheduler, where a double fire is a
	// duplicated side effect.
	go st.SweepStatus(schedCtx, *statusSweep, store.ConsumedGrace, slog.Default())

	// Rate limiting (M6c, ADR-0021). Burst ~2x rps (min 1). Disabled classes
	// (rps<=0) are no-ops. Runners poll leases, so they get a higher ceiling.
	burst := func(rps float64) int {
		if b := int(rps * 2); b > 0 {
			return b
		}
		return 1
	}
	opts.RateLimit = ratelimit.New(map[string]ratelimit.Cfg{
		"admin":  {RPS: *rlAdminRPS, Burst: burst(*rlAdminRPS)},
		"runner": {RPS: *rlRunnerRPS, Burst: burst(*rlRunnerRPS)},
		"public": {RPS: *rlPublicRPS, Burst: burst(*rlPublicRPS)},
	})
	defer opts.RateLimit.Stop()

	// Prometheus /metrics (M6a, ADR-0020). Sources platform-wide stats per
	// scrape via a background context (no tenant scope — operational metrics).
	metricsH, err := telemetry.NewHub(func(ctx context.Context) (telemetry.Snapshot, error) {
		s, err := st.PlatformStats(ctx)
		if err != nil {
			return telemetry.Snapshot{}, err
		}
		tasks := make(map[string]int64, len(s.Tasks))
		for k, v := range s.Tasks {
			tasks[k] = int64(v)
		}
		return telemetry.Snapshot{
			Tasks: tasks, OldestQueuedSec: s.OldestQueuedSec,
			RunnersActive: int64(s.RunnersActive), RunnersTotal: int64(s.RunnersTotal),
			SchedulesDue: int64(s.SchedulesDue), Schedules: int64(s.Schedules), Flows: int64(s.Flows),
		}, nil
	}, func() map[string]int64 {
		out := map[string]int64{}
		for _, c := range opts.RateLimit.Classes() {
			out[c] = opts.RateLimit.Rejected(c)
		}
		return out
	})
	if err != nil {
		shiftlog.Fatalf("hubd: metrics: %v", err)
	}
	opts.MetricsHandler = metricsH.Handler
	opts.RecordHTTP = metricsH.RecordHTTP

	h, err := api.Handler(st, opts)
	if err != nil {
		shiftlog.Fatalf("hubd: %v (set SHIFT_HUB_ADMIN_TOKEN or configure OIDC)", err)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if opts.RunnerCA != nil {
		// VerifyClientCertIfGiven, not Require: this listener also serves the
		// dashboard and the human realm, and requiring a client certificate
		// would lock every browser out. "If given" is still fail-closed for
		// what matters — an INVALID certificate aborts the handshake, and a
		// verified chain is the only way r.TLS.VerifiedChains is non-empty,
		// which is the only thing the runner realm reads.
		srv.TLSConfig = &tls.Config{
			ClientAuth: tls.VerifyClientCertIfGiven,
			ClientCAs:  opts.RunnerCA.Pool(),
			MinVersion: tls.VersionTLS12,
		}
		if *tlsCert == "" {
			slog.Warn("a runner CA is configured but this hub serves plaintext — client certificates "+
				"cannot be presented over HTTP, so runners must use bearer secrets",
				shiftlog.KeyEvent, "hub.runner_mtls.unreachable")
		}
	}
	go func() {
		var err error
		if *tlsCert != "" || *tlsKey != "" {
			slog.Info("hub started", shiftlog.KeyEvent, "hub.started",
				"listen", *listen, "tls", true, "lease_ttl", leaseTTL.String(),
				"runner_auth", string(opts.RunnerAuth))
			err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			slog.Info("hub started on plaintext HTTP — keep it loopback or TLS-terminated",
				shiftlog.KeyEvent, "hub.started",
				"listen", *listen, "tls", false, "lease_ttl", leaseTTL.String(),
				"runner_auth", string(opts.RunnerAuth))
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			shiftlog.Fatalf("hubd: serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	slog.Info("shutting down", shiftlog.KeyEvent, "hub.stopped")
	stopSched()
	<-schedDone
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// mustOIDC discovers the issuer with retry — in compose the IdP may
// start after hubd, and failing the whole hub for a slow IdP would be a
// worse failure mode than a short boot delay.
func mustOIDC(issuer, clientID, clientSecret, redirectURL string) (*oidcauth.Verifier, *oidcauth.Flow) {
	const attempts = 30
	for i := 1; ; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		verifier, err := oidcauth.New(ctx, oidcauth.Config{IssuerURL: issuer, ClientID: clientID})
		if err == nil && redirectURL != "" {
			var flow *oidcauth.Flow
			flow, err = oidcauth.NewFlow(ctx, oidcauth.FlowConfig{
				Config:       oidcauth.Config{IssuerURL: issuer, ClientID: clientID},
				ClientSecret: clientSecret,
				RedirectURL:  redirectURL,
			})
			if err == nil {
				cancel()
				return verifier, flow
			}
		} else if err == nil {
			cancel()
			return verifier, nil
		}
		cancel()
		if i >= attempts {
			shiftlog.Fatalf("hubd: OIDC discovery for %s failed after %d attempts: %v", issuer, attempts, err) //nolint:gosec // G706: operator-supplied issuer flag
		}
		slog.Warn("OIDC discovery failed, retrying",
			shiftlog.KeyEvent, "hub.oidc.retry", "attempt", i, "attempts", attempts, shiftlog.KeyError, err.Error())
		time.Sleep(2 * time.Second)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
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

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// selfIssue mints the hub's own client certificate from the gateway CA
// (ADR-0049 §2) — the credential a gateway checks before believing a
// configuration push.
//
// In memory and short-lived by construction: the hub already holds the CA key,
// so writing a second key pair to disk would add a file to protect without
// adding a boundary. A restart rotates it.
func selfIssue(ca *pki.CA) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("hub identity key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return nil, fmt.Errorf("hub identity request: %w", err)
	}
	issued, err := ca.Sign(csr, gwpush.HubSubject, pki.UsageClient)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(issued.CertPEM)
	if blk == nil {
		return nil, errors.New("hub identity: the issued certificate is not PEM")
	}
	return &tls.Certificate{Certificate: [][]byte{blk.Bytes}, PrivateKey: key}, nil
}
