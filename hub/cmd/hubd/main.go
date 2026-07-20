// Command hubd is the SHIFT hub: the HA control plane owning identity,
// flow versions, and the durable task queue (ADR-0002, ADR-0009).
// Stateless over Postgres — run as many replicas as you like; the queue
// coordinates through SKIP LOCKED and leases, migrations through an
// advisory lock.
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

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/connpolicy"
	"github.com/aaron-au/shift/hub/internal/kek"
	"github.com/aaron-au/shift/hub/internal/oidcauth"
	"github.com/aaron-au/shift/hub/internal/ratelimit"
	"github.com/aaron-au/shift/hub/internal/scheduler"
	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/hub/internal/telemetry"
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

		oidcIssuer   = flag.String("oidc-issuer", os.Getenv("SHIFT_HUB_OIDC_ISSUER"), "OIDC issuer URL (enables the OIDC admin realm)")
		oidcClientID = flag.String("oidc-client-id", os.Getenv("SHIFT_HUB_OIDC_CLIENT_ID"), "OIDC client id")
		oidcRedirect = flag.String("oidc-redirect-url", os.Getenv("SHIFT_HUB_OIDC_REDIRECT_URL"), "dashboard login callback URL, e.g. https://hub.example:8400/auth/callback (enables browser login)")
		kekFile      = flag.String("kek-file", os.Getenv("SHIFT_HUB_KEK_FILE"), "active KEK file, 32 raw bytes (enables the secrets store)")
		kekFilesOld  = flag.String("kek-files-old", os.Getenv("SHIFT_HUB_KEK_FILES_OLD"), "comma-separated retired KEK files still needed to unwrap")

		schedInterval = flag.Duration("sched-interval", envDuration("SHIFT_HUB_SCHED_INTERVAL", 5*time.Second), "scheduler poll interval")

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
	if *dsn == "" {
		log.Fatal("hubd: -db (or SHIFT_HUB_DB) is required")
	}
	// Env only — a flag would leak the token into process listings.
	adminToken := os.Getenv("SHIFT_HUB_ADMIN_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	st, err := store.Open(ctx, *dsn)
	cancel()
	if err != nil {
		log.Fatalf("hubd: %v", err)
	}
	defer st.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	err = st.Migrate(ctx)
	cancel()
	if err != nil {
		log.Fatalf("hubd: migrate: %v", err) //nolint:gocritic // exitAfterDefer: startup-fatal; process exits and the OS reclaims the pool/fds — deferred st.Close() is moot
	}

	opts := api.Options{AdminToken: adminToken, LeaseTTL: *leaseTTL}
	if policy := connpolicy.Parse(*connAllow, *connDeny); policy.Restricted() {
		opts.ConnectorPolicy = policy
		log.Print("hubd: connector capability policy active")
	}

	if *oidcIssuer != "" {
		// Client secret is env-only, like the admin token.
		clientSecret := os.Getenv("SHIFT_HUB_OIDC_CLIENT_SECRET")
		opts.OIDC, opts.OIDCFlow = mustOIDC(*oidcIssuer, *oidcClientID, clientSecret, *oidcRedirect)
		if adminToken != "" {
			log.Print("hubd: WARNING: break-glass admin token is set alongside OIDC — unset SHIFT_HUB_ADMIN_TOKEN once OIDC login works")
		}
	}
	if *kekFile != "" {
		var old []string
		if *kekFilesOld != "" {
			old = strings.Split(*kekFilesOld, ",")
		}
		provider, err := kek.NewLocalFiles(*kekFile, old...)
		if err != nil {
			log.Fatalf("hubd: %v", err)
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
		log.Fatalf("hubd: metrics: %v", err)
	}
	opts.MetricsHandler = metricsH

	h, err := api.Handler(st, opts)
	if err != nil {
		log.Fatalf("hubd: %v (set SHIFT_HUB_ADMIN_TOKEN or configure OIDC)", err)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		var err error
		if *tlsCert != "" || *tlsKey != "" {
			log.Printf("hubd %s: https://%s (lease TTL %s)", version, *listen, *leaseTTL)
			err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			log.Printf("hubd %s: http://%s (lease TTL %s) — plaintext HTTP, keep it loopback/TLS-terminated", version, *listen, *leaseTTL)
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("hubd: serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	log.Print("hubd: shutting down")
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
			log.Fatalf("hubd: OIDC discovery for %s failed after %d attempts: %v", issuer, attempts, err) //nolint:gosec // G706: operator-supplied issuer flag
		}
		log.Printf("hubd: OIDC discovery (%d/%d): %v — retrying", i, attempts, err)
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
