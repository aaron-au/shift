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
	"syscall"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/store"
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
		log.Fatalf("hubd: migrate: %v", err)
	}

	h, err := api.Handler(st, api.Options{AdminToken: adminToken, LeaseTTL: *leaseTTL})
	if err != nil {
		log.Fatalf("hubd: %v (set SHIFT_HUB_ADMIN_TOKEN)", err)
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
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
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
