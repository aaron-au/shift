// Package pgtest provides real Postgres for hub tests — the gate runs
// identically everywhere (ADR-0006): against SHIFT_TEST_PG when set (CI
// service container, or the deploy/compose.dev.yml database), otherwise
// against a private throwaway cluster booted with initdb/pg_ctl on a unix
// socket in a temp dir. Tests skip only when neither is available.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// DSN returns a connection string to a fresh, empty database that is
// dropped (env server) or destroyed with its cluster (local pg_ctl) when
// the test ends.
func DSN(t *testing.T) string {
	t.Helper()
	base := os.Getenv("SHIFT_TEST_PG")
	if base == "" {
		base = localCluster(t)
	}

	name := "shift_test_" + randHex(6)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("pgtest: connect %s: %v", redact(base), err)
	}
	// Identifier is generated hex, not user input.
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("pgtest: create database: %v", err)
	}
	_ = admin.Close(ctx)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, base)
		if err != nil {
			return
		}
		defer func() { _ = admin.Close(ctx) }()
		_, _ = admin.Exec(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
	})
	return withDatabase(t, base, name)
}

// withDatabase swaps the database name in a URL- or keyword-form DSN.
func withDatabase(t *testing.T, base, name string) string {
	t.Helper()
	if strings.Contains(base, "://") {
		u, err := url.Parse(base)
		if err != nil {
			t.Fatalf("pgtest: bad SHIFT_TEST_PG url: %v", err)
		}
		u.Path = "/" + name
		return u.String()
	}
	// Keyword form: a later dbname key overrides an earlier one.
	return base + " dbname=" + name
}

// localCluster boots one throwaway cluster per test and tears it down
// with the test. Unix-socket only: nothing listens on TCP.
func localCluster(t *testing.T) string {
	t.Helper()
	pgctl, initdb := findPG(t)

	dir, err := os.MkdirTemp("", "shiftpg") // short path: unix sockets cap at ~104 bytes
	if err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(dir, "data")

	run(t, initdb, "-D", data, "-U", "shift", "-A", "trust", "--no-sync")
	run(t, pgctl, "-D", data, "-w", "-t", "60", "-l", filepath.Join(dir, "log"),
		"-o", fmt.Sprintf("-k %s -c listen_addresses='' -c fsync=off -c full_page_writes=off", dir),
		"start")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, pgctl, "-D", data, "-m", "immediate", "stop") //nolint:gosec // G204: resolved postgres tool path
		_ = cmd.Run()
		_ = os.RemoveAll(dir)
	})
	return fmt.Sprintf("host=%s port=5432 user=shift dbname=postgres", dir)
}

// findPG locates pg_ctl and initdb, skipping the test when Postgres
// tooling is absent and SHIFT_TEST_PG is unset.
func findPG(t *testing.T) (pgctl, initdb string) {
	t.Helper()
	if p, err := exec.LookPath("pg_ctl"); err == nil {
		i, err := exec.LookPath("initdb")
		if err == nil {
			return p, i
		}
	}
	// Common non-PATH locations (Debian/Ubuntu packaging, Homebrew).
	globs := []string{
		"/usr/lib/postgresql/*/bin/pg_ctl",
		"/opt/homebrew/opt/postgresql*/bin/pg_ctl",
		"/usr/local/opt/postgresql*/bin/pg_ctl",
	}
	for _, g := range globs {
		if hits, _ := filepath.Glob(g); len(hits) > 0 {
			p := hits[len(hits)-1]
			return p, filepath.Join(filepath.Dir(p), "initdb")
		}
	}
	t.Skip("pgtest: postgres unavailable — set SHIFT_TEST_PG or install postgresql")
	return "", ""
}

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), name, args...) //nolint:gosec // G204: resolved postgres tool path
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pgtest: %s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// redact hides any password when a DSN appears in test failure output.
func redact(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		u.User = url.User(u.User.Username())
		return u.String()
	}
	return dsn
}
