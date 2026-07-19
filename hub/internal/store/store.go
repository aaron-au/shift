// Package store is the hub's Postgres persistence layer: schema
// migrations, runner identity, flow versions, and the durable task queue
// (SKIP LOCKED + leases — ADR-0002). All SQL is parameterized; secrets
// are stored as SHA-256 hashes only.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// DefaultAccountID is the seed account until OIDC multi-tenancy (M4b).
const DefaultAccountID = "00000000-0000-0000-0000-000000000001"

// Store wraps a pgx pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects and pings.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Ping checks connectivity (readiness probe).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies embedded migrations in filename order, tracked in
// schema_migrations. Safe to run concurrently from multiple hub replicas:
// an advisory lock serializes appliers.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Cluster-wide advisory lock: one migrator at a time (HA hubs).
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(823400)`); err != nil {
		return fmt.Errorf("store: migrate lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock(823400)`) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("store: migrations table: %w", err)
	}

	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var done bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name=$1)`, name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("store: migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Audit appends an audit_log row. Failures are returned, not fatal —
// callers decide whether the action itself already succeeded.
func (s *Store) Audit(ctx context.Context, actor, action, entity string, detail any) error {
	var raw []byte
	if detail != nil {
		var err error
		if raw, err = json.Marshal(detail); err != nil {
			return err
		}
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO audit_log (actor, action, entity, detail) VALUES ($1,$2,$3,$4)`,
		actor, action, entity, raw)
	return err
}

// newUUID returns a random (v4) UUID string.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	dst := make([]byte, 36)
	hex.Encode(dst, b[:4])
	dst[8] = '-'
	hex.Encode(dst[9:], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], b[10:])
	return string(dst)
}

// newSecret returns a bearer secret with the given prefix and its SHA-256.
func newSecret(prefix string) (plaintext string, hash []byte) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	plaintext = prefix + hex.EncodeToString(b[:])
	return plaintext, hashSecret(plaintext)
}

func hashSecret(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}
