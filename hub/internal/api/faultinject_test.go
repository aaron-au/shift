package api_test

// Postgres-level store fault injection (TC-008).
//
// WHY the fault is injected at the DATABASE rather than behind a Go interface:
// hub/internal/api holds a CONCRETE *store.Store, so there is no seam to wrap
// without editing production code. A test-package interface would need the API
// to accept one, and driving a hand-written stub through it would answer the
// wrong question anyway. The claim worth testing is not "a handler returns 500
// when the store returns an error" — it is "when a statement fails PART WAY
// THROUGH a multi-statement store operation, nothing partial survives". Only a
// real failure inside the real transaction exercises the real rollback, and
// only the real database can say what is left behind afterwards.
//
// The mechanism is a BEFORE-ROW trigger on a chosen table that raises with a
// chosen SQLSTATE on a chosen firing. That covers what the register asks for:
//
//	(a) the Nth call   — a sequence counts firings. nextval is deliberately NOT
//	                     transactional, so the count survives the rollback the
//	                     fault itself causes; "fail the 2nd call" keeps meaning
//	                     that even when the 2nd call is rolled back.
//	(b) a chosen error — any SQLSTATE, including the ones with distinct
//	                     semantics: serialization failure (40001), unique
//	                     violation (23505) and connection failure (08006). A
//	                     delay instead of a raise blows the CALLER's context
//	                     deadline mid-statement, and killConnections drops the
//	                     pool's backends for a genuinely broken connection.
//
// A trigger targets a (table, operation) pair, and an optional WHEN condition
// narrows it to one store method where a table has several writers — e.g. the
// gateways UPDATE that marks adoption versus the one that learns a fingerprint.
//
// NB: this harness lives in the api test package because that is where the
// error arms under test are. If gwsync/gwpush ever want it, it belongs in
// hub/internal/pgtest — which is production code and out of scope here.

import (
	"context"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// SQLSTATEs whose handling differs semantically, not just in the message.
const (
	sqlStateSerializationFailure = "40001" // a lost race: the caller's retry is the fix
	sqlStateUniqueViolation      = "23505" // a constraint said no: retrying never helps
	sqlStateConnectionFailure    = "08006" // the connection is gone mid-operation
)

// faultIdent guards every identifier that reaches DDL. Everything here is
// test-authored, but DDL cannot be parameterised, so the check is what makes
// the concatenation below safe to read.
var faultIdent = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// pgFault describes one injected database fault.
type pgFault struct {
	Label string // names the trigger, its counter sequence and its config row
	Table string // table the trigger is attached to
	Op    string // INSERT | UPDATE | DELETE
	// When narrows the trigger to one writer of the table — a trigger WHEN
	// condition over NEW/OLD, e.g. "NEW.adopted_at IS NOT NULL".
	When string
	// FailOn is the first firing to fail (1 = the very first; default 1), and
	// FailTo the last (default: every firing from FailOn onwards).
	FailOn int
	FailTo int
	// SQLState raises with that error code. Empty raises nothing — combined
	// with DelayMS that turns the fault into a stall, which is how a caller's
	// context deadline is made to expire mid-statement.
	SQLState string
	Message  string
	DelayMS  int
}

// faultDB installs and controls faults over its own connection, deliberately
// separate from the pool under test: enabling a fault must not depend on the
// connection the fault is about to break.
type faultDB struct {
	t    *testing.T
	dsn  string
	conn *pgx.Conn
}

func newFaultDB(t *testing.T, dsn string) *faultDB {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("fault injector: connect: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})
	f := &faultDB{t: t, dsn: dsn, conn: conn}

	f.exec(`CREATE TABLE IF NOT EXISTS shift_fault (
		  label     text PRIMARY KEY,
		  enabled   boolean NOT NULL DEFAULT false,
		  fail_from bigint  NOT NULL DEFAULT 1,
		  fail_to   bigint  NOT NULL DEFAULT 9223372036854775807,
		  sqlstate  text    NOT NULL DEFAULT '',
		  message   text    NOT NULL DEFAULT 'injected fault',
		  delay_ms  integer NOT NULL DEFAULT 0)`)

	// The counter is a SEQUENCE on purpose. A counter column would be rolled
	// back by the very exception it triggers, so "fail on the 2nd call" would
	// stall forever on call 2. nextval is exempt from rollback.
	f.exec(`CREATE OR REPLACE FUNCTION shift_fault_fire() RETURNS trigger LANGUAGE plpgsql AS $fn$
		DECLARE f shift_fault%ROWTYPE; n bigint;
		BEGIN
		  SELECT * INTO f FROM shift_fault WHERE label = TG_ARGV[0];
		  IF FOUND AND f.enabled THEN
		    n := nextval('shift_fault_seq_' || TG_ARGV[0]);
		    IF n >= f.fail_from AND n <= f.fail_to THEN
		      IF f.delay_ms > 0 THEN
		        PERFORM pg_sleep(f.delay_ms::numeric / 1000);
		      END IF;
		      IF f.sqlstate <> '' THEN
		        RAISE EXCEPTION USING ERRCODE = f.sqlstate, MESSAGE = f.message;
		      END IF;
		    END IF;
		  END IF;
		  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
		  RETURN NEW;
		END $fn$`)
	return f
}

// install attaches a fault, disabled. Tests enable it around the one call they
// mean to break, so setup traffic is never caught by it.
func (f *faultDB) install(spec pgFault) {
	f.t.Helper()
	if !faultIdent.MatchString(spec.Label) {
		f.t.Fatalf("fault label %q is not a bare identifier", spec.Label)
	}
	if !faultIdent.MatchString(spec.Table) {
		f.t.Fatalf("fault table %q is not a bare identifier", spec.Table)
	}
	switch spec.Op {
	case "INSERT", "UPDATE", "DELETE":
	default:
		f.t.Fatalf("fault op %q is not INSERT/UPDATE/DELETE", spec.Op)
	}
	failFrom, failTo := int64(spec.FailOn), int64(spec.FailTo)
	if failFrom <= 0 {
		failFrom = 1
	}
	if failTo <= 0 {
		failTo = math.MaxInt64
	}
	msg := spec.Message
	if msg == "" {
		msg = "injected fault"
	}

	seq, trigger := "shift_fault_seq_"+spec.Label, "shift_fault_trg_"+spec.Label
	//nolint:gosec // G202: identifiers are validated against faultIdent above; DDL cannot be parameterised
	f.exec(`CREATE SEQUENCE IF NOT EXISTS ` + seq)
	f.exec(`INSERT INTO shift_fault (label, enabled, fail_from, fail_to, sqlstate, message, delay_ms)
	        VALUES ($1,false,$2,$3,$4,$5,$6)
	        ON CONFLICT (label) DO UPDATE SET enabled = false, fail_from = $2, fail_to = $3,
	          sqlstate = $4, message = $5, delay_ms = $6`,
		spec.Label, failFrom, failTo, spec.SQLState, msg, spec.DelayMS)

	when := ""
	if spec.When != "" {
		when = " WHEN (" + spec.When + ")"
	}
	//nolint:gosec // G202: identifiers validated above; the WHEN clause is a test-authored literal
	f.exec(`DROP TRIGGER IF EXISTS ` + trigger + ` ON ` + spec.Table)
	//nolint:gosec // G202: as above — trigger DDL takes no bind parameters
	f.exec(`CREATE TRIGGER ` + trigger + ` BEFORE ` + spec.Op + ` ON ` + spec.Table +
		` FOR EACH ROW` + when + ` EXECUTE FUNCTION shift_fault_fire('` + spec.Label + `')`)

	f.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		//nolint:gosec // G202: identifiers validated above
		_, _ = f.conn.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON `+spec.Table)
	})
}

func (f *faultDB) enable(label string)  { f.setEnabled(label, true) }
func (f *faultDB) disable(label string) { f.setEnabled(label, false) }

func (f *faultDB) setEnabled(label string, on bool) {
	f.t.Helper()
	f.exec(`UPDATE shift_fault SET enabled = $2 WHERE label = $1`, label, on)
}

// fired reports how many times the fault's trigger has evaluated its counter —
// the proof that a test provoked the path it thought it was provoking rather
// than failing somewhere else entirely.
func (f *faultDB) fired(label string) int64 {
	f.t.Helper()
	if !faultIdent.MatchString(label) {
		f.t.Fatalf("fault label %q is not a bare identifier", label)
	}
	var last int64
	var called bool
	//nolint:gosec // G202: label validated against faultIdent
	if err := f.conn.QueryRow(f.t.Context(),
		`SELECT last_value, is_called FROM shift_fault_seq_`+label).Scan(&last, &called); err != nil {
		f.t.Fatalf("fault %q: reading the firing count: %v", label, err)
	}
	if !called {
		return 0
	}
	return last
}

// killConnections terminates every other backend on this database — a genuine
// broken connection rather than a raised error code, which is what a failover
// or an OOM-killed Postgres looks like to a live pool.
func (f *faultDB) killConnections() {
	f.t.Helper()
	f.exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	        WHERE datname = current_database() AND pid <> pg_backend_pid()`)
}

// count runs a scalar COUNT(*)-shaped query for the "nothing partial was left
// behind" assertions, which have to look at the database directly: a handler
// that leaked half a write would happily keep answering the API correctly.
func (f *faultDB) count(sql string, args ...any) int64 {
	f.t.Helper()
	var n int64
	if err := f.conn.QueryRow(f.t.Context(), sql, args...).Scan(&n); err != nil {
		f.t.Fatalf("fault injector: %s: %v", sql, err)
	}
	return n
}

func (f *faultDB) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.conn.Exec(f.t.Context(), sql, args...); err != nil {
		f.t.Fatalf("fault injector: %v", err)
	}
}

// hideTable renames a table out of the way. It is how a read-only route whose
// OWN credential check hits the database is made to fail: taking the whole
// database away would only produce a 401 from the auth middleware, which says
// nothing about the handler behind it.
func (f *faultDB) hideTable(name string) {
	f.t.Helper()
	if !faultIdent.MatchString(name) {
		f.t.Fatalf("table %q is not a bare identifier", name)
	}
	//nolint:gosec // G202: identifier validated above; DDL cannot be parameterised
	f.exec(`ALTER TABLE ` + name + ` RENAME TO ` + name + `_hidden`)
}

func (f *faultDB) unhideTable(name string) {
	f.t.Helper()
	if !faultIdent.MatchString(name) {
		f.t.Fatalf("table %q is not a bare identifier", name)
	}
	//nolint:gosec // G202: identifier validated above
	f.exec(`ALTER TABLE ` + name + `_hidden RENAME TO ` + name)
}
