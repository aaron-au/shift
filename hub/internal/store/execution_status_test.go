package store_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aaron-au/shift/hub/internal/store"
)

// The lifecycle a status URL depends on: a row exists from ACCEPT, so the 202's
// URL resolves immediately rather than 404ing until the flow happens to finish
// (ADR-0042 §3).
func TestExecutionStatusLifecycle(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)

	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: id, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
	}, "", 0); err != nil {
		t.Fatal(err)
	}

	got, err := s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", "")
	if err != nil {
		t.Fatalf("reading an accepted execution: %v", err)
	}
	if got.State != store.StatusAccepted {
		t.Errorf("state = %q, want %q", got.State, store.StatusAccepted)
	}
	if got.Terminal() {
		t.Error("an accepted execution reported itself terminal")
	}

	started := time.Now().UTC().Add(-time.Second)
	finished := time.Now().UTC()
	if err := s.FinishExecution(ctx, store.ExecutionStatus{
		ID: id, State: store.StatusCompleted, RecordsIn: 41233, RecordsOut: 41233,
		StartedAt: &started, FinishedAt: &finished,
	}); err != nil {
		t.Fatal(err)
	}

	got, err = s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", "")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Terminal() || got.State != store.StatusCompleted {
		t.Errorf("state = %q, want completed", got.State)
	}
	if got.RecordsIn != 41233 || got.RecordsOut != 41233 {
		t.Errorf("counts = %d/%d, want 41233/41233", got.RecordsIn, got.RecordsOut)
	}
	if got.StartedAt == nil || got.FinishedAt == nil {
		t.Error("terminal row is missing its timestamps")
	}
}

// A failure reports the STEP and the class, never record content: an internet
// caller reads this (ADR-0031 canonical error).
func TestFailedExecutionCarriesTheCanonicalError(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)

	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: id, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
	}, "", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishExecution(ctx, store.ExecutionStatus{
		ID: id, State: store.StatusFailed,
		ErrorStep: "post-to-warehouse", ErrorCode: "connector_timeout",
		Error: "the warehouse did not respond",
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ErrorStep != "post-to-warehouse" || got.ErrorCode != "connector_timeout" {
		t.Errorf("error = %+v, want the step and code", got)
	}
}

// THE authorisation property (ADR-0042 §3): every refusal looks identical.
// A distinguishable response confirms that someone else's task exists under
// that id, which is exactly the fact being protected.
func TestEveryRefusalLooksTheSame(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)

	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: id, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
	}, "", 0); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name             string
		id, route, princ string
	}{
		{"an id that does not exist", newID(t), "/orders", "acme-erp"},
		{"the right id under another route", id, "/payroll", "acme-erp"},
		{"the right id as another principal", id, "/orders", "other-corp"},
		{"the right id with no principal", id, "/orders", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.ExecutionStatusByID(ctx, tc.id, tc.route, tc.princ, "")
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Errorf("err = %v, want the same not-found every other refusal gives", err)
			}
		})
	}
}

// An anonymous route has one principal for every caller, so the per-task token
// IS the authorisation (ADR-0042 §3b). Only its digest is stored.
func TestAnonymousStatusNeedsItsCapabilityToken(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)
	digest := sha256Hex("s3cret-capability")

	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: id, FlowName: "hook", Route: "/hooks/shopify", Principal: "anonymous",
	}, digest, 0); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ExecutionStatusByID(ctx, id, "/hooks/shopify", "anonymous", digest); err != nil {
		t.Fatalf("the right token was refused: %v", err)
	}
	for _, tc := range []struct{ name, token string }{
		{"no token", ""},
		{"the wrong token", sha256Hex("guess")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.ExecutionStatusByID(ctx, id, "/hooks/shopify", "anonymous", tc.token)
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Errorf("err = %v, want not-found — never 401 or 403", err)
			}
		})
	}
}

// A runner-minted id that collides must be a fresh id, never a silent
// overwrite: two requests sharing one status row would report each other's
// outcome.
func TestACollidingIDIsRefused(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)

	first := store.ExecutionStatus{ID: id, FlowName: "orders", Route: "/orders", Principal: "a"}
	if err := s.AcceptExecution(ctx, "", first, "", 0); err != nil {
		t.Fatal(err)
	}
	second := store.ExecutionStatus{ID: id, FlowName: "payroll", Route: "/payroll", Principal: "b"}
	if err := s.AcceptExecution(ctx, "", second, "", 0); !errors.Is(err, store.ErrStatusIDTaken) {
		t.Fatalf("err = %v, want ErrStatusIDTaken", err)
	}

	got, err := s.ExecutionStatusByID(ctx, id, "/orders", "a", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.FlowName != "orders" {
		t.Errorf("flow = %q; the second accept overwrote the first", got.FlowName)
	}
}

// Finalising must not reach across accounts. Without the account clause a
// buggy or compromised runner could finalise another tenant's row.
func TestFinishCannotReachAnotherAccountsRow(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)

	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: id, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
	}, "", 0); err != nil {
		t.Fatal(err)
	}

	other := store.WithAccount(ctx, newID(t))
	err := s.FinishExecution(other, store.ExecutionStatus{ID: id, State: store.StatusFailed, Error: "hijack"})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("err = %v, want no rows — another account finalised this row", err)
	}

	got, err := s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.StatusAccepted {
		t.Errorf("state = %q, want it untouched at accepted", got.State)
	}
}

// A consumed row answers Gone rather than not-found while it awaits the
// sweeper: the caller has already proved the capability, so 410 leaks nothing
// and is far kinder than a 404 that reads as "you got the id wrong".
func TestASecondReadIsGoneNotMissing(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)

	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: id, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
	}, "", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishExecution(ctx, store.ExecutionStatus{ID: id, State: store.StatusCompleted}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.ConsumeExecutionStatus(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", ""); !errors.Is(err, store.ErrStatusGone) {
		t.Errorf("err = %v, want ErrStatusGone", err)
	}
}

// Consuming applies only to TERMINAL rows: marking a running task consumed
// would prune it while it is still the caller's only handle on the work.
func TestARunningRowIsNotConsumed(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	id := newID(t)

	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: id, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
	}, "", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.ConsumeExecutionStatus(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", ""); err != nil {
		t.Fatalf("a running row was marked consumed: %v", err)
	}
}

// The sweeper removes what has been read (after a grace) and what nobody ever
// read (after its TTL), and nothing else.
func TestSweepRemovesConsumedAndExpiredOnly(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	live := newID(t)
	consumed := newID(t)
	expired := newID(t)

	for _, id := range []string{live, consumed} {
		if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
			ID: id, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
		}, "", time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	// A sub-second TTL rounds to an expiry of "now", so by the time the sweep
	// runs this row is already past it — which is how a row nobody ever polled
	// looks once its window closes. (A NEGATIVE ttl would not work: the store
	// treats <= 0 as "use the default", which is the right API and made the
	// first version of this test silently assert nothing.)
	if err := s.AcceptExecution(ctx, "", store.ExecutionStatus{
		ID: expired, FlowName: "orders", Route: "/orders", Principal: "acme-erp",
	}, "", time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishExecution(ctx, store.ExecutionStatus{ID: consumed, State: store.StatusCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := s.ConsumeExecutionStatus(ctx, consumed); err != nil {
		t.Fatal(err)
	}

	// Grace of zero means "sweep what has been read", which is what a test
	// wants and what an operator forcing a clean-up would ask for.
	n, err := s.SweepExecutionStatus(ctx, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Errorf("swept %d rows, want at least the consumed and the expired one", n)
	}
	if _, err := s.ExecutionStatusByID(ctx, live, "/orders", "acme-erp", ""); err != nil {
		t.Errorf("the sweeper removed a live row: %v", err)
	}
	for _, id := range []string{consumed, expired} {
		if _, err := s.ExecutionStatusByID(ctx, id, "/orders", "acme-erp", ""); err == nil {
			t.Errorf("row %s survived the sweep", id)
		}
	}
}

// newID mints a UUIDv4 the way a RUNNER does (ADR-0042 §3a): uniqueness from
// randomness rather than from a sequence the hub owns, which is what lets a
// runner quote an id in the 202 without coordinating with anyone.
func newID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
