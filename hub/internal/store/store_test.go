package store_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/store"
)

var flowDoc = json.RawMessage(`{"name":"orders",
  "source":{"connector":"gen","action":"gen","config":{"records":10}},
  "sink":{"connector":"gen","action":"discard"}}`)

func open(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Idempotent re-run must be a no-op.
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	return s
}

// deployPublished deploys a flowDoc version and publishes it — the
// state most queue tests need (deploys are drafts since M4b).
func deployPublished(t *testing.T, s *store.Store, name string) int {
	t.Helper()
	v, err := s.DeployFlow(t.Context(), name, flowDoc)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PublishFlow(t.Context(), name, v); err != nil {
		t.Fatal(err)
	}
	return v
}

func registerRunner(t *testing.T, s *store.Store, name string) (id, secret string) {
	t.Helper()
	tok, _, err := s.CreateRegistrationToken(t.Context(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	id, secret, err = s.RegisterRunner(t.Context(), tok, name)
	if err != nil {
		t.Fatal(err)
	}
	return id, secret
}

func TestRunnerRegistration(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	tok, _, err := s.CreateRegistrationToken(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	id, secret, err := s.RegisterRunner(ctx, tok, "runner-a")
	if err != nil {
		t.Fatal(err)
	}

	// Single-use: the same token must not register twice.
	if _, _, err := s.RegisterRunner(ctx, tok, "runner-b"); !errors.Is(err, store.ErrTokenInvalid) {
		t.Fatalf("token reuse: err = %v", err)
	}
	// Garbage token.
	if _, _, err := s.RegisterRunner(ctx, "srt_nope", "x"); !errors.Is(err, store.ErrTokenInvalid) {
		t.Fatalf("bad token: err = %v", err)
	}

	// Secret authenticates to the same id and account; garbage does not.
	got, account, err := s.AuthRunner(ctx, secret)
	if err != nil || got != id {
		t.Fatalf("auth = %q, %v (want %q)", got, err, id)
	}
	if account != store.DefaultAccountID {
		t.Fatalf("auth account = %q, want default", account)
	}
	if _, _, err := s.AuthRunner(ctx, "rs_nope"); !errors.Is(err, store.ErrUnauthorized) {
		t.Fatalf("bad secret: err = %v", err)
	}

	runners, err := s.Runners(ctx)
	if err != nil || len(runners) != 1 || runners[0].LastSeen == nil {
		t.Fatalf("runners = %+v, %v", runners, err)
	}
}

func TestFlowVersioning(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	v1, err := s.DeployFlow(ctx, "orders", flowDoc)
	if err != nil || v1 != 1 {
		t.Fatalf("deploy 1 = %d, %v", v1, err)
	}
	v2, err := s.DeployFlow(ctx, "orders", flowDoc)
	if err != nil || v2 != 2 {
		t.Fatalf("deploy 2 = %d, %v", v2, err)
	}

	// Drafts: version 0 resolves nothing until a publish.
	if _, _, err := s.GetFlow(ctx, "orders", 0); !errors.Is(err, store.ErrNotPublished) {
		t.Fatalf("unpublished default: %v (want ErrNotPublished)", err)
	}
	if err := s.PublishFlow(ctx, "orders", 2); err != nil {
		t.Fatal(err)
	}
	f, doc, err := s.GetFlow(ctx, "orders", 0)
	if err != nil || f.LatestVersion != 2 || f.PublishedVersion != 2 || len(doc) == 0 {
		t.Fatalf("published = %+v, %v", f, err)
	}
	// Rollback: publishing an older version repoints the default.
	if err := s.PublishFlow(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}
	if f, _, _ := s.GetFlow(ctx, "orders", 0); f.PublishedVersion != 1 {
		t.Fatalf("rollback publish = %+v", f)
	}
	// Draft versions stay directly addressable.
	if _, _, err := s.GetFlow(ctx, "orders", 2); err != nil {
		t.Fatalf("pinned draft version: %v", err)
	}
	if err := s.PublishFlow(ctx, "orders", 99); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("publish missing version: %v", err)
	}
	if _, _, err := s.GetFlow(ctx, "ghost", 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing flow: %v", err)
	}
}

func TestQueueLifecycle(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "runner-a")

	deployPublished(t, s, "orders")

	// Idempotent enqueue: same key → same task.
	id1, err := s.Enqueue(ctx, "orders", 0, "key-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.Enqueue(ctx, "orders", 0, "key-1", 3)
	if err != nil || id2 != id1 {
		t.Fatalf("idempotent enqueue: %q vs %q, %v", id1, id2, err)
	}

	// Claim → run → heartbeat → complete.
	lt, err := s.Claim(ctx, runnerID, 30*time.Second)
	if err != nil || lt == nil || lt.ID != id1 || lt.Attempt != 1 {
		t.Fatalf("claim = %+v, %v", lt, err)
	}
	if len(lt.Document) == 0 || lt.FlowName != "orders" {
		t.Fatalf("claimed task incomplete: %+v", lt)
	}
	// Queue is now empty for other claimants.
	if other, err := s.Claim(ctx, runnerID, 30*time.Second); err != nil || other != nil {
		t.Fatalf("second claim = %+v, %v", other, err)
	}
	if err := s.Heartbeat(ctx, lt.ID, runnerID, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, lt.ID, runnerID, json.RawMessage(`{"records_in":10}`)); err != nil {
		t.Fatal(err)
	}
	// Completing again = lease lost (already finished).
	if err := s.Complete(ctx, lt.ID, runnerID, nil); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("double complete: %v", err)
	}

	got, err := s.GetTask(ctx, lt.ID)
	if err != nil || got.State != "completed" || got.Attempt != 1 {
		t.Fatalf("task = %+v, %v", got, err)
	}
	atts, err := s.TaskAttempts(ctx, lt.ID)
	if err != nil || len(atts) != 1 || atts[0].Outcome != "completed" {
		t.Fatalf("attempts = %+v, %v", atts, err)
	}
}

func TestFailRequeueAndExhaustion(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "runner-a")

	deployPublished(t, s, "orders")
	id, err := s.Enqueue(ctx, "orders", 0, "", 2)
	if err != nil {
		t.Fatal(err)
	}

	// Attempt 1 fails → requeued.
	lt, _ := s.Claim(ctx, runnerID, 30*time.Second)
	if lt == nil || lt.ID != id {
		t.Fatalf("claim = %+v", lt)
	}
	requeued, err := s.Fail(ctx, lt.ID, runnerID, "boom")
	if err != nil || !requeued {
		t.Fatalf("fail 1: requeued=%v err=%v", requeued, err)
	}

	// Attempt 2 fails → terminal.
	lt, _ = s.Claim(ctx, runnerID, 30*time.Second)
	if lt == nil || lt.Attempt != 2 {
		t.Fatalf("reclaim = %+v", lt)
	}
	requeued, err = s.Fail(ctx, lt.ID, runnerID, "boom again")
	if err != nil || requeued {
		t.Fatalf("fail 2: requeued=%v err=%v", requeued, err)
	}
	got, _ := s.GetTask(ctx, id)
	if got.State != "failed" || got.Error != "boom again" {
		t.Fatalf("task = %+v", got)
	}
}

func TestLeaseExpiryRedispatch(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deadRunner, _ := registerRunner(t, s, "dead")
	liveRunner, _ := registerRunner(t, s, "live")

	deployPublished(t, s, "orders")
	id, err := s.Enqueue(ctx, "orders", 0, "", 3)
	if err != nil {
		t.Fatal(err)
	}

	// "Crash": claim with a tiny lease and never heartbeat.
	lt, _ := s.Claim(ctx, deadRunner, 50*time.Millisecond)
	if lt == nil {
		t.Fatal("no claim")
	}
	time.Sleep(100 * time.Millisecond)

	// Expired lease cannot heartbeat or complete.
	if err := s.Heartbeat(ctx, lt.ID, deadRunner, time.Second); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("heartbeat on expired lease: %v", err)
	}

	// Another runner claims the same task, next attempt.
	lt2, err := s.Claim(ctx, liveRunner, 30*time.Second)
	if err != nil || lt2 == nil || lt2.ID != id || lt2.Attempt != 2 {
		t.Fatalf("re-dispatch = %+v, %v", lt2, err)
	}
	// The dead runner's late completion is rejected.
	if err := s.Complete(ctx, id, deadRunner, nil); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("zombie complete: %v", err)
	}
	if err := s.Complete(ctx, id, liveRunner, nil); err != nil {
		t.Fatal(err)
	}

	atts, _ := s.TaskAttempts(ctx, id)
	if len(atts) != 2 || atts[0].Outcome != "lease-expired" || atts[1].Outcome != "completed" {
		t.Fatalf("attempts = %+v", atts)
	}
}

func TestLeaseExpiryExhaustionFails(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	runnerID, _ := registerRunner(t, s, "flaky")

	deployPublished(t, s, "orders")
	id, err := s.Enqueue(ctx, "orders", 0, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if lt, _ := s.Claim(ctx, runnerID, 50*time.Millisecond); lt == nil {
		t.Fatal("no claim")
	}
	time.Sleep(100 * time.Millisecond)

	// Reap happens on the next claim; the exhausted task fails terminally.
	if lt, err := s.Claim(ctx, runnerID, time.Second); err != nil || lt != nil {
		t.Fatalf("claim after exhaustion = %+v, %v", lt, err)
	}
	got, _ := s.GetTask(ctx, id)
	if got.State != "failed" {
		t.Fatalf("task = %+v", got)
	}
}

func TestAuditAndPing(t *testing.T) {
	s := open(t)
	if err := s.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Audit(t.Context(), "admin", "flow.deploy", "orders", map[string]int{"version": 1}); err != nil {
		t.Fatal(err)
	}
}
