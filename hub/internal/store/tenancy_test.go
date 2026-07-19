package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// TestTenantIsolation proves that once a second account exists, nothing
// leaks across the boundary: flows, tasks, claims, runners, and secrets
// are all scoped by the account carried in the context.
func TestTenantIsolation(t *testing.T) {
	s := open(t)
	base := t.Context()

	acctB, err := s.CreateAccount(base, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	ctxA := base // default account
	ctxB := store.WithAccount(base, acctB)

	// Same flow name in both accounts — distinct flows.
	if _, err := s.DeployFlow(ctxA, "orders", flowDoc); err != nil {
		t.Fatal(err)
	}
	if err := s.PublishFlow(ctxA, "orders", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeployFlow(ctxB, "orders", flowDoc); err != nil {
		t.Fatal(err)
	}
	// B's publish must not touch A's same-named flow.
	if err := s.PublishFlow(ctxB, "orders", 1); err != nil {
		t.Fatal(err)
	}
	flowsB, err := s.Flows(ctxB)
	if err != nil || len(flowsB) != 1 {
		t.Fatalf("account B flows = %d, %v (want 1)", len(flowsB), err)
	}

	// A task enqueued in A is invisible to B: listing, direct get, claim.
	taskA, err := s.Enqueue(ctxA, "orders", 0, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if tasksB, err := s.Tasks(ctxB, 10); err != nil || len(tasksB) != 0 {
		t.Fatalf("account B sees %d of A's tasks, %v (want 0)", len(tasksB), err)
	}
	if _, err := s.GetTask(ctxB, taskA); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-account GetTask err = %v, want ErrNotFound", err)
	}
	if attempts, err := s.TaskAttempts(ctxB, taskA); err != nil || len(attempts) != 0 {
		t.Fatalf("cross-account TaskAttempts = %d, %v (want 0)", len(attempts), err)
	}

	// Runner registered under B must not claim A's queued task.
	tokB, _, err := s.CreateRegistrationToken(ctxB, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	runnerB, secretB, err := s.RegisterRunner(ctxB, tokB, "runner-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, acct, err := s.AuthRunner(base, secretB); err != nil || acct != acctB {
		t.Fatalf("AuthRunner account = %q, %v (want %q)", acct, err, acctB)
	}
	if claimed, err := s.Claim(ctxB, runnerB, time.Minute); err != nil || claimed != nil {
		t.Fatalf("cross-account claim = %+v, %v (want nil)", claimed, err)
	}
	// The rightful account still gets it.
	tokA, _, err := s.CreateRegistrationToken(ctxA, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	runnerA, _, err := s.RegisterRunner(ctxA, tokA, "runner-a")
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(ctxA, runnerA, time.Minute)
	if err != nil || claimed == nil || claimed.ID != taskA {
		t.Fatalf("same-account claim = %+v, %v (want task %s)", claimed, err, taskA)
	}

	// Runner listings are per account.
	if runnersA, err := s.Runners(ctxA); err != nil || len(runnersA) != 1 || runnersA[0].ID != runnerA {
		t.Fatalf("account A runners = %+v, %v", runnersA, err)
	}

	// Secrets are per account: same name, independent envelopes;
	// cross-account fetch sees nothing.
	if _, _, err := s.UpsertSecret(ctxA, "api_key", []byte("ct-a"), []byte("dek-a"), "kek-1", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.UpsertSecret(ctxB, "api_key", []byte("ct-b"), []byte("dek-b"), "kek-1", ""); err != nil {
		t.Fatal(err)
	}
	envsB, err := s.SecretEnvelopes(ctxB, []string{"api_key"})
	if err != nil || len(envsB) != 1 || string(envsB[0].Ciphertext) != "ct-b" {
		t.Fatalf("account B envelopes = %+v, %v", envsB, err)
	}
	if err := s.DeleteSecret(ctxB, "api_key"); err != nil {
		t.Fatal(err)
	}
	if envsA, err := s.SecretEnvelopes(ctxA, []string{"api_key"}); err != nil || len(envsA) != 1 {
		t.Fatalf("A's secret survived B's delete? envelopes = %+v, %v", envsA, err)
	}
}

func TestUpsertUserByOIDC(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	u1, err := s.UpsertUserByOIDC(ctx, "https://idp.example", "sub-123", "a@example.com", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	if u1.Role != "admin" || u1.AccountID != store.DefaultAccountID {
		t.Fatalf("first login user = %+v", u1)
	}

	// Second login: same identity, refreshed profile, same id.
	u2, err := s.UpsertUserByOIDC(ctx, "https://idp.example", "sub-123", "new@example.com", "Alice L")
	if err != nil {
		t.Fatal(err)
	}
	if u2.ID != u1.ID || u2.Email != "new@example.com" {
		t.Fatalf("relogin user = %+v (want id %s, refreshed email)", u2, u1.ID)
	}

	// Same subject at a different issuer is a different user.
	u3, err := s.UpsertUserByOIDC(ctx, "https://other.example", "sub-123", "a@example.com", "Alice")
	if err != nil || u3.ID == u1.ID {
		t.Fatalf("cross-issuer user = %+v, %v (must differ)", u3, err)
	}
}

func TestSecretStore(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	_, v1, err := s.UpsertSecret(ctx, "token", []byte("ct1"), []byte("dek1"), "kek-old", "")
	if err != nil || v1 != 1 {
		t.Fatalf("first upsert version = %d, %v", v1, err)
	}
	id, v2, err := s.UpsertSecret(ctx, "token", []byte("ct2"), []byte("dek2"), "kek-old", "")
	if err != nil || v2 != 2 {
		t.Fatalf("replace version = %d, %v (want 2)", v2, err)
	}

	metas, err := s.Secrets(ctx)
	if err != nil || len(metas) != 1 || metas[0].Version != 2 {
		t.Fatalf("metas = %+v, %v", metas, err)
	}

	// Rotation work list + rewrap.
	stale, err := s.SecretEnvelopesNotWrappedBy(ctx, "kek-new")
	if err != nil || len(stale) != 1 {
		t.Fatalf("stale = %+v, %v", stale, err)
	}
	if err := s.UpdateSecretWrap(ctx, id, []byte("dek2-rewrapped"), "kek-new"); err != nil {
		t.Fatal(err)
	}
	stale, err = s.SecretEnvelopesNotWrappedBy(ctx, "kek-new")
	if err != nil || len(stale) != 0 {
		t.Fatalf("post-rotate stale = %+v, %v (want none)", stale, err)
	}
	envs, err := s.SecretEnvelopes(ctx, []string{"token"})
	if err != nil || len(envs) != 1 || string(envs[0].Ciphertext) != "ct2" {
		t.Fatalf("rewrap touched ciphertext: %+v, %v", envs, err)
	}

	if err := s.DeleteSecret(ctx, "token"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSecret(ctx, "token"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double delete err = %v, want ErrNotFound", err)
	}
}
