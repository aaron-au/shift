package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// TestSetUserRole pins role administration: only the two known roles are
// writable, the change is addressed by email within the caller's account, and
// a later OIDC login preserves the new role instead of resetting it.
func TestSetUserRole(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	u, err := s.UpsertUserByOIDC(ctx, "https://idp.example", "sub-1", "aaron@example.com", "Aaron")
	if err != nil {
		t.Fatal(err)
	}
	if u.Role != "admin" {
		t.Fatalf("JIT-provisioned role = %q, want admin (default)", u.Role)
	}

	// An unrecognised role is rejected outright — no silent write.
	if _, err := s.SetUserRole(ctx, "aaron@example.com", "superuser"); err == nil {
		t.Fatal("SetUserRole accepted an unknown role")
	} else if !strings.Contains(err.Error(), "superuser") {
		t.Fatalf("unknown-role error = %v, want it to name the rejected role", err)
	}
	// An unknown email is ErrNotFound, not a no-op success.
	if _, err := s.SetUserRole(ctx, "nobody@example.com", "viewer"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("SetUserRole(unknown email) = %v, want ErrNotFound", err)
	}

	n, err := s.SetUserRole(ctx, "aaron@example.com", "viewer")
	if err != nil || n != 1 {
		t.Fatalf("SetUserRole = %d, %v (want 1 row)", n, err)
	}
	// Re-login must not re-elevate: the JIT upsert refreshes profile fields
	// only, never the role an operator set.
	again, err := s.UpsertUserByOIDC(ctx, "https://idp.example", "sub-1", "aaron@example.com", "Aaron Lees")
	if err != nil {
		t.Fatal(err)
	}
	if again.Role != "viewer" {
		t.Fatalf("role after re-login = %q, want viewer (demotion must stick)", again.Role)
	}
	if again.Name != "Aaron Lees" {
		t.Fatalf("display name after re-login = %q, want the refreshed value", again.Name)
	}
	// Promotion back to admin works.
	if _, err := s.SetUserRole(ctx, "aaron@example.com", "admin"); err != nil {
		t.Fatal(err)
	}
}

// TestSetUserRoleIsAccountScoped: an admin in one account cannot flip a role
// in another, even knowing the email.
func TestSetUserRoleIsAccountScoped(t *testing.T) {
	s := open(t)
	ctxA := t.Context()

	acctB, err := s.CreateAccount(ctxA, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	ctxB := store.WithAccount(ctxA, acctB)

	if _, err := s.UpsertUserByOIDC(ctxA, "https://idp.example", "sub-a", "a@example.com", "A"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetUserRole(ctxB, "a@example.com", "viewer"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-account SetUserRole = %v, want ErrNotFound", err)
	}
}

// TestPlatformStatsSpansAllAccounts: /metrics has no tenant context, so
// PlatformStats deliberately aggregates every account — while Stats stays
// scoped to the caller's. Both must be true at once.
func TestPlatformStatsSpansAllAccounts(t *testing.T) {
	s := open(t)
	ctxA := t.Context()

	acctB, err := s.CreateAccount(ctxA, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	ctxB := store.WithAccount(ctxA, acctB)

	deployPublished(t, s, "orders") // account A
	if _, err := s.DeployFlow(ctxB, "invoices", flowDoc); err != nil {
		t.Fatal(err)
	}
	if err := s.PublishFlow(ctxB, "invoices", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(ctxA, "orders", 0, "", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(ctxB, "invoices", 0, "", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertSchedule(ctxB, "invoices", "* * * * *", true, 3, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	registerRunner(t, s, "runner-a") // account A

	scoped, err := s.Stats(ctxA)
	if err != nil {
		t.Fatal(err)
	}
	if scoped.Flows != 1 || scoped.Tasks["queued"] != 1 {
		t.Fatalf("Stats (account A) = %+v, want 1 flow / 1 queued task", scoped)
	}
	if scoped.Schedules != 0 {
		t.Fatalf("Stats leaked account B's schedule: %+v", scoped)
	}

	all, err := s.PlatformStats(ctxA)
	if err != nil {
		t.Fatalf("PlatformStats: %v", err)
	}
	if all.Flows != 2 {
		t.Fatalf("PlatformStats flows = %d, want 2 (both accounts)", all.Flows)
	}
	if all.Tasks["queued"] != 2 {
		t.Fatalf("PlatformStats queued = %d, want 2 (both accounts)", all.Tasks["queued"])
	}
	if all.Schedules != 1 || all.SchedulesDue != 1 {
		t.Fatalf("PlatformStats schedules = %d due %d, want 1/1", all.Schedules, all.SchedulesDue)
	}
	if all.RunnersTotal != 1 {
		t.Fatalf("PlatformStats runners = %d, want 1", all.RunnersTotal)
	}
	if all.OldestQueuedSec < 0 {
		t.Fatalf("PlatformStats oldest queued = %v", all.OldestQueuedSec)
	}
}

// TestDueCount counts enabled, past-due schedules across the platform (the
// scheduler-lag stat): disabled and future schedules must not inflate it.
func TestDueCount(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deployPublished(t, s, "orders")
	deployPublished(t, s, "invoices")

	if n, err := s.DueCount(ctx); err != nil || n != 0 {
		t.Fatalf("DueCount with no schedules = %d, %v (want 0)", n, err)
	}
	// Past-due but disabled → not counted.
	if _, err := s.UpsertSchedule(ctx, "orders", "* * * * *", false, 3, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.DueCount(ctx); err != nil || n != 0 {
		t.Fatalf("DueCount with a disabled past-due schedule = %d, %v (want 0)", n, err)
	}
	// Enabled but in the future → not counted.
	if _, err := s.UpsertSchedule(ctx, "invoices", "* * * * *", true, 3, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.DueCount(ctx); err != nil || n != 0 {
		t.Fatalf("DueCount with a future schedule = %d, %v (want 0)", n, err)
	}
	// Enabled and past-due → counted.
	if _, err := s.UpsertSchedule(ctx, "orders", "* * * * *", true, 3, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.DueCount(ctx); err != nil || n != 1 {
		t.Fatalf("DueCount = %d, %v (want 1)", n, err)
	}
}

// TestOpenRejectsUnusableDSN: Open must fail loudly rather than hand back a
// half-built Store — the hub cannot serve control traffic without its DB.
func TestOpenRejectsUnusableDSN(t *testing.T) {
	if _, err := store.Open(t.Context(), "://not a dsn"); err == nil {
		t.Fatal("Open accepted a malformed DSN")
	} else if !strings.Contains(err.Error(), "store:") {
		t.Fatalf("malformed DSN error = %v, want a store-tagged error", err)
	}

	// Reachable syntax, unreachable server: the ping must fail the open.
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if _, err := store.Open(ctx, "postgres://nobody@127.0.0.1:1/shift?sslmode=disable&connect_timeout=1"); err == nil {
		t.Fatal("Open succeeded against an unreachable server")
	}
}

// TestQueriesFailLoudlyWhenDBIsDown: with the pool closed, every reader must
// return an error. Silently returning empty lists (or ErrNotFound) during an
// outage would let the API report "no flows" / "404" for data that exists.
func TestQueriesFailLoudlyWhenDBIsDown(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deployPublished(t, s, "orders")
	taskID, err := s.Enqueue(ctx, "orders", 0, "", 3)
	if err != nil {
		t.Fatal(err)
	}

	s.Close() // simulate the DB going away under a live hub

	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping succeeded with a closed pool (readiness would stay green)")
	}

	// Lookups of rows that DO exist must not be reported as "not found".
	if _, _, err := s.GetFlow(ctx, "orders", 0); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetFlow during outage = %v, want a transport error (never ErrNotFound)", err)
	}
	if _, err := s.GetTask(ctx, taskID); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetTask during outage = %v, want a transport error (never ErrNotFound)", err)
	}
	if _, err := s.PublisherKeyByName(ctx, "pub1"); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("PublisherKeyByName during outage = %v, want a transport error", err)
	}
	if _, err := s.ResolveConnector(ctx, "myconn", "latest", "linux", "amd64"); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ResolveConnector during outage = %v, want a transport error", err)
	}

	// Every listing errors instead of returning an empty page.
	lists := map[string]func() (int, error){
		"Flows":             func() (int, error) { v, err := s.Flows(ctx); return len(v), err },
		"Tasks":             func() (int, error) { v, err := s.Tasks(ctx, 10); return len(v), err },
		"TaskAttempts":      func() (int, error) { v, err := s.TaskAttempts(ctx, taskID); return len(v), err },
		"Runners":           func() (int, error) { v, err := s.Runners(ctx); return len(v), err },
		"Schedules":         func() (int, error) { v, err := s.Schedules(ctx); return len(v), err },
		"Webhooks":          func() (int, error) { v, err := s.Webhooks(ctx); return len(v), err },
		"EnabledWebhooks":   func() (int, error) { v, err := s.EnabledWebhookConfigs(ctx); return len(v), err },
		"Secrets":           func() (int, error) { v, err := s.Secrets(ctx); return len(v), err },
		"SecretEnvelopes":   func() (int, error) { v, err := s.SecretEnvelopes(ctx, []string{"a"}); return len(v), err },
		"DirectExecutions":  func() (int, error) { v, err := s.DirectExecutions(ctx, 10); return len(v), err },
		"UsageEventsSince":  func() (int, error) { v, err := s.UsageEventsSince(ctx, 0, 10); return len(v), err },
		"ListAudit":         func() (int, error) { v, err := s.ListAudit(ctx, store.AuditFilter{}); return len(v), err },
		"TrustedKeys":       func() (int, error) { v, err := s.TrustedKeys(ctx); return len(v), err },
		"Connectors":        func() (int, error) { v, err := s.Connectors(ctx); return len(v), err },
		"ConnectorVersions": func() (int, error) { v, err := s.ConnectorVersions(ctx, "myconn"); return len(v), err },
	}
	for name, fn := range lists {
		n, err := fn()
		if err == nil {
			t.Errorf("%s during outage = %d rows, nil error (want an error)", name, n)
		}
	}

	// Aggregates and writes error too.
	if _, err := s.Stats(ctx); err == nil {
		t.Error("Stats during outage returned no error")
	}
	if _, err := s.PlatformStats(ctx); err == nil {
		t.Error("PlatformStats during outage returned no error")
	}
	if _, err := s.DueCount(ctx); err == nil {
		t.Error("DueCount during outage returned no error")
	}
	if _, err := s.Usage(ctx, time.Now().Add(-time.Hour), time.Now()); err == nil {
		t.Error("Usage during outage returned no error")
	}
	if _, err := s.Enqueue(ctx, "orders", 0, "", 1); err == nil {
		t.Error("Enqueue during outage returned no error")
	}
	if _, err := s.Claim(ctx, "00000000-0000-0000-0000-000000000000", time.Minute); err == nil {
		t.Error("Claim during outage returned no error")
	}
	if err := s.ReapExpired(ctx); err == nil {
		t.Error("ReapExpired during outage returned no error")
	}
	if err := s.Audit(ctx, "actor", "action", "entity", nil); err == nil {
		t.Error("Audit during outage returned no error")
	}
	if _, err := s.CreateAccount(ctx, "x"); err == nil {
		t.Error("CreateAccount during outage returned no error")
	}
	if _, _, err := s.CreateRegistrationToken(ctx, time.Minute); err == nil {
		t.Error("CreateRegistrationToken during outage returned no error")
	}
	if _, _, err := s.RegisterRunner(ctx, "srt_whatever", "r"); err == nil {
		t.Error("RegisterRunner during outage returned no error")
	}
	if _, _, err := s.AuthRunner(ctx, "sr_whatever"); err == nil {
		t.Error("AuthRunner during outage returned no error")
	}
	if _, err := s.DeployFlow(ctx, "orders", flowDoc); err == nil {
		t.Error("DeployFlow during outage returned no error")
	}
	if err := s.PublishFlow(ctx, "orders", 1); err == nil {
		t.Error("PublishFlow during outage returned no error")
	}
	if _, _, err := s.UpsertSecret(ctx, "n", []byte("c"), []byte("d"), "k", ""); err == nil {
		t.Error("UpsertSecret during outage returned no error")
	}
	if _, err := s.AddPublisherKey(ctx, "pub1", make([]byte, 32)); err == nil {
		t.Error("AddPublisherKey during outage returned no error")
	}
	if err := s.PutConnectorVersion(ctx, store.NewVersion{
		Name: "c", Version: "1", OS: "linux", Arch: "amd64",
		Digest: []byte("d"), Signature: []byte("s"),
		PublisherKeyID: "00000000-0000-0000-0000-000000000000", Data: []byte("x")}); err == nil {
		t.Error("PutConnectorVersion during outage returned no error")
	}
	if err := s.Migrate(ctx); err == nil {
		t.Error("Migrate during outage returned no error")
	}
}

// TestUsageEventsSinceBounds: the export cursor is strictly exclusive and the
// page size is clamped, so a caller cannot ask for an unbounded scan.
func TestUsageEventsSinceBounds(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	for i := range 5 {
		if _, err := s.RecordDirectExecution(ctx, "", store.DirectExecution{
			FlowName: "f", Trigger: "api", State: "completed", RecordsIn: int64(i),
		}); err != nil {
			t.Fatal(err)
		}
	}

	all, err := s.UsageEventsSince(ctx, 0, 100)
	if err != nil || len(all) != 5 {
		t.Fatalf("UsageEventsSince = %d rows, %v (want 5)", len(all), err)
	}
	// Ascending id order — the cursor depends on it.
	for i := 1; i < len(all); i++ {
		if all[i].ID <= all[i-1].ID {
			t.Fatalf("events are not in ascending id order: %d then %d", all[i-1].ID, all[i].ID)
		}
	}
	// The cursor is exclusive: resuming from row 2's id skips rows 1-2.
	rest, err := s.UsageEventsSince(ctx, all[1].ID, 100)
	if err != nil || len(rest) != 3 || rest[0].ID != all[2].ID {
		t.Fatalf("resumed page = %d rows (first id %v), %v", len(rest), rest, err)
	}
	// Page size is honoured, and out-of-range limits clamp to the 1000 cap
	// rather than erroring or scanning unbounded.
	if page, err := s.UsageEventsSince(ctx, 0, 2); err != nil || len(page) != 2 {
		t.Fatalf("limit=2 page = %d rows, %v", len(page), err)
	}
	for _, limit := range []int{0, -1, 100000} {
		if page, err := s.UsageEventsSince(ctx, 0, limit); err != nil || len(page) != 5 {
			t.Fatalf("limit=%d page = %d rows, %v (want all 5, clamped)", limit, len(page), err)
		}
	}
	// Past the end: no rows, no error.
	if page, err := s.UsageEventsSince(ctx, all[4].ID, 10); err != nil || len(page) != 0 {
		t.Fatalf("caught-up page = %d rows, %v", len(page), err)
	}
}

// TestTasksLimitClamped: an absurd or absent limit falls back to the default
// page rather than dumping the whole queue.
func TestTasksLimitClamped(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deployPublished(t, s, "orders")
	for range 3 {
		if _, err := s.Enqueue(ctx, "orders", 0, "", 3); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := s.Tasks(ctx, 2); err != nil || len(got) != 2 {
		t.Fatalf("Tasks(2) = %d, %v", len(got), err)
	}
	for _, limit := range []int{0, -5, 501} {
		got, err := s.Tasks(ctx, limit)
		if err != nil || len(got) != 3 {
			t.Fatalf("Tasks(%d) = %d rows, %v (want the 3 queued, default page)", limit, len(got), err)
		}
	}
}

// TestGetFlowVersionAddressing: version 0 follows the published pointer,
// explicit versions address drafts, and an out-of-range version is a miss —
// distinct from "the flow does not exist".
func TestGetFlowVersionAddressing(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	if _, err := s.DeployFlow(ctx, "orders", flowDoc); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.GetFlow(ctx, "orders", 0); !errors.Is(err, store.ErrNotPublished) {
		t.Fatalf("draft-only flow at version 0 = %v, want ErrNotPublished", err)
	}
	if _, _, err := s.GetFlow(ctx, "orders", 99); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("nonexistent version of an existing flow = %v, want ErrNotFound", err)
	}
	// Enqueue surfaces the same distinction to the API layer.
	if _, err := s.Enqueue(ctx, "orders", 0, "", 1); !errors.Is(err, store.ErrNotPublished) {
		t.Fatalf("Enqueue of an unpublished flow = %v, want ErrNotPublished", err)
	}
	if _, err := s.Enqueue(ctx, "ghost", 0, "", 1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Enqueue of an unknown flow = %v, want ErrNotFound", err)
	}
}

// FlowByName is the design-time lookup: it answers "which versions exist"
// without resolving to the published one, because a draft is exactly what a
// review is for.
func TestFlowByNameSeesDrafts(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	if _, err := s.FlowByName(ctx, "ghost"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown flow = %v, want ErrNotFound", err)
	}
	v, err := s.DeployFlow(ctx, "orders", flowDoc)
	if err != nil {
		t.Fatal(err)
	}
	f, err := s.FlowByName(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if f.LatestVersion != v {
		t.Errorf("latest = %d, want the just-deployed %d", f.LatestVersion, v)
	}
	if f.PublishedVersion != 0 {
		t.Errorf("published = %d; nothing has been published", f.PublishedVersion)
	}
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatal(err)
	}
	if f, err = s.FlowByName(ctx, "orders"); err != nil || f.PublishedVersion != v {
		t.Errorf("after publish: %+v, %v", f, err)
	}
}
