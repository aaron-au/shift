package store_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

func fingerprint(seed string) string {
	return strings.Repeat(seed, 64/len(seed))
}

func TestGatewayAdoptionLifecycle(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	gw, err := s.CreateGateway(ctx, "dmz-1", "https://gw.example:8443", fingerprint("ab"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if gw.AdoptedAt != nil {
		t.Fatal("a new gateway is already adopted; nothing has dialled it yet")
	}
	if gw.ConfigVersion != 0 || gw.PushedVersion != 0 {
		t.Fatalf("versions = %d/%d, want 0/0", gw.ConfigVersion, gw.PushedVersion)
	}

	notAfter := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	if err := s.MarkGatewayAdopted(ctx, gw.ID, "serial-1", notAfter); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	// A second adoption is REFUSED, not applied. Overwriting would let anything
	// that reaches the URL with a fresh key inherit a live gateway.
	err = s.MarkGatewayAdopted(ctx, gw.ID, "serial-2", notAfter)
	if !errors.Is(err, store.ErrAlreadyAdopted) {
		t.Fatalf("second adoption = %v, want ErrAlreadyAdopted", err)
	}

	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.AdoptedAt == nil {
		t.Fatal("adoption was not recorded")
	}
	if got.CertSerial != "serial-1" {
		t.Fatalf("cert serial = %q, want serial-1 (the second adoption must not have applied)", got.CertSerial)
	}
}

func TestAdoptingAGatewayThatIsGoneIsNotFound(t *testing.T) {
	s := open(t)
	err := s.MarkGatewayAdopted(t.Context(), "00000000-0000-0000-0000-000000000000", "x", time.Now())
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("adopting a missing gateway = %v, want ErrNotFound", err)
	}
}

// Rotation is the recovery path for a gateway that lost its state directory.
// It must keep the RECORD and reset only the identity.
func TestRotatingAdoptionResetsIdentityButKeepsTheRecord(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	gw, err := s.CreateGateway(ctx, "dmz-2", "https://gw2.example", fingerprint("cd"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, gw.ID, "serial-1", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := s.BumpGatewayConfig(ctx); err != nil {
		t.Fatalf("bump: %v", err)
	}
	if err := s.RecordGatewayPush(ctx, gw.ID, 1, nil); err != nil {
		t.Fatalf("record push: %v", err)
	}

	newFP := fingerprint("ef")
	if err := s.RotateGatewayAdoption(ctx, gw.ID, newFP); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "dmz-2" || got.URL != "https://gw2.example" {
		t.Fatal("rotation lost the record the administrator configured")
	}
	if got.Fingerprint != newFP {
		t.Fatalf("fingerprint = %q, want the rotated one", got.Fingerprint)
	}
	if got.AdoptedAt != nil || got.CertSerial != "" {
		t.Fatal("rotation left the old identity in place, so the gateway would never be re-adopted")
	}
	if got.PushedVersion != 0 {
		t.Fatal("rotation kept the acknowledged version, so a re-adopted gateway would never be pushed to")
	}

	if err := s.RotateGatewayAdoption(ctx, "00000000-0000-0000-0000-000000000000", newFP); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rotating a missing gateway = %v, want ErrNotFound", err)
	}
}

// GatewaysDue folds two questions the gateway cannot ask on its own behalf:
// "am I behind on configuration" and "is my identity about to lapse".
func TestGatewaysDueCoversDriftAndExpiry(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	// Converged, identity healthy: not due.
	healthy, err := s.CreateGateway(ctx, "healthy", "https://a.example", fingerprint("11"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, healthy.ID, "s", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	// Adopted, identity expiring soon: due even though config is converged.
	expiring, err := s.CreateGateway(ctx, "expiring", "https://b.example", fingerprint("22"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, expiring.ID, "s", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	// Never adopted: never due. Nothing may be pushed to a gateway the hub has
	// not yet proven it is talking to.
	if _, err := s.CreateGateway(ctx, "unadopted", "https://c.example", fingerprint("33")); err != nil {
		t.Fatalf("create: %v", err)
	}

	due, err := s.GatewaysDue(ctx, 4*time.Hour)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	names := map[string]bool{}
	for _, g := range due {
		names[g.Name] = true
	}
	if !names["expiring"] {
		t.Fatal("a gateway whose identity is about to lapse was not due; it would be stranded")
	}
	if names["healthy"] {
		t.Fatal("a converged, healthy gateway was reconciled needlessly")
	}
	if names["unadopted"] {
		t.Fatal("an unadopted gateway was due for a push")
	}

	// Config drift alone is enough.
	if err := s.BumpGatewayConfig(ctx); err != nil {
		t.Fatalf("bump: %v", err)
	}
	due, err = s.GatewaysDue(ctx, 4*time.Hour)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	found := false
	for _, g := range due {
		if g.Name == "healthy" {
			found = true
		}
	}
	if !found {
		t.Fatal("a gateway behind on configuration was not due")
	}
}

// A failed push keeps the drift visible and does not advance the acknowledged
// version — otherwise a gateway would look converged while running nothing.
func TestAFailedPushLeavesTheDriftVisible(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	gw, err := s.CreateGateway(ctx, "flaky", "https://d.example", fingerprint("44"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, gw.ID, "s", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := s.BumpGatewayConfig(ctx); err != nil {
		t.Fatalf("bump: %v", err)
	}
	if err := s.RecordGatewayPush(ctx, gw.ID, 1, errors.New("connection refused")); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.PushedVersion != 0 {
		t.Fatal("a failed push advanced the acknowledged version")
	}
	if !strings.Contains(got.LastPushError, "connection refused") {
		t.Fatalf("last push error = %q, want the failure recorded", got.LastPushError)
	}

	// Success clears it — a stale message beside a healthy gateway is worse
	// than none.
	if err := s.RecordGatewayPush(ctx, gw.ID, 1, nil); err != nil {
		t.Fatalf("record success: %v", err)
	}
	got, err = s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LastPushError != "" || got.PushedVersion != 1 {
		t.Fatalf("after success: error=%q version=%d, want cleared and 1", got.LastPushError, got.PushedVersion)
	}
}

func TestDeletingAGatewayIsRevocation(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	gw, err := s.CreateGateway(ctx, "doomed", "https://e.example", fingerprint("55"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.DeleteGateway(ctx, gw.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetGateway(ctx, gw.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
	if err := s.DeleteGateway(ctx, gw.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}

	list, err := s.ListGateways(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, g := range list {
		if g.ID == gw.ID {
			t.Fatal("a deleted gateway is still listed")
		}
	}
}
