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

// learn is what the hub does after a successful pairing: record the key it
// observed and burn the token.
func learn(t *testing.T, s *store.Store, id, fp string) {
	t.Helper()
	if err := s.LearnGatewayFingerprint(t.Context(), id, fp); err != nil {
		t.Fatalf("learn fingerprint: %v", err)
	}
}

// adopt is the whole pairing: learn the key, then record the identity issued
// during it. Only the second half sets adopted_at, which is what everything
// downstream — pushes, renewals, the runner's address list — keys off.
func adopt(t *testing.T, s *store.Store, id, fp string) {
	t.Helper()
	learn(t, s, id, fp)
	if err := s.MarkGatewayAdopted(t.Context(), id, "01", time.Now().Add(90*24*time.Hour)); err != nil {
		t.Fatalf("mark adopted: %v", err)
	}
}

func TestGatewayAdoptionLifecycle(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	gw, err := s.CreateGateway(ctx, "dmz-1", "https://gw.example:8443", time.Now().Add(time.Hour))
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

	gw, err := s.CreateGateway(ctx, "dmz-2", "https://gw2.example", time.Now().Add(time.Hour))
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

	if _, err := s.RotateGatewayAdoption(ctx, gw.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "dmz-2" || got.URL != "https://gw2.example" {
		t.Fatal("rotation lost the record the administrator configured")
	}
	if got.Fingerprint != "" {
		t.Fatal("rotation kept the old fingerprint; the hub would pin a key the redeployed gateway no longer has")
	}
	if got.InstallToken == "" {
		t.Fatal("rotation issued no new install token, so the gateway could never pair again")
	}
	if got.AdoptedAt != nil || got.CertSerial != "" {
		t.Fatal("rotation left the old identity in place, so the gateway would never be re-adopted")
	}
	if got.PushedVersion != 0 {
		t.Fatal("rotation kept the acknowledged version, so a re-adopted gateway would never be pushed to")
	}

	if _, err := s.RotateGatewayAdoption(ctx, "00000000-0000-0000-0000-000000000000", time.Now().Add(time.Hour)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rotating a missing gateway = %v, want ErrNotFound", err)
	}
}

// GatewaysDue folds two questions the gateway cannot ask on its own behalf:
// "am I behind on configuration" and "is my identity about to lapse".
func TestGatewaysDueCoversDriftAndExpiry(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	// Converged, identity healthy: not due.
	healthy, err := s.CreateGateway(ctx, "healthy", "https://a.example", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, healthy.ID, "s", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	// Adopted, identity expiring soon: due even though config is converged.
	expiring, err := s.CreateGateway(ctx, "expiring", "https://b.example", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.MarkGatewayAdopted(ctx, expiring.ID, "s", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	// Never adopted: never due. Nothing may be pushed to a gateway the hub has
	// not yet proven it is talking to.
	if _, err := s.CreateGateway(ctx, "unadopted", "https://c.example", time.Now().Add(time.Hour)); err != nil {
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

	gw, err := s.CreateGateway(ctx, "flaky", "https://d.example", time.Now().Add(time.Hour))
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

	gw, err := s.CreateGateway(ctx, "doomed", "https://e.example", time.Now().Add(time.Hour))
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

// The token is minted once, returned once, and burned the moment the hub
// learns the gateway's key. A surviving token would be a standing credential
// that could adopt the record a second time.
func TestTheInstallTokenIsIssuedOnceAndBurnedOnPairing(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	gw, err := s.CreateGateway(ctx, "dmz", "https://gw.example:8444", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if gw.InstallToken == "" {
		t.Fatal("no install token was issued, so the gateway could never pair")
	}
	if gw.Fingerprint != "" {
		t.Fatal("a fingerprint was recorded before the gateway existed to have one")
	}
	if gw.TokenExpires == nil {
		t.Fatal("the install token never expires; an unclaimed one would stand forever")
	}

	learn(t, s, gw.ID, fingerprint("ab"))

	got, err := s.GetGateway(ctx, gw.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.InstallToken != "" {
		t.Fatal("the install token survived pairing")
	}
	if got.Fingerprint != fingerprint("ab") {
		t.Fatalf("fingerprint = %q, want the learned one", got.Fingerprint)
	}

	// A second learn must not succeed — that is the replay this guards.
	if err := s.LearnGatewayFingerprint(ctx, gw.ID, fingerprint("cd")); err == nil {
		t.Fatal("a spent install token was accepted a second time")
	}
}

// The reconcile loop pairs whatever is pending; an expired token is left alone
// rather than retried forever.
func TestGatewaysPendingSkipsAdoptedAndExpired(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	waiting, err := s.CreateGateway(ctx, "waiting", "https://a.example", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	expired, err := s.CreateGateway(ctx, "expired", "https://b.example", time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	done, err := s.CreateGateway(ctx, "done", "https://c.example", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	learn(t, s, done.ID, fingerprint("11"))
	if err := s.MarkGatewayAdopted(ctx, done.ID, "serial", time.Now().Add(24*time.Hour)); err != nil {
		t.Fatalf("adopt: %v", err)
	}

	pending, err := s.GatewaysPending(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	names := map[string]bool{}
	for _, g := range pending {
		names[g.ID] = true
	}
	if !names[waiting.ID] {
		t.Fatal("a gateway waiting to be paired was not listed; it would never be adopted")
	}
	if names[expired.ID] {
		t.Fatal("a gateway with an expired token is still being retried")
	}
	if names[done.ID] {
		t.Fatal("an adopted gateway is still listed as pending")
	}

	// Rotation puts it back in the queue with a fresh token.
	token, err := s.RotateGatewayAdoption(ctx, expired.ID, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if token == "" {
		t.Fatal("rotation issued no token")
	}
	pending, err = s.GatewaysPending(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	found := false
	for _, g := range pending {
		if g.ID == expired.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("a rotated gateway was not queued for pairing")
	}
}
