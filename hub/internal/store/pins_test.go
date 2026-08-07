package store_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// pinsOf reads the pinned connector versions out of a stored flow version.
func pinsOf(t *testing.T, s *store.Store, name string, version int) map[string]string {
	t.Helper()
	_, raw, err := s.GetFlow(t.Context(), name, version)
	if err != nil {
		t.Fatalf("get flow: %v", err)
	}
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		t.Fatalf("parse stored document: %v", err)
	}
	out := map[string]string{}
	for _, p := range doc.ConnectorPins() {
		out[p.Connector] = p.Version
	}
	return out
}

func deploy(t *testing.T, s *store.Store, name, doc string) int {
	t.Helper()
	v, err := s.DeployFlow(t.Context(), name, json.RawMessage(doc))
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	return v
}

// The whole point of ADR-0047: publishing a connector must not change what an
// already-published flow does. Before pinning, this test's second assertion
// would have read v2 — silently, on the next task, against live data.
func TestPublishingAConnectorDoesNotChangeAPublishedFlow(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")
	putArtifact(t, s, ctx, key, "gen", "1.0.0", "linux", "amd64", []byte("v1"))

	v := deploy(t, s, "orders", `{
	  "name": "orders",
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	// A DRAFT is unpinned: it has no promise to keep.
	if got := pinsOf(t, s, "orders", v)["gen"]; got != "" {
		t.Fatalf("draft pinned to %q; a draft should still mean newest", got)
	}
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := pinsOf(t, s, "orders", v)["gen"]; got != "1.0.0" {
		t.Fatalf("published pin = %q, want 1.0.0", got)
	}

	// A new connector release lands.
	putArtifact(t, s, ctx, key, "gen", "2.0.0", "linux", "amd64", []byte("v2"))
	if got := pinsOf(t, s, "orders", v)["gen"]; got != "1.0.0" {
		t.Fatalf("published flow now runs %q; publishing a connector changed a live flow", got)
	}

	// Republishing the SAME version does not drag it forward either — moving a
	// flow to a new build is an edit somebody makes.
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatalf("republish: %v", err)
	}
	if got := pinsOf(t, s, "orders", v)["gen"]; got != "1.0.0" {
		t.Fatalf("republish moved the pin to %q", got)
	}

	// A NEW version, published now, picks up the new build. That is the
	// upgrade path: explicit, and visible as a diff between flow versions.
	v2 := deploy(t, s, "orders", `{
	  "name": "orders",
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "orders", v2); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if got := pinsOf(t, s, "orders", v2)["gen"]; got != "2.0.0" {
		t.Fatalf("new flow version pinned %q, want 2.0.0", got)
	}
	// Rolling back to the first version brings its ORIGINAL pin back with it,
	// which is the only thing that makes a rollback a rollback.
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got := pinsOf(t, s, "orders", v)["gen"]; got != "1.0.0" {
		t.Fatalf("rolled-back version runs %q, want the build it was published against", got)
	}
}

// Pinning fails closed for the same reasons resolution does. A yanked version
// must not become a NEW pin — that is what yank means (ADR-0047 §3) — and a
// connector with nothing to pin is a publish that would fail on its first task.
func TestPinningFailsClosed(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")

	// A connector the registry has never heard of is left UNPINNED rather than
	// refused: a self-hosted deployment may provision binaries into the
	// runner's directory and publish no artifacts at all, and refusing here
	// would make the registry mandatory by accident. The `connector-pin`
	// review check is what makes it visible.
	unknown := deploy(t, s, "unknown", `{
	  "name": "unknown",
	  "source": {"connector":"nosuch","action":"get"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "unknown", unknown); err != nil {
		t.Fatalf("publish with an unregistered connector: %v", err)
	}
	if got := pinsOf(t, s, "unknown", unknown)["nosuch"]; got != "" {
		t.Fatalf("pinned %q for a connector the registry does not have", got)
	}
	if notices := flowdoc.ReviewRaw(docOf(t, s, "unknown", unknown)); !hasCode(notices, "connector-pin.unpinned") {
		t.Fatalf("an unpinned published flow raised no notice: %+v", notices)
	}

	putArtifact(t, s, ctx, key, "gen", "1.0.0", "linux", "amd64", []byte("v1"))
	putArtifact(t, s, ctx, key, "gen", "2.0.0", "linux", "amd64", []byte("v2"))
	if err := s.SetConnectorYanked(ctx, "gen", "2.0.0", "linux", "amd64", true); err != nil {
		t.Fatalf("yank: %v", err)
	}
	v := deploy(t, s, "orders", `{
	  "name": "orders",
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := pinsOf(t, s, "orders", v)["gen"]; got != "1.0.0" {
		t.Fatalf("pinned %q; a yanked version must not become a new pin", got)
	}
}

// A revoked publisher key removes its versions from every trust-serving query,
// and pinning is one: a flow must not be pinned to a build nothing will
// resolve, so it is left unpinned and reported rather than pinned to a dead
// artifact.
func TestARevokedKeyLeavesNothingToPin(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")
	putArtifact(t, s, ctx, key, "gen", "1.0.0", "linux", "amd64", []byte("v1"))
	if err := s.RevokePublisherKey(ctx, key); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	v := deploy(t, s, "orders", `{
	  "name": "orders",
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "orders", v); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := pinsOf(t, s, "orders", v)["gen"]; got != "" {
		t.Fatalf("pinned %q from a revoked key; nothing would resolve it", got)
	}
}

// docOf returns a stored flow version's raw document.
func docOf(t *testing.T, s *store.Store, name string, version int) []byte {
	t.Helper()
	_, raw, err := s.GetFlow(t.Context(), name, version)
	if err != nil {
		t.Fatalf("get flow: %v", err)
	}
	return raw
}

func hasCode(notices []flowdoc.Notice, code string) bool {
	for _, n := range notices {
		if n.Code == code {
			return true
		}
	}
	return false
}

func TestLatestConnectorVersionIsTheNewestPublish(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")

	if _, err := s.LatestConnectorVersion(ctx, "gen"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown connector = %v, want ErrNotFound", err)
	}
	putArtifact(t, s, ctx, key, "gen", "1.0.0", "linux", "amd64", []byte("v1"))
	putArtifact(t, s, ctx, key, "gen", "1.1.0", "linux", "amd64", []byte("v11"))
	got, err := s.LatestConnectorVersion(ctx, "gen")
	if err != nil {
		t.Fatal(err)
	}
	if got != "1.1.0" {
		t.Fatalf("latest = %q, want 1.1.0", got)
	}

	// Publishing for another platform must not change the answer: a pin names
	// a BUILD, and which os/arch artifact to fetch is the runner's question.
	putArtifact(t, s, ctx, key, "gen", "1.1.0", "darwin", "arm64", []byte("v11-darwin"))
	if got, err := s.LatestConnectorVersion(ctx, "gen"); err != nil || got != "1.1.0" {
		t.Fatalf("latest = %q, %v", got, err)
	}
}
