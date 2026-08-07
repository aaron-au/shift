package store_test

import (
	"fmt"
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
)

// flowUsing deploys and publishes a flow whose source is gen, returning the
// flow version.
func flowUsing(t *testing.T, s *store.Store, name string) int {
	t.Helper()
	v := deploy(t, s, name, fmt.Sprintf(`{
	  "name": %q,
	  "source": {"connector":"gen","action":"records"},
	  "sink": {"connector":"@discard","action":""}
	}`, name))
	if err := s.PublishFlow(t.Context(), name, v); err != nil {
		t.Fatalf("publish %s: %v", name, err)
	}
	return v
}

func refNames(refs []store.ConnectorRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Name+"@"+r.Version)
	}
	return out
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// The reference index answers "which published flows run this build?" — the
// question behind yank warnings, EOL notices, GC and bulk locate. All four
// agree because they read this one index instead of each deciding for
// themselves what "in use" means.
func TestReferencesNameTheFlowsRunningABuild(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")
	putArtifact(t, s, ctx, key, "gen", "1.0.0", "linux", "amd64", []byte("v1"))

	if refs, err := s.ConnectorReferences(ctx, "gen", "1.0.0"); err != nil || len(refs) != 0 {
		t.Fatalf("references before any flow = %+v, %v", refs, err)
	}

	flowUsing(t, s, "orders")
	refs, err := s.ConnectorReferences(ctx, "gen", "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].Flow != "orders" || !refs[0].Current {
		t.Fatalf("references = %+v, want the current published version of orders", refs)
	}
	if len(refs[0].Steps) != 1 || refs[0].Steps[0] != "source" {
		t.Fatalf("steps = %v, want the pinning step named", refs[0].Steps)
	}

	// A newer connector, a newer flow version: the OLD build keeps its
	// reference from the version that would be rolled back to, and the new one
	// picks up the current version.
	putArtifact(t, s, ctx, key, "gen", "2.0.0", "linux", "amd64", []byte("v2"))
	flowUsing(t, s, "orders")

	old, err := s.ConnectorReferences(ctx, "gen", "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 1 || old[0].Current {
		t.Fatalf("old build references = %+v, want one that is no longer current", old)
	}
	current, err := s.ConnectorReferences(ctx, "gen", "2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 || !current[0].Current {
		t.Fatalf("new build references = %+v, want the current version", current)
	}
}

// GC deletes bytes, so what it must never do is take a build a live flow runs.
// Every case here is a version it has to leave alone.
func TestCollectionLeavesAnythingAFlowCouldRun(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")

	// Five versions, published oldest to newest.
	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0", "4.0.0", "5.0.0"} {
		putArtifact(t, s, ctx, key, "gen", v, "linux", "amd64", []byte("gen-"+v))
	}
	// A flow pinned to the OLDEST, which nothing else would keep.
	v1 := deploy(t, s, "orders", `{
	  "name": "orders",
	  "source": {"connector":"gen","action":"records","version":"1.0.0"},
	  "sink": {"connector":"@discard","action":""}
	}`)
	if err := s.PublishFlow(ctx, "orders", v1); err != nil {
		t.Fatalf("publish: %v", err)
	}

	collectable, err := s.CollectableConnectorVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := refNames(collectable)
	// Retained: 1.0.0 (a live flow runs it), 5.0.0 and 4.0.0 (the floor, so a
	// rollback has somewhere to land). Collectable: the middle two.
	if contains(got, "gen@1.0.0") {
		t.Fatalf("collectable = %v; a build a published flow runs was offered for deletion", got)
	}
	if contains(got, "gen@5.0.0") || contains(got, "gen@4.0.0") {
		t.Fatalf("collectable = %v; the latest and n-1 floor was not honoured", got)
	}
	if !contains(got, "gen@2.0.0") || !contains(got, "gen@3.0.0") {
		t.Fatalf("collectable = %v, want the unreferenced middle versions", got)
	}

	// A dry run deletes nothing. The publisher's private key is not held
	// server-side, so a wrongly deleted artifact cannot be regenerated.
	if again, err := s.CollectableConnectorVersions(ctx); err != nil || len(again) != len(collectable) {
		t.Fatalf("reporting changed the registry: %v, %v", again, err)
	}

	collected, err := s.CollectConnectorVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(collected) != len(collectable) {
		t.Fatalf("collected %d, reported %d", len(collected), len(collectable))
	}
	if _, err := s.ResolveConnector(ctx, "gen", "2.0.0", "linux", "amd64"); err == nil {
		t.Fatal("a collected version still resolves")
	}
	if _, err := s.ResolveConnector(ctx, "gen", "1.0.0", "linux", "amd64"); err != nil {
		t.Fatalf("the version a live flow runs stopped resolving: %v", err)
	}
	// Idempotent: a second pass has nothing left to do.
	if again, err := s.CollectConnectorVersions(ctx); err != nil || len(again) != 0 {
		t.Fatalf("second pass collected %v (%v)", again, err)
	}
}

// The flow version a rollback would land on keeps its pins alive. Counting
// only the CURRENT published version would let GC delete the build the
// previous version runs, and the rollback would fail at the first task.
func TestTheRollbackTargetKeepsItsBuild(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")
	for _, v := range []string{"1.0.0", "2.0.0", "3.0.0", "4.0.0"} {
		putArtifact(t, s, ctx, key, "gen", v, "linux", "amd64", []byte("gen-"+v))
	}

	pinned := func(name string, version string) int {
		t.Helper()
		v := deploy(t, s, name, fmt.Sprintf(`{
		  "name": %q,
		  "source": {"connector":"gen","action":"records","version":%q},
		  "sink": {"connector":"@discard","action":""}
		}`, name, version))
		if err := s.PublishFlow(ctx, name, v); err != nil {
			t.Fatalf("publish: %v", err)
		}
		return v
	}
	pinned("orders", "1.0.0") // v1: the version before last
	pinned("orders", "2.0.0") // v2: current

	got := refNames(mustCollectable(t, s))
	if contains(got, "gen@1.0.0") {
		t.Fatalf("collectable = %v; the rollback target's build was offered for deletion", got)
	}

	// Publish once more. 1.0.0 is now two rollbacks back — beyond what is
	// promised — so it becomes collectable, which is what stops the registry
	// growing forever.
	pinned("orders", "3.0.0")
	got = refNames(mustCollectable(t, s))
	if !contains(got, "gen@1.0.0") {
		t.Fatalf("collectable = %v; nothing is ever released", got)
	}
	if contains(got, "gen@2.0.0") {
		t.Fatalf("collectable = %v; the new rollback target was offered for deletion", got)
	}
}

func mustCollectable(t *testing.T, s *store.Store) []store.ConnectorRef {
	t.Helper()
	refs, err := s.CollectableConnectorVersions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return refs
}

// Blobs are content-addressed and shared. Deleting a version's bytes along
// with its row would destroy a build another version deduped against.
func TestASharedBlobSurvivesItsFirstVersion(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	key := addKey(t, s, ctx, "publisher")

	// Same bytes under two connectors: identical builds dedupe to one blob.
	same := []byte("identical-artifact")
	putArtifact(t, s, ctx, key, "gen", "1.0.0", "linux", "amd64", same)
	putArtifact(t, s, ctx, key, "gen", "2.0.0", "linux", "amd64", []byte("two"))
	putArtifact(t, s, ctx, key, "gen", "3.0.0", "linux", "amd64", []byte("three"))
	putArtifact(t, s, ctx, key, "keeper", "1.0.0", "linux", "amd64", same)

	// gen@1.0.0 is unreferenced and below the floor, so it goes; keeper@1.0.0
	// shares its bytes and is the newest of its connector, so it stays.
	if _, err := s.CollectConnectorVersions(ctx); err != nil {
		t.Fatal(err)
	}
	cv, err := s.ResolveConnector(ctx, "keeper", "1.0.0", "linux", "amd64")
	if err != nil {
		t.Fatalf("the surviving connector stopped resolving: %v", err)
	}
	if cv.SizeBytes != int64(len(same)) {
		t.Fatalf("size = %d, want %d", cv.SizeBytes, len(same))
	}
	raw, err := s.ConnectorBlob(ctx, cv.Digest)
	if err != nil {
		t.Fatalf("its bytes were deleted with the other version's row: %v", err)
	}
	if string(raw) != string(same) {
		t.Fatalf("blob = %q", raw)
	}
}
