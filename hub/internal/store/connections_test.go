package store_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
)

func TestConnectionUpsertAndFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	s := open(t)
	ctx := t.Context()

	id, version, err := s.UpsertConnection(ctx, "prod-sftp", "sftp",
		[]byte(`{"host":"sftp.example.com"}`), "")
	if err != nil {
		t.Fatalf("UpsertConnection: %v", err)
	}
	if id == "" || version != 1 {
		t.Fatalf("id=%q version=%d, want an id and version 1", id, version)
	}

	// Replacing keeps the row (same id) and bumps the version, so a
	// rotated credential does not orphan every reference to it.
	id2, version2, err := s.UpsertConnection(ctx, "prod-sftp", "sftp",
		[]byte(`{"host":"sftp2.example.com"}`), "")
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if id2 != id || version2 != 2 {
		t.Fatalf("id=%q version=%d, want the same id and version 2", id2, version2)
	}

	got, err := s.ConnectionsByName(ctx, []string{"prod-sftp", "absent"})
	if err != nil {
		t.Fatalf("ConnectionsByName: %v", err)
	}
	// Missing names are absent, not an error — the caller decides.
	if len(got) != 1 || got[0].Name != "prod-sftp" || got[0].Connector != "sftp" {
		t.Fatalf("got %+v, want just prod-sftp", got)
	}
	var cfg struct {
		Host string `json:"host"`
	}
	if err := json.Unmarshal(got[0].Config, &cfg); err != nil {
		t.Fatalf("stored config is not valid JSON: %v", err)
	}
	if cfg.Host != "sftp2.example.com" {
		t.Fatalf("host = %q, want the replacement value", cfg.Host)
	}

	all, err := s.Connections(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("Connections() = %+v, %v; want one row", all, err)
	}

	if err := s.DeleteConnection(ctx, "prod-sftp"); err != nil {
		t.Fatalf("DeleteConnection: %v", err)
	}
	if err := s.DeleteConnection(ctx, "prod-sftp"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
}

// Connections are tenant data: one account must not see or delete
// another's, since the name is chosen by the author and will collide.
func TestConnectionAccountScoped(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	s := open(t)
	base := t.Context()

	acctB, err := s.CreateAccount(base, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	ctxB := store.WithAccount(base, acctB)

	if _, _, err := s.UpsertConnection(base, "shared-name", "sftp", []byte(`{"host":"a"}`), ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.UpsertConnection(ctxB, "shared-name", "sftp", []byte(`{"host":"b"}`), ""); err != nil {
		t.Fatalf("the same name in another account must be allowed: %v", err)
	}

	got, err := s.ConnectionsByName(ctxB, []string{"shared-name"})
	if err != nil || len(got) != 1 {
		t.Fatalf("ConnectionsByName = %+v, %v", got, err)
	}
	var cfg struct {
		Host string `json:"host"`
	}
	if err := json.Unmarshal(got[0].Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "b" {
		t.Fatalf("account B read host %q, want its own document", cfg.Host)
	}

	if err := s.DeleteConnection(ctxB, "shared-name"); err != nil {
		t.Fatal(err)
	}
	// A's row must survive B's delete.
	if got, err := s.ConnectionsByName(base, []string{"shared-name"}); err != nil || len(got) != 1 {
		t.Fatalf("account A's connection = %+v, %v; want it intact", got, err)
	}
}

// FlowsUsingConnection is what turns deleting a live connection into a
// refusal. Only published versions count: an unpublished draft cannot be
// dispatched, so it must not hold a connection hostage.
func TestFlowsUsingConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	s := open(t)
	ctx := t.Context()

	withConn := json.RawMessage(`{"name":"uses","source":{"connector":"gen","action":"gen",
	  "connection":"gen-conn"},"sink":{"connector":"gen","action":"discard"}}`)

	if _, err := s.DeployFlow(ctx, "uses", withConn); err != nil {
		t.Fatal(err)
	}
	using, err := s.FlowsUsingConnection(ctx, "gen-conn")
	if err != nil {
		t.Fatalf("FlowsUsingConnection: %v", err)
	}
	if len(using) != 0 {
		t.Fatalf("using = %v, want none while the reference is only a draft", using)
	}

	if err := s.PublishFlow(ctx, "uses", 1); err != nil {
		t.Fatal(err)
	}
	if using, err = s.FlowsUsingConnection(ctx, "gen-conn"); err != nil {
		t.Fatal(err)
	}
	if len(using) != 1 || using[0] != "uses" {
		t.Fatalf("using = %v, want [uses] once published", using)
	}

	// A published flow that references nothing must not be reported, and
	// an unrelated connection name must come back clean.
	deployPublished(t, s, "plain")
	if using, err = s.FlowsUsingConnection(ctx, "other-conn"); err != nil {
		t.Fatal(err)
	}
	if len(using) != 0 {
		t.Fatalf("using = %v, want none for an unreferenced connection", using)
	}
}
