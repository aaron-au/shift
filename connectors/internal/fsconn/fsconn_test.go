package fsconn

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// cfgJSON builds a config document from a field map.
func cfgJSON(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPutGetRoundTrip writes records with put, then reads them back with get.
func TestPutGetRoundTrip(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	// put: relative path "sub/out.ndjson" (the dir does not exist yet).
	sink := &putSink{}
	if err := sink.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": "sub/out.ndjson", "format": "ndjson"})); err != nil {
		t.Fatalf("put open: %v", err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	for i := range 3 {
		bld.BeginMap()
		bld.KeyLiteral("i")
		bld.Int(int64(i))
		bld.EndMap()
		batch.Append(bld.Finish())
	}
	if err := sink.Write(ctx, batch); err != nil {
		t.Fatalf("put write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("put close: %v", err)
	}

	dest := filepath.Join(root, "sub", "out.ndjson")
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("destination missing: %v", err)
	}
	// No temp files left behind after the atomic rename.
	des, err := os.ReadDir(filepath.Join(root, "sub"))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range des {
		if strings.Contains(d.Name(), ".shift-") {
			t.Fatalf("temp file left behind: %s", d.Name())
		}
	}

	// get: read the file back.
	src := &getSource{}
	if err := src.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": "sub/out.ndjson", "format": "ndjson"})); err != nil {
		t.Fatalf("get open: %v", err)
	}
	defer func() { _ = src.Close() }()
	var got []int64
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("get next: %v", err)
		}
		for _, rec := range b.Records() {
			v, _ := rec.Field("i")
			got = append(got, v.Int())
		}
	}
	if len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Fatalf("records = %v, want [0 1 2]", got)
	}
}

// TestGetCSV round-trips through the csv format path.
func TestGetCSV(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	sink := &putSink{}
	if err := sink.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": "data.csv", "format": "csv"})); err != nil {
		t.Fatalf("put open: %v", err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	for _, name := range []string{"ada", "grace"} {
		bld.BeginMap()
		bld.KeyLiteral("name")
		bld.StringLiteral(name)
		bld.EndMap()
		batch.Append(bld.Finish())
	}
	if err := sink.Write(ctx, batch); err != nil {
		t.Fatalf("put write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("put close: %v", err)
	}

	src := &getSource{}
	if err := src.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": "data.csv", "format": "csv"})); err != nil {
		t.Fatalf("get open: %v", err)
	}
	defer func() { _ = src.Close() }()
	var names []string
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("get next: %v", err)
		}
		for _, rec := range b.Records() {
			v, _ := rec.Field("name")
			names = append(names, v.String())
		}
	}
	if len(names) != 2 || names[0] != "ada" || names[1] != "grace" {
		t.Fatalf("names = %v, want [ada grace]", names)
	}
}

// TestList lists a directory (flat and recursive), emitting one record per
// entry with a root-relative path.
func TestList(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	for _, f := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "c.txt"), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}

	collect := func(recursive bool) map[string]bool {
		s := &listSource{}
		if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": ".", "recursive": recursive})); err != nil {
			t.Fatalf("list open: %v", err)
		}
		defer func() { _ = s.Close() }()
		got := map[string]bool{}
		for {
			b, err := s.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("list next: %v", err)
			}
			for _, rec := range b.Records() {
				p, _ := rec.Field("path")
				isDir, _ := rec.Field("is_dir")
				got[p.String()] = isDir.Bool()
			}
		}
		return got
	}

	flat := collect(false)
	if len(flat) != 3 || flat["a.txt"] || !flat["sub"] {
		t.Fatalf("flat listing = %v, want a.txt/b.txt (files) + sub (dir)", flat)
	}
	rec := collect(true)
	// Recursive should surface the nested file at its root-relative path.
	nested := filepath.Join("sub", "c.txt")
	if _, ok := rec[nested]; !ok {
		t.Fatalf("recursive listing = %v, want it to contain %q", rec, nested)
	}
}

// TestOps exercises the config-driven mkdir/delete/rmdir verbs, including their
// idempotency (repeat on a missing target still succeeds).
func TestOps(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	runOp := func(op opKind, path string) record.Value {
		t.Helper()
		s := &opSource{op: op}
		if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": path})); err != nil {
			t.Fatalf("%s open: %v", op.name(), err)
		}
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("%s next: %v", op.name(), err)
		}
		recs := b.Records()
		if len(recs) != 1 {
			t.Fatalf("%s emitted %d records, want 1", op.name(), len(recs))
		}
		if _, err := s.Next(ctx); !errors.Is(err, io.EOF) {
			t.Fatalf("%s second Next = %v, want EOF", op.name(), err)
		}
		if ok, _ := recs[0].Field("ok"); !ok.Bool() {
			t.Fatalf("%s status not ok: %v", op.name(), recs[0])
		}
		_ = s.Close()
		return recs[0]
	}

	// mkdir — single node, path in config, runs standalone.
	rec := runOp(opMkdir, "created")
	if v, _ := rec.Field("op"); v.String() != "mkdir" {
		t.Fatalf("status op = %q, want mkdir", v.String())
	}
	if fi, err := os.Stat(filepath.Join(root, "created")); err != nil || !fi.IsDir() {
		t.Fatalf("mkdir: %v", err)
	}
	runOp(opMkdir, "created") // idempotent

	// delete, then delete again (missing → idempotent success).
	gone := filepath.Join(root, "gone.txt")
	if err := os.WriteFile(gone, []byte("z"), 0o600); err != nil {
		t.Fatal(err)
	}
	runOp(opDelete, "gone.txt")
	if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete: file still present: %v", err)
	}
	runOp(opDelete, "gone.txt") // idempotent

	// rmdir (empty)
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o750); err != nil {
		t.Fatal(err)
	}
	runOp(opRmdir, "empty")
	if _, err := os.Stat(filepath.Join(root, "empty")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rmdir: dir still present")
	}
	runOp(opRmdir, "empty") // idempotent

	// rmdir recursive removes a non-empty tree.
	tree := filepath.Join(root, "tree")
	if err := os.MkdirAll(filepath.Join(tree, "nested"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "nested", "f.txt"), []byte("q"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &opSource{op: opRmdir}
	if err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": "tree", "recursive": true})); err != nil {
		t.Fatalf("rmdir recursive open: %v", err)
	}
	if _, err := s.Next(ctx); err != nil {
		t.Fatalf("rmdir recursive next: %v", err)
	}
	_ = s.Close()
	if _, err := os.Stat(tree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rmdir recursive: tree still present")
	}
}

// TestTraversalRejected proves the jail: paths escaping root are rejected fail-
// closed for every verb, before any filesystem side effect.
func TestTraversalRejected(t *testing.T) {
	root := t.TempDir()
	// A secret file outside the jail; the traversal targets it.
	outside := filepath.Join(filepath.Dir(root), "secret.txt")
	if err := os.WriteFile(outside, []byte("top secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(outside) })
	ctx := context.Background()

	// escapes lists paths that must all be rejected.
	escapes := []string{
		"../secret.txt",
		"../../etc/passwd",
		"sub/../../secret.txt",
		outside, // absolute path outside root
	}

	for _, p := range escapes {
		t.Run("get_"+p, func(t *testing.T) {
			s := &getSource{}
			err := s.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": p, "format": "ndjson"}))
			if err == nil {
				_ = s.Close()
				t.Fatalf("get %q: expected escape rejection", p)
			}
			if !strings.Contains(err.Error(), "escapes root") {
				t.Fatalf("get %q: error = %v, want 'escapes root'", p, err)
			}
		})
	}

	// delete must also refuse an escaping target — and must NOT delete the
	// outside file.
	del := &opSource{op: opDelete}
	if err := del.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": outside})); err != nil {
		t.Fatalf("delete open: %v", err)
	}
	if _, err := del.Next(ctx); err == nil || !strings.Contains(err.Error(), "escapes root") {
		t.Fatalf("delete escaping path: err = %v, want 'escapes root'", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("delete escaped the jail and removed the outside file: %v", err)
	}

	// put must refuse to write outside root.
	sink := &putSink{}
	if err := sink.Open(ctx, cfgJSON(t, map[string]any{"root": root, "path": "../evil.ndjson", "format": "ndjson"})); err == nil {
		_ = sink.Close()
		t.Fatal("put ../evil.ndjson: expected escape rejection")
	}
}

// TestConfigValidation covers the connection/action-level validation gates.
func TestConfigValidation(t *testing.T) {
	ctx := context.Background()

	// Missing root → fail closed.
	if err := (&getSource{}).Open(ctx, []byte(`{"path":"f.ndjson"}`)); err == nil || !strings.Contains(err.Error(), "root is required") {
		t.Fatalf("missing root: err = %v, want 'root is required'", err)
	}
	// Missing path for get.
	if err := (&getSource{}).Open(ctx, cfgJSON(t, map[string]any{"root": t.TempDir()})); err == nil {
		t.Fatal("missing path: expected error")
	}
	// Unsupported format.
	if err := (&getSource{}).Open(ctx, cfgJSON(t, map[string]any{"root": t.TempDir(), "path": "f", "format": "xml"})); err == nil {
		t.Fatal("bad format: expected error")
	}
	// Op verb without a path.
	if err := (&opSource{op: opDelete}).Open(ctx, cfgJSON(t, map[string]any{"root": t.TempDir()})); err == nil {
		t.Fatal("delete without path: expected error")
	}
	// Bad JSON.
	var c config
	if err := parseConfig([]byte(`{not json`), &c); err == nil {
		t.Fatal("bad json: expected error")
	}
}

// TestConnectorDefinition sanity-checks the connector wiring the runner sees.
func TestConnectorDefinition(t *testing.T) {
	c := Connector()
	if c.Name != "fs" {
		t.Fatalf("name = %q", c.Name)
	}
	for _, v := range []string{"get", "list", "delete", "mkdir", "rmdir"} {
		if _, ok := c.Sources[v]; !ok {
			t.Fatalf("missing source verb %q", v)
		}
		if _, ok := c.Schemas[v]; !ok {
			t.Fatalf("missing schema for %q", v)
		}
	}
	if _, ok := c.Sinks["put"]; !ok {
		t.Fatal("missing put sink")
	}
	// Every declared schema must be valid JSON (the studio renders it).
	for name, raw := range c.Schemas {
		var js map[string]any
		if err := json.Unmarshal(raw, &js); err != nil {
			t.Fatalf("schema %q is not valid JSON: %v", name, err)
		}
	}
}
