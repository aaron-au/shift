package ftpconn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/textproto"
	"strings"
	"sync"
	"testing"

	"github.com/aaron-au/shift/engine/record"
	"github.com/jlaffaye/ftp"
)

// fakeFS is an in-memory FTP backend shared by fakeConn instances, so tests
// exercise every verb (including the streaming Stor path via an io.Pipe) with
// no real FTP server and no network.
type fakeFS struct {
	mu      sync.Mutex
	files   map[string][]byte
	dirs    map[string]bool
	entries []*ftp.Entry // List returns this when set
	storErr error        // when set, Stor fails without storing (transfer-error path)
}

func newFakeFS() *fakeFS { return &fakeFS{files: map[string][]byte{}, dirs: map[string]bool{}} }

// dialer returns a dialFunc yielding a fakeConn over this fs.
func (fs *fakeFS) dialer() dialFunc {
	return func(_ context.Context, _ *config) (ftpConn, func() error, error) {
		return fakeConn{fs}, func() error { return nil }, nil
	}
}

func notFound() error { return &textproto.Error{Code: ftp.StatusFileUnavailable, Msg: "550 not found"} }

type fakeConn struct{ fs *fakeFS }

func (c fakeConn) Login(string, string) error { return nil }

func (c fakeConn) Retr(path string) (io.ReadCloser, error) {
	c.fs.mu.Lock()
	defer c.fs.mu.Unlock()
	b, ok := c.fs.files[path]
	if !ok {
		return nil, notFound()
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (c fakeConn) Stor(path string, r io.Reader) error {
	if c.fs.storErr != nil {
		return c.fs.storErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	c.fs.mu.Lock()
	defer c.fs.mu.Unlock()
	c.fs.files[path] = data
	return nil
}

func (c fakeConn) List(string) ([]*ftp.Entry, error) { return c.fs.entries, nil }

func (c fakeConn) Delete(path string) error {
	c.fs.mu.Lock()
	defer c.fs.mu.Unlock()
	if _, ok := c.fs.files[path]; !ok {
		return notFound()
	}
	delete(c.fs.files, path)
	return nil
}

func (c fakeConn) MakeDir(path string) error {
	c.fs.mu.Lock()
	defer c.fs.mu.Unlock()
	if c.fs.dirs[path] {
		return notFound() // 550: already exists
	}
	c.fs.dirs[path] = true
	return nil
}

func (c fakeConn) RemoveDir(path string) error {
	c.fs.mu.Lock()
	defer c.fs.mu.Unlock()
	if !c.fs.dirs[path] {
		return notFound()
	}
	delete(c.fs.dirs, path)
	return nil
}

func (c fakeConn) RemoveDirRecur(path string) error { return c.RemoveDir(path) }

func (c fakeConn) Rename(from, to string) error {
	c.fs.mu.Lock()
	defer c.fs.mu.Unlock()
	if b, ok := c.fs.files[from]; ok {
		c.fs.files[to] = b
		delete(c.fs.files, from)
		return nil
	}
	if c.fs.dirs[from] {
		c.fs.dirs[to] = true
		delete(c.fs.dirs, from)
		return nil
	}
	return notFound()
}

func (c fakeConn) Quit() error { return nil }

// --- source (get) ---

func TestGetSourceReadsNDJSON(t *testing.T) {
	fs := newFakeFS()
	fs.files["/in.ndjson"] = []byte("{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n")

	s := &getSource{dial: fs.dialer()}
	ctx := context.Background()
	cfg := []byte(`{"host":"h","path":"/in.ndjson","format":"ndjson","allow_local":true}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	var got []int64
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			v, _ := rec.Field("i")
			got = append(got, v.Int())
		}
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("records = %v, want [1 2 3]", got)
	}
}

func TestGetSourceReadsCSV(t *testing.T) {
	fs := newFakeFS()
	fs.files["/in.csv"] = []byte("i\n1\n2\n3\n")

	s := &getSource{dial: fs.dialer()}
	ctx := context.Background()
	cfg := []byte(`{"host":"h","path":"/in.csv","format":"csv","allow_local":true}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	var rows int
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		rows += len(b.Records())
	}
	if rows != 3 {
		t.Fatalf("csv rows = %d, want 3", rows)
	}
}

func TestGetSourceMissingFile(t *testing.T) {
	fs := newFakeFS()
	s := &getSource{dial: fs.dialer()}
	cfg := []byte(`{"host":"h","path":"/nope.ndjson","allow_local":true}`)
	if err := s.Open(context.Background(), cfg); err == nil {
		t.Fatal("expected Retr error for missing file")
	}
}

// --- sink (put) ---

func TestPutSinkRoundTrip(t *testing.T) {
	fs := newFakeFS()
	s := &putSink{dial: fs.dialer()}
	ctx := context.Background()
	cfg := []byte(`{"host":"h","path":"/out.ndjson","format":"ndjson","allow_local":true}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
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
	if err := s.Write(ctx, batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, ok := fs.files["/out.ndjson"]
	if !ok {
		t.Fatalf("destination not written; files=%v", keys(fs.files))
	}
	lines := strings.Count(strings.TrimSpace(string(data)), "\n") + 1
	if lines != 3 || !strings.Contains(string(data), `"i":2`) {
		t.Fatalf("written file = %q", data)
	}
	// Atomic write: temp file renamed onto the destination, so it must be gone.
	if _, exists := fs.files["/out.ndjson.shift-partial"]; exists {
		t.Fatal("temp file not cleaned up after rename")
	}
}

func TestPutSinkTransferErrorAbortsRename(t *testing.T) {
	fs := newFakeFS()
	fs.storErr = errors.New("boom: connection reset")
	s := &putSink{dial: fs.dialer()}
	ctx := context.Background()
	cfg := []byte(`{"host":"h","path":"/out.ndjson","allow_local":true}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	err := s.Close()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected transfer error surfaced, got %v", err)
	}
	// A failed transfer must never publish the destination.
	if _, exists := fs.files["/out.ndjson"]; exists {
		t.Fatal("destination written despite transfer failure")
	}
}

// --- list ---

func TestListSource(t *testing.T) {
	fs := newFakeFS()
	fs.entries = []*ftp.Entry{
		{Name: "a.txt", Type: ftp.EntryTypeFile, Size: 10},
		{Name: "b.txt", Type: ftp.EntryTypeFile, Size: 20},
		{Name: "sub", Type: ftp.EntryTypeFolder},
		{Name: "ln", Type: ftp.EntryTypeLink},
	}
	s := &listSource{dial: fs.dialer()}
	ctx := context.Background()
	cfg := []byte(`{"host":"h","path":"/dir","allow_local":true}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	types := map[string]string{}
	paths := map[string]string{}
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			name, _ := rec.Field("name")
			typ, _ := rec.Field("type")
			p, _ := rec.Field("path")
			types[name.String()] = typ.String()
			paths[name.String()] = p.String()
		}
	}
	if types["a.txt"] != "file" || types["sub"] != "dir" || types["ln"] != "link" {
		t.Fatalf("entry types = %v", types)
	}
	if paths["a.txt"] != "/dir/a.txt" {
		t.Fatalf("joined path = %q, want /dir/a.txt", paths["a.txt"])
	}
}

// --- config-driven ops ---

func runOp(t *testing.T, op opKind, dial dialFunc, cfg string) record.Value {
	t.Helper()
	s := &opSource{op: op, dial: dial}
	ctx := context.Background()
	if err := s.Open(ctx, []byte(cfg)); err != nil {
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

func TestOps(t *testing.T) {
	fs := newFakeFS()
	dial := fs.dialer()

	// mkdir
	rec := runOp(t, opMkdir, dial, `{"host":"h","path":"/created","allow_local":true}`)
	if v, _ := rec.Field("op"); v.String() != "mkdir" {
		t.Fatalf("op = %q, want mkdir", v.String())
	}
	if !fs.dirs["/created"] {
		t.Fatal("mkdir did not create dir")
	}
	// mkdir again → 550 (exists) swallowed → idempotent ok
	runOp(t, opMkdir, dial, `{"host":"h","path":"/created","allow_local":true}`)

	// delete, then delete again (missing → idempotent)
	fs.files["/gone.txt"] = []byte("z")
	runOp(t, opDelete, dial, `{"host":"h","path":"/gone.txt","allow_local":true}`)
	if _, ok := fs.files["/gone.txt"]; ok {
		t.Fatal("delete left file behind")
	}
	runOp(t, opDelete, dial, `{"host":"h","path":"/gone.txt","allow_local":true}`) // idempotent

	// rename, then rename again (missing source → idempotent)
	fs.files["/old.txt"] = []byte("r")
	rec = runOp(t, opRename, dial, `{"host":"h","from":"/old.txt","to":"/new.txt","allow_local":true}`)
	if v, _ := rec.Field("to"); v.String() != "/new.txt" {
		t.Fatalf("rename to = %q", v.String())
	}
	if _, ok := fs.files["/new.txt"]; !ok {
		t.Fatal("rename did not move file")
	}
	runOp(t, opRename, dial, `{"host":"h","from":"/old.txt","to":"/new.txt","allow_local":true}`) // idempotent

	// rmdir empty + recursive
	fs.dirs["/empty"] = true
	runOp(t, opRmdir, dial, `{"host":"h","path":"/empty","allow_local":true}`)
	if fs.dirs["/empty"] {
		t.Fatal("rmdir left dir behind")
	}
	fs.dirs["/tree"] = true
	runOp(t, opRmdir, dial, `{"host":"h","path":"/tree","recursive":true,"allow_local":true}`)
	if fs.dirs["/tree"] {
		t.Fatal("recursive rmdir left dir behind")
	}
}

func TestOpMissingArgs(t *testing.T) {
	ctx := context.Background()
	if err := (&opSource{op: opDelete}).Open(ctx, []byte(`{"host":"h","allow_local":true}`)); err == nil {
		t.Fatal("delete without path: expected error")
	}
	if err := (&opSource{op: opRename}).Open(ctx, []byte(`{"host":"h","allow_local":true}`)); err == nil {
		t.Fatal("rename without from/to: expected error")
	}
}

func TestOpKindName(t *testing.T) {
	for k, want := range map[opKind]string{opDelete: "delete", opMkdir: "mkdir", opRmdir: "rmdir", opRename: "rename", opKind(99): "unknown"} {
		if got := k.name(); got != want {
			t.Fatalf("opKind(%d).name() = %q, want %q", k, got, want)
		}
	}
}

// --- config validation ---

func TestConfigValidation(t *testing.T) {
	cases := map[string]struct {
		cfg     string
		wantErr bool
	}{
		"missing host":                 {`{"user":"u","allow_local":true}`, true},
		"plaintext creds refused":      {`{"host":"h","password":"p","explicit_tls":false}`, true},
		"plaintext creds allow_local":  {`{"host":"h","password":"p","explicit_tls":false,"allow_local":true}`, false},
		"plaintext anonymous ok":       {`{"host":"h","explicit_tls":false}`, false},
		"ftps creds ok (tls default)":  {`{"host":"h","password":"p"}`, false},
		"ftps creds ok (explicit tls)": {`{"host":"h","password":"p","explicit_tls":true}`, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var c config
			err := parseConfig([]byte(tc.cfg), &c)
			if tc.wantErr != (err != nil) {
				t.Fatalf("parseConfig err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	var c config
	if err := parseConfig([]byte(`{"host":"h"}`), &c); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.Port != 21 {
		t.Fatalf("default port = %d, want 21", c.Port)
	}
	if c.User != "anonymous" {
		t.Fatalf("default user = %q, want anonymous", c.User)
	}
	if !c.explicitTLS() {
		t.Fatal("explicit_tls should default to true")
	}
	if c.timeout().Seconds() != 30 {
		t.Fatalf("default timeout = %v, want 30s", c.timeout())
	}
}

func TestRequireFileFormat(t *testing.T) {
	if err := (&config{Format: "ndjson"}).requireFileFormat(); err == nil {
		t.Fatal("missing path: expected error")
	}
	c := &config{Path: "/f", Format: "parquet"}
	if err := c.requireFileFormat(); err == nil {
		t.Fatal("bad format: expected error")
	}
	c2 := &config{Path: "/f"}
	if err := c2.requireFileFormat(); err != nil || c2.Format != "ndjson" {
		t.Fatalf("format default: err=%v format=%q", err, c2.Format)
	}
}

// --- TLS config ---

func TestTLSConfig(t *testing.T) {
	c := &config{Host: "example.com"}
	tc := c.tlsConfig()
	if tc.ServerName != "example.com" {
		t.Fatalf("ServerName = %q", tc.ServerName)
	}
	if tc.InsecureSkipVerify {
		t.Fatal("cert verification must be on by default")
	}
	// allow_local decides which hosts are reachable, not whether their
	// certificate is trusted. Every on-prem FTPS server needs allow_local, so
	// conflating the two downgraded internal FTPS to MITM-able by default.
	cl := &config{Host: "example.com", AllowLocal: true}
	if cl.tlsConfig().InsecureSkipVerify {
		t.Fatal("allow_local must not disable certificate verification")
	}
	ins := &config{Host: "example.com", InsecureTLS: true}
	if !ins.tlsConfig().InsecureSkipVerify {
		t.Fatal("insecure_tls should disable certificate verification")
	}
}

// --- network guard ---

func TestGuard(t *testing.T) {
	deny := guard(false)
	// Internal/loopback/link-local/CGNAT targets are refused by default.
	for _, addr := range []string{"127.0.0.1:21", "10.0.0.1:21", "192.168.1.5:21", "100.64.0.1:21", "169.254.169.254:21", "[::1]:21"} {
		if err := deny("tcp", addr, nil); err == nil {
			t.Fatalf("guard(false) allowed %s, want refused", addr)
		}
	}
	// A public address is allowed.
	if err := deny("tcp", "8.8.8.8:21", nil); err != nil {
		t.Fatalf("guard(false) refused public 8.8.8.8: %v", err)
	}
	// Malformed / unresolvable inputs fail closed.
	if err := deny("tcp", "not-an-address", nil); err == nil {
		t.Fatal("guard should reject a malformed address")
	}
	if err := deny("tcp", "example.com:21", nil); err == nil {
		t.Fatal("guard should reject a non-IP (unresolvable) host")
	}
	// allow_local lifts the restriction for internal targets.
	allow := guard(true)
	for _, addr := range []string{"127.0.0.1:21", "10.0.0.1:21"} {
		if err := allow("tcp", addr, nil); err != nil {
			t.Fatalf("guard(true) refused %s: %v", addr, err)
		}
	}
}

func TestRealDialGuardRefusesPrivate(t *testing.T) {
	// realDial with a private target and the guard on must fail fast (the guard
	// fires during the control-connection dial) — no real FTP server involved.
	tls := false
	c := &config{Host: "10.0.0.1", Port: 21, ExplicitTLS: &tls, TimeoutSeconds: 2}
	_, _, err := realDial(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("realDial to private target err = %v, want guard refusal", err)
	}
}

// --- descriptor / connector shape ---

func TestConnectorShape(t *testing.T) {
	c := Connector()
	if c.Name != "ftp" {
		t.Fatalf("name = %q", c.Name)
	}
	for _, v := range []string{"get", "list", "delete", "mkdir", "rmdir", "rename"} {
		if _, ok := c.Sources[v]; !ok {
			t.Fatalf("missing source verb %q", v)
		}
	}
	if _, ok := c.Sinks["put"]; !ok {
		t.Fatal("put must be a sink")
	}
	for verb := range c.Sources {
		if _, ok := c.Schemas[verb]; !ok {
			t.Fatalf("verb %q has no schema", verb)
		}
	}
}

func TestIgnore550(t *testing.T) {
	if err := ignore550(nil); err != nil {
		t.Fatalf("nil should stay nil, got %v", err)
	}
	if err := ignore550(&textproto.Error{Code: ftp.StatusFileUnavailable, Msg: "550"}); err != nil {
		t.Fatalf("550 should be swallowed, got %v", err)
	}
	// A different FTP status must propagate.
	if err := ignore550(&textproto.Error{Code: 421, Msg: "service not available"}); err == nil {
		t.Fatal("non-550 status must propagate")
	}
	// A non-FTP error must propagate.
	if err := ignore550(errors.New("network down")); err == nil {
		t.Fatal("plain error must propagate")
	}
}

func TestEntryTypeUnknown(t *testing.T) {
	if got := entryType(ftp.EntryType(99)); got != "unknown" {
		t.Fatalf("entryType(99) = %q, want unknown", got)
	}
}

func TestListRequiresDir(t *testing.T) {
	fs := newFakeFS()
	s := &listSource{dial: fs.dialer()}
	if err := s.Open(context.Background(), []byte(`{"host":"h","allow_local":true}`)); err == nil {
		t.Fatal("list without a directory path: expected error")
	}
}

// serverConn must satisfy the ftpConn interface the actions depend on.
var _ ftpConn = serverConn{}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
