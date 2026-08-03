package azureblobconn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/sdk"
)

// fakeStore is an in-memory blobStore: every verb is tested against it, so no
// test touches the network or Azurite.
type fakeStore struct {
	mu        sync.Mutex
	blobs     map[string]*fakeBlob
	uploadErr error // injected: fail an upload
	listErr   error // injected: fail a listing
}

type fakeBlob struct {
	data        []byte
	etag        string
	contentType string
	modified    time.Time
}

func newFakeStore() *fakeStore { return &fakeStore{blobs: map[string]*fakeBlob{}} }

func (f *fakeStore) put(name string, data []byte, ct string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs[name] = &fakeBlob{data: data, etag: `"etag-` + name + `"`, contentType: ct, modified: time.Unix(1700000000, 0).UTC()}
}

func (f *fakeStore) Download(_ context.Context, blob string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.blobs[blob]
	if !ok {
		return nil, errNotFound
	}
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

func (f *fakeStore) Upload(_ context.Context, blob string, r io.Reader) error {
	// Always drain r so the pipe writer never blocks, even on injected failure.
	data, readErr := io.ReadAll(r)
	if f.uploadErr != nil {
		return f.uploadErr
	}
	if readErr != nil {
		return readErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs[blob] = &fakeBlob{data: data, etag: `"etag"`, modified: time.Now().UTC()}
	return nil
}

func (f *fakeStore) Delete(_ context.Context, blob string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blobs[blob]; !ok {
		return errNotFound
	}
	delete(f.blobs, blob)
	return nil
}

func (f *fakeStore) List(_ context.Context, prefix string, fn func(blobInfo) error) error {
	if f.listErr != nil {
		return f.listErr
	}
	f.mu.Lock()
	names := make([]string, 0, len(f.blobs))
	for n := range f.blobs {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	f.mu.Unlock()
	sort.Strings(names)
	for _, n := range names {
		f.mu.Lock()
		b := f.blobs[n]
		f.mu.Unlock()
		if err := fn(blobInfo{Name: n, Size: int64(len(b.data)), ETag: b.etag, LastModified: b.modified, ContentType: b.contentType}); err != nil {
			return err
		}
	}
	return nil
}

// opener returns a storeOpener that always yields the given fake.
func opener(f *fakeStore) storeOpener {
	return func(context.Context, *config) (blobStore, error) { return f, nil }
}

func blobCfg(blob, format string) []byte {
	return fmt.Appendf(nil, `{"sas_url":"https://a.blob.core.windows.net/c?sig=x","container":"c","blob":%q,"format":%q}`, blob, format)
}

// --- get ---------------------------------------------------------------------

func TestGetSourceNDJSON(t *testing.T) {
	f := newFakeStore()
	f.put("in.ndjson", []byte("{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n"), "application/x-ndjson")

	s := &getSource{open: opener(f)}
	ctx := context.Background()
	if err := s.Open(ctx, blobCfg("in.ndjson", "ndjson")); err != nil {
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

func TestGetSourceCSV(t *testing.T) {
	f := newFakeStore()
	f.put("in.csv", []byte("name,age\nalice,30\nbob,25\n"), "text/csv")

	s := &getSource{open: opener(f)}
	ctx := context.Background()
	if err := s.Open(ctx, blobCfg("in.csv", "csv")); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	var names []string
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			v, _ := rec.Field("name")
			names = append(names, v.String())
		}
	}
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("names = %v, want [alice bob]", names)
	}
}

func TestGetSourceNotFound(t *testing.T) {
	s := &getSource{open: opener(newFakeStore())}
	err := s.Open(context.Background(), blobCfg("missing", "ndjson"))
	if !errors.Is(err, errNotFound) {
		t.Fatalf("open missing blob = %v, want errNotFound", err)
	}
}

// --- put ---------------------------------------------------------------------

func TestPutSinkRoundTrip(t *testing.T) {
	f := newFakeStore()
	s := &putSink{open: opener(f)}
	ctx := context.Background()
	if err := s.Open(ctx, blobCfg("out.ndjson", "ndjson")); err != nil {
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

	b, ok := f.blobs["out.ndjson"]
	if !ok {
		t.Fatal("blob not uploaded")
	}
	lines := strings.Count(strings.TrimSpace(string(b.data)), "\n") + 1
	if lines != 3 || !strings.Contains(string(b.data), `"i":2`) {
		t.Fatalf("uploaded = %q", b.data)
	}
}

func TestPutSinkUploadError(t *testing.T) {
	f := newFakeStore()
	f.uploadErr = errors.New("boom")
	s := &putSink{open: opener(f)}
	ctx := context.Background()
	if err := s.Open(ctx, blobCfg("out.ndjson", "ndjson")); err != nil {
		t.Fatalf("open: %v", err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(1)
	bld.EndMap()
	batch.Append(bld.Finish())
	// Write may or may not observe the pipe error depending on timing; Close
	// must surface it deterministically.
	_ = s.Write(ctx, batch)
	if err := s.Close(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("close = %v, want upload error surfaced", err)
	}
}

// --- list --------------------------------------------------------------------

func TestListSource(t *testing.T) {
	f := newFakeStore()
	f.put("logs/a.txt", []byte("aaaa"), "text/plain")
	f.put("logs/b.txt", []byte("bb"), "text/plain")
	f.put("other.txt", []byte("z"), "text/plain")

	s := &listSource{open: opener(f)}
	ctx := context.Background()
	cfg := []byte(`{"sas_url":"https://a.blob.core.windows.net/c?sig=x","prefix":"logs/"}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	got := map[string]int64{}
	var etag, ct string
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
			size, _ := rec.Field("size")
			got[name.String()] = size.Int()
			e, _ := rec.Field("etag")
			c, _ := rec.Field("content_type")
			etag, ct = e.String(), c.String()
		}
	}
	if len(got) != 2 || got["logs/a.txt"] != 4 || got["logs/b.txt"] != 2 {
		t.Fatalf("listing = %v, want logs/a.txt=4 logs/b.txt=2 (prefix-filtered)", got)
	}
	if etag == "" || ct != "text/plain" {
		t.Fatalf("metadata missing: etag=%q content_type=%q", etag, ct)
	}
}

func TestListSourceError(t *testing.T) {
	f := newFakeStore()
	f.listErr = errors.New("list failed")
	s := &listSource{open: opener(f)}
	cfg := []byte(`{"sas_url":"https://a.blob.core.windows.net/c?sig=x"}`)
	if err := s.Open(context.Background(), cfg); err == nil {
		t.Fatal("expected list error surfaced at Open")
	}
}

// --- delete ------------------------------------------------------------------

func TestDelete(t *testing.T) {
	f := newFakeStore()
	f.put("gone.txt", []byte("x"), "")
	s := &deleteSource{open: opener(f)}
	ctx := context.Background()
	cfg := []byte(`{"sas_url":"https://a.blob.core.windows.net/c?sig=x","container":"c","blob":"gone.txt"}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	b, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	recs := b.Records()
	if len(recs) != 1 {
		t.Fatalf("emitted %d records, want 1", len(recs))
	}
	if op, _ := recs[0].Field("op"); op.String() != "delete" {
		t.Fatalf("op = %q, want delete", op.String())
	}
	if ok, _ := recs[0].Field("ok"); !ok.Bool() {
		t.Fatal("status not ok")
	}
	if blob, _ := recs[0].Field("blob"); blob.String() != "gone.txt" {
		t.Fatalf("blob field = %q", blob.String())
	}
	if _, err := s.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next = %v, want EOF", err)
	}
	if _, ok := f.blobs["gone.txt"]; ok {
		t.Fatal("blob not deleted")
	}
	_ = s.Close()
}

func TestDeleteIdempotent(t *testing.T) {
	// Deleting an absent blob is a success (errNotFound swallowed).
	s := &deleteSource{open: opener(newFakeStore())}
	ctx := context.Background()
	cfg := []byte(`{"sas_url":"https://a.blob.core.windows.net/c?sig=x","container":"c","blob":"nope"}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	b, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("delete missing blob: %v", err)
	}
	if ok, _ := b.Records()[0].Field("ok"); !ok.Bool() {
		t.Fatal("idempotent delete not reported ok")
	}
}

func TestDeleteStoreError(t *testing.T) {
	// A non-not-found store error propagates (not swallowed).
	bad := storeOpener(func(context.Context, *config) (blobStore, error) {
		return nil, errors.New("dial failed")
	})
	s := &deleteSource{open: bad}
	ctx := context.Background()
	cfg := []byte(`{"sas_url":"https://a.blob.core.windows.net/c?sig=x","container":"c","blob":"b"}`)
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Next(ctx); err == nil {
		t.Fatal("expected store error propagated")
	}
}

// --- config / auth -----------------------------------------------------------

func TestValidateAuth(t *testing.T) {
	cases := []struct {
		name    string
		cfg     string
		wantErr bool
	}{
		{"no credentials", `{"container":"c"}`, true},
		{"account without key", `{"account":"a","container":"c"}`, true},
		{"account+key no container", `{"account":"a","account_key":"k"}`, true},
		{"account+key+container", `{"account":"a","account_key":"k","container":"c"}`, false},
		{"connection string+container", `{"connection_string":"cs","container":"c"}`, false},
		{"sas url only", `{"sas_url":"https://x/c?sig=y"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c config
			err := parseConfig([]byte(tc.cfg), &c)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseConfig err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestRequireBlobFormat(t *testing.T) {
	t.Run("missing blob", func(t *testing.T) {
		c := config{Format: "ndjson"}
		if err := c.requireBlobFormat(); err == nil {
			t.Fatal("expected blob-required error")
		}
	})
	t.Run("bad format", func(t *testing.T) {
		c := config{Blob: "b", Format: "xml"}
		if err := c.requireBlobFormat(); err == nil {
			t.Fatal("expected unsupported-format error")
		}
	})
	t.Run("default ndjson", func(t *testing.T) {
		c := config{Blob: "b"}
		if err := c.requireBlobFormat(); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
		if c.Format != "ndjson" {
			t.Fatalf("format = %q, want ndjson default", c.Format)
		}
	})
}

func TestParseConfigBadJSON(t *testing.T) {
	var c config
	if err := parseConfig([]byte(`{not json`), &c); err == nil {
		t.Fatal("expected JSON error")
	}
}

// --- network guard -----------------------------------------------------------

func TestCheckAddr(t *testing.T) {
	cases := []struct {
		name       string
		allowLocal bool
		address    string
		wantErr    bool
	}{
		{"public allowed", false, "20.150.34.4:443", false},
		{"loopback refused", false, "127.0.0.1:443", true},
		{"private refused", false, "10.1.2.3:443", true},
		{"link-local refused", false, "169.254.169.254:80", true},
		{"cgnat refused", false, "100.64.0.1:443", true},
		{"loopback allowed with flag", true, "127.0.0.1:443", false},
		{"private allowed with flag", true, "10.1.2.3:443", false},
		{"unresolvable host", false, "not-an-ip:443", true},
		{"missing port", false, "10.0.0.1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkAddr(tc.allowLocal, tc.address)
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkAddr(%v,%q) = %v, wantErr = %v", tc.allowLocal, tc.address, err, tc.wantErr)
			}
		})
	}
}

func TestGuardedClientBuilds(t *testing.T) {
	if c := guardedClient(false); c == nil || c.Transport == nil {
		t.Fatal("guardedClient returned an unusable client")
	}
}

// --- real client construction (offline; no dial) -----------------------------

// azuriteKey is a benign dummy base64 value ("test-key"), not a real account
// key. The offline SharedKeyCredential/connection-string constructors only
// base64-decode it — they never dial — so any valid base64 exercises the
// credential-parsing paths.
const azuriteKey = "dGVzdC1rZXk=" //nolint:gosec // G101: dummy base64 test fixture, not a credential

func TestContainerClientModes(t *testing.T) {
	t.Run("shared key", func(t *testing.T) {
		c := config{Account: "devstoreaccount1", AccountKey: azuriteKey, Container: "c"}
		if cc, err := c.containerClient(); err != nil || cc == nil {
			t.Fatalf("shared-key client: cc=%v err=%v", cc, err)
		}
	})
	t.Run("shared key custom endpoint", func(t *testing.T) {
		c := config{Account: "devstoreaccount1", AccountKey: azuriteKey, Container: "c", Endpoint: "http://127.0.0.1:10000/devstoreaccount1"}
		if _, err := c.containerClient(); err != nil {
			t.Fatalf("endpoint override: %v", err)
		}
	})
	t.Run("bad shared key", func(t *testing.T) {
		c := config{Account: "a", AccountKey: "not+valid+base64+==...", Container: "c"}
		if _, err := c.containerClient(); err == nil {
			t.Fatal("expected bad-key error")
		}
	})
	t.Run("connection string", func(t *testing.T) {
		cs := "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=" + azuriteKey + ";EndpointSuffix=core.windows.net"
		c := config{ConnectionString: cs, Container: "c"}
		if _, err := c.containerClient(); err != nil {
			t.Fatalf("connection string: %v", err)
		}
	})
	t.Run("sas url", func(t *testing.T) {
		c := config{SASURL: "https://devstoreaccount1.blob.core.windows.net/c?sv=2021&sig=abc"}
		if _, err := c.containerClient(); err != nil {
			t.Fatalf("sas url: %v", err)
		}
	})
}

func TestServiceURL(t *testing.T) {
	if got := (&config{Account: "acct"}).serviceURL(); got != "https://acct.blob.core.windows.net/" {
		t.Fatalf("default serviceURL = %q", got)
	}
	if got := (&config{Endpoint: "http://127.0.0.1:10000/x"}).serviceURL(); got != "http://127.0.0.1:10000/x" {
		t.Fatalf("endpoint override = %q", got)
	}
}

// --- connector wiring --------------------------------------------------------

func TestConnectorDescriptor(t *testing.T) {
	c := Connector()
	if c.Name != "azureblob" {
		t.Fatalf("name = %q", c.Name)
	}
	for _, v := range []string{"get", "list", "delete"} {
		if _, ok := c.Sources[v]; !ok {
			t.Fatalf("missing source verb %q", v)
		}
	}
	if _, ok := c.Sinks["put"]; !ok {
		t.Fatal("missing put sink")
	}
	// Descriptor must build (schemas well-formed enough for the SDK).
	d := sdk.BuildDescriptor(c)
	if len(d.Actions) != 4 {
		t.Fatalf("descriptor actions = %d, want 4", len(d.Actions))
	}
	// Every action has a schema mentioning the secret marker for its secret fields.
	for name, sc := range c.Schemas {
		if !strings.Contains(string(sc), "x-shift-secret") {
			t.Fatalf("schema %q missing x-shift-secret tag", name)
		}
	}
}
