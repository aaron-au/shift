package s3conn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeS3 is an in-memory implementation of the s3API surface. No network, no
// MinIO: every verb is exercised against this fake.
type fakeS3 struct {
	objects map[string][]byte // key -> body
	deleted []string          // keys passed to DeleteObject

	// Optional error injection per verb.
	getErr, putErr, listErr, delErr error

	// listPages, when set, is returned page-by-page keyed off the request's
	// continuation token ("" for the first page). Otherwise ListObjectsV2
	// derives a single page from objects filtered by prefix.
	listPages map[string]*s3.ListObjectsV2Output
}

func newFake() *fakeS3 { return &fakeS3{objects: map[string][]byte{}} }

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	body, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, fmt.Errorf("NoSuchKey: %s", aws.ToString(in.Key))
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	// Consume the streamed body fully, as real S3 would.
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.objects[aws.ToString(in.Key)] = body
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.listPages != nil {
		return f.listPages[aws.ToString(in.ContinuationToken)], nil
	}
	out := &s3.ListObjectsV2Output{}
	prefix := aws.ToString(in.Prefix)
	for k, v := range f.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		out.Contents = append(out.Contents, types.Object{
			Key:          aws.String(k),
			Size:         aws.Int64(int64(len(v))),
			ETag:         aws.String(`"etag-` + k + `"`),
			LastModified: aws.Time(time.Unix(0, 0).UTC()),
		})
	}
	return out, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if f.delErr != nil {
		return nil, f.delErr
	}
	f.deleted = append(f.deleted, aws.ToString(in.Key))
	delete(f.objects, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

// withFake redirects newClient to return f for the duration of the test.
func withFake(t *testing.T, f s3API) {
	t.Helper()
	orig := newClient
	newClient = func(*config) (s3API, error) { return f, nil }
	t.Cleanup(func() { newClient = orig })
}

func cfgJSON(t *testing.T, extra string) []byte {
	t.Helper()
	return fmt.Appendf(nil, `{"bucket":"b","access_key_id":"AK","secret_access_key":"SK"%s}`, extra)
}

func TestGetSourceNDJSON(t *testing.T) {
	f := newFake()
	f.objects["data.ndjson"] = []byte("{\"i\":1}\n{\"i\":2}\n{\"i\":3}\n")
	withFake(t, f)

	s := &getSource{}
	ctx := context.Background()
	if err := s.Open(ctx, cfgJSON(t, `,"key":"data.ndjson","format":"ndjson"`)); err != nil {
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
	f := newFake()
	f.objects["data.csv"] = []byte("i\n1\n2\n3\n")
	withFake(t, f)

	s := &getSource{}
	ctx := context.Background()
	if err := s.Open(ctx, cfgJSON(t, `,"key":"data.csv","format":"csv"`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	var n int
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			if v, ok := rec.Field("i"); !ok || v.String() == "" {
				t.Fatalf("row %d missing field i: %v", n, rec)
			}
			n++
		}
	}
	if n != 3 {
		t.Fatalf("csv rows = %d, want 3", n)
	}
}

func TestGetSourceError(t *testing.T) {
	f := newFake()
	f.getErr = errors.New("boom")
	withFake(t, f)
	err := (&getSource{}).Open(context.Background(), cfgJSON(t, `,"key":"k"`))
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected GetObject error, got %v", err)
	}
}

func TestPutSinkRoundTrip(t *testing.T) {
	f := newFake()
	withFake(t, f)

	s := &putSink{}
	ctx := context.Background()
	if err := s.Open(ctx, cfgJSON(t, `,"key":"out.ndjson","format":"ndjson"`)); err != nil {
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
	data := f.objects["out.ndjson"]
	lines := strings.Count(strings.TrimSpace(string(data)), "\n") + 1
	if lines != 3 || !strings.Contains(string(data), `"i":2`) {
		t.Fatalf("uploaded object = %q", data)
	}
}

func TestPutSinkCSV(t *testing.T) {
	f := newFake()
	withFake(t, f)

	s := &putSink{}
	ctx := context.Background()
	if err := s.Open(ctx, cfgJSON(t, `,"key":"out.csv","format":"csv"`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	batch := record.NewBatch()
	bld := batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(7)
	bld.EndMap()
	batch.Append(bld.Finish())
	if err := s.Write(ctx, batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	data := string(f.objects["out.csv"])
	if !strings.Contains(data, "i") || !strings.Contains(data, "7") {
		t.Fatalf("uploaded csv = %q", data)
	}
}

func TestPutSinkError(t *testing.T) {
	f := newFake()
	f.putErr = errors.New("denied")
	withFake(t, f)

	s := &putSink{}
	if err := s.Open(context.Background(), cfgJSON(t, `,"key":"k"`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	// The upload fails in the background; Close surfaces the error.
	if err := s.Close(); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("expected PutObject error from Close, got %v", err)
	}
}

func TestListSourcePagination(t *testing.T) {
	f := newFake()
	f.listPages = map[string]*s3.ListObjectsV2Output{
		"": {
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("p2"),
			Contents: []types.Object{
				{Key: aws.String("a"), Size: aws.Int64(1), ETag: aws.String(`"e1"`), LastModified: aws.Time(time.Unix(10, 0))},
			},
		},
		"p2": {
			IsTruncated: aws.Bool(false),
			Contents: []types.Object{
				{Key: aws.String("b"), Size: aws.Int64(2), ETag: aws.String(`"e2"`), LastModified: aws.Time(time.Unix(20, 0))},
			},
		},
	}
	withFake(t, f)

	s := &listSource{}
	ctx := context.Background()
	if err := s.Open(ctx, cfgJSON(t, `,"prefix":""`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	keys := map[string]int64{}
	for {
		b, err := s.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			k, _ := rec.Field("key")
			sz, _ := rec.Field("size")
			if _, ok := rec.Field("etag"); !ok {
				t.Fatalf("record missing etag: %v", rec)
			}
			if lm, ok := rec.Field("last_modified"); !ok || lm.String() == "" {
				t.Fatalf("record missing last_modified: %v", rec)
			}
			keys[k.String()] = sz.Int()
		}
	}
	if len(keys) != 2 || keys["a"] != 1 || keys["b"] != 2 {
		t.Fatalf("listing = %v, want a=1 b=2 across two pages", keys)
	}
}

func TestDeleteSource(t *testing.T) {
	f := newFake()
	f.objects["gone"] = []byte("x")
	withFake(t, f)

	s := &deleteSource{}
	ctx := context.Background()
	if err := s.Open(ctx, cfgJSON(t, `,"key":"gone"`)); err != nil {
		t.Fatalf("open: %v", err)
	}
	b, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	recs := b.Records()
	if len(recs) != 1 {
		t.Fatalf("delete emitted %d records, want 1", len(recs))
	}
	if op, _ := recs[0].Field("op"); op.String() != "delete" {
		t.Fatalf("status op = %q, want delete", op.String())
	}
	if ok, _ := recs[0].Field("ok"); !ok.Bool() {
		t.Fatalf("status not ok: %v", recs[0])
	}
	if _, err := s.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next = %v, want EOF", err)
	}
	if len(f.deleted) != 1 || f.deleted[0] != "gone" {
		t.Fatalf("deleted = %v, want [gone]", f.deleted)
	}
	_ = s.Close()
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]string{ //nolint:gosec // G101: literal JSON test fixtures, not real credentials
		"missing bucket": `{"access_key_id":"AK","secret_access_key":"SK"}`,
		"missing creds":  `{"bucket":"b"}`,
		"missing secret": `{"bucket":"b","access_key_id":"AK"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var c config
			if err := parseConfig([]byte(raw), &c); err == nil {
				t.Fatalf("%s: expected validation error", name)
			}
		})
	}
	t.Run("defaults region and timeout", func(t *testing.T) {
		var c config
		if err := parseConfig([]byte(`{"bucket":"b","access_key_id":"AK","secret_access_key":"SK"}`), &c); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
		if c.Region != "us-east-1" || c.TimeoutSeconds != 300 {
			t.Fatalf("defaults not applied: region=%q timeout=%d", c.Region, c.TimeoutSeconds)
		}
	})
	t.Run("bad format", func(t *testing.T) {
		c := config{Key: "k", Format: "xml"}
		if err := c.requireKeyFormat(); err == nil {
			t.Fatal("expected unsupported-format error")
		}
	})
	t.Run("missing key", func(t *testing.T) {
		c := config{}
		if err := c.requireKey(); err == nil {
			t.Fatal("expected missing-key error")
		}
	})
}

func TestGuard(t *testing.T) {
	g := guard(false)
	for _, addr := range []string{"127.0.0.1:443", "10.0.0.5:443", "169.254.169.254:80", "100.64.1.1:443"} {
		if err := g("tcp", addr, nil); err == nil {
			t.Fatalf("guard allowed internal target %s", addr)
		}
	}
	// A public IP is allowed.
	if err := g("", "93.184.216.34:443", nil); err != nil {
		t.Fatalf("guard refused public IP: %v", err)
	}
	// allow_local disables the guard entirely.
	if err := guard(true)("", "127.0.0.1:443", nil); err != nil {
		t.Fatalf("allow_local should bypass guard: %v", err)
	}
}

func TestBuildClientStaticCreds(t *testing.T) {
	// buildClient must construct a real client from static creds without any
	// network call, honouring a custom endpoint + path style.
	c := config{Bucket: "b", AccessKeyID: "AK", SecretAccessKey: "SK", Region: "us-east-1", TimeoutSeconds: 300, Endpoint: "https://minio.example:9000", PathStyle: true}
	api, err := c.buildClient()
	if err != nil {
		t.Fatalf("buildClient: %v", err)
	}
	if api == nil {
		t.Fatal("buildClient returned nil client")
	}
	if c.httpClient() == nil {
		t.Fatal("httpClient returned nil")
	}
}

func TestConnectorShape(t *testing.T) {
	c := Connector()
	if c.Name != "s3" {
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
	// Every verb declares a config schema.
	for _, v := range []string{"get", "put", "list", "delete"} {
		if len(c.Schemas[v]) == 0 {
			t.Fatalf("verb %q has no schema", v)
		}
	}
}
