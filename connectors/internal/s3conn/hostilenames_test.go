package s3conn

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// hostileKeys are keys a traversal audit would flag. In S3 they are not
// traversals at all: a key is an OPAQUE byte string within a bucket, "/" is a
// display convention with no directory semantics, and this connector never
// turns a key into a local filesystem path. They are used here to pin that the
// connector neither cleans them (which would act on a different object than the
// flow document names) nor lets them alter the shape of the HTTP request.
var hostileKeys = []string{
	"../../etc/passwd",
	"data/../../etc/passwd",
	"data/..",
	"..",
	".",
	"/etc/passwd",
	"C:\\secrets\\key.pem",
	"key\x00.ndjson",
	"key\r\nGET /other HTTP/1.1\r\n",
	"key\nX-Injected: 1",
	strings.Repeat("../", 200) + "etc/passwd",
	strings.Repeat("k", 1024), // the S3 key-length maximum
	"invoices 2026-08.ndjson",
	"rapport-ao\u00fbt.ndjson",
	"\u202Etxt.exe", // right-to-left override
}

func hostileCfg(t *testing.T, key string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"bucket": "b", "key": key, "prefix": key,
		"access_key_id": "k", "secret_access_key": "s", "region": "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestAnObjectKeyReachesS3ByteIdenticalAndNeverBecomesAPath is the audit record
// for s3conn: the connector was found already safe, and this pins why.
//
// (a) No key is ever used to build a LOCAL filesystem path — get streams the
// response body, put streams into PutObject, delete names the key. There is no
// download-to-disk path for a traversal to land in.
// (b) No key is ever cleaned or normalised. That is the property worth
// asserting: path.Clean-ing "a/../b" would make every verb operate on an object
// other than the one the flow document names, which is the same class of
// mistake as following a traversal.
func TestAnObjectKeyReachesS3ByteIdenticalAndNeverBecomesAPath(t *testing.T) {
	ctx := context.Background()
	for _, key := range hostileKeys {
		t.Run(key, func(t *testing.T) {
			f := newFake()
			f.objects[key] = []byte(`{"a":1}` + "\n")
			withFake(t, f)

			// get: the key must arrive verbatim, or the fake would not find it.
			src := &getSource{}
			if err := src.Open(ctx, hostileCfg(t, key)); err != nil {
				t.Fatalf("get: key %q was altered before reaching S3: %v", key, err)
			}
			_ = src.Close()

			// delete: assert the exact bytes the API received.
			del := &deleteSource{}
			if err := del.Open(ctx, hostileCfg(t, key)); err != nil {
				t.Fatal(err)
			}
			if _, err := del.Next(ctx); err != nil {
				t.Fatal(err)
			}
			if len(f.deleted) != 1 || f.deleted[0] != key {
				t.Errorf("DeleteObject received %q, want the configured key %q", f.deleted, key)
			}
		})
	}
}

// TestAListedKeyTravelsAsDataNotAsAPath. A listing's keys are chosen by whoever
// can write to the bucket, so they are untrusted — but in this connector they
// only ever become a record field. They must therefore arrive unmodified: a
// downstream step that feeds one back into a get needs the byte string S3
// actually holds, and any cleaning here would silently address a different
// object.
func TestAListedKeyTravelsAsDataNotAsAPath(t *testing.T) {
	f := newFake()
	page := &s3.ListObjectsV2Output{}
	for _, k := range hostileKeys {
		page.Contents = append(page.Contents, types.Object{
			Key: aws.String(k), Size: aws.Int64(1), ETag: aws.String(`"e"`),
		})
	}
	f.listPages = map[string]*s3.ListObjectsV2Output{"": page}
	withFake(t, f)

	src := &listSource{}
	if err := src.Open(context.Background(), hostileCfg(t, "")); err != nil {
		t.Fatal(err)
	}
	b, err := src.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Len() != len(hostileKeys) {
		t.Fatalf("emitted %d records for %d keys", b.Len(), len(hostileKeys))
	}
	for i, want := range hostileKeys {
		got, _ := b.Records()[i].Field("key")
		if got.String() != want {
			t.Errorf("key %d = %q, want %q — a listed key must reach the record unmodified", i, got.String(), want)
		}
	}
	if _, err := src.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Errorf("second Next = %v, want EOF", err)
	}
}

// TestAKeyCannotInjectASecondHTTPRequest. The one way a key COULD cross a
// protocol boundary is the request line: the key is interpolated into the
// object URL, and a raw CR/LF or NUL there would be request smuggling. The AWS
// SDK percent-encodes it, so this is a dependency property rather than one this
// connector implements — which is exactly why it needs an assertion: an SDK
// bump that changed it would otherwise be silent.
func TestAKeyCannotInjectASecondHTTPRequest(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.RequestURI)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	for _, key := range hostileKeys {
		c := &config{
			Bucket: "b", Key: key, Region: "us-east-1",
			AccessKeyID: "k", SecretAccessKey: "s",
			Endpoint: srv.URL, PathStyle: true, AllowLocal: true, TimeoutSeconds: 5,
		}
		api, err := c.buildClient()
		if err != nil {
			t.Fatal(err)
		}
		seen = nil
		if _, err := api.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(c.Bucket), Key: aws.String(c.Key),
		}); err != nil {
			t.Fatalf("key %q: %v", key, err)
		}
		if len(seen) != 1 {
			t.Fatalf("key %q produced %d requests, want 1", key, len(seen))
		}
		// A control character reaching the wire raw would end the request line.
		if i := strings.IndexAny(seen[0], "\r\n\x00"); i >= 0 {
			t.Errorf("key %q put a raw control character on the request line at %d: %q", key, i, seen[0])
		}
		// The escaped request line must still decode back to the key, so the
		// object addressed is the one configured.
		if _, err := url.PathUnescape(seen[0]); err != nil {
			t.Errorf("key %q produced an undecodable request line %q: %v", key, seen[0], err)
		}
	}
}
