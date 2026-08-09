package azureblobconn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// hostileBlobNames are names a traversal audit would flag. In Azure Blob
// Storage they are not traversals: a blob name is an OPAQUE string within a
// container, "/" is a naming convention with no directory semantics, and this
// connector never turns a blob name into a local filesystem path. They are used
// here to pin that the connector neither cleans them (which would act on a
// different blob than the flow document names) nor lets them change the shape
// of the HTTP request.
var hostileBlobNames = []string{
	"../../etc/passwd",
	"data/../../etc/passwd",
	"data/..",
	"..",
	".",
	"/etc/passwd",
	"C:\\secrets\\key.pem",
	"blob\x00.ndjson",
	"blob\r\nGET /other HTTP/1.1\r\n",
	"blob\nx-ms-injected: 1",
	strings.Repeat("../", 200) + "etc/passwd",
	"invoices 2026-08.ndjson",
	"rapport-ao\u00fbt.ndjson",
	"\u202Etxt.exe", // right-to-left override
}

// hostileCfg builds a config document with encoding/json rather than %q, so a
// NUL in the blob name survives as valid JSON instead of a Go-style escape.
func hostileCfg(t *testing.T, blob string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"sas_url": "https://a.blob.core.windows.net/c?sig=x", "container": "c",
		"blob": blob, "format": "ndjson",
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestABlobNameReachesAzureByteIdenticalAndNeverBecomesAPath is the audit record
// for azureblobconn: the connector was found already safe, and this pins why.
//
// (a) No blob name is ever used to build a LOCAL filesystem path — get streams
// the download body, put streams into UploadStream, delete names the blob.
// There is no download-to-disk path for a traversal to land in.
// (b) No blob name is ever cleaned or normalised, which is the property worth
// asserting: path.Clean-ing "a/../b" would make every verb operate on a blob
// other than the one configured — the same class of mistake as following a
// traversal, just in the opposite direction.
func TestABlobNameReachesAzureByteIdenticalAndNeverBecomesAPath(t *testing.T) {
	ctx := context.Background()
	for _, blob := range hostileBlobNames {
		t.Run(blob, func(t *testing.T) {
			f := newFakeStore()
			f.put(blob, []byte(`{"a":1}`+"\n"), "application/x-ndjson")

			// get: the name must arrive verbatim or the fake would not find it.
			src := &getSource{open: opener(f)}
			if err := src.Open(ctx, hostileCfg(t, blob)); err != nil {
				t.Fatalf("get: blob name %q was altered before reaching Azure: %v", blob, err)
			}
			_ = src.Close()

			// delete: same, and it must remove exactly that key.
			del := &deleteSource{open: opener(f)}
			if err := del.Open(ctx, hostileCfg(t, blob)); err != nil {
				t.Fatal(err)
			}
			if _, err := del.Next(ctx); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			_, still := f.blobs[blob]
			left := len(f.blobs)
			f.mu.Unlock()
			if still || left != 0 {
				t.Errorf("delete did not remove the configured blob %q (%d left)", blob, left)
			}
		})
	}
}

// TestAListedBlobNameTravelsAsDataNotAsAPath. A listing's names are chosen by
// whoever can write to the container, so they are untrusted — but here they only
// ever become a record field. They must arrive unmodified: a downstream step
// feeding one back into a get needs the byte string Azure actually holds.
func TestAListedBlobNameTravelsAsDataNotAsAPath(t *testing.T) {
	f := newFakeStore()
	for _, n := range hostileBlobNames {
		f.put(n, []byte("x\n"), "text/plain")
	}
	src := &listSource{open: opener(f)}
	ctx := context.Background()
	if err := src.Open(ctx, []byte(`{"sas_url":"https://a.blob.core.windows.net/c?sig=x","container":"c"}`)); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	b, err := src.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range b.Records() {
		n, _ := rec.Field("name")
		got[n.String()] = true
	}
	for _, want := range hostileBlobNames {
		if !got[want] {
			t.Errorf("listed name %q did not reach the record unmodified", want)
		}
	}
}

// TestABlobNameCannotInjectASecondHTTPRequest. The one place a blob name COULD
// cross a protocol boundary is the request line: it is interpolated into the
// blob URL, and a raw CR/LF or NUL there would be request smuggling. The Azure
// SDK percent-encodes the whole name — "/" included, so the name cannot even
// add a path segment. That is a dependency property rather than one this
// connector implements, which is why it needs an assertion: an SDK bump that
// changed it would otherwise be silent.
func TestABlobNameCannotInjectASecondHTTPRequest(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.RequestURI)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	const container = "/devstoreaccount1/c/"
	// Any syntactically valid base64 satisfies the SDK's key parsing, which is
	// all these tests need — so this is deliberately SHORT and low-entropy.
	//
	// Not the well-known Azurite emulator key, and not a synthetic 64-byte one
	// either: both match gitleaks' generic-api-key rule, which keys on the
	// SHAPE of the assignment rather than the value. Either would have needed a
	// gitleaks:allow, and a scanner people learn to wave through stops being a
	// scanner. Twelve characters is cheaper than an exemption (ADR-0006).
	const testAccountKey = "dGVzdGtleQ=="
	for _, blob := range hostileBlobNames {
		c := &config{
			Account:    "devstoreaccount1",
			AccountKey: testAccountKey,
			Container:  "c", Endpoint: srv.URL + "/devstoreaccount1", AllowLocal: true,
		}
		store, err := openStore(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		seen = nil
		if err := store.Delete(context.Background(), blob); err != nil {
			t.Fatalf("blob %q: %v", blob, err)
		}
		if len(seen) != 1 {
			t.Fatalf("blob %q produced %d requests, want 1", blob, len(seen))
		}
		if i := strings.IndexAny(seen[0], "\r\n\x00"); i >= 0 {
			t.Errorf("blob %q put a raw control character on the request line at %d: %q", blob, i, seen[0])
		}
		// The name stays inside the container's path prefix: it can add no
		// segment of its own, so no name can address another container.
		if !strings.HasPrefix(seen[0], container) {
			t.Errorf("blob %q escaped the container prefix: %q", blob, seen[0])
		}
		if strings.Contains(strings.TrimPrefix(seen[0], container), "/") {
			t.Errorf("blob %q added a path segment to the request line: %q", blob, seen[0])
		}
		if _, err := url.PathUnescape(seen[0]); err != nil {
			t.Errorf("blob %q produced an undecodable request line %q: %v", blob, seen[0], err)
		}
	}
}
