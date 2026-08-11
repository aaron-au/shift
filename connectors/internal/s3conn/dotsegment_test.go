package s3conn

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TC-032. An S3 key is an opaque byte string, so `data/../x` names an object
// literally called `data/../x` and there is nothing to traverse. That is why
// this connector never cleans a key, and why the keys still reach the API
// byte-identical (hostilenames_test.go).
//
// The risk is a middlebox: `.` and `/` are legal path characters, so the key
// reaches the wire as written, and a reverse proxy in front of an S3-compatible
// endpoint — nginx and friends normalise `..` by default — can resolve it to a
// different resource. The refusal is therefore scoped to exactly that case, a
// configured custom `endpoint`, and it is LOUD: silently rewriting the key would
// read the wrong object and report success.

func cfgWith(t *testing.T, key, extra string) []byte {
	t.Helper()
	return fmt.Appendf(nil,
		`{"bucket":"b","access_key_id":"AK","secret_access_key":"SK","key":%q%s}`, key, extra)
}

func TestADotSegmentIsRefusedLoudlyWhenACustomEndpointIsConfigured(t *testing.T) {
	ctx := context.Background()
	for _, key := range []string{
		"../../etc/passwd",
		"data/../../etc/passwd",
		"data/..",
		"..",
		strings.Repeat("../", 200) + "etc/passwd",
	} {
		t.Run(key, func(t *testing.T) {
			cfg := cfgWith(t, key, `,"endpoint":"https://minio.internal:9000","allow_local":true`)

			err := (&getSource{}).Open(ctx, cfg)
			if err == nil {
				t.Fatalf("key %q was accepted against a custom endpoint: a normalising proxy could resolve it to another object", key)
			}
			// Loud means the operator learns WHY, not just that something failed.
			if !strings.Contains(err.Error(), "..") || !strings.Contains(err.Error(), "endpoint") {
				t.Fatalf("key %q refused as %q, which does not explain the dot segment or the endpoint condition", key, err)
			}

			// Every verb that takes a key, not just get.
			if err := (&deleteSource{}).Open(ctx, cfg); err == nil {
				t.Fatalf("delete accepted key %q against a custom endpoint", key)
			}
			if err := (&putSink{}).Open(ctx, cfg); err == nil {
				t.Fatalf("put accepted key %q against a custom endpoint", key)
			}
		})
	}
}

// TestTheSameKeysAreAcceptedAgainstAWS is the whole point of scoping the rule.
// Against AWS there is no proxy and no traversal, and refusing these would
// refuse technically legal keys for no benefit.
func TestTheSameKeysAreAcceptedAgainstAWS(t *testing.T) {
	ctx := context.Background()
	for _, key := range []string{"../../etc/passwd", "data/..", ".."} {
		t.Run(key, func(t *testing.T) {
			f := newFake()
			f.objects[key] = []byte(`{"a":1}` + "\n")
			withFake(t, f)

			// No endpoint configured: AWS proper.
			if err := (&getSource{}).Open(ctx, cfgWith(t, key, "")); err != nil {
				t.Fatalf("key %q was refused against AWS, where it is a legal opaque key: %v", key, err)
			}
		})
	}
}

// TestAKeyThatMerelyCONTAINSDotsIsStillAccepted: the rule is about a `..` path
// SEGMENT, not about dots. A key called `report..final` or `v1.2..3/data` is
// ordinary and no proxy would rewrite it.
func TestAKeyThatMerelyContainsDotsIsStillAccepted(t *testing.T) {
	ctx := context.Background()
	for _, key := range []string{"report..final.ndjson", "v1.2..3/data.ndjson", "a/b..c/d.ndjson", "...", "..x/y"} {
		t.Run(key, func(t *testing.T) {
			// The fake stands in for the endpoint: this test is about what
			// validation ACCEPTS, and a real dial would fail for reasons that
			// have nothing to do with the rule.
			f := newFake()
			f.objects[key] = []byte(`{"a":1}` + "\n")
			withFake(t, f)

			cfg := cfgWith(t, key, `,"endpoint":"https://minio.internal:9000","allow_local":true`)
			if err := (&getSource{}).Open(ctx, cfg); err != nil {
				t.Fatalf("key %q was refused, but it contains no %q segment: %v", key, "..", err)
			}
		})
	}
}
