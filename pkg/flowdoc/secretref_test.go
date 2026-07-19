package flowdoc

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func docWithConfigs(t *testing.T, source, sink string) *Document {
	t.Helper()
	raw := fmt.Sprintf(`{"name":"f",
	  "source":{"connector":"http","action":"get","config":%s},
	  "sink":{"connector":"http","action":"post","config":%s}}`, source, sink)
	d, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSecretRefs(t *testing.T) {
	d := docWithConfigs(t,
		`{"url":"https://api.example","headers":{"Authorization":{"$secret":"api_token"}}}`,
		`{"url":"https://sink.example",
		  "auth":[{"$secret":"sink_user"},{"$secret":"sink_pass"}],
		  "dup":{"$secret":"api_token"}}`)

	refs, err := d.SecretRefs()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"api_token", "sink_pass", "sink_user"}
	if len(refs) != len(want) {
		t.Fatalf("refs = %v, want %v", refs, want)
	}
	for i := range want {
		if refs[i] != want[i] {
			t.Fatalf("refs = %v, want %v (sorted, deduped)", refs, want)
		}
	}
}

func TestSecretRefsNoRefs(t *testing.T) {
	d := docWithConfigs(t, `{"url":"https://x"}`, `{"plain":"value"}`)
	refs, err := d.SecretRefs()
	if err != nil || len(refs) != 0 {
		t.Fatalf("refs = %v, %v (want none)", refs, err)
	}
}

// Objects that merely contain a $secret key among others, or whose
// $secret value is not a string, are NOT references.
func TestSecretRefAdjacentShapesUntouched(t *testing.T) {
	d := docWithConfigs(t,
		`{"a":{"$secret":"name","other":"key"},"b":{"$secret":42}}`,
		`{}`)
	refs, err := d.SecretRefs()
	if err != nil || len(refs) != 0 {
		t.Fatalf("refs = %v, %v (want none — not strict one-key string form)", refs, err)
	}
	// And resolution leaves them byte-identical.
	resolved, err := d.ResolveSecrets(func(string) (string, error) {
		t.Fatal("lookup called for a non-ref")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(resolved.Source.Config) != string(d.Source.Config) {
		t.Fatalf("non-ref config rewritten: %s", resolved.Source.Config)
	}
}

func TestSecretRefsRejectsBadNames(t *testing.T) {
	d := docWithConfigs(t, `{"k":{"$secret":"has space"}}`, `{}`)
	if _, err := d.SecretRefs(); err == nil {
		t.Fatal("invalid secret name accepted")
	}
}

func TestResolveSecrets(t *testing.T) {
	d := docWithConfigs(t,
		`{"url":"https://api.example","token":{"$secret":"api_token"}}`,
		`{"nested":{"deep":[{"$secret":"sink_pass"}]},"keep":123}`)

	values := map[string]string{"api_token": "tok-123", "sink_pass": "p@ss"}
	resolved, err := d.ResolveSecrets(func(name string) (string, error) {
		v, ok := values[name]
		if !ok {
			return "", fmt.Errorf("unknown secret %q", name)
		}
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var src struct {
		URL   string `json:"url"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal(resolved.Source.Config, &src); err != nil {
		t.Fatal(err)
	}
	if src.Token != "tok-123" || src.URL != "https://api.example" {
		t.Fatalf("resolved source = %+v", src)
	}
	if !strings.Contains(string(resolved.Sink.Config), `"p@ss"`) ||
		!strings.Contains(string(resolved.Sink.Config), `"keep":123`) {
		t.Fatalf("resolved sink = %s", resolved.Sink.Config)
	}

	// Original untouched.
	if !strings.Contains(string(d.Source.Config), `"$secret"`) {
		t.Fatal("ResolveSecrets mutated the receiver")
	}

	// Lookup failure propagates.
	if _, err := d.ResolveSecrets(func(name string) (string, error) {
		return "", fmt.Errorf("nope: %s", name)
	}); err == nil {
		t.Fatal("lookup error swallowed")
	}
}
