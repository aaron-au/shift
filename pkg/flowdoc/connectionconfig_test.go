package flowdoc_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

// ConnectionUses carries what a validator needs per node — Connections()
// alone cannot answer "does this node's connector match the connection it
// names" or "does its config collide".
func TestConnectionUsesCarriesConnectorAndConfig(t *testing.T) {
	d, err := flowdoc.Parse([]byte(`{"name":"f",
	  "source":{"connector":"sftp","action":"get","connection":"prod-sftp","config":{"path":"/in"}},
	  "sink":{"connector":"@discard"}}`))
	if err != nil {
		t.Fatal(err)
	}
	uses := d.ConnectionUses()
	if len(uses) != 1 {
		t.Fatalf("uses = %+v, want only the node that names a connection", uses)
	}
	u := uses[0]
	if u.Label != "source" || u.Connector != "sftp" || u.Connection != "prod-sftp" {
		t.Fatalf("use = %+v, want the source's connector and connection", u)
	}
	if string(u.Config) == "" {
		t.Fatal("use carries no config; the collision check has nothing to compare")
	}
}

func TestConnectionUsesCoversGraphSteps(t *testing.T) {
	d, err := flowdoc.Parse([]byte(`{"name":"g","start":"in","steps":[
	  {"id":"in","type":"source","connector":"sftp","action":"get","connection":"c","onSuccess":"out"},
	  {"id":"out","type":"sink","connector":"@discard"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	uses := d.ConnectionUses()
	if len(uses) != 1 || uses[0].Label != "step in" || uses[0].Connection != "c" {
		t.Fatalf("uses = %+v, want [step in -> c]", uses)
	}
}

func TestConfigSecretRefsFindsNestedRefs(t *testing.T) {
	cfg := json.RawMessage(`{
		"host": "sftp.example.com",
		"auth": {"password": {"$secret": "sftp-pw"}},
		"keys": [{"$secret": "sftp-key"}, {"$secret": "sftp-pw"}]
	}`)
	got, err := flowdoc.ConfigSecretRefs(cfg)
	if err != nil {
		t.Fatalf("ConfigSecretRefs: %v", err)
	}
	want := []string{"sftp-key", "sftp-pw"}
	if len(got) != len(want) {
		t.Fatalf("refs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("refs = %v, want %v (sorted, de-duplicated)", got, want)
		}
	}
}

func TestConfigSecretRefsRejectsInvalidName(t *testing.T) {
	_, err := flowdoc.ConfigSecretRefs(json.RawMessage(`{"pw": {"$secret": "bad name!"}}`))
	if err == nil {
		t.Fatal("accepted a secret name outside SecretNameRE")
	}
}

func TestConfigSecretRefsEmptyConfig(t *testing.T) {
	got, err := flowdoc.ConfigSecretRefs(nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("ConfigSecretRefs(nil) = %v, %v; want no refs, no error", got, err)
	}
}

func TestResolveConfigSecretsReplacesRefInPlace(t *testing.T) {
	cfg := json.RawMessage(`{"host":"h","auth":{"password":{"$secret":"pw"}}}`)
	out, err := flowdoc.ResolveConfigSecrets(cfg, func(name string) (string, error) {
		if name != "pw" {
			t.Errorf("looked up %q, want pw", name)
		}
		return "s3cret", nil
	})
	if err != nil {
		t.Fatalf("ResolveConfigSecrets: %v", err)
	}
	var got struct {
		Host string `json:"host"`
		Auth struct {
			Password string `json:"password"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("resolved config is not valid JSON: %v", err)
	}
	if got.Auth.Password != "s3cret" || got.Host != "h" {
		t.Fatalf("resolved = %+v, want the ref replaced and host untouched", got)
	}
	// The caller's copy must stay inert: the hub-side document is the one
	// that gets stored and logged, so plaintext must not travel back into it.
	if strings.Contains(string(cfg), "s3cret") {
		t.Fatal("ResolveConfigSecrets mutated the input config")
	}
}

func TestResolveConfigSecretsPropagatesLookupError(t *testing.T) {
	want := errors.New("no such secret")
	_, err := flowdoc.ResolveConfigSecrets(json.RawMessage(`{"pw":{"$secret":"nope"}}`),
		func(string) (string, error) { return "", want })
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestMergeConnectionConfigUnionsDisjointKeys(t *testing.T) {
	merged, err := flowdoc.MergeConnectionConfig(
		json.RawMessage(`{"host":"sftp.example.com","port":22}`),
		json.RawMessage(`{"path":"/in/orders.csv"}`))
	if err != nil {
		t.Fatalf("MergeConnectionConfig: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(merged, &got); err != nil {
		t.Fatalf("merged config is not valid JSON: %v", err)
	}
	if got["host"] != "sftp.example.com" || got["path"] != "/in/orders.csv" || got["port"] != float64(22) {
		t.Fatalf("merged = %v, want the union of both documents", got)
	}
}

// The rule that gives connections their value: a node cannot quietly point
// somewhere its siblings do not (ADR-0034 §3).
func TestMergeConnectionConfigRejectsOverride(t *testing.T) {
	_, err := flowdoc.MergeConnectionConfig(
		json.RawMessage(`{"host":"prod.example.com","port":22}`),
		json.RawMessage(`{"host":"staging.example.com","path":"/in"}`))
	var collision *flowdoc.ConnectionCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("err = %v, want ConnectionCollisionError", err)
	}
	if len(collision.Keys) != 1 || collision.Keys[0] != "host" {
		t.Fatalf("Keys = %v, want [host]", collision.Keys)
	}
	if !strings.Contains(collision.Error(), `"host"`) {
		t.Fatalf("error %q does not name the offending key", collision)
	}
}

func TestMergeConnectionConfigReportsEveryCollisionSorted(t *testing.T) {
	_, err := flowdoc.MergeConnectionConfig(
		json.RawMessage(`{"host":"h","port":22,"user":"u"}`),
		json.RawMessage(`{"user":"other","host":"other","path":"/in"}`))
	var collision *flowdoc.ConnectionCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("err = %v, want ConnectionCollisionError", err)
	}
	// Reported together: fixing one key per deploy cycle is the slow path a
	// batched error avoids.
	if len(collision.Keys) != 2 || collision.Keys[0] != "host" || collision.Keys[1] != "user" {
		t.Fatalf("Keys = %v, want [host user]", collision.Keys)
	}
}

// Top-level comparison only, by design: a nested override would slip past a
// recursive merge without ever registering as a collision.
func TestMergeConnectionConfigNestedKeyIsNotACollision(t *testing.T) {
	merged, err := flowdoc.MergeConnectionConfig(
		json.RawMessage(`{"tls":{"insecure":false}}`),
		json.RawMessage(`{"options":{"insecure":true}}`))
	if err != nil {
		t.Fatalf("MergeConnectionConfig: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(merged, &got); err != nil {
		t.Fatalf("merged config is not valid JSON: %v", err)
	}
	if string(got["tls"]) != `{"insecure":false}` {
		t.Fatalf("tls = %s, want the connection's value untouched", got["tls"])
	}
}

func TestMergeConnectionConfigEmptyConnectionReturnsNodeUnchanged(t *testing.T) {
	node := json.RawMessage(`{"path":"/in"}`)
	for _, conn := range []json.RawMessage{nil, json.RawMessage(`{}`)} {
		merged, err := flowdoc.MergeConnectionConfig(conn, node)
		if err != nil {
			t.Fatalf("MergeConnectionConfig(%s): %v", conn, err)
		}
		var got map[string]any
		if err := json.Unmarshal(merged, &got); err != nil {
			t.Fatalf("merged config is not valid JSON: %v", err)
		}
		if got["path"] != "/in" || len(got) != 1 {
			t.Fatalf("merged = %v, want the node config alone", got)
		}
	}
}

func TestMergeConnectionConfigEmptyNode(t *testing.T) {
	merged, err := flowdoc.MergeConnectionConfig(json.RawMessage(`{"host":"h"}`), nil)
	if err != nil {
		t.Fatalf("MergeConnectionConfig: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(merged, &got); err != nil {
		t.Fatalf("merged config is not valid JSON: %v", err)
	}
	if got["host"] != "h" || len(got) != 1 {
		t.Fatalf("merged = %v, want the connection config alone", got)
	}
}

func TestMergeConnectionConfigRejectsNonObject(t *testing.T) {
	for _, tc := range []struct{ name, conn, node string }{
		{"connection is an array", `[1,2]`, `{"path":"/in"}`},
		{"node is a string", `{"host":"h"}`, `"just a string"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := flowdoc.MergeConnectionConfig(
				json.RawMessage(tc.conn), json.RawMessage(tc.node)); err == nil {
				t.Fatal("accepted a config document that is not a JSON object")
			}
		})
	}
}
