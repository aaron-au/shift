package bind_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/bind"
)

func parse(t *testing.T, doc string) *flowdoc.Document {
	t.Helper()
	d, err := flowdoc.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return d
}

const flowWithRef = `{"name":"f",
  "source":{"connector":"gen","action":"gen","config":{"records":1,"auth":{"$secret":"api-key"}}},
  "sink":{"connector":"gen","action":"discard"}}`

const plainFlow = `{"name":"f",
  "source":{"connector":"gen","action":"gen","config":{"records":1}},
  "sink":{"connector":"gen","action":"discard"}}`

const flowWithConnection = `{"name":"f",
  "source":{"connector":"sftp","action":"get","connection":"prod-sftp","config":{"path":"/in"}},
  "sink":{"connector":"gen","action":"discard"}}`

// secretsOnly is a Fetch that serves secrets and no connections.
func secretsOnly(values map[string]string) bind.Fetch {
	return func(context.Context, []string, []string) (map[string]bind.Connection, map[string]string, error) {
		return nil, values, nil
	}
}

// connOnly is a Fetch that serves one connection and no secrets.
func connOnly(name, connector, config string) bind.Fetch {
	return func(context.Context, []string, []string) (map[string]bind.Connection, map[string]string, error) {
		return map[string]bind.Connection{
			name: {Connector: connector, Config: json.RawMessage(config)},
		}, nil, nil
	}
}

func TestApplySubstitutesAndReportsValues(t *testing.T) {
	b := bind.New(func(_ context.Context, conns, secrets []string) (map[string]bind.Connection, map[string]string, error) {
		if len(conns) != 0 {
			t.Errorf("asked for connections %v, want none", conns)
		}
		if len(secrets) != 1 || secrets[0] != "api-key" {
			t.Errorf("asked for secrets %v, want [api-key]", secrets)
		}
		return nil, map[string]string{"api-key": "s3cret"}, nil
	})
	doc, values, err := b.Apply(t.Context(), parse(t, flowWithRef))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !strings.Contains(string(doc.Source.Config), "s3cret") {
		t.Fatalf("config = %s, want the reference replaced", doc.Source.Config)
	}
	// The values must come back so the service can redact them; without
	// this a leaked secret would reach a result or a capture sample.
	if len(values) != 1 || values[0] != "s3cret" {
		t.Fatalf("values = %v, want the resolved plaintext for redaction", values)
	}
}

// A document needing nothing must not cost a hub round trip — that is what
// keeps an uncached binder viable on high-frequency triggers.
func TestApplySkipsFetchWhenNothingIsReferenced(t *testing.T) {
	called := false
	b := bind.New(func(context.Context, []string, []string) (map[string]bind.Connection, map[string]string, error) {
		called = true
		return nil, nil, nil
	})
	doc, values, err := b.Apply(t.Context(), parse(t, plainFlow))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if called {
		t.Error("a document with no refs still called the hub")
	}
	if values != nil || doc == nil {
		t.Fatalf("values = %v, want none", values)
	}
}

// The standalone-runner case. Passing a reference through would hand a
// connector `{"$secret":…}` where a value belongs, and the failure would
// then describe a bad password rather than a missing hub.
func TestApplyWithoutFetchFailsLoudly(t *testing.T) {
	for _, doc := range []string{flowWithRef, flowWithConnection} {
		for _, b := range []*bind.Binder{bind.New(nil), nil} {
			_, _, err := b.Apply(t.Context(), parse(t, doc))
			if !errors.Is(err, bind.ErrNoResolver) {
				t.Fatalf("err = %v, want ErrNoResolver", err)
			}
		}
	}
	// ...but a document that needs nothing still runs on a hub-less runner.
	if _, _, err := bind.New(nil).Apply(t.Context(), parse(t, plainFlow)); err != nil {
		t.Fatalf("a reference-free document must run standalone: %v", err)
	}
}

func TestApplyPropagatesFetchError(t *testing.T) {
	want := errors.New("hub unreachable")
	b := bind.New(func(context.Context, []string, []string) (map[string]bind.Connection, map[string]string, error) {
		return nil, nil, want
	})
	if _, _, err := b.Apply(t.Context(), parse(t, flowWithRef)); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// A hub that omits a requested name must fail the task, not leave the
// reference in place for a connector to choke on.
func TestApplyFailsOnMissingName(t *testing.T) {
	_, _, err := bind.New(secretsOnly(map[string]string{"other": "v"})).
		Apply(t.Context(), parse(t, flowWithRef))
	if err == nil {
		t.Fatal("a missing secret was accepted")
	}
	if !strings.Contains(err.Error(), "api-key") {
		t.Fatalf("err = %v, want it to name the missing secret", err)
	}
}

// Errors travel to task results and logs, so they must carry names only.
func TestApplyErrorsCarryNoPlaintext(t *testing.T) {
	_, _, err := bind.New(secretsOnly(map[string]string{"wrong-name": "SUPERSECRET"})).
		Apply(t.Context(), parse(t, flowWithRef))
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "SUPERSECRET") {
		t.Fatalf("error leaked a secret value: %v", err)
	}
}

// --- connections (ADR-0034) ------------------------------------------------

func TestApplyMergesConnectionIntoNodeConfig(t *testing.T) {
	b := bind.New(func(_ context.Context, conns, _ []string) (map[string]bind.Connection, map[string]string, error) {
		if len(conns) != 1 || conns[0] != "prod-sftp" {
			t.Errorf("asked for connections %v, want [prod-sftp]", conns)
		}
		return map[string]bind.Connection{"prod-sftp": {
			Connector: "sftp",
			Config:    json.RawMessage(`{"host":"sftp.example.com","port":22}`),
		}}, nil, nil
	})
	doc, _, err := b.Apply(t.Context(), parse(t, flowWithConnection))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(doc.Source.Config, &cfg); err != nil {
		t.Fatalf("merged config is not valid JSON: %v", err)
	}
	if cfg["host"] != "sftp.example.com" || cfg["path"] != "/in" || cfg["port"] != float64(22) {
		t.Fatalf("merged = %v, want connection and node config combined", cfg)
	}
}

// A connection's own {"$secret":…} refs must be substituted too. The runner
// cannot name them until it has the connection, which is why the fetch is
// one call and the merge happens before substitution.
func TestApplyResolvesSecretsInsideAConnection(t *testing.T) {
	b := bind.New(func(context.Context, []string, []string) (map[string]bind.Connection, map[string]string, error) {
		return map[string]bind.Connection{"prod-sftp": {
				Connector: "sftp",
				Config:    json.RawMessage(`{"host":"h","password":{"$secret":"sftp-pw"}}`),
			}},
			map[string]string{"sftp-pw": "hunter2"}, nil
	})
	doc, values, err := b.Apply(t.Context(), parse(t, flowWithConnection))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	var cfg struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(doc.Source.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Password != "hunter2" {
		t.Fatalf("password = %q, want the connection's reference resolved", cfg.Password)
	}
	if len(values) != 1 || values[0] != "hunter2" {
		t.Fatalf("values = %v; a connection's secret must reach the redactor too", values)
	}
}

// Connections are not versioned (ADR-0034 open question 3), so one can be
// re-pointed at a different connector after the deploy-time check passed.
// That mismatch is reachable at run time and must fail.
func TestApplyRejectsConnectorMismatchAtRunTime(t *testing.T) {
	_, _, err := bind.New(connOnly("prod-sftp", "http", `{"url":"http://x"}`)).
		Apply(t.Context(), parse(t, flowWithConnection))
	if err == nil {
		t.Fatal("a connector mismatch was accepted")
	}
	if !strings.Contains(err.Error(), "sftp") || !strings.Contains(err.Error(), "http") {
		t.Fatalf("err = %v, want both connectors named", err)
	}
}

// ADR-0034 §3: a node may not restate what its connection supplies.
func TestApplyRejectsNodeOverridingConnectionKey(t *testing.T) {
	doc := `{"name":"f",
	  "source":{"connector":"sftp","action":"get","connection":"prod-sftp",
	    "config":{"host":"other.example.com"}},
	  "sink":{"connector":"gen","action":"discard"}}`
	_, _, err := bind.New(connOnly("prod-sftp", "sftp", `{"host":"sftp.example.com"}`)).
		Apply(t.Context(), parse(t, doc))
	var collision *flowdoc.ConnectionCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("err = %v, want ConnectionCollisionError", err)
	}
}

func TestApplyFailsWhenHubOmitsAConnection(t *testing.T) {
	b := bind.New(func(context.Context, []string, []string) (map[string]bind.Connection, map[string]string, error) {
		return map[string]bind.Connection{}, nil, nil
	})
	_, _, err := b.Apply(t.Context(), parse(t, flowWithConnection))
	if err == nil {
		t.Fatal("a missing connection was accepted")
	}
	if !strings.Contains(err.Error(), "prod-sftp") {
		t.Fatalf("err = %v, want it to name the missing connection", err)
	}
}

// Graph-form documents bind through Step, not Endpoint, so the merged
// config must land on the right step.
func TestApplyMergesOntoGraphSteps(t *testing.T) {
	doc := `{"name":"g","start":"in","steps":[
	  {"id":"in","type":"source","connector":"sftp","action":"get","connection":"prod-sftp",
	   "config":{"path":"/in"},"onSuccess":"out"},
	  {"id":"out","type":"sink","connector":"@discard"}]}`
	out, _, err := bind.New(connOnly("prod-sftp", "sftp", `{"host":"h"}`)).
		Apply(t.Context(), parse(t, doc))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(out.Steps[0].Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["host"] != "h" || cfg["path"] != "/in" {
		t.Fatalf("step config = %v, want the merge applied to step in", cfg)
	}
}

// Apply must not mutate the caller's document: the webhook registry shares
// one parsed document across every concurrent invocation of a hook.
func TestApplyDoesNotMutateTheInput(t *testing.T) {
	in := parse(t, flowWithConnection)
	before := string(in.Source.Config)
	if _, _, err := bind.New(connOnly("prod-sftp", "sftp", `{"host":"h"}`)).Apply(t.Context(), in); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := string(in.Source.Config); got != before {
		t.Fatalf("input mutated: %s -> %s", before, got)
	}
}
