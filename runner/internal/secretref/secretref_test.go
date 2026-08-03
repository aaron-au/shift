package secretref_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/secretref"
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

func TestApplySubstitutesAndReportsValues(t *testing.T) {
	r := secretref.New(func(_ context.Context, names []string) (map[string]string, error) {
		if len(names) != 1 || names[0] != "api-key" {
			t.Errorf("fetched %v, want [api-key]", names)
		}
		return map[string]string{"api-key": "s3cret"}, nil
	})
	doc, values, err := r.Apply(t.Context(), parse(t, flowWithRef))
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

// A document with no references must not cost a hub round trip — that is
// what keeps an uncached resolver viable on high-frequency triggers.
func TestApplySkipsFetchWhenNoRefs(t *testing.T) {
	called := false
	r := secretref.New(func(context.Context, []string) (map[string]string, error) {
		called = true
		return nil, nil
	})
	doc, values, err := r.Apply(t.Context(), parse(t, plainFlow))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if called {
		t.Error("a document with no secret refs still called the hub")
	}
	if values != nil || doc == nil {
		t.Fatalf("values = %v, want none", values)
	}
}

// The standalone-runner case. Passing the reference through would hand a
// connector `{"$secret":…}` where a value belongs, and the failure would
// then describe a bad password rather than a missing hub.
func TestApplyWithoutFetchFailsLoudly(t *testing.T) {
	for _, r := range []*secretref.Resolver{secretref.New(nil), nil} {
		_, _, err := r.Apply(t.Context(), parse(t, flowWithRef))
		if !errors.Is(err, secretref.ErrNoResolver) {
			t.Fatalf("err = %v, want ErrNoResolver", err)
		}
	}
	// ...but a document that needs nothing still runs on a hub-less runner.
	if _, _, err := secretref.New(nil).Apply(t.Context(), parse(t, plainFlow)); err != nil {
		t.Fatalf("a secret-free document must run standalone: %v", err)
	}
}

func TestApplyPropagatesFetchError(t *testing.T) {
	want := errors.New("hub unreachable")
	r := secretref.New(func(context.Context, []string) (map[string]string, error) {
		return nil, want
	})
	if _, _, err := r.Apply(t.Context(), parse(t, flowWithRef)); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// A hub that omits a requested name must fail the task, not leave the
// reference in place for a connector to choke on.
func TestApplyFailsOnMissingName(t *testing.T) {
	r := secretref.New(func(context.Context, []string) (map[string]string, error) {
		return map[string]string{"other": "v"}, nil
	})
	_, _, err := r.Apply(t.Context(), parse(t, flowWithRef))
	if err == nil {
		t.Fatal("a missing secret was accepted")
	}
	if !strings.Contains(err.Error(), "api-key") {
		t.Fatalf("err = %v, want it to name the missing secret", err)
	}
}

// Errors travel to task results and logs, so they must carry names only.
func TestApplyErrorsCarryNoPlaintext(t *testing.T) {
	r := secretref.New(func(context.Context, []string) (map[string]string, error) {
		return map[string]string{"wrong-name": "SUPERSECRET"}, nil
	})
	_, _, err := r.Apply(t.Context(), parse(t, flowWithRef))
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "SUPERSECRET") {
		t.Fatalf("error leaked a secret value: %v", err)
	}
}
