package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/runner/internal/auth"
	"github.com/aaron-au/shift/runner/internal/secretref"
	"github.com/aaron-au/shift/runner/internal/task"
	"github.com/aaron-au/shift/runner/internal/webhook"
)

// Secret resolution used to live only in the lease loop, so the three
// runner-direct paths (webhook, execute, run) shipped `{"$secret":…}`
// straight to the connector — a reference object where a value belongs.
// These tests pin all three to the shared resolver (ADR-0010, ADR-0035).

const flowWithRef = `{"name":"f",
  "source":{"connector":"gen","action":"gen","config":{"records":1,"auth":{"$secret":"api-key"}}},
  "sink":{"connector":"gen","action":"discard"}}`

// recordingResolver reports which names each path asked for.
func recordingResolver(t *testing.T, got *[]string, mu *sync.Mutex) *secretref.Resolver {
	t.Helper()
	return secretref.New(func(_ context.Context, names []string) (map[string]string, error) {
		mu.Lock()
		*got = append(*got, names...)
		mu.Unlock()
		return map[string]string{"api-key": "s3cret"}, nil
	})
}

func TestDirectPathsResolveSecrets(t *testing.T) {
	for _, tc := range []struct {
		name, method, target, body string
		register                   bool
	}{
		{name: "execute", method: http.MethodPost, target: "/api/flows/execute", body: flowWithRef},
		{name: "run", method: http.MethodPost, target: "/api/flows/run", body: flowWithRef},
		{name: "webhook", method: http.MethodPost, target: "/hooks/ingest", body: `{"a":1}`, register: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var asked []string
			reg := webhook.NewRegistry()
			h := Handler(newSvc(t), Options{
				RunnerName: "r", Version: "0", Started: time.Now(),
				Guard: auth.NewGuard(nil), Hooks: reg,
				Secrets: recordingResolver(t, &asked, &mu),
			})
			if tc.register {
				hookDoc := `{"document":{"name":"hook",` +
					`"source":{"connector":"@webhook","action":"ndjson","config":{"auth":{"$secret":"api-key"}}},` +
					`"sink":{"connector":"gen","action":"discard"}}}`
				if rec := serve(h, req(http.MethodPut, "/api/webhooks/ingest", hookDoc)); rec.Code != http.StatusOK {
					t.Fatalf("register hook = %d", rec.Code)
				}
			}
			serve(h, req(tc.method, tc.target, tc.body))

			mu.Lock()
			defer mu.Unlock()
			if len(asked) == 0 {
				t.Fatal("path never resolved its secret references")
			}
			if asked[0] != "api-key" {
				t.Fatalf("asked for %v, want [api-key]", asked)
			}
		})
	}
}

// A hub-less runner must fail a secret-using flow with a clear error
// rather than hand the connector a reference object.
func TestDirectPathsFailWithoutAResolver(t *testing.T) {
	h := Handler(newSvc(t), Options{
		RunnerName: "r", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: webhook.NewRegistry(),
		Secrets: secretref.New(nil),
	})
	for _, target := range []string{"/api/flows/execute", "/api/flows/run"} {
		rec := serve(h, req(http.MethodPost, target, flowWithRef))
		if rec.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s = %d, want 422", target, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "not attached to a hub") {
			t.Errorf("%s body = %s, want it to name the real cause", target, rec.Body)
		}
	}
}

// A nil resolver must not panic the handler: a secret-free flow still runs
// on a runner configured without one.
func TestDirectPathsTolerateNilResolver(t *testing.T) {
	h := Handler(newSvc(t), Options{
		RunnerName: "r", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: webhook.NewRegistry(),
	})
	plain := `{"name":"f","source":{"connector":"gen","action":"gen","config":{"records":1}},
	  "sink":{"connector":"gen","action":"discard"}}`
	if rec := serve(h, req(http.MethodPost, "/api/flows/execute", plain)); rec.Code != http.StatusAccepted {
		t.Fatalf("execute = %d, want 202", rec.Code)
	}
}

// The synchronous path is request-reply: the caller is waiting and the work
// is already done, so reporting metadata to the hub must not sit on the
// response. Inline reporting added a full hub round trip to every call —
// on a remote hub, the dominant cost of the request (ADR-0035 §3).
func TestSyncRunDoesNotBlockOnTheHubReport(t *testing.T) {
	release := make(chan struct{})
	reported := make(chan struct{})
	report := func(task.Task, string) {
		<-release // a hub that is slow, or unreachable
		close(reported)
	}
	h := Handler(newSvc(t), Options{
		RunnerName: "r", Version: "0", Started: time.Now(),
		Guard: auth.NewGuard(nil), Hooks: webhook.NewRegistry(), Report: report,
	})

	done := make(chan int, 1)
	go func() {
		plain := `{"name":"f","source":{"connector":"gen","action":"gen","config":{"records":1}},
		  "sink":{"connector":"gen","action":"discard"}}`
		done <- serve(h, req(http.MethodPost, "/api/flows/run", plain)).Code
	}()

	select {
	case <-done: // responded while the reporter is still blocked — correct
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("synchronous run blocked on the hub execution report")
	}
	close(release)
	select {
	case <-reported:
	case <-time.After(10 * time.Second):
		t.Fatal("the report never ran; it must still be delivered, just off the response path")
	}
}

func TestNoResolverErrorIsTyped(t *testing.T) {
	if !errors.Is(secretref.ErrNoResolver, secretref.ErrNoResolver) {
		t.Fatal("ErrNoResolver must be comparable for callers branching on it")
	}
}
