package gwclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/service"
)

// stubGateway answers every poll with an empty window and counts them. The
// count is the only thing these tests care about: whether this runner is
// currently parked against this gateway at all.
type stubGateway struct {
	url   string
	polls atomic.Int64
}

func newStubGateway(t *testing.T) *stubGateway {
	t.Helper()
	g := &stubGateway{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pollPath {
			g.polls.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	g.url = srv.URL
	return g
}

// polled waits for the runner to poll this gateway.
func (g *stubGateway) polled(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if g.polls.Load() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("gateway %s was never polled", g.url)
}

// quiet asserts the runner has STOPPED polling: a withdrawn gateway that kept
// being polled would still be able to hand this runner work.
func (g *stubGateway) quiet(t *testing.T) {
	t.Helper()
	// Settle first — a poll already in flight when the gateway was withdrawn
	// may still land, and that is not the thing being asserted.
	time.Sleep(200 * time.Millisecond)
	at := g.polls.Load()
	time.Sleep(300 * time.Millisecond)
	if got := g.polls.Load(); got != at {
		t.Fatalf("gateway %s was polled %d more times after being withdrawn", g.url, got-at)
	}
}

func runLoop(t *testing.T, opts Options) {
	t.Helper()
	opts.Service = service.New(service.Options{})
	opts.Lookup = func(string) (*flowdoc.Document, bool) { return nil, false }
	opts.PollWait = 20 * time.Millisecond
	opts.PollConcurrency = 1
	if opts.DiscoverEvery == 0 {
		opts.DiscoverEvery = 10 * time.Millisecond
	}
	l := New(opts)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() { l.Run(ctx); close(stopped) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("the intake loop did not stop with its context")
		}
	})
}

// The address list is live (ADR-0038 §4). A gateway added at the hub is polled
// without touching the runner, and one removed stops being polled — which is
// the point of asking the hub rather than baking the list into a deployment.
func TestDiscoveredGatewaysAreAddedAndWithdrawn(t *testing.T) {
	a, b := newStubGateway(t), newStubGateway(t)

	var list atomic.Pointer[[]Gateway]
	set := func(urls ...string) {
		gws := make([]Gateway, 0, len(urls))
		for i, u := range urls {
			gws = append(gws, Gateway{URL: u, ID: fmt.Sprintf("gw-%d", i)})
		}
		list.Store(&gws)
	}
	set(a.url)

	runLoop(t, Options{
		Discover: func(context.Context) ([]Gateway, error) { return *list.Load(), nil },
	})

	a.polled(t)

	set(b.url)
	b.polled(t)
	a.quiet(t)
}

// A hub that cannot be reached says nothing about which gateways exist. If a
// failed discovery pass were read as "no gateways", every inbound request
// would stop being served the moment the CONTROL plane hiccupped — exactly the
// coupling the two-plane split exists to prevent.
func TestDiscoveryFailureKeepsTheCurrentGateways(t *testing.T) {
	a := newStubGateway(t)

	var fail atomic.Bool
	runLoop(t, Options{
		Discover: func(context.Context) ([]Gateway, error) {
			if fail.Load() {
				return nil, errors.New("hub unreachable")
			}
			return []Gateway{{URL: a.url, ID: "gw-a"}}, nil
		},
	})

	a.polled(t)
	fail.Store(true)

	// Still being polled a good few discovery intervals later.
	time.Sleep(200 * time.Millisecond)
	at := a.polls.Load()
	time.Sleep(300 * time.Millisecond)
	if a.polls.Load() <= at {
		t.Fatal("the runner stopped polling its gateway because the hub was unreachable")
	}
}

// A statically configured gateway was set locally, so no remote answer may
// turn it off. Otherwise a hub misconfiguration could silently disable the
// operator's own explicit setting.
func TestAStaticGatewayIsNeverWithdrawn(t *testing.T) {
	a := newStubGateway(t)

	runLoop(t, Options{
		Addrs: []string{a.url + "/"}, // trailing slash: normalised, not a second gateway
		// The hub knows nothing about it.
		Discover: func(context.Context) ([]Gateway, error) { return nil, nil },
	})

	a.polled(t)
	time.Sleep(200 * time.Millisecond)
	at := a.polls.Load()
	time.Sleep(300 * time.Millisecond)
	if a.polls.Load() <= at {
		t.Fatal("a statically configured gateway was withdrawn by the hub's answer")
	}
}
