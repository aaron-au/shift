package runners

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
)

// prodLabels are what a production API runner IS; prodSel is what a route
// asks for. They are separate values on purpose — matching is superset, not
// equality, so a runner may legitimately carry labels no route mentions.
var (
	prodLabels    = map[string]string{"environment": "production", "workload": "api", "region": "au"}
	stagingLabels = map[string]string{"environment": "staging", "workload": "api"}
	prodSel       = config.Selector{"environment": "production", "workload": "api"}
	stagingSel    = config.Selector{"environment": "staging"}
)

func req(id string) *Request {
	return &Request{ID: id, Flow: "f", Method: http.MethodPost, Path: "/hook",
		Headers: http.Header{}, Body: strings.NewReader("{}")}
}

// The core loop: a parked runner receives work, answers, and the caller gets
// the answer back.
func TestDispatchHandsWorkToAParkedRunnerAndReturnsItsResponse(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		got := r.Poll(t.Context(), prodLabels, time.Second)
		if got == nil {
			t.Error("runner polled and received nothing")
			return
		}
		r.Deliver(t.Context(), got.ID, &Response{Status: 200, Body: strings.NewReader("ok")})
	}()

	// Let the runner park before dispatching.
	waitFor(t, func() bool { return r.Available(prodSel) == 1 })

	resp, release, err := r.Dispatch(t.Context(), prodSel, req("r1"))
	if err != nil {
		release()
		t.Fatalf("dispatch: %v", err)
	}
	status := resp.Status

	// Release BEFORE joining the runner, not in a defer. Deliver deliberately
	// blocks until the caller is finished with the body — that is what keeps
	// the body alive — so a deferred release here would wait for the runner
	// while the runner waits for the release.
	release()
	wg.Wait()

	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
}

// No runner waiting is a 503, never a queue. A gateway that buffers is a
// gateway with durable state, which is what this component exists to avoid.
func TestDispatchWithNoRunnerFailsImmediately(t *testing.T) {
	r := New()
	start := time.Now()
	_, release, err := r.Dispatch(t.Context(), prodSel, req("r1"))
	defer release()
	if !errors.Is(err, ErrNoRunner) {
		t.Fatalf("err = %v, want ErrNoRunner", err)
	}
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("dispatch waited for a runner; it must fail fast so the caller gets a 503")
	}
}

// Eligibility is selector-scoped: a staging runner must never be handed
// production work (ADR-0030 placement).
func TestDispatchNeverCrossesSelectors(t *testing.T) {
	r := New()
	go r.Poll(t.Context(), stagingLabels, 500*time.Millisecond)
	waitFor(t, func() bool { return r.Available(stagingSel) == 1 })

	_, release, err := r.Dispatch(t.Context(), prodSel, req("r1"))
	release()
	if !errors.Is(err, ErrNoRunner) {
		t.Fatalf("err = %v; a non-prod runner was eligible for prod work", err)
	}
}

// A runner that gives up must stop being available. This is the property that
// removes the liveness table: parked means available, full stop.
func TestPollTimeoutUnparksTheRunner(t *testing.T) {
	r := New()
	if got := r.Poll(t.Context(), prodLabels, 20*time.Millisecond); got != nil {
		t.Fatal("poll returned work that was never dispatched")
	}
	if n := r.Available(prodSel); n != 0 {
		t.Fatalf("available = %d after timeout, want 0 — a departed runner still looked available", n)
	}
}

// The race the unpark path exists for: a request handed over in the same
// instant the poll times out must still reach the runner, not vanish. Losing
// it would leave the caller waiting out the full delivery timeout for a runner
// that had already gone.
func TestWorkHandedOverAtTheInstantOfTimeoutIsNotLost(t *testing.T) {
	for range 200 {
		r := New()
		r.DeliveryTimeout = 250 * time.Millisecond
		got := make(chan *Request, 1)
		go func() { got <- r.Poll(t.Context(), prodLabels, time.Millisecond) }()

		// Dispatch racing the 1ms poll timeout: either it finds no runner, or
		// it finds one — and if it finds one, that runner MUST see the work.
		go func() {
			resp, release, err := r.Dispatch(t.Context(), prodSel, req("r1"))
			defer release()
			_, _ = resp, err
		}()

		select {
		case req := <-got:
			if req != nil {
				r.Deliver(t.Context(), req.ID, &Response{Status: 200, Body: strings.NewReader("")})
			}
		case <-time.After(2 * time.Second):
			t.Fatal("poll never returned")
		}
	}
}

// A runner that dies mid-execution must not hold the caller forever.
func TestDispatchTimesOutWhenTheRunnerNeverDelivers(t *testing.T) {
	r := New()
	r.DeliveryTimeout = 50 * time.Millisecond
	go r.Poll(t.Context(), prodLabels, time.Second) // takes the work, never delivers
	waitFor(t, func() bool { return r.Available(prodSel) == 1 })

	_, release, err := r.Dispatch(t.Context(), prodSel, req("r1"))
	defer release()
	if !errors.Is(err, ErrDeliveryTimeout) {
		t.Fatalf("err = %v, want ErrDeliveryTimeout", err)
	}
}

// Delivering against an abandoned or unknown request must tell the runner to
// stop rather than blocking it forever.
func TestDeliverToAnUnknownRequestReportsFalse(t *testing.T) {
	r := New()
	if r.Deliver(t.Context(), "nope", &Response{Status: 200}) {
		t.Fatal("Deliver reported success for a request nobody is waiting on")
	}
}

// FIFO spreads work without tracking load: a busy runner is not polling, so
// the queue is self-balancing.
func TestDispatchIsFIFOAcrossParkedRunners(t *testing.T) {
	r := New()
	order := make(chan int, 2)
	for i := range 2 {
		go func() {
			if got := r.Poll(t.Context(), prodLabels, 2*time.Second); got != nil {
				order <- i
				r.Deliver(t.Context(), got.ID, &Response{Status: 200, Body: strings.NewReader("")})
			}
		}()
		// Park them in a known order.
		waitFor(t, func() bool { return r.Available(prodSel) == i+1 })
	}

	_, release, err := r.Dispatch(t.Context(), prodSel, req("r1"))
	release()
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	select {
	case first := <-order:
		if first != 0 {
			t.Fatalf("runner %d served first, want the one that parked first", first)
		}
	case <-time.After(time.Second):
		t.Fatal("no runner served the request")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// Parked feeds the health endpoint, and it must count runners ACTUALLY
// holding a poll — a count that outlived its runners would tell the hub the
// gateway can serve traffic it would immediately 503.
func TestParkedCountsOnlyRunnersStillHoldingAPoll(t *testing.T) {
	r := New()
	if got := r.Parked(); got != 0 {
		t.Fatalf("Parked = %d on an empty registry, want 0", got)
	}

	go r.Poll(t.Context(), prodLabels, 500*time.Millisecond)
	waitFor(t, func() bool { return r.Parked() == 1 })

	// Once the poll window closes the runner must stop being counted.
	waitFor(t, func() bool { return r.Parked() == 0 })
}

// Superset matching is the whole point of selectors over a group name: a
// runner carrying extra labels still matches, and a selector naming a label
// the runner lacks does not.
func TestSelectorMatchesOnSupersetNotEquality(t *testing.T) {
	r := New()
	go r.Poll(t.Context(), prodLabels, 2*time.Second) // has an extra "region"
	waitFor(t, func() bool { return r.Parked() == 1 })

	// A narrower selector matches the broader runner.
	if n := r.Available(config.Selector{"environment": "production"}); n != 1 {
		t.Errorf("Available(environment=production) = %d, want 1", n)
	}
	// The empty selector means "any runner".
	if n := r.Available(nil); n != 1 {
		t.Errorf("Available(nil) = %d, want 1", n)
	}
	// A label the runner does not carry excludes it, even though every other
	// label matches.
	if n := r.Available(config.Selector{"environment": "production", "tier": "gold"}); n != 0 {
		t.Errorf("Available(+tier=gold) = %d, want 0", n)
	}
	// A label it carries with a DIFFERENT value excludes it too.
	if n := r.Available(config.Selector{"region": "eu"}); n != 0 {
		t.Errorf("Available(region=eu) = %d, want 0", n)
	}
}

// A runner delivering twice — a retry, or a confused client — must be told to
// stop on the second attempt rather than blocking on a caller who has already
// been answered and gone.
func TestSecondDeliveryForTheSameRequestIsRejected(t *testing.T) {
	r := New()
	r.DeliveryTimeout = time.Second
	first := make(chan *Request, 1)
	go func() { first <- r.Poll(t.Context(), prodLabels, 2*time.Second) }()
	waitFor(t, func() bool { return r.Available(prodSel) == 1 })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, release, err := r.Dispatch(t.Context(), prodSel, req("r1"))
		release()
		if err != nil {
			t.Errorf("dispatch: %v", err)
		}
	}()

	got := <-first
	if got == nil {
		t.Fatal("runner received nothing")
	}
	if !r.Deliver(t.Context(), got.ID, &Response{Status: 200, Body: strings.NewReader("a")}) {
		t.Fatal("first delivery was rejected")
	}
	<-done

	// The caller is gone; the exchange is finished.
	if r.Deliver(t.Context(), got.ID, &Response{Status: 200, Body: strings.NewReader("b")}) {
		t.Fatal("a second delivery was accepted after the caller had been answered")
	}
}

// Deliver blocks until the caller has consumed the body, because that body is
// a live stream from the runner's own request. If the runner's context ends
// first it must stop waiting rather than hanging.
func TestDeliverStopsWaitingWhenTheRunnerGivesUp(t *testing.T) {
	r := New()
	r.DeliveryTimeout = 2 * time.Second
	parked := make(chan *Request, 1)
	go func() { parked <- r.Poll(t.Context(), prodLabels, 2*time.Second) }()
	waitFor(t, func() bool { return r.Available(prodSel) == 1 })

	// Dispatch, but never read the response: the caller is slow/absent.
	go func() { _, release, _ := r.Dispatch(t.Context(), prodSel, req("r1")); release() }()
	got := <-parked
	if got == nil {
		t.Fatal("runner received nothing")
	}

	// Deliver with an already-cancelled context returns rather than blocking.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan bool, 1)
	go func() { done <- r.Deliver(ctx, got.ID, &Response{Status: 200, Body: strings.NewReader("x")}) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Deliver reported success despite its context ending")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver hung after its context ended")
	}
}
