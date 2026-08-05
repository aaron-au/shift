package runners

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
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
		got := r.Poll(t.Context(), "prod", time.Second)
		if got == nil {
			t.Error("runner polled and received nothing")
			return
		}
		r.Deliver(t.Context(), got.ID, &Response{Status: 200, Body: strings.NewReader("ok")})
	}()

	// Let the runner park before dispatching.
	waitFor(t, func() bool { return r.Available("prod") == 1 })

	resp, err := r.Dispatch(t.Context(), "prod", req("r1"))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	wg.Wait()
}

// No runner waiting is a 503, never a queue. A gateway that buffers is a
// gateway with durable state, which is what this component exists to avoid.
func TestDispatchWithNoRunnerFailsImmediately(t *testing.T) {
	r := New()
	start := time.Now()
	_, err := r.Dispatch(t.Context(), "prod", req("r1"))
	if !errors.Is(err, ErrNoRunner) {
		t.Fatalf("err = %v, want ErrNoRunner", err)
	}
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("dispatch waited for a runner; it must fail fast so the caller gets a 503")
	}
}

// Availability is group-scoped: a runner serving non-prod must never be handed
// prod work (ADR-0030 placement).
func TestDispatchNeverCrossesGroups(t *testing.T) {
	r := New()
	go r.Poll(t.Context(), "non-prod", 500*time.Millisecond)
	waitFor(t, func() bool { return r.Available("non-prod") == 1 })

	if _, err := r.Dispatch(t.Context(), "prod", req("r1")); !errors.Is(err, ErrNoRunner) {
		t.Fatalf("err = %v; a non-prod runner was eligible for prod work", err)
	}
}

// A runner that gives up must stop being available. This is the property that
// removes the liveness table: parked means available, full stop.
func TestPollTimeoutUnparksTheRunner(t *testing.T) {
	r := New()
	if got := r.Poll(t.Context(), "prod", 20*time.Millisecond); got != nil {
		t.Fatal("poll returned work that was never dispatched")
	}
	if n := r.Available("prod"); n != 0 {
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
		go func() { got <- r.Poll(t.Context(), "prod", time.Millisecond) }()

		// Dispatch racing the 1ms poll timeout: either it finds no runner, or
		// it finds one — and if it finds one, that runner MUST see the work.
		go func() {
			resp, err := r.Dispatch(t.Context(), "prod", req("r1"))
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
	go r.Poll(t.Context(), "prod", time.Second) // takes the work, never delivers
	waitFor(t, func() bool { return r.Available("prod") == 1 })

	_, err := r.Dispatch(t.Context(), "prod", req("r1"))
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
			if got := r.Poll(t.Context(), "prod", 2*time.Second); got != nil {
				order <- i
				r.Deliver(t.Context(), got.ID, &Response{Status: 200, Body: strings.NewReader("")})
			}
		}()
		// Park them in a known order.
		waitFor(t, func() bool { return r.Available("prod") == i+1 })
	}

	if _, err := r.Dispatch(t.Context(), "prod", req("r1")); err != nil {
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

// Groups feeds the health endpoint, and it must report only groups with a
// runner actually parked — a group listed with nobody waiting would tell the
// hub the gateway can serve traffic it would immediately 503.
func TestGroupsReportsOnlyGroupsWithAParkedRunner(t *testing.T) {
	r := New()
	if got := r.Groups(); len(got) != 0 {
		t.Fatalf("Groups = %v on an empty registry, want none", got)
	}

	go r.Poll(t.Context(), "prod", 500*time.Millisecond)
	waitFor(t, func() bool { return r.Available("prod") == 1 })
	got := r.Groups()
	if len(got) != 1 || got[0] != "prod" {
		t.Fatalf("Groups = %v, want [prod]", got)
	}

	// Once that runner leaves, the group must stop being reported even though
	// the registry still holds an (empty) entry for it.
	waitFor(t, func() bool { return r.Available("prod") == 0 })
	if got := r.Groups(); len(got) != 0 {
		t.Fatalf("Groups = %v after the runner left, want none", got)
	}
}

// A runner delivering twice — a retry, or a confused client — must be told to
// stop on the second attempt rather than blocking on a caller who has already
// been answered and gone.
func TestSecondDeliveryForTheSameRequestIsRejected(t *testing.T) {
	r := New()
	r.DeliveryTimeout = time.Second
	first := make(chan *Request, 1)
	go func() { first <- r.Poll(t.Context(), "prod", 2*time.Second) }()
	waitFor(t, func() bool { return r.Available("prod") == 1 })

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := r.Dispatch(t.Context(), "prod", req("r1")); err != nil {
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
	go func() { parked <- r.Poll(t.Context(), "prod", 2*time.Second) }()
	waitFor(t, func() bool { return r.Available("prod") == 1 })

	// Dispatch, but never read the response: the caller is slow/absent.
	go func() { _, _ = r.Dispatch(t.Context(), "prod", req("r1")) }()
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
