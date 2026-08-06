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

// A request handed to a runner must always reach its caller, even when the
// hand-off collides with that runner's poll expiring.
//
// The interleaving under guard (see Dispatch) is:
//
//	Dispatch: lock, take the waiter off the queue, unlock
//	Poll:     time out, remove (already gone), re-check the channel — EMPTY
//	Dispatch: send into a buffer with no reader
//	Caller:   block for the full delivery timeout, then 504
//
// STATUS: this test has never reproduced that loss — 344k dispatches against
// polls expiring every millisecond, zero lost, both with and without the
// hand-off inside the lock. It is a guard, not a demonstrated regression, and
// it should not be described as one.
//
// It is kept anyway because it exercises the concurrent path hard and would
// catch a coarser mistake in the same area. If it ever fails, the failure is
// real: with runners that deliver unconditionally, ErrDeliveryTimeout has no
// other explanation.
func TestDispatchNeverLosesARequestToAnExpiringPoll(t *testing.T) {
	r := New()
	// Short enough that a lost request is a fast failure rather than a hung
	// test, and far longer than any legitimate hand-off here.
	r.DeliveryTimeout = 2 * time.Second

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Runners whose poll windows keep expiring, maximising the collision rate
	// with the dispatches below.
	const runners = 8
	var runnerWG sync.WaitGroup
	for range runners {
		runnerWG.Add(1)
		go func() {
			defer runnerWG.Done()
			for ctx.Err() == nil {
				req := r.Poll(ctx, map[string]string{"env": "prod"}, time.Millisecond)
				if req == nil {
					continue // window closed with nothing: normal
				}
				// Answer immediately. Any request that reaches a runner must
				// reach its caller.
				_ = r.Deliver(ctx, req.ID, &Response{
					Status: http.StatusOK, Body: strings.NewReader("ok"),
				})
			}
		}()
	}

	sel := map[string]string{"env": "prod"}
	var lost, served, noRunner int
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		req := &Request{
			ID: "r" + itoa(i), Flow: "f", Method: http.MethodPost, Path: "/x",
			Headers: http.Header{}, Body: strings.NewReader("{}"),
		}
		resp, release, err := r.Dispatch(ctx, sel, req)
		defer release()
		switch {
		case errors.Is(err, ErrNoRunner):
			// Legitimate: every runner happened to be between polls. The
			// caller gets an immediate 503 and can retry.
			noRunner++
		case errors.Is(err, ErrDeliveryTimeout):
			// A runner took this request and never answered. With runners that
			// deliver unconditionally, the only way here is a lost hand-off.
			lost++
		case err != nil:
			t.Fatalf("dispatch: %v", err)
		default:
			if resp == nil {
				t.Fatal("nil response with no error")
			}
			served++
		}
	}
	cancel()
	runnerWG.Wait()

	t.Logf("served=%d no-runner=%d lost=%d", served, noRunner, lost)
	if lost > 0 {
		t.Errorf("%d request(s) handed to a runner and never delivered — "+
			"the hand-off raced an expiring poll", lost)
	}
	if served == 0 {
		t.Fatal("no requests served; the test proved nothing")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
