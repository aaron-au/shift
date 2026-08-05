// Package runners tracks which runners are available to serve inbound
// requests, and hands each request to one of them.
//
// The whole mechanism rests on one inversion (ADR-0038 §4): the gateway never
// opens a connection into the internal network. Runners come to it. A runner
// that wants work holds an outbound long-poll against the gateway, so:
//
//	the set of runners currently polling IS the set of available runners.
//
// That is not a convenience, it is what removes an entire subsystem. There is
// no liveness table to keep fresh, no capacity query on the request path, and
// no way to route to a dead backend — a runner that died is a runner no longer
// holding a poll. And because the runner's own lease loop is capacity-gated
// (ADR-0005), a runner only polls when it can actually accept work, so
// admission control and load balancing fall out of the same fact.
package runners

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// ErrNoRunner reports that no eligible runner was waiting. The caller must
// turn this into a 503 and NEVER into a queue: a gateway that buffers is a
// gateway with durable state, and durable state in the DMZ is the thing this
// component exists to avoid (ADR-0038 §2).
var ErrNoRunner = errors.New("runners: no eligible runner is available")

// ErrDeliveryTimeout reports that a runner claimed a request and did not
// deliver a response within the deadline — it died mid-execution, or its
// execution outran the caller's patience.
var ErrDeliveryTimeout = errors.New("runners: runner did not respond in time")

// Request is one inbound request being handed to a runner. Body streams; it
// is never buffered by the gateway.
type Request struct {
	ID      string
	Flow    string
	Method  string
	Path    string
	Headers http.Header
	Body    io.Reader
}

// Response is what a runner sends back. Body streams straight through to the
// original caller.
type Response struct {
	Status  int
	Headers http.Header
	Body    io.Reader
}

// waiter is one runner parked on a long-poll, waiting to be handed work.
type waiter struct {
	group string
	// ch receives at most one request. Buffered so a hand-off never blocks on
	// a runner that gave up between being chosen and being sent to.
	ch chan *Request
}

// exchange is one in-flight request: the runner has been handed it, and the
// caller's goroutine is blocked waiting for the response to come back on a
// separate call (ADR-0038 §4 — two requests, no persistent connection).
type exchange struct {
	resp chan *Response
	done chan struct{} // closed when the caller stops waiting
}

// Registry is the set of parked runners plus the in-flight exchanges.
// Safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	waiting  map[string][]*waiter // by group, FIFO
	inflight map[string]*exchange // by request id

	// DeliveryTimeout bounds how long a caller waits for a runner's response
	// once the request has been handed over. Zero uses a default.
	DeliveryTimeout time.Duration
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{
		waiting:  make(map[string][]*waiter),
		inflight: make(map[string]*exchange),
	}
}

const defaultDeliveryTimeout = 60 * time.Second

func (r *Registry) deliveryTimeout() time.Duration {
	if r.DeliveryTimeout > 0 {
		return r.DeliveryTimeout
	}
	return defaultDeliveryTimeout
}

// Poll parks a runner until work arrives for its group, the wait elapses, or
// ctx ends. It returns nil when nothing arrived, which the caller answers with
// 204 so the runner immediately polls again.
//
// Parking is what makes this runner "available"; returning unparks it. A
// runner that is executing is not polling, and is therefore not available —
// no separate busy/idle state to keep in sync with reality.
func (r *Registry) Poll(ctx context.Context, group string, wait time.Duration) *Request {
	w := &waiter{group: group, ch: make(chan *Request, 1)}
	r.mu.Lock()
	r.waiting[group] = append(r.waiting[group], w)
	r.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case req := <-w.ch:
		return req
	case <-timer.C:
	case <-ctx.Done():
	}

	// Timed out or cancelled: unpark. A request may have been handed to us in
	// the same instant, so re-check the channel after removing — otherwise
	// that request would be delivered to nobody and the caller would wait out
	// the full delivery timeout for a runner that had already gone.
	r.remove(w)
	select {
	case req := <-w.ch:
		return req
	default:
		return nil
	}
}

// remove unparks a waiter. Safe to call for a waiter already removed.
func (r *Registry) remove(w *waiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.waiting[w.group]
	for i, cand := range q {
		if cand == w {
			r.waiting[w.group] = append(q[:i], q[i+1:]...)
			return
		}
	}
}

// Dispatch hands req to a waiting runner in its group and blocks until that
// runner delivers a response, the delivery deadline passes, or ctx ends.
//
// It returns ErrNoRunner immediately when nobody is waiting. That is a 503,
// deliberately: the alternative is holding the request until a runner appears,
// which is a queue, which is durable state in the DMZ.
func (r *Registry) Dispatch(ctx context.Context, group string, req *Request) (*Response, error) {
	ex := &exchange{resp: make(chan *Response, 1), done: make(chan struct{})}

	r.mu.Lock()
	q := r.waiting[group]
	if len(q) == 0 {
		r.mu.Unlock()
		return nil, ErrNoRunner
	}
	// FIFO: the runner waiting longest goes first, which spreads work evenly
	// without tracking load. A busy runner is not polling, so it cannot be
	// chosen — the queue is self-balancing.
	w := q[0]
	r.waiting[group] = q[1:]
	r.inflight[req.ID] = ex
	r.mu.Unlock()

	defer func() {
		close(ex.done)
		r.mu.Lock()
		delete(r.inflight, req.ID)
		r.mu.Unlock()
	}()

	w.ch <- req // buffered: never blocks

	timer := time.NewTimer(r.deliveryTimeout())
	defer timer.Stop()
	select {
	case resp := <-ex.resp:
		return resp, nil
	case <-timer.C:
		return nil, ErrDeliveryTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Deliver hands a runner's response back to the caller blocked in Dispatch.
// It reports whether a caller was still waiting: false means the caller gave
// up or the request id is unknown, and the runner should stop streaming.
//
// Deliver BLOCKS until the caller has consumed the response, because the body
// is a live stream from the runner's own request — returning early would let
// that request's body close underneath the caller mid-copy.
func (r *Registry) Deliver(ctx context.Context, id string, resp *Response) bool {
	r.mu.Lock()
	ex, ok := r.inflight[id]
	r.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ex.resp <- resp:
	default:
		return false // already answered or abandoned
	}
	// Wait for the caller to finish with the body before letting the runner's
	// request return.
	select {
	case <-ex.done:
		return true
	case <-ctx.Done():
		return false
	}
}

// Available reports how many runners are currently parked for a group. It is
// the honest answer to "can we serve this right now", because it counts
// runners actually waiting rather than runners believed to be alive.
func (r *Registry) Available(group string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.waiting[group])
}

// Groups returns the groups with at least one runner parked, for the health
// endpoint. Order is unspecified.
func (r *Registry) Groups() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.waiting))
	for g, q := range r.waiting {
		if len(q) > 0 {
			out = append(out, g)
		}
	}
	return out
}
