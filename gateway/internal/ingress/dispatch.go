package ingress

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aaron-au/shift/gateway/internal/runners"
)

// The runner-facing side of the gateway: two endpoints, both called BY the
// runner (ADR-0038 §4). Nothing here dials inward — that inversion is the
// reason the gateway can sit in a DMZ at all.
//
//	POST /api/v1/gw/poll              park until work arrives (long-poll)
//	POST /api/v1/gw/deliver/{id}      hand back the response
//
// One inbound request becomes two runner-side calls rather than one duplex
// connection. That costs an extra round trip on the deliver — sub-millisecond
// on a LAN, against a fixed trigger-path cost already measured in hundreds of
// microseconds — and buys plain HTTP semantics: no framing protocol, no
// half-open state to reason about, and a poll that can be abandoned by simply
// letting the request time out.

// Wire representation of a request handed to a runner. The metadata rides in
// response headers and the caller's body streams as the response body, so
// nothing is buffered on the way through. The caller's own headers are
// re-emitted under a prefix because they share a namespace with the poll
// response's own headers — `Content-Type` would otherwise mean two things.
const fwdPrefix = "X-Shift-Fwd-"

// deliverStatus carries the runner's intended HTTP status back to the caller.
const deliverStatus = "X-Shift-Status"

// defaultPollWait is used when a runner asks for none. Long enough that poll
// traffic is negligible (a 30s window is ~3 requests/sec across 100 runners),
// short enough to stay inside anything's idle timeout.
const defaultPollWait = 30 * time.Second

// maxPollWait bounds what a runner may ask for. A poll parked for an hour is
// indistinguishable from a leaked goroutine.
const maxPollWait = 5 * time.Minute

// pollRequest is what a runner sends to park itself. Labels are what the
// runner IS; the gateway matches route selectors against them (ADR-0030).
type pollRequest struct {
	Labels      map[string]string `json:"labels,omitempty"`
	WaitSeconds float64           `json:"wait_seconds,omitempty"`
}

// DispatchHandler serves the runner-facing endpoints.
type DispatchHandler struct {
	reg *runners.Registry
	log *slog.Logger
}

// NewDispatch returns the runner-facing handler.
func NewDispatch(reg *runners.Registry, log *slog.Logger) *DispatchHandler {
	if log == nil {
		log = slog.Default()
	}
	return &DispatchHandler{reg: reg, log: log}
}

// Routes registers the runner-facing endpoints on mux.
//
// These belong on the gateway's CONTROL listener (mutually authenticated),
// never on the public one: an unauthenticated caller able to reach /poll
// would be able to intercept inbound payloads by impersonating a runner.
func (d *DispatchHandler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/gw/poll", d.poll)
	mux.HandleFunc("POST /api/v1/gw/deliver/{id}", d.deliver)
}

func (d *DispatchHandler) poll(w http.ResponseWriter, r *http.Request) {
	var pr pollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&pr); err != nil {
		http.Error(w, "bad poll request", http.StatusBadRequest)
		return
	}
	wait := time.Duration(pr.WaitSeconds * float64(time.Second))
	switch {
	case wait <= 0:
		wait = defaultPollWait
	case wait > maxPollWait:
		wait = maxPollWait
	}

	req := d.reg.Poll(r.Context(), pr.Labels, wait)
	if req == nil {
		// Nothing arrived in the window. 204 rather than an error: an empty
		// poll is the NORMAL outcome, and a runner that treated it as a
		// failure would back off exactly when it should be re-polling.
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h := w.Header()
	h.Set(HeaderRequestID, req.ID)
	h.Set(HeaderFlow, req.Flow)
	h.Set(HeaderMethod, req.Method)
	h.Set(HeaderPath, req.Path)
	// The stamped identity headers are already on req.Headers (strip-then-
	// stamp happened at ingress); they pass through the loop below like any
	// other, but under the forward prefix so the runner reads them from one
	// place rather than two.
	for k, vs := range req.Headers {
		for _, v := range vs {
			h.Add(fwdPrefix+k, v)
		}
	}
	w.WriteHeader(http.StatusOK)
	if req.Body != nil {
		if _, err := io.Copy(w, req.Body); err != nil {
			// The runner hung up, or the caller's body ended early. Either
			// way the exchange is dead; the caller's Dispatch will time out
			// and answer 504. Nothing to recover here.
			d.log.Warn("poll body copy failed", "request", req.ID, "error", err)
		}
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func (d *DispatchHandler) deliver(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing request id", http.StatusBadRequest)
		return
	}
	status := http.StatusOK
	if s := r.Header.Get(deliverStatus); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 100 || n > 599 {
			http.Error(w, "bad status", http.StatusBadRequest)
			return
		}
		status = n
	}

	resp := &runners.Response{
		Status:  status,
		Headers: deliverHeaders(r.Header),
		Body:    r.Body,
	}
	// Deliver BLOCKS until the caller has consumed the body — r.Body is a
	// live stream from this very request, so returning first would close it
	// underneath the caller mid-copy.
	if !d.reg.Deliver(r.Context(), id, resp) {
		// The caller gave up, or this id was never in flight. 410 rather than
		// 404: it says "stop streaming, the work is wasted" without implying
		// the runner did anything wrong.
		http.Error(w, "no caller waiting", http.StatusGone)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deliverHeaders selects what a runner may set on the caller's response.
//
// An allowlist, not a strip-list. The runner is inside the trust boundary,
// but the response goes to the public internet, and "everything the runner
// sent, minus what we remembered to remove" is the shape that leaks an
// internal header the day someone adds one.
func deliverHeaders(h http.Header) http.Header {
	out := make(http.Header, 4)
	for k, vs := range h {
		ck := http.CanonicalHeaderKey(k)
		switch {
		case ck == "Content-Type", ck == "Cache-Control", ck == "Etag":
			out[ck] = vs
		case strings.HasPrefix(ck, fwdPrefix):
			// Explicitly opted in by the runner for return to the caller.
			out[strings.TrimPrefix(ck, fwdPrefix)] = vs
		}
	}
	return out
}
