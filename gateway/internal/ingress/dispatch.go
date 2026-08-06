package ingress

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
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

// pollRequest is what a runner sends to park itself.
//
// It carries NO labels. It used to (ADR-0038 §5 as first built), and that was
// a runner asserting its own placement — so a compromised or misconfigured one
// could claim `environment: production` and be handed production traffic, with
// nothing in the system to disagree. Labels now come from the hub's roster,
// keyed by the identity the runner PROVED with its client certificate
// (ADR-0041 §3).
//
// Anything a DMZ component accepts from inside the network is surface; this
// message is now a wait hint and nothing else.
type pollRequest struct {
	WaitSeconds float64 `json:"wait_seconds,omitempty"`
}

// LabelSource supplies the hub-asserted labels for a proven runner id, and
// reports whether the roster knows that runner at all.
type LabelSource func(runnerID string) (map[string]string, bool)

// DispatchHandler serves the runner-facing endpoints.
type DispatchHandler struct {
	reg *runners.Registry
	log *slog.Logger
	// labels resolves a runner id to its hub-asserted labels. nil means the
	// roster is not in use yet (pre-ADR-0041 deployments), in which case a
	// runner parks with no labels and can serve only unrestricted routes.
	labels LabelSource
	// peerID extracts the proven runner id from a request. Injectable so the
	// wire tests need no TLS.
	peerID func(*http.Request) string
	// tokenSHA256 is the hex SHA-256 of the shared secret a runner must
	// present. Empty means the endpoints are UNAUTHENTICATED, which is only
	// tenable on a loopback bind — see RequireAuth.
	tokenSHA256 string
}

// NewDispatch returns the runner-facing handler. token is the shared secret
// runners must present; "" leaves the endpoints unauthenticated.
func NewDispatch(reg *runners.Registry, log *slog.Logger, token string) *DispatchHandler {
	if log == nil {
		log = slog.Default()
	}
	d := &DispatchHandler{reg: reg, log: log, peerID: tlsPeerID}
	if token != "" {
		sum := sha256.Sum256([]byte(token))
		// Only the digest is retained. The plaintext is never stored, never
		// logged, and never comparable by anything that reads this struct.
		d.tokenSHA256 = hex.EncodeToString(sum[:])
	}
	return d
}

// WithLabels wires the hub-pushed roster in. Once set, a runner whose proven
// identity is absent from the roster is REFUSED rather than treated as
// label-less: label-less satisfies every empty selector, so an unvouched
// runner would receive precisely the traffic nobody restricted.
func (d *DispatchHandler) WithLabels(src LabelSource) *DispatchHandler {
	d.labels = src
	return d
}

// WithPeerID overrides how a proven runner id is read from a request.
func (d *DispatchHandler) WithPeerID(fn func(*http.Request) string) *DispatchHandler {
	d.peerID = fn
	return d
}

// tlsPeerID reads the runner id from the client certificate subject. Empty
// when the connection is not mutually authenticated.
func tlsPeerID(r *http.Request) string {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ""
	}
	return r.TLS.PeerCertificates[0].Subject.CommonName
}

// Authenticated reports whether a shared secret is configured.
func (d *DispatchHandler) Authenticated() bool { return d.tokenSHA256 != "" }

// Routes registers the runner-facing endpoints on mux, behind authentication.
//
// These belong on the gateway's CONTROL listener, never on the public one. A
// caller who reaches /poll unauthenticated can park a fake runner and be
// handed real inbound payloads, and can deliver forged responses to real
// callers — interception and response-forgery from one open port.
//
// The shared secret here is an INTERIM measure. ADR-0038 §6a specifies mutual
// TLS with a per-gateway identity bundle, which additionally authenticates the
// gateway TO the runner and removes the shared-secret distribution problem.
// This exists so that the window before that lands is not "an open port".
func (d *DispatchHandler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/gw/poll", d.authed(d.poll))
	mux.HandleFunc("POST /api/v1/gw/deliver/{id}", d.authed(d.deliver))
}

// authed wraps a runner-facing handler with bearer-token verification.
func (d *DispatchHandler) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.tokenSHA256 != "" && !d.validToken(r) {
			// No detail: an unauthenticated caller learns only that it failed,
			// not whether the endpoint exists or what shape a token takes.
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (d *DispatchHandler) validToken(r *http.Request) bool {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	sum := sha256.Sum256([]byte(tok))
	got := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(got), []byte(d.tokenSHA256)) == 1
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

	// Labels are ASSERTED BY THE HUB, never taken from the request. The runner
	// proves who it is; the roster says what it is.
	var labels map[string]string
	if d.labels != nil {
		id := d.peerID(r)
		got, known := d.labels(id)
		if !known {
			// Fail closed. A runner the hub has not vouched for may be new
			// (the roster push is seconds behind) or may not belong here at
			// all, and the two are indistinguishable from this side.
			d.log.Warn("poll from a runner absent from the roster", "runner", id)
			http.Error(w, "runner not in roster", http.StatusForbidden)
			return
		}
		labels = got
	}

	req := d.reg.Poll(r.Context(), labels, wait)
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
