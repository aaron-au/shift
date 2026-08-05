// Package ingress is the gateway's public-facing handler: authenticate the
// caller, filter the request, route it to an eligible runner group, and stream
// the exchange both ways (ADR-0038 §1).
//
// It never persists anything. The request body streams to the runner and the
// runner's response streams back; no buffer, no spool, no queue. When no
// eligible runner is available the answer is 503 — a gateway that holds work
// is a gateway with durable state, which is exactly what a DMZ component must
// not have.
package ingress

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// defaultMaxBody bounds a request body when a route does not say. Unbounded is
// not an option on a public edge: it is a memory-exhaustion primitive.
const defaultMaxBody = 8 << 20

// Handler serves the public listener. The configuration pointer is swapped
// atomically when the hub pushes a new one, so a request either sees the whole
// old policy or the whole new one — never a half-applied mix.
type Handler struct {
	cfg  atomic.Pointer[config.Config]
	reg  *runners.Registry
	log  *slog.Logger
	newI func() string // request id; injectable for tests
}

// New returns a handler with no configuration. Until the hub pushes one it
// answers 503 to everything, which is the correct state for a gateway that
// does not yet know what it serves.
func New(reg *runners.Registry, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{reg: reg, log: log, newI: newID}
}

// SetConfig swaps the active configuration. It rejects anything invalid rather
// than applying it: the hub validates first and is the authority, but a bad
// config here means the internet cannot reach us, so it is worth refusing
// twice.
func (h *Handler) SetConfig(c *config.Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	h.cfg.Store(c)
	return nil
}

// Configured reports whether the hub has pushed a configuration yet. The
// health endpoint surfaces this so an ungreeted gateway is visible FROM THE
// HUB — a passive component that is never dialled is otherwise silent.
func (h *Handler) Configured() bool { return h.cfg.Load() != nil }

// ConfigVersion returns the active configuration version, or 0.
func (h *Handler) ConfigVersion() int64 {
	if c := h.cfg.Load(); c != nil {
		return c.Version
	}
	return 0
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cfg := h.cfg.Load()
	if cfg == nil {
		http.Error(w, "gateway not configured", http.StatusServiceUnavailable)
		return
	}
	route := cfg.Lookup(r.Method, r.URL.Path)
	if route == nil {
		http.NotFound(w, r)
		return
	}

	ip := config.ClientIP(r, cfg.TrustedProxyNets())
	if !route.Allowed(ip) {
		// Deliberately indistinguishable from an unknown path: telling an
		// unauthorised caller that a route exists is free reconnaissance.
		http.NotFound(w, r)
		return
	}
	if !headersPresent(r, route.RequireHeaders) {
		http.Error(w, "missing required header", http.StatusBadRequest)
		return
	}
	if !authorized(r, route) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	maxBody := route.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBody
	}

	id := h.newI()
	req := &runners.Request{
		ID:      id,
		Flow:    route.Flow,
		Method:  r.Method,
		Path:    r.URL.Path,
		Headers: stamp(forwardable(r.Header), route, id, ip),
		// The body streams to the runner under a hard cap. MaxBytesReader
		// makes an over-long body an error at the point of reading rather
		// than something the gateway has already accepted into memory.
		Body: http.MaxBytesReader(w, r.Body, maxBody),
	}

	resp, release, err := h.reg.Dispatch(r.Context(), route.Selector, req)
	// The response body is a live stream off the runner's request; releasing
	// tells the runner it may close. Deferred here so every path below is
	// covered, and it MUST NOT run before the copy at the end of this function
	// or the caller gets a truncated body (see runners.Dispatch).
	defer release()
	switch {
	case errors.Is(err, runners.ErrNoRunner):
		// 503, never a queue. Retry-After tells a well-behaved sender to come
		// back rather than treating this as a permanent failure.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no runner available", http.StatusServiceUnavailable)
		h.log.Warn("no runner available", "flow", route.Flow, "selector", route.Selector.String())
		return
	case errors.Is(err, runners.ErrDeliveryTimeout):
		http.Error(w, "runner did not respond", http.StatusGatewayTimeout)
		h.log.Warn("runner delivery timeout", "flow", route.Flow, "request", req.ID)
		return
	case errors.Is(err, context.Canceled):
		return // caller went away; nothing to say to a closed connection
	case err != nil:
		http.Error(w, "gateway error", http.StatusBadGateway)
		h.log.Error("dispatch failed", "flow", route.Flow, "error", err)
		return
	}

	for k, vs := range resp.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	status := resp.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if resp.Body != nil {
		// Streamed straight through. A copy error here means the caller hung
		// up mid-response, which is not a gateway fault and not worth an
		// error-level log.
		if _, err := io.Copy(w, resp.Body); err != nil {
			h.log.Debug("response copy ended early", "request", req.ID, "error", err)
		}
	}
}

// authorized checks the caller's bearer token against the route's stored
// digest. The token is never stored or logged — only its SHA-256 — and the
// comparison is constant-time.
func authorized(r *http.Request, route *config.Route) bool {
	if route.AuthTokenSHA256 == "" {
		return true // route is deliberately open
	}
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	sum := sha256.Sum256([]byte(tok))
	got := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(got), []byte(route.AuthTokenSHA256)) == 1
}

func headersPresent(r *http.Request, required map[string]string) bool {
	for k, want := range required {
		got := r.Header.Get(k)
		if got == "" {
			return false
		}
		if want != "" && got != want {
			return false
		}
	}
	return true
}

// shiftPrefix is the reserved namespace for gateway-asserted facts. Nothing
// carrying it may survive from the caller (see forwardable).
const shiftPrefix = "X-Shift-"

// The statically coded stamp set (ADR-0038 §4b). Not configurable and not
// per-route, so a reader of the runner side knows the complete list of things
// a gateway can assert.
const (
	HeaderPrincipal = shiftPrefix + "Principal"  // who the gateway authenticated
	HeaderRoute     = shiftPrefix + "Route"      // the route that matched
	HeaderRequestID = shiftPrefix + "Request-Id" // correlates gateway/runner/hub
	HeaderClientIP  = shiftPrefix + "Client-Ip"  // caller, trusted-proxy aware
	HeaderFlow      = shiftPrefix + "Flow"       // the flow to run
	HeaderMethod    = shiftPrefix + "Method"     // the caller's HTTP method
	HeaderPath      = shiftPrefix + "Path"       // the caller's path
)

// anonymous is the principal for a route that declares no credential. It is a
// named value rather than an empty header so "nobody authenticated" and "the
// gateway forgot to stamp" are distinguishable downstream.
const anonymous = "anonymous"

// hopByHop headers are connection-scoped and must not be forwarded to the
// runner; Authorization is dropped because the gateway has already consumed
// it and the runner has no business seeing the caller's credential.
var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true,
	"Proxy-Authorization": true, "Te": true, "Trailer": true,
	"Transfer-Encoding": true, "Upgrade": true, "Authorization": true,
}

// forwardable copies the caller's headers, dropping hop-by-hop ones and
// STRIPPING the entire X-Shift-* namespace.
//
// The strip is unconditional and has no opt-out. It is the whole security
// property of §4b: if a caller could send `X-Shift-Principal: admin` and have
// it survive, the gateway would be an authentication bypass with an audit
// trail that lies about it — worse than propagating no identity at all.
// Matching is case-insensitive because HTTP header names are, and a check
// that missed `x-shift-principal` would be no check.
func forwardable(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		ck := http.CanonicalHeaderKey(k)
		if hopByHop[ck] {
			continue
		}
		// CanonicalHeaderKey normalises case, so one prefix test covers every
		// spelling a caller might try.
		if strings.HasPrefix(ck, shiftPrefix) {
			continue
		}
		// Store under the CANONICAL key, not the one the caller sent. Header
		// values are looked up with Get, which canonicalises what it is asked
		// for — so a key written raw would be invisible to every reader.
		//
		// Go's server canonicalises on parse, so this cannot currently differ
		// for a real request. It differs the moment anything builds a Header
		// by direct map assignment, which is exactly how the strip above is
		// tested, and a rule that holds only for well-formed input is not a
		// rule worth relying on.
		out[ck] = vs
	}
	return out
}

// stamp asserts the gateway's own facts onto headers ALREADY stripped by
// forwardable. Order matters and is the point: strip, then stamp.
func stamp(h http.Header, route *config.Route, requestID, clientIP string) http.Header {
	principal := route.AuthPrincipal
	if principal == "" {
		principal = anonymous
	}
	h.Set(HeaderPrincipal, principal)
	h.Set(HeaderRoute, route.Path)
	h.Set(HeaderRequestID, requestID)
	h.Set(HeaderClientIP, clientIP)
	return h
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b[:])
}
