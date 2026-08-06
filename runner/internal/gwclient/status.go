package gwclient

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/aaron-au/shift/runner/internal/hubclient"
)

// The caller-facing status path (ADR-0042 §3), runner side.
//
// A status read arrives through the gateway like any other request, carrying
// X-Shift-Op: status instead of a flow to run. The runner asks the hub and
// returns the answer. The gateway learns nothing about task state and never
// dials inward; the hub never speaks to the caller.

// StatusReader is the hub-facing half. It is an interface so a runner with no
// hub degrades cleanly rather than panicking on a nil client.
type StatusReader interface {
	AcceptExecution(ctx context.Context, e hubclient.AcceptedExecution) error
	FinishExecution(ctx context.Context, id string, out hubclient.ExecutionOutcome) error
	ExecutionStatusByID(ctx context.Context, id, route, principal, tokenSHA256 string) (*hubclient.ExecutionStatus, error)
}

// anonymousPrincipal mirrors what the gateway stamps for a route with no
// credential. On such a route a principal comparison authorises nothing —
// every caller is the same person — so the status URL carries a per-task
// capability token instead (ADR-0042 §3b).
const anonymousPrincipal = "anonymous"

// statusSegment must match the gateway's reserved segment. The two are
// separate modules by design (depguard), so this contract has no compiler
// enforcing it.
const statusSegment = "_status"

// serveStatus answers a status read.
func (l *Loop) serveStatus(ctx context.Context, in *inbound) (int, []byte, string) {
	if l.opts.Status == nil {
		// No hub means no status record to read. Honest rather than a 500:
		// this deployment never handed out a status URL in the first place.
		return http.StatusNotFound,
			problem(http.StatusNotFound, "status_unavailable",
				"this runner has no execution history to read", nil), "application/json"
	}
	taskID := in.headers.Get(hdrTask)
	if taskID == "" {
		return http.StatusBadRequest,
			problem(http.StatusBadRequest, "status_invalid", "no task id", nil), "application/json"
	}

	st, err := l.opts.Status.ExecutionStatusByID(ctx, taskID,
		in.headers.Get(hdrRoute), in.headers.Get(hdrPrincipal), capabilityDigest(in))
	switch {
	case errors.Is(err, hubclient.ErrExecutionNotFound):
		// Unknown id, wrong route, wrong principal and wrong capability token
		// are ONE answer. Distinguishing them tells an attacker which ids
		// exist, which is the fact being protected.
		return http.StatusNotFound,
			problem(http.StatusNotFound, "not_found", "no such task", nil), "application/json"
	case errors.Is(err, hubclient.ErrExecutionGone):
		return http.StatusGone,
			problem(http.StatusGone, "status_consumed",
				"this status has already been read", nil), "application/json"
	case err != nil:
		l.log.Error("reading execution status failed", "task", taskID, "error", err)
		return http.StatusBadGateway,
			problem(http.StatusBadGateway, "status_unavailable",
				"the status could not be read", nil), "application/json"
	}

	body, err := json.Marshal(st)
	if err != nil {
		return http.StatusInternalServerError,
			problem(http.StatusInternalServerError, "status_unavailable", "", nil), "application/json"
	}
	return http.StatusOK, body, "application/json"
}

// capabilityDigest returns the SHA-256 of the capability token on the request,
// or "" when there is none. Only the digest ever leaves this function — the
// token itself is never logged and never reaches the hub.
func capabilityDigest(in *inbound) string {
	tok := in.query.Get(capabilityParam)
	if tok == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// capabilityParam is the query parameter carrying an anonymous route's
// per-task token. Short because it rides in a URL that humans copy.
const capabilityParam = "t"

// statusURL builds the absolute URL a caller polls, or "" when it cannot be
// built (no public base, or no hub to record against).
//
// The path is the caller's own route plus the reserved segment, so the read
// inherits that route's policy and a caller with access to one route has no
// path on which to try another's ids (ADR-0042 §3).
func statusURL(base, route, id, token string) string {
	if base == "" || route == "" || id == "" {
		return ""
	}
	u := strings.TrimSuffix(base, "/") + strings.TrimSuffix(route, "/") + "/" + statusSegment + "/" + id
	if token != "" {
		u += "?" + capabilityParam + "=" + url.QueryEscape(token)
	}
	return u
}

// newCapabilityToken mints the per-task secret for an anonymous route.
func newCapabilityToken() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// newTaskID mints the id quoted in the 202 (ADR-0042 §3a). Uniqueness comes
// from randomness rather than from a sequence the hub owns, which is exactly
// what lets a runner quote one without coordinating.
func newTaskID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
