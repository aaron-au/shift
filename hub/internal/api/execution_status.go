package api

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aaron-au/shift/hub/internal/store"
)

// Caller-facing execution status (ADR-0042 §3).
//
// Both endpoints are RUNNER realm. The hub never speaks to the caller: a status
// request arrives at the gateway, is dispatched to a runner, and the runner asks
// here. That keeps ADR-0038's direction property intact — nothing in the DMZ
// ever dials inward, and the gateway learns neither the hub's address nor
// anything about task state.

// acceptRequest is what a runner sends when it accepts an async request, before
// answering 202. The id is the runner's (ADR-0042 §3a): it is already quoted to
// the caller, so the hub takes it rather than inventing one.
type acceptRequest struct {
	ID       string `json:"id"`
	FlowName string `json:"flow_name"`
	Route    string `json:"route,omitempty"`
	// Principal is who the gateway said the caller was. The hub stores it; the
	// runner does not get to be believed about anything else.
	Principal string `json:"principal,omitempty"`
	// TokenSHA256 is set only for anonymous routes, where the status URL is a
	// capability URL. The token itself never reaches the hub.
	TokenSHA256 string `json:"token_sha256,omitempty"`
	TTLSeconds  int64  `json:"ttl_seconds,omitempty"`
}

// acceptExecution records an accepted request so its status URL resolves from
// the moment the caller receives it.
func (a *api) acceptExecution(w http.ResponseWriter, r *http.Request) {
	var req acceptRequest
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.ID == "" || req.FlowName == "" {
		writeErr(w, http.StatusUnprocessableEntity, errors.New("id and flow_name are required"))
		return
	}
	ttl := time.Duration(req.TTLSeconds) * time.Second
	err := a.st.AcceptExecution(r.Context(), runnerID(r), store.ExecutionStatus{
		ID: req.ID, FlowName: req.FlowName, Route: req.Route, Principal: req.Principal,
	}, req.TokenSHA256, ttl)
	if errors.Is(err, store.ErrStatusIDTaken) {
		// The runner mints another rather than overwriting: two requests sharing
		// one status row would report each other's outcome.
		writeErrCode(w, http.StatusConflict, "status_id_taken", err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// finishExecution moves a status row to its terminal state.
func (a *api) finishExecution(w http.ResponseWriter, r *http.Request) {
	var e store.ExecutionStatus
	if err := readBody(r, &e); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	e.ID = r.PathValue("id")
	switch e.State {
	case store.StatusRunning, store.StatusCompleted, store.StatusFailed:
	default:
		writeErr(w, http.StatusUnprocessableEntity, errors.New("state must be running, completed or failed"))
		return
	}
	err := a.st.FinishExecution(r.Context(), e)
	if errors.Is(err, pgx.ErrNoRows) {
		// Unknown id, or an id belonging to another account. From here those
		// are the same answer, deliberately.
		writeErr(w, http.StatusNotFound, errors.New("no such execution"))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// getExecutionStatus serves one status read on behalf of a caller.
//
// The runner passes through what the gateway established about the caller —
// the route, the principal, and (for an anonymous route) the capability token's
// digest. Trusting a RUNNER with that is deliberate and is not the same as
// trusting a gateway: a runner already holds decrypted secrets and live payload,
// whereas ADR-0041 exists precisely because a gateway-adjacent component should
// not be believed about identity it merely asserts.
func (a *api) getExecutionStatus(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	e, err := a.st.ExecutionStatusByID(r.Context(),
		r.PathValue("id"), q.Get("route"), q.Get("principal"), q.Get("token_sha256"))

	switch {
	case errors.Is(err, store.ErrStatusGone):
		// Already read and awaiting the sweeper. Gone rather than not-found:
		// this caller has already proved the capability, so it leaks nothing,
		// and a 404 here would read as "you got the id wrong".
		writeErrCode(w, http.StatusGone, "status_consumed", errors.New("this status has already been read"))
		return
	case errors.Is(err, pgx.ErrNoRows):
		// Unknown id, wrong route, wrong principal and wrong capability token
		// all land here. A distinguishable response would confirm that someone
		// else's task exists under that id, which is the fact being protected.
		writeErr(w, http.StatusNotFound, errors.New("no such execution"))
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	if e.Terminal() {
		// Stamped only after a SUCCESSFUL read, so a refused one never starts
		// the pruning clock.
		if err := a.st.ConsumeExecutionStatus(r.Context(), e.ID); err != nil {
			// The caller has their answer; failing the response over
			// bookkeeping would be the wrong trade. The sweeper's TTL still
			// bounds the row.
			slog.Warn("marking an execution status consumed failed", "id", e.ID, "error", err)
		}
	}
	writeJSON(w, http.StatusOK, e)
}
