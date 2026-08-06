package hubclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Caller-facing execution status (ADR-0042 §3), from the runner's side.
//
// The runner is the only component that talks to the hub about this: a status
// request lands at the gateway, is dispatched here, and this asks. That is what
// keeps ADR-0038's direction property intact — the gateway never dials inward
// and never learns the hub's address.

// AcceptedExecution is what a runner records before it answers 202.
type AcceptedExecution struct {
	ID       string `json:"id"`
	FlowName string `json:"flow_name"`
	Route    string `json:"route,omitempty"`
	// Principal is what the GATEWAY said about the caller, passed through
	// unaltered — the runner does not invent identity.
	Principal string `json:"principal,omitempty"`
	// TokenSHA256 is set only for an anonymous route, where the status URL is
	// a capability URL. The token itself never leaves the runner.
	TokenSHA256 string `json:"token_sha256,omitempty"`
	TTLSeconds  int64  `json:"ttl_seconds,omitempty"`
}

// ExecutionOutcome finalises a status row.
type ExecutionOutcome struct {
	State      string     `json:"state"`
	RecordsIn  int64      `json:"records_in,omitempty"`
	RecordsOut int64      `json:"records_out,omitempty"`
	ErrorStep  string     `json:"error_step,omitempty"`
	ErrorCode  string     `json:"error_code,omitempty"`
	Error      string     `json:"error,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// ExecutionStatus is what a caller sees. Metadata only — never payload.
type ExecutionStatus struct {
	Task       string     `json:"task"`
	Flow       string     `json:"flow"`
	State      string     `json:"state"`
	RecordsIn  int64      `json:"records_in"`
	RecordsOut int64      `json:"records_out"`
	ErrorStep  string     `json:"error_step,omitempty"`
	ErrorCode  string     `json:"error_code,omitempty"`
	Error      string     `json:"error,omitempty"`
	AcceptedAt time.Time  `json:"accepted_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// ErrExecutionIDTaken means the minted id already exists. The caller mints
// another rather than overwriting: two requests sharing one status row would
// report each other's outcome.
var ErrExecutionIDTaken = errors.New("hubclient: execution id already taken")

// ErrExecutionNotFound is the single answer to every refusal — unknown id,
// wrong route, wrong principal, wrong capability token. Distinguishing them
// would confirm that someone else's task exists under that id.
var ErrExecutionNotFound = errors.New("hubclient: no such execution")

// ErrExecutionGone means the status has already been read and is awaiting the
// sweeper.
var ErrExecutionGone = errors.New("hubclient: execution status already read")

// AcceptExecution records an accepted request so its status URL resolves the
// instant the caller receives it.
func (c *Client) AcceptExecution(ctx context.Context, e AcceptedExecution) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/execution-status", string(raw))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusCreated:
		return nil
	case http.StatusConflict:
		return ErrExecutionIDTaken
	default:
		return fmt.Errorf("hubclient: accept execution: %s", readErr(resp))
	}
}

// FinishExecution moves a status row to its terminal state.
func (c *Client) FinishExecution(ctx context.Context, id string, out ExecutionOutcome) error {
	raw, err := json.Marshal(out)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodPost,
		"/api/v1/execution-status/"+url.PathEscape(id)+"/finish", string(raw))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		return ErrExecutionNotFound
	default:
		return fmt.Errorf("hubclient: finish execution: %s", readErr(resp))
	}
}

// ExecutionStatusByID reads a status on a caller's behalf. route, principal and
// tokenSHA256 are what the GATEWAY established about the caller; the hub does
// the authorising with them.
func (c *Client) ExecutionStatusByID(ctx context.Context, id, route, principal, tokenSHA256 string) (*ExecutionStatus, error) {
	q := url.Values{}
	q.Set("route", route)
	q.Set("principal", principal)
	if tokenSHA256 != "" {
		q.Set("token_sha256", tokenSHA256)
	}
	resp, err := c.do(ctx, http.MethodGet,
		"/api/v1/execution-status/"+url.PathEscape(id)+"?"+q.Encode(), "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, ErrExecutionNotFound
	case http.StatusGone:
		return nil, ErrExecutionGone
	default:
		return nil, fmt.Errorf("hubclient: execution status: %s", readErr(resp))
	}
	var out ExecutionStatus
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return nil, fmt.Errorf("hubclient: execution status: %w", err)
	}
	return &out, nil
}
