// Package hubclient is the runner's HTTP client for the hub control API
// (ADR-0009): registration, lease claims, heartbeats, and result
// reporting. Control-plane only — payload data never passes through it.
package hubclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrLeaseLost mirrors the hub's 409: this runner no longer holds the
// task's lease and must abandon reporting for it.
var ErrLeaseLost = errors.New("hubclient: lease lost")

// Client talks to one hub as one registered runner.
type Client struct {
	base   string
	secret string
	hc     *http.Client
}

// New builds a client from a registered runner's bearer secret.
func New(baseURL, secret string) *Client {
	return &Client{
		base:   strings.TrimRight(baseURL, "/"),
		secret: secret,
		// Generous timeout: lease long-polls hold the connection ~30s.
		hc: &http.Client{Timeout: 90 * time.Second},
	}
}

// Register consumes a single-use registration token and returns the
// runner's issued identity plus a ready-to-use client.
func Register(ctx context.Context, baseURL, token, name string) (runnerID string, c *Client, err error) {
	body, _ := json.Marshal(map[string]string{"token": token, "name": name})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/api/v1/runners/register", bytes.NewReader(body))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("hubclient: register: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		return "", nil, fmt.Errorf("hubclient: register: %s", readErr(resp))
	}
	var out struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", nil, err
	}
	return out.RunnerID, New(baseURL, out.Secret), nil
}

// LeasedTask is one claimed queue entry.
type LeasedTask struct {
	ID             string          `json:"id"`
	FlowName       string          `json:"flow_name"`
	FlowVersion    int             `json:"flow_version"`
	Document       json.RawMessage `json:"document"`
	IdempotencyKey string          `json:"idempotency_key"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
}

// Lease long-polls for work. Returns (nil, nil) when the queue stayed
// empty for the wait window.
func (c *Client) Lease(ctx context.Context, wait time.Duration) (*LeasedTask, time.Duration, error) {
	body := fmt.Sprintf(`{"wait_seconds":%d}`, int(wait.Seconds()))
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/lease", body)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, 0, nil
	case http.StatusOK:
		var out struct {
			Task            LeasedTask `json:"task"`
			LeaseTTLSeconds int        `json:"lease_ttl_seconds"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, 0, err
		}
		return &out.Task, time.Duration(out.LeaseTTLSeconds) * time.Second, nil
	default:
		return nil, 0, fmt.Errorf("hubclient: lease: %s", readErr(resp))
	}
}

// Heartbeat extends the lease on a running task.
func (c *Client) Heartbeat(ctx context.Context, taskID string) error {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/tasks/"+taskID+"/heartbeat", "")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return ErrLeaseLost
	default:
		return fmt.Errorf("hubclient: heartbeat: %s", readErr(resp))
	}
}

// Result is the execution report attached to a completed task.
type Result struct {
	RecordsIn     int64    `json:"records_in"`
	RecordsOut    int64    `json:"records_out"`
	SinkConfirmed int64    `json:"sink_confirmed,omitempty"`
	Ops           []OpStat `json:"ops,omitempty"`
	RunnerTaskID  string   `json:"runner_task_id,omitempty"`
}

// OpStat mirrors the runner's per-operator stats.
type OpStat struct {
	Name       string  `json:"name"`
	RecordsIn  int64   `json:"records_in"`
	RecordsOut int64   `json:"records_out"`
	Seconds    float64 `json:"seconds"`
}

// Complete reports success.
func (c *Client) Complete(ctx context.Context, taskID string, res Result) error {
	raw, err := json.Marshal(res)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/tasks/"+taskID+"/complete", string(raw))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return ErrLeaseLost
	default:
		return fmt.Errorf("hubclient: complete: %s", readErr(resp))
	}
}

// Fail reports a failed attempt; the hub decides requeue vs terminal.
func (c *Client) Fail(ctx context.Context, taskID, msg string) error {
	raw, _ := json.Marshal(map[string]string{"error": msg})
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/tasks/"+taskID+"/fail", string(raw))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return ErrLeaseLost
	default:
		return fmt.Errorf("hubclient: fail: %s", readErr(resp))
	}
}

func (c *Client) do(ctx context.Context, method, path, body string) (*http.Response, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.hc.Do(req)
}

// readErr extracts the hub's {"error": ...} body for error messages.
func readErr(resp *http.Response) string {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &e) == nil && e.Error != "" {
		return fmt.Sprintf("%d: %s", resp.StatusCode, e.Error)
	}
	return fmt.Sprintf("%d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
}
