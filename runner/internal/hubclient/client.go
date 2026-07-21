// Package hubclient is the runner's HTTP client for the hub control API
// (ADR-0009): registration, lease claims, heartbeats, and result
// reporting. Control-plane only — payload data never passes through it.
package hubclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
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
	return registerWith(ctx, http.DefaultClient, baseURL, token, name)
}

func registerWith(ctx context.Context, hc *http.Client, baseURL, token, name string) (runnerID string, c *Client, err error) {
	body, _ := json.Marshal(map[string]string{"token": token, "name": name})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/api/v1/runners/register", bytes.NewReader(body))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
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
	// The returned client keeps the standard 90s-timeout transport;
	// Connect swaps in a CA-trusting one when configured.
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

// OpStat mirrors the runner's per-operator stats. Field layout matches
// task.OpStat so the lease loop can convert directly.
type OpStat struct {
	Name       string  `json:"name"`
	StepID     string  `json:"step_id,omitempty"`
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

// ExecutionReport is the metadata for a direct (push) execution the runner
// ran outside the queue (webhook / direct API, ADR-0016). No payload.
type ExecutionReport struct {
	FlowName   string     `json:"flow_name"`
	Trigger    string     `json:"trigger"` // webhook | api
	State      string     `json:"state"`   // completed | failed
	RecordsIn  int64      `json:"records_in"`
	RecordsOut int64      `json:"records_out"`
	Error      string     `json:"error,omitempty"`
	Started    *time.Time `json:"started_at,omitempty"`
	Finished   *time.Time `json:"finished_at,omitempty"`
}

// ReportExecution tells the hub about a direct execution (fleet load +
// history). Best-effort: callers log and move on.
func (c *Client) ReportExecution(ctx context.Context, rep ExecutionReport) error {
	raw, err := json.Marshal(rep)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/executions", string(raw))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("hubclient: report execution: %s", readErr(resp))
	}
	return nil
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

// ConnectorManifest is what a runner needs to fetch and verify one
// connector artifact (mirrors the hub's resolve response).
type ConnectorManifest struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	Digest       string `json:"digest"`        // hex SHA-256
	Signature    string `json:"signature"`     // base64 Ed25519
	PublisherKey string `json:"publisher_key"` // base64 Ed25519 public key
	SizeBytes    int64  `json:"size_bytes"`
	// Descriptor is base64 of the exact signed action-catalog bytes
	// (ADR-0018); empty for pre-descriptor (v1) artifacts. The runner
	// re-hashes it to verify the v2 manifest; it does not parse it.
	Descriptor string `json:"descriptor,omitempty"`
}

// ResolveConnector asks the hub for the named connector's manifest for
// this runner's platform (version "" = latest).
func (c *Client) ResolveConnector(ctx context.Context, name, version string) (ConnectorManifest, error) {
	path := fmt.Sprintf("/api/v1/connectors/%s/resolve?version=%s&os=%s&arch=%s",
		url.PathEscape(name), url.QueryEscape(version), runtime.GOOS, runtime.GOARCH)
	resp, err := c.do(ctx, http.MethodGet, path, "")
	if err != nil {
		return ConnectorManifest{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return ConnectorManifest{}, fmt.Errorf("hubclient: resolve connector %s: %s", name, readErr(resp))
	}
	var m ConnectorManifest
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&m); err != nil {
		return ConnectorManifest{}, fmt.Errorf("hubclient: resolve connector %s: %w", name, err)
	}
	return m, nil
}

// FetchConnector streams the manifest's artifact into w. The caller
// verifies digest+signature — this just moves bytes.
func (c *Client) FetchConnector(ctx context.Context, m ConnectorManifest, w io.Writer) error {
	path := fmt.Sprintf("/api/v1/connectors/%s/versions/%s/artifact?os=%s&arch=%s",
		url.PathEscape(m.Name), url.PathEscape(m.Version), m.OS, m.Arch)
	resp, err := c.do(ctx, http.MethodGet, path, "")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("hubclient: fetch connector %s: %s", m.Name, readErr(resp))
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		return fmt.Errorf("hubclient: fetch connector %s: %w", m.Name, err)
	}
	return nil
}

// PublisherKeys fetches the hub's trusted signing keys (raw Ed25519
// public keys) — the runner's default trust root for artifact
// verification.
func (c *Client) PublisherKeys(ctx context.Context) ([][]byte, error) {
	resp, err := c.do(ctx, http.MethodGet, "/api/v1/publisher-keys", "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hubclient: publisher keys: %s", readErr(resp))
	}
	var out struct {
		Keys []struct {
			PublicKey string `json:"public_key"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(out.Keys))
	for _, k := range out.Keys {
		raw, err := base64.StdEncoding.DecodeString(k.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("hubclient: publisher keys: bad base64: %w", err)
		}
		keys = append(keys, raw)
	}
	return keys, nil
}

// ResolveSecrets decrypts the named secrets hub-side and returns their
// values. Callers must never log the returned map or wrap its contents
// into errors — names only.
func (c *Client) ResolveSecrets(ctx context.Context, names []string) (map[string]string, error) {
	raw, err := json.Marshal(map[string][]string{"names": names})
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/secrets/resolve", string(raw))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hubclient: resolve secrets: %s", readErr(resp))
	}
	var out struct {
		Secrets map[string]string `json:"secrets"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return nil, fmt.Errorf("hubclient: resolve secrets: %w", err)
	}
	return out.Secrets, nil
}

// WebhookConfig is a hub-authored hook the runner should serve: the name,
// the token hash to check, and the published flow document to run.
type WebhookConfig struct {
	Name      string          `json:"name"`
	TokenHash string          `json:"token_hash,omitempty"`
	Document  json.RawMessage `json:"document"`
}

// SyncWebhooks fetches the runner's webhook configs from the hub.
func (c *Client) SyncWebhooks(ctx context.Context) ([]WebhookConfig, error) {
	resp, err := c.do(ctx, http.MethodGet, "/api/v1/webhooks/sync", "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hubclient: sync webhooks: %s", readErr(resp))
	}
	var out struct {
		Webhooks []WebhookConfig `json:"webhooks"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&out); err != nil {
		return nil, fmt.Errorf("hubclient: sync webhooks: %w", err)
	}
	return out.Webhooks, nil
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

// readErr extracts the hub's error envelope (ADR-0023:
// {"error":{status,code?,message}}) for diagnostics, falling back to the raw
// body for any non-envelope response.
func readErr(resp *http.Response) string {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &e) == nil && e.Error.Message != "" {
		if e.Error.Code != "" {
			return fmt.Sprintf("%d (%s): %s", resp.StatusCode, e.Error.Code, e.Error.Message)
		}
		return fmt.Sprintf("%d: %s", resp.StatusCode, e.Error.Message)
	}
	return fmt.Sprintf("%d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
}
