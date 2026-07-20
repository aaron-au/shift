package hubclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// HTTPClient builds the client used to reach the hub, trusting an extra
// CA file when given (the compose bundle's self-signed hub cert).
func HTTPClient(caFile string) (*http.Client, error) {
	if caFile == "" {
		return &http.Client{Timeout: 90 * time.Second}, nil
	}
	pem, err := os.ReadFile(caFile) //nolint:gosec // G304: operator-configured CA path (flag/env)
	if err != nil {
		return nil, fmt.Errorf("hubclient: CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("hubclient: CA file %s: no certificates found", caFile)
	}
	return &http.Client{
		Timeout: 90 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}, nil
}

// WithHTTPClient swaps the client's transport (CA trust).
func (c *Client) WithHTTPClient(hc *http.Client) *Client {
	c.hc = hc
	return c
}

// Credentials is a runner's persisted hub identity. Registration tokens
// are single-use, so a restarted runner MUST reuse its issued secret
// instead of re-registering.
type Credentials struct {
	HubURL   string `json:"hub_url"`
	RunnerID string `json:"runner_id"`
	Secret   string `json:"secret"`
}

// LoadCredentials reads a previously saved identity ("" path or missing
// file → zero Credentials, no error).
func LoadCredentials(path string) (Credentials, error) {
	var c Credentials
	if path == "" {
		return c, nil
	}
	raw, err := os.ReadFile(path) //nolint:gosec // G304: operator-configured credentials path (flag/env)
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return Credentials{}, fmt.Errorf("hubclient: credentials file %s: %w", path, err)
	}
	return c, nil
}

// SaveCredentials persists the identity, 0600.
func SaveCredentials(path string, c Credentials) error {
	raw, err := json.Marshal(c) //nolint:gosec // G117: persisting the bearer secret IS this file's purpose (0600; reg tokens are single-use)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

// Connect resolves a runner's hub client: saved credentials when
// present, otherwise it registers with the single-use token (retrying
// while the hub boots — compose ordering) and persists the result.
func Connect(ctx context.Context, hc *http.Client, hubURL, credFile, token, name string) (string, *Client, error) {
	if creds, err := LoadCredentials(credFile); err != nil {
		return "", nil, err
	} else if creds.Secret != "" && creds.HubURL == hubURL {
		return creds.RunnerID, New(hubURL, creds.Secret).WithHTTPClient(hc), nil
	}
	if token == "" {
		return "", nil, errors.New("hubclient: no saved credentials and no registration token")
	}

	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for attempt := 1; ; attempt++ {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		id, client, err := registerWith(rctx, hc, hubURL, token, name)
		cancel()
		if err == nil {
			if credFile != "" {
				if err := SaveCredentials(credFile, Credentials{HubURL: hubURL, RunnerID: id, Secret: client.secret}); err != nil {
					return "", nil, err
				}
			}
			return id, client.WithHTTPClient(hc), nil
		}
		lastErr = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			return "", nil, fmt.Errorf("hubclient: registration failed after %d attempts: %w", attempt, lastErr)
		}
		log.Printf("hubclient: registration attempt %d: %v — retrying", attempt, err)
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
