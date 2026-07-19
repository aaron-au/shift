// Package httpconn is the HTTP connector: a streaming source (GET a URL,
// parse the NDJSON/JSON response into batches without buffering the body)
// and a sink (POST each batch as NDJSON).
package httpconn

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/aaron-au/shift/sdk"
)

// Connector returns the http connector definition.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "http",
		Version: "0.1.0",
		Sources: map[string]func() sdk.SourceAction{
			"get": func() sdk.SourceAction { return &getSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"post": func() sdk.SinkAction { return &postSink{} },
		},
	}
}

// commonConfig is shared by source and sink.
type commonConfig struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	// Auth: {"type":"basic","user":..,"pass":..} or
	// {"type":"bearer","token":..}.
	Auth struct {
		Type  string `json:"type"`
		User  string `json:"user"`
		Pass  string `json:"pass"`
		Token string `json:"token"`
	} `json:"auth"`
	// AllowLocal permits loopback/link-local targets (off by default:
	// SSRF guard, ADR-0007).
	AllowLocal bool `json:"allow_local"`
	// TimeoutSeconds bounds the whole request (default 300).
	TimeoutSeconds int `json:"timeout_seconds"`
}

func (c *commonConfig) validate() error {
	if c.URL == "" {
		return fmt.Errorf("http: url is required")
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 300
	}
	switch c.Auth.Type {
	case "", "basic", "bearer":
	default:
		return fmt.Errorf("http: unknown auth type %q", c.Auth.Type)
	}
	return nil
}

func (c *commonConfig) apply(req *http.Request) {
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}
	switch c.Auth.Type {
	case "basic":
		req.SetBasicAuth(c.Auth.User, c.Auth.Pass)
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+c.Auth.Token)
	}
}

// client builds an http.Client whose dialer refuses loopback and
// link-local (cloud metadata) addresses unless AllowLocal is set. The
// check runs post-resolution, so DNS names cannot smuggle a blocked IP.
func (c *commonConfig) client() *http.Client {
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			if c.AllowLocal {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf("http: unresolvable address %q", host)
			}
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
				return fmt.Errorf("http: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
			}
			return nil
		},
	}
	return &http.Client{
		Timeout: time.Duration(c.TimeoutSeconds) * time.Second,
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			MaxIdleConns:        4,
			IdleConnTimeout:     60 * time.Second,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: 15 * time.Second,
		},
	}
}

func parseConfig[T any](config []byte, into *T) error {
	if err := json.Unmarshal(config, into); err != nil {
		return fmt.Errorf("http: bad config: %w", err)
	}
	return nil
}
