// Package smtpconn is the SMTP connector: a sink that sends one email per
// incoming record over SMTP (AUTH + STARTTLS via net/smtp). The subject and
// body are $field templates substituted from each record; with no body
// template the record is rendered as the body. Credentials (username/password)
// arrive already-resolved as plaintext — the runner resolves {"$secret":...}
// refs before spawn (ADR-0010); this connector only tags secret fields in its
// schema and never logs their values.
package smtpconn

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
	"time"

	"github.com/aaron-au/shift/sdk"
)

// Connector returns the smtp connector definition. One canvas node, one verb:
// send (a sink — it consumes the flowing records and emits an email per
// record). ADR-0024: the node's sink role follows the action's direction.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "smtp",
		Version: "0.1.0",
		Meta: &sdk.ConnectorMeta{
			Description: "Send email over SMTP (AUTH + STARTTLS). One email per incoming record; subject/body are $field templates. Network-guarded.",
			Category:    "messaging",
			Icon:        "✉️",
			Tags:        []string{"smtp", "email", "mail", "notify"},
		},
		Sinks: map[string]func() sdk.SinkAction{
			"send": func() sdk.SinkAction { return &sendSink{} },
		},
		Schemas: map[string][]byte{
			"send": []byte(sendConfigSchema),
		},
	}
}

// sendConfigSchema is the JSON Schema (draft-07 subset) for the send config.
// Secret-typed fields carry x-shift-secret so the studio offers a secret
// picker; the runner substitutes the resolved value into the config before
// spawn.
const sendConfigSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "title": "Send email (SMTP)",
  "required": ["host", "from", "to"],
  "properties": {
    "host": {"type": "string", "title": "SMTP host", "description": "Mail server hostname or IP"},
    "port": {"type": "integer", "title": "Port", "default": 587},
    "username": {"type": "string", "title": "Username", "x-shift-secret": true},
    "password": {"type": "string", "title": "Password", "x-shift-secret": true},
    "from": {"type": "string", "title": "From", "description": "Sender address (may include a display name)"},
    "to": {"type": "array", "title": "To", "items": {"type": "string"}, "description": "Recipient addresses"},
    "cc": {"type": "array", "title": "Cc", "items": {"type": "string"}},
    "subject": {"type": "string", "title": "Subject", "description": "Supports $field placeholders substituted from each record"},
    "body_template": {"type": "string", "title": "Body template", "description": "Supports $field placeholders; if empty the whole record is rendered as the body"},
    "starttls": {"type": "boolean", "title": "STARTTLS", "description": "Upgrade the connection with STARTTLS before AUTH (recommended)", "default": true},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal targets (network guard off; also permits sending without STARTTLS)", "default": false},
    "timeout_seconds": {"type": "integer", "title": "Connect timeout (seconds)", "default": 30}
  }
}`

// config is the send action's configuration.
type config struct {
	Host     string   `json:"host"`
	Port     int      `json:"port"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	From     string   `json:"from"`
	To       []string `json:"to"`
	Cc       []string `json:"cc"`
	Subject  string   `json:"subject"`
	// BodyTemplate is the $field-templated body; empty renders the record.
	BodyTemplate string `json:"body_template"`
	// StartTLS defaults to true when omitted (encrypted by default): nil means
	// "upgrade with STARTTLS if the server advertises it".
	StartTLS       *bool `json:"starttls"`
	AllowLocal     bool  `json:"allow_local"`
	TimeoutSeconds int   `json:"timeout_seconds"`
}

// parseConfig unmarshals and validates the send configuration.
func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("smtp: bad config: %w", err)
	}
	return into.validate()
}

func (c *config) validate() error {
	if c.Host == "" {
		return errors.New("smtp: host is required")
	}
	if c.From == "" {
		return errors.New("smtp: from is required")
	}
	if len(c.To) == 0 {
		return errors.New("smtp: at least one to recipient is required")
	}
	if c.Password != "" && c.Username == "" {
		return errors.New("smtp: password set without a username")
	}
	if c.Port == 0 {
		c.Port = 587
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 30
	}
	return nil
}

// startTLS reports whether STARTTLS should be attempted (default on).
func (c *config) startTLS() bool { return c.StartTLS == nil || *c.StartTLS }

func (c *config) timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// recipients is the SMTP envelope recipient list: every To and Cc address.
func (c *config) recipients() []string {
	rcpts := make([]string, 0, len(c.To)+len(c.Cc))
	rcpts = append(rcpts, c.To...)
	rcpts = append(rcpts, c.Cc...)
	return rcpts
}

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), parsed once. It is
// not covered by net.IP.IsPrivate but is an internal-network range (and hosts
// some clouds' metadata endpoints), so the guard blocks it too.
var _, cgNAT, _ = net.ParseCIDR("100.64.0.0/10")

// guard returns a net.Dialer.Control hook that refuses loopback/link-local and
// (unless allowLocal) private/internal targets, evaluated on the concrete
// post-DNS IP so a rebind can't slip past. Mirrors the http connector's SSRF
// guard: on a shared/cloud runner an attacker-influenced host must not reach
// internal services or a metadata endpoint. On-prem relays set allow_local.
func guard(allowLocal bool) func(string, string, syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("smtp: bad dial address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("smtp: unresolvable address %q", host)
		}
		switch {
		case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
			if allowLocal {
				return nil
			}
			return fmt.Errorf("smtp: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
		case ip.IsPrivate(), cgNAT.Contains(ip):
			if allowLocal {
				return nil
			}
			return fmt.Errorf("smtp: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
		}
		return nil
	}
}

func (c *config) addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }
