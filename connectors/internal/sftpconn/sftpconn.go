// Package sftpconn is the SFTP connector: a streaming source that pulls a
// remote file and emits typed record batches, and a sink that serializes
// batches to a remote file. Records are parsed/written via engine/format
// (ndjson or csv). Credentials arrive already-resolved as plaintext (the
// runner resolves {"$secret":...} refs before spawn — ADR-0010); this
// connector only tags secret fields in its schema.
package sftpconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
	"time"

	"github.com/aaron-au/shift/sdk"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Connector returns the sftp connector definition.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "sftp",
		Version: "0.1.0",
		Meta: &sdk.ConnectorMeta{
			Description: "Pull a remote file as a record stream (source) or write records to a remote file (sink) over SFTP. Host-key verified.",
			Category:    "file-transfer",
			Icon:        "📁",
			Tags:        []string{"sftp", "ssh", "file", "ndjson", "csv"},
		},
		Sources: map[string]func() sdk.SourceAction{
			"get": func() sdk.SourceAction { return &getSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"put": func() sdk.SinkAction { return &putSink{} },
		},
		Schemas: map[string][]byte{
			"get": []byte(configSchema),
			"put": []byte(configSchema),
		},
	}
}

// configSchema is the JSON Schema (draft-07 subset) for config. Secret-typed
// fields carry x-shift-secret so the studio offers a secret picker.
const configSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "title": "SFTP",
  "required": ["host", "user", "path"],
  "properties": {
    "host": {"type": "string", "title": "Host", "description": "SFTP server hostname or IP"},
    "port": {"type": "integer", "title": "Port", "default": 22},
    "user": {"type": "string", "title": "Username"},
    "password": {"type": "string", "title": "Password", "x-shift-secret": true},
    "private_key": {"type": "string", "title": "Private key (PEM)", "x-shift-secret": true},
    "host_key": {"type": "string", "title": "Host key", "description": "Server public key (authorized_keys line, e.g. 'ssh-ed25519 AAAA...'). Required unless allow_local."},
    "path": {"type": "string", "title": "Remote path", "description": "Path to the remote file"},
    "format": {"type": "string", "title": "Format", "enum": ["ndjson", "csv"], "default": "ndjson"},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal targets (network guard off; also permits an unverified host key)", "default": false},
    "timeout_seconds": {"type": "integer", "title": "Connect timeout (seconds)", "default": 30}
  }
}`

// config is the shared source/sink configuration.
type config struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	User           string `json:"user"`
	Password       string `json:"password"`
	PrivateKey     string `json:"private_key"`
	HostKey        string `json:"host_key"`
	Path           string `json:"path"`
	Format         string `json:"format"`
	AllowLocal     bool   `json:"allow_local"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("sftp: bad config: %w", err)
	}
	return into.validate()
}

func (c *config) validate() error {
	if c.Host == "" || c.User == "" || c.Path == "" {
		return errors.New("sftp: host, user and path are required")
	}
	if c.Password == "" && c.PrivateKey == "" {
		return errors.New("sftp: password or private_key is required")
	}
	if c.Port == 0 {
		c.Port = 22
	}
	if c.Format == "" {
		c.Format = "ndjson"
	}
	if c.Format != "ndjson" && c.Format != "csv" {
		return fmt.Errorf("sftp: unsupported format %q (want ndjson or csv)", c.Format)
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 30
	}
	return nil
}

func (c *config) timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), parsed once.
var _, cgNAT, _ = net.ParseCIDR("100.64.0.0/10")

// guard returns a net.Dialer.Control hook that refuses loopback/link-local and
// (unless allowLocal) private/internal targets, evaluated on the concrete
// post-DNS IP so a rebind can't slip past. Mirrors the http connector's SSRF
// guard — on a shared/cloud runner an attacker-influenced host must not reach
// internal services or the metadata endpoint. On-prem SFTP to an internal
// server sets allow_local.
func guard(allowLocal bool) func(string, string, syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("sftp: bad dial address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("sftp: unresolvable address %q", host)
		}
		switch {
		case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
			if allowLocal {
				return nil
			}
			return fmt.Errorf("sftp: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
		case ip.IsPrivate(), cgNAT.Contains(ip):
			if allowLocal {
				return nil
			}
			return fmt.Errorf("sftp: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
		}
		return nil
	}
}

// hostKeyCallback verifies the server's key. A pinned host_key is required by
// default (fail closed); allow_local permits an unverified key for dev/loopback.
func (c *config) hostKeyCallback() (ssh.HostKeyCallback, error) {
	if c.HostKey != "" {
		pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(c.HostKey))
		if err != nil {
			return nil, fmt.Errorf("sftp: bad host_key: %w", err)
		}
		return ssh.FixedHostKey(pk), nil
	}
	if c.AllowLocal {
		return ssh.InsecureIgnoreHostKey(), nil //nolint:gosec // G106: unverified host key gated behind explicit allow_local (dev/loopback)
	}
	return nil, errors.New("sftp: host_key is required (or set allow_local for dev/loopback use)")
}

func (c *config) authMethods() ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod
	if c.Password != "" {
		methods = append(methods, ssh.Password(c.Password))
	}
	if c.PrivateKey != "" {
		signer, err := ssh.ParsePrivateKey([]byte(c.PrivateKey))
		if err != nil {
			return nil, fmt.Errorf("sftp: bad private_key: %w", err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}
	return methods, nil
}

// dial opens the SSH transport (network-guarded, host-key-verified) and an
// SFTP session over it. The returned closer tears both down.
func (c *config) dial(ctx context.Context) (*sftp.Client, func() error, error) {
	auth, err := c.authMethods()
	if err != nil {
		return nil, nil, err
	}
	hkcb, err := c.hostKeyCallback()
	if err != nil {
		return nil, nil, err
	}
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	dialer := &net.Dialer{Timeout: c.timeout(), Control: guard(c.AllowLocal)}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("sftp: dial %s: %w", addr, err)
	}
	sshConf := &ssh.ClientConfig{User: c.User, Auth: auth, HostKeyCallback: hkcb, Timeout: c.timeout()}
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, sshConf)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("sftp: ssh handshake: %w", err)
	}
	sshClient := ssh.NewClient(sshConn, chans, reqs)
	sc, err := sftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return nil, nil, fmt.Errorf("sftp: session: %w", err)
	}
	closer := func() error {
		_ = sc.Close()
		return sshClient.Close()
	}
	return sc, closer, nil
}
