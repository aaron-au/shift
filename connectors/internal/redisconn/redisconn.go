// Package redisconn is the Redis connector: one canvas node whose verb picks
// the action. get is a source (SCAN a key pattern, emit one record per key with
// its decoded value); set is a sink (one record -> SET key value, optional TTL);
// delete is a config-driven source (DEL the configured key, emit one status
// record). Credentials (password) arrive already-resolved as plaintext — the
// runner resolves {"$secret":...} refs before spawn (ADR-0010); this connector
// only tags the secret field in its schema and never logs its value.
//
// The go-redis surface is confined to client.go behind the redisClient
// interface, so every verb's logic is unit-tested against an in-memory fake
// with no real broker (see redisconn_test.go).
package redisconn

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/sdk"
)

// Connector returns the redis connector definition. A connector is one node;
// the author picks a verb and the node's role follows the verb's direction
// (ADR-0024): get/delete are sources, set is the sink.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "redis",
		Version: "0.1.0",
		Meta: &sdk.ConnectorMeta{
			Description: "Redis key/value operations: get (SCAN a pattern, emit key/value/type), set (record -> SET with optional TTL), delete (DEL a key). Network-guarded.",
			Category:    "database",
			Icon:        "🧠",
			Tags:        []string{"redis", "kv", "cache", "key-value"},
		},
		Sources: map[string]func() sdk.SourceAction{
			"get":    func() sdk.SourceAction { return &getSource{open: openClient} },
			"delete": func() sdk.SourceAction { return &deleteSource{open: openClient} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"set": func() sdk.SinkAction { return &setSink{open: openClient} },
		},
		Schemas: map[string][]byte{
			"get":    []byte(getConfigSchema),
			"set":    []byte(setConfigSchema),
			"delete": []byte(deleteConfigSchema),
		},
	}
}

// connProps is the shared connection portion of every action's schema. The
// password field carries x-shift-secret so the studio offers a secret picker.
const connProps = `
    "addr": {"type": "string", "title": "Address", "description": "Redis server host:port (e.g. redis:6379)"},
    "username": {"type": "string", "title": "Username", "description": "ACL username (Redis 6+); leave blank for the default user"},
    "password": {"type": "string", "title": "Password", "x-shift-secret": true},
    "db": {"type": "integer", "title": "Database", "default": 0},
    "tls": {"type": "boolean", "title": "Use TLS", "default": false},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal targets (network guard off)", "default": false}`

var (
	getConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"Redis get",
  "required":["addr"],"properties":{` + connProps + `,
    "pattern": {"type": "string", "title": "Key pattern", "description": "SCAN MATCH glob (e.g. user:*); defaults to * (all keys)", "default": "*"}
  }}`

	setConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"Redis set",
  "required":["addr"],"properties":{` + connProps + `,
    "key": {"type": "string", "title": "Key", "description": "Static key for every record; leave blank to read the key from each record's \"key\" field"},
    "value_field": {"type": "string", "title": "Value field", "description": "Record field holding the value to store", "default": "value"},
    "ttl_seconds": {"type": "integer", "title": "TTL (seconds)", "description": "Optional expiry; 0 = no expiry", "default": 0}
  }}`

	deleteConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"Redis delete",
  "required":["addr","key"],"properties":{` + connProps + `,
    "key": {"type": "string", "title": "Key", "description": "Key to DEL (idempotent: missing key = 0 deleted, still ok)"}
  }}`
)

// config is the shared source/sink configuration. Secrets (password) arrive
// already resolved; the field is read, never logged.
type config struct {
	Addr       string `json:"addr"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	DB         int    `json:"db"`
	Pattern    string `json:"pattern"`     // get: SCAN MATCH glob
	Key        string `json:"key"`         // set: static key (optional); delete: key (required)
	ValueField string `json:"value_field"` // set: record field holding the value
	TTLSeconds int    `json:"ttl_seconds"` // set: optional expiry
	TLS        bool   `json:"tls"`
	AllowLocal bool   `json:"allow_local"`
}

// parseConfig unmarshals and validates the connection fields shared by every
// action. Action-specific requirements are checked by each Open via the helpers.
func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("redis: bad config: %w", err)
	}
	return into.validateConn()
}

func (c *config) validateConn() error {
	if c.Addr == "" {
		return errors.New("redis: addr is required")
	}
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return fmt.Errorf("redis: addr must be host:port: %w", err)
	}
	if c.DB < 0 {
		return errors.New("redis: db must be >= 0")
	}
	return nil
}

// requirePattern defaults the get SCAN pattern to * (all keys).
func (c *config) requirePattern() error {
	if c.Pattern == "" {
		c.Pattern = "*"
	}
	return nil
}

// requireValueField defaults the set value field to "value".
func (c *config) requireValueField() error {
	if c.ValueField == "" {
		c.ValueField = "value"
	}
	return nil
}

// requireKey validates the delete config: a key to remove.
func (c *config) requireKey() error {
	if c.Key == "" {
		return errors.New("redis: key is required")
	}
	return nil
}

func (c *config) ttl() time.Duration { return time.Duration(c.TTLSeconds) * time.Second }

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), parsed once. It is not
// covered by net.IP.IsPrivate but is an internal-network range (and hosts some
// clouds' metadata endpoints), so the guard blocks it alongside RFC1918/ULA.
var cgNAT = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// guard returns a net.Dialer.Control hook that refuses loopback/link-local and
// (unless allowLocal) private/internal targets, evaluated on the concrete
// post-DNS IP so a rebind can't slip past. Mirrors the http/sftp connectors'
// SSRF guard: on a shared/cloud runner an attacker-influenced addr must not
// reach internal services or the cloud metadata endpoint. A self-hosted runner
// pointing at an internal Redis sets allow_local.
func guard(allowLocal bool) func(string, string, syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("redis: bad dial address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("redis: unresolvable address %q", host)
		}
		switch {
		case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
			if allowLocal {
				return nil
			}
			return fmt.Errorf("redis: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
		case ip.IsPrivate(), cgNAT.Contains(ip):
			if allowLocal {
				return nil
			}
			return fmt.Errorf("redis: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
		}
		return nil
	}
}

// valueToString renders a scalar record value as a Redis string. Redis stores
// bytes, so composite values (map/list) are rejected here — the set verb keeps
// string values first-class (ADR-0024 base-connector scope).
func valueToString(v record.Value) (string, error) {
	switch v.Kind() {
	case record.KindString, record.KindBytes:
		return v.String(), nil
	case record.KindInt:
		return strconv.FormatInt(v.Int(), 10), nil
	case record.KindFloat:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64), nil
	case record.KindDecimal, record.KindTimestamp, record.KindDate, record.KindTime:
		// The canonical text, so a decimal stored in Redis keeps every digit
		// its scale claims rather than arriving as a rounded float.
		return v.Text(), nil
	case record.KindBool:
		return strconv.FormatBool(v.Bool()), nil
	case record.KindNull:
		return "", nil
	default:
		return "", fmt.Errorf("redis: value must be a scalar, got %s", v.Kind())
	}
}
