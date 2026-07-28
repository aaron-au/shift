// Package amqpconn is the AMQP 0-9-1 connector (RabbitMQ and compatible
// brokers). It is a single canvas node whose verb selects the role
// (ADR-0024):
//
//   - publish (SINK): publish one message per record to an exchange with a
//     routing key; the record is encoded as a JSON body with a JSON
//     content-type.
//   - consume (SOURCE): drain messages from a queue, emitting one record per
//     message ({body, routing_key, exchange, headers, delivery_tag}); each
//     message is acked after it is placed in the batch. Consumption is
//     bounded (max_messages, or the queue draining empty) so Next always
//     terminates — it never blocks waiting for new deliveries.
//
// Credentials arrive already-resolved as plaintext (the runner resolves
// {"$secret":...} refs before spawn — ADR-0010); this connector only tags
// secret fields in its schema and never logs a secret value. Broker egress is
// SSRF-guarded like the http connector: loopback/link-local/private/CGNAT
// targets are refused unless allow_local is set (fail closed).
package amqpconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"syscall"
	"time"

	"github.com/aaron-au/shift/sdk"
)

// Connector returns the amqp connector definition. One node, two verbs; the
// verb's declared direction fixes whether the node is a source or a sink.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "amqp",
		Version: "0.1.0",
		Meta: &sdk.ConnectorMeta{
			Description: "AMQP 0-9-1 (RabbitMQ): publish records to an exchange, or consume records from a queue. SSRF-guarded.",
			Category:    "messaging",
			Icon:        "🐰",
			Tags:        []string{"amqp", "rabbitmq", "queue", "messaging", "ndjson"},
		},
		Sources: map[string]func() sdk.SourceAction{
			"consume": func() sdk.SourceAction { return &consumeSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"publish": func() sdk.SinkAction { return &publishSink{} },
		},
		Schemas: map[string][]byte{
			"publish": []byte(publishSchema),
			"consume": []byte(consumeSchema),
		},
	}
}

// connProps is the shared connection portion of every verb's config schema.
// Either provide a full url (which may embed credentials, so the whole value
// is secret) or the split host/user/password fields (password secret).
const connProps = `
    "url": {"type": "string", "title": "AMQP URL", "description": "amqp://user:pass@host:port/vhost or amqps://... (overrides the split fields below)", "x-shift-secret": true},
    "host": {"type": "string", "title": "Host", "description": "Broker hostname or IP (used when url is empty)"},
    "port": {"type": "integer", "title": "Port", "description": "Default 5672 (amqp) or 5671 (amqps)"},
    "user": {"type": "string", "title": "Username", "default": "guest"},
    "password": {"type": "string", "title": "Password", "x-shift-secret": true},
    "vhost": {"type": "string", "title": "Virtual host", "default": "/"},
    "tls": {"type": "boolean", "title": "Use TLS (amqps)", "default": false},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal brokers (SSRF guard off)", "default": false},
    "timeout_seconds": {"type": "integer", "title": "Connect timeout (seconds)", "default": 30}`

var (
	publishSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"AMQP publish",
  "properties":{` + connProps + `,
    "exchange": {"type": "string", "title": "Exchange", "description": "Target exchange ('' = the default exchange, where routing_key is the queue name)"},
    "routing_key": {"type": "string", "title": "Routing key"}
  }}`

	consumeSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"AMQP consume",
  "required":["queue"],"properties":{` + connProps + `,
    "queue": {"type": "string", "title": "Queue", "description": "Queue to consume from"},
    "durable": {"type": "boolean", "title": "Declare queue durable", "default": false},
    "max_messages": {"type": "integer", "title": "Max messages", "description": "Stop after this many messages (0 = drain the queue then stop)", "default": 0},
    "prefetch": {"type": "integer", "title": "Prefetch / batch size", "description": "Messages fetched per Next batch (also the channel QoS prefetch)", "default": 256}
  }}`
)

// defaults applied when config omits them.
const (
	defaultAMQPPort  = 5672
	defaultAMQPSPort = 5671
	defaultTimeout   = 30
	defaultPrefetch  = 256
	// maxBodyDepth bounds JSON nesting when parsing a message body, so an
	// adversarial deeply-nested message cannot exhaust the stack.
	maxBodyDepth = 64
)

// Config is the shared publish/consume configuration.
type Config struct {
	URL      string `json:"url"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Vhost    string `json:"vhost"`
	TLS      bool   `json:"tls"`

	Exchange    string `json:"exchange"`     // publish
	RoutingKey  string `json:"routing_key"`  // publish
	Queue       string `json:"queue"`        // consume
	Durable     bool   `json:"durable"`      // consume
	MaxMessages int    `json:"max_messages"` // consume
	Prefetch    int    `json:"prefetch"`     // consume

	AllowLocal     bool `json:"allow_local"`
	TimeoutSeconds int  `json:"timeout_seconds"`

	// IdempotencyKey, when set (the runner injects the hub task's key), is
	// sent as each published message's MessageId suffixed with the message
	// ordinal — at-least-once re-dispatch replays the same id sequence, so an
	// idempotent broker/consumer can dedup (ADR-0002/0009). Publish only.
	IdempotencyKey string `json:"idempotency_key"`
}

// validateConn checks the connection fields shared by both verbs and fills in
// defaults. Either a url or a host is required.
func (c *Config) validateConn() error {
	if c.URL == "" && c.Host == "" {
		return errors.New("amqp: url or host is required")
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = defaultTimeout
	}
	if c.Port == 0 {
		if c.TLS {
			c.Port = defaultAMQPSPort
		} else {
			c.Port = defaultAMQPPort
		}
	}
	if c.Vhost == "" {
		c.Vhost = "/"
	}
	return nil
}

func (c *Config) timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// amqpURL returns the dial URL: the explicit url if given, else one assembled
// from the split fields (credentials URL-escaped, vhost path-escaped).
func (c *Config) amqpURL() string {
	if c.URL != "" {
		return c.URL
	}
	scheme := "amqp"
	if c.TLS {
		scheme = "amqps"
	}
	user := c.User
	if user == "" {
		user = "guest"
	}
	u := &url.URL{
		Scheme: scheme,
		User:   url.UserPassword(user, c.Password),
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   "/" + url.PathEscape(vhostPath(c.Vhost)),
	}
	return u.String()
}

// vhostPath normalises the vhost for the URL path segment: the default "/"
// vhost is encoded as an empty path segment (amqp URL convention), any other
// value is used verbatim.
func vhostPath(v string) string {
	if v == "" || v == "/" {
		return ""
	}
	return v
}

// serverName is the TLS SNI / cert host, parsed from the effective URL.
func (c *Config) serverName() string {
	if c.URL == "" {
		return c.Host
	}
	if u, err := url.Parse(c.URL); err == nil {
		return u.Hostname()
	}
	return ""
}

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), parsed once. It is
// not covered by net.IP.IsPrivate but is an internal range (and hosts some
// clouds' metadata endpoint), so the guard blocks it alongside RFC1918/ULA.
var cgNAT = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// guard returns a net.Dialer.Control hook that refuses loopback/link-local and
// (unless allowLocal) private/internal broker targets, evaluated on the
// concrete post-DNS IP so a rebind can't slip past. Mirrors the http/sftp SSRF
// guard: on a shared/cloud runner an attacker-influenced host must not reach
// internal services or the cloud metadata endpoint. On-prem brokers set
// allow_local.
func guard(allowLocal bool) func(string, string, syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("amqp: bad dial address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("amqp: unresolvable address %q", host)
		}
		switch {
		case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
			if allowLocal {
				return nil
			}
			return fmt.Errorf("amqp: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
		case ip.IsPrivate(), cgNAT.Contains(ip):
			if allowLocal {
				return nil
			}
			return fmt.Errorf("amqp: refusing %s (private/internal range; set allow_local to reach internal brokers)", ip)
		}
		return nil
	}
}

// guardedDial is the net.Dialer.Control-guarded dial function handed to the
// AMQP driver. It applies the connect timeout and, mirroring the driver's own
// default dialer, arms a handshake deadline the driver clears once the
// connection is open.
func (c *Config) guardedDial(network, addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: c.timeout(), Control: guard(c.AllowLocal)}
	conn, err := d.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(c.timeout())); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// publishChannel is the minimal broker surface the publish sink needs. The
// real implementation wraps an amqp091 connection+channel; tests supply an
// in-memory fake. Keeping the seam this narrow means unit tests never touch a
// real broker.
type publishChannel interface {
	// PublishBody publishes one message. contentType/messageID are set on the
	// AMQP message; an empty messageID is omitted.
	PublishBody(ctx context.Context, exchange, key, contentType, messageID string, body []byte) error
	Close() error
}

// consumeChannel is the minimal broker surface the consume source needs.
type consumeChannel interface {
	// GetNext fetches the next message without blocking. ok is false when the
	// queue is presently empty (basic.get semantics), which is how the source
	// detects a drained queue and terminates.
	GetNext(ctx context.Context) (delivery, bool, error)
	Close() error
}

// delivery is the decoded, driver-agnostic form of one consumed message.
type delivery struct {
	Body        []byte
	RoutingKey  string
	Exchange    string
	Headers     map[string]any // driver table (amqp091.Table); metadata only
	DeliveryTag uint64
	// Ack acknowledges the message to the broker. It is called once, after the
	// record is built into the batch.
	Ack func() error
}

// Dial seams — overridable in tests. Production code leaves them nil and the
// verb's Open binds the real amqp091-backed dialer (see driver.go).
type (
	publishDialer func(ctx context.Context, cfg *Config) (publishChannel, error)
	consumeDialer func(ctx context.Context, cfg *Config) (consumeChannel, error)
)
