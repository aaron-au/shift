// Package soapconn is the SOAP/XML connector: a single config-driven source
// verb, `call`, that POSTs a SOAP envelope to an endpoint and parses the XML
// response body into typed record batches. SOAP Faults are detected and
// surfaced as errors. Credentials arrive already-resolved as plaintext (the
// runner resolves {"$secret":...} refs before spawn — ADR-0010); this
// connector only tags secret fields in its schema and never logs them.
//
// The generic XML-element-tree → record.Builder mapping here (xml.go) is a
// deliberate seed for the engine's XML/EDI streaming format work (M1.5): a
// lossless, predictable element→map convention (attributes as "@name", text
// as "#text", repeated children as lists). WSDL operation discovery is a
// designed-but-deferred plain function (wsdl.go), a seed for ADR-0025.
package soapconn

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/aaron-au/shift/sdk"
)

// Connector returns the soap connector definition. One canvas node, one verb
// (`call`), whose declared direction is source: it performs the request and
// emits the parsed response as records (ADR-0024).
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "soap",
		Version: "0.1.0",
		Meta: &sdk.ConnectorMeta{
			Description: "Call a SOAP/XML web service: POST an envelope (with ${param} placeholders) and parse the XML response into records. Faults surface as errors. SSRF-guarded.",
			Category:    "protocol",
			Icon:        "🧼",
			Tags:        []string{"soap", "xml", "wsdl", "web-service", "rpc"},
		},
		// `call` is a config-driven source: you give it an endpoint + envelope
		// template + params, it POSTs and emits the response body's elements as
		// records. There is no sink verb — a request-reply SOAP op is a source.
		Sources: map[string]func() sdk.SourceAction{
			"call": func() sdk.SourceAction { return &callSource{} },
		},
		Schemas: map[string][]byte{
			"call": []byte(callConfigSchema),
		},
	}
}

// callConfigSchema is the JSON Schema (draft-07 subset) for the `call` config.
// Secret-typed fields carry x-shift-secret so the studio offers a secret picker.
const callConfigSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "title": "SOAP call",
  "required": ["endpoint", "envelope_template"],
  "properties": {
    "endpoint": {"type": "string", "title": "Endpoint URL", "description": "SOAP service URL (http/https)"},
    "soap_action": {"type": "string", "title": "SOAPAction", "description": "SOAPAction header value (may be empty)"},
    "soap_version": {"type": "string", "title": "SOAP version", "enum": ["1.1", "1.2"], "default": "1.1"},
    "envelope_template": {"type": "string", "title": "Envelope template", "description": "SOAP envelope XML; ${name} placeholders are filled from params (values XML-escaped)"},
    "params": {"type": "object", "title": "Parameters", "description": "Values substituted into ${name} placeholders in the template", "additionalProperties": {"type": "string"}},
    "auth": {
      "type": "object",
      "title": "Basic authentication",
      "properties": {
        "username": {"type": "string", "title": "Username", "x-shift-secret": true},
        "password": {"type": "string", "title": "Password", "x-shift-secret": true}
      }
    },
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal targets (SSRF guard off)", "default": false},
    "timeout_seconds": {"type": "integer", "title": "Timeout (seconds)", "default": 60},
    "max_response_bytes": {"type": "integer", "title": "Max response size (bytes)", "default": 16777216}
  }
}`

// defaultMaxResponseBytes bounds how much of a response body is buffered for
// parsing (the tree is built in memory). SOAP is request-reply over a single
// bounded document, not a large stream, so buffering is appropriate — but the
// bound keeps a hostile/oversized response from exhausting memory. 16 MiB.
const defaultMaxResponseBytes = 16 << 20

// config is the `call` action configuration.
type config struct {
	Endpoint         string            `json:"endpoint"`
	SOAPAction       string            `json:"soap_action"`
	SOAPVersion      string            `json:"soap_version"`
	EnvelopeTemplate string            `json:"envelope_template"`
	Params           map[string]string `json:"params"`
	Auth             struct {
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"auth"`
	// AllowLocal permits internal-network targets — loopback, link-local,
	// unspecified, RFC1918/ULA private, and CGNAT (off by default: SSRF guard,
	// mirrors the http connector, issue #5). Self-hosted runners set it to
	// reach internal SOAP services.
	AllowLocal       bool `json:"allow_local"`
	TimeoutSeconds   int  `json:"timeout_seconds"`
	MaxResponseBytes int  `json:"max_response_bytes"`
}

func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("soap: bad config: %w", err)
	}
	return into.validate()
}

func (c *config) validate() error {
	if c.Endpoint == "" {
		return errors.New("soap: endpoint is required")
	}
	if c.EnvelopeTemplate == "" {
		return errors.New("soap: envelope_template is required")
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 60
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = defaultMaxResponseBytes
	}
	switch c.SOAPVersion {
	case "", "1.1", "1.2":
	default:
		return fmt.Errorf("soap: unsupported soap_version %q (want 1.1 or 1.2)", c.SOAPVersion)
	}
	return nil
}

// contentType returns the request Content-Type for the configured SOAP version.
func (c *config) contentType() string {
	if c.SOAPVersion == "1.2" {
		return "application/soap+xml; charset=utf-8"
	}
	return "text/xml; charset=utf-8"
}

// apply sets the SOAP headers and Basic auth on the outbound request.
func (c *config) apply(req *http.Request) {
	req.Header.Set("Content-Type", c.contentType())
	// SOAP 1.1 requires a (possibly empty) quoted SOAPAction header. For 1.2
	// the action rides in the Content-Type action parameter, but a redundant
	// header is harmless and some gateways still read it.
	req.Header.Set("SOAPAction", quoteAction(c.SOAPAction))
	if c.Auth.Username != "" || c.Auth.Password != "" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}
}

// quoteAction wraps a SOAPAction in double quotes if not already quoted.
func quoteAction(a string) string {
	if strings.HasPrefix(a, `"`) && strings.HasSuffix(a, `"`) {
		return a
	}
	return `"` + a + `"`
}

// varRe matches ${name} and $name placeholders (word chars only).
var varRe = regexp.MustCompile(`\$\{(\w+)\}|\$(\w+)`)

// renderEnvelope substitutes ${name}/$name placeholders in the template with
// XML-escaped values from params. Unknown placeholders are left verbatim so a
// literal '$' in the template survives. Escaping prevents a param value from
// injecting markup into the envelope.
func renderEnvelope(tmpl string, params map[string]string) (string, error) {
	var outErr error
	out := varRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		name := strings.Trim(m, "${}")
		v, ok := params[name]
		if !ok {
			return m
		}
		var buf bytes.Buffer
		if err := xml.EscapeText(&buf, []byte(v)); err != nil {
			outErr = fmt.Errorf("soap: escaping param %q: %w", name, err)
			return m
		}
		return buf.String()
	})
	if outErr != nil {
		return "", outErr
	}
	return out, nil
}

// cgNAT is the RFC 6598 carrier-grade-NAT range (100.64.0.0/10), NOT covered by
// net.IP.IsPrivate but an internal-network range (and it hosts some cloud
// metadata endpoints), so the SSRF guard blocks it alongside RFC1918/ULA.
var cgNAT = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// client builds an http.Client whose dialer refuses internal-network targets
// unless AllowLocal is set: loopback, link-local (incl. 169.254.169.254 cloud
// metadata), unspecified, RFC1918/ULA private ranges, and CGNAT. The check runs
// post-resolution on the concrete dialed IP, so a DNS name — or a rebind —
// cannot smuggle a blocked IP past it. Mirrors the http connector's SSRF guard;
// default-deny is safe for a multi-tenant cloud hub, and a self-hosted runner
// targeting an internal service sets allow_local (issue #5).
func (c *config) client() *http.Client {
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
				return fmt.Errorf("soap: unresolvable address %q", host)
			}
			switch {
			case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
				return fmt.Errorf("soap: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
			case ip.IsPrivate(), cgNAT.Contains(ip):
				return fmt.Errorf("soap: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
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
