// Package config is the gateway's configuration model — everything the hub
// pushes (ADR-0038 §6).
//
// The gateway owns NO policy. Routes, allowlists, rate limits, TLS mode and
// certificate material all arrive from the hub and live only in memory. What
// may sit in a local file is facts about the host — listen addresses, the
// identity bundle path, log level — and nothing else. The moment a route or an
// allowlist lands locally there are two sources of truth, and the failure mode
// is serving stale policy instead of a clean 503.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// Config is one complete gateway configuration. The hub sends it whole; the
// gateway swaps it atomically. Partial updates are deliberately not modelled —
// a half-applied policy is worse than a stale one.
type Config struct {
	// Version lets the gateway report what it is running so drift is visible
	// from the hub, which is where the administrator actually is.
	Version int64   `json:"version"`
	Routes  []Route `json:"routes"`
	// Runners is the hub's roster: which runners may serve this gateway and
	// what each one IS (ADR-0041 §3). Labels come from here, never from a
	// runner's poll — see roster.go.
	Runners []Runner `json:"runners,omitempty"`
	// TrustedProxies are the CIDRs whose X-Forwarded-* headers are believed.
	// Empty means believe none, which is the safe default: a spoofable
	// forwarded header would defeat every per-route IP allowlist below.
	TrustedProxies []string `json:"trusted_proxies,omitempty"`
}

// Route maps a public path to a flow, and to the runners allowed to run it
// (ADR-0030 placement — eligibility is the hub's, availability is whoever is
// polling).
type Route struct {
	Path   string `json:"path"`
	Flow   string `json:"flow"`
	Method string `json:"method,omitempty"` // empty = any

	// Selector names the runners eligible to serve this route by LABEL SET
	// rather than by a single group name (ADR-0038 §5): a single string
	// cannot express "any production API runner", which is the shape real
	// fleets have. Empty matches any runner.
	Selector Selector `json:"selector,omitempty"`

	// AuthTokenSHA256 is the hex SHA-256 of the caller's bearer token. The
	// token itself is never stored or logged, and comparison is
	// constant-time — the gateway authenticates callers, it does not mint
	// identities.
	AuthTokenSHA256 string `json:"auth_token_sha256,omitempty"`

	// AuthPrincipal is WHO that credential belongs to — the name stamped on
	// X-Shift-Principal for the runner (ADR-0038 §4b). It travels with the
	// verification material so "who" is a configured fact rather than
	// something each auth method has to derive its own way; that is what lets
	// a certificate-authenticated caller be identified without the runner
	// touching any PKI.
	AuthPrincipal string `json:"auth_principal,omitempty"`

	// AllowCIDRs restricts callers by source address. Empty allows any.
	AllowCIDRs []string `json:"allow_cidrs,omitempty"`
	// RequireHeaders must all be present (value empty = presence only).
	RequireHeaders map[string]string `json:"require_headers,omitempty"`
	// MaxBodyBytes caps the request body. Zero uses the gateway default;
	// unbounded is not offered, because an unbounded body on the public edge
	// is a memory-exhaustion primitive.
	MaxBodyBytes int64 `json:"max_body_bytes,omitempty"`
}

// Validate rejects a configuration the gateway cannot serve. The hub validates
// too and is the authority; this is the second line, because the blast radius
// of a bad config here is "the internet cannot reach us".
func (c *Config) Validate() error {
	if len(c.Routes) == 0 {
		return errors.New("config: no routes")
	}
	seen := make(map[string]bool, len(c.Routes))
	for i := range c.Routes {
		r := &c.Routes[i]
		switch {
		case r.Path == "" || !strings.HasPrefix(r.Path, "/"):
			return fmt.Errorf("config: route %d: path must start with /", i)
		case r.Flow == "":
			return fmt.Errorf("config: route %q: no flow", r.Path)
		}
		if err := r.Selector.Validate(); err != nil {
			return fmt.Errorf("config: route %q: %w", r.Path, err)
		}
		key := r.Method + " " + r.Path
		if seen[key] {
			return fmt.Errorf("config: duplicate route %q", key)
		}
		seen[key] = true
		if r.Method != "" && r.Method != strings.ToUpper(r.Method) {
			return fmt.Errorf("config: route %q: method %q must be upper case", r.Path, r.Method)
		}
		for _, c := range r.AllowCIDRs {
			if _, _, err := net.ParseCIDR(c); err != nil {
				return fmt.Errorf("config: route %q: bad CIDR %q: %w", r.Path, c, err)
			}
		}
		if r.MaxBodyBytes < 0 {
			return fmt.Errorf("config: route %q: negative max_body_bytes", r.Path)
		}
	}
	for _, p := range c.TrustedProxies {
		if _, _, err := net.ParseCIDR(p); err != nil {
			return fmt.Errorf("config: bad trusted proxy CIDR %q: %w", p, err)
		}
	}
	return c.validateRunners()
}

// Lookup finds the route serving a request, or nil. Method-specific routes win
// over method-agnostic ones so a narrower rule is never shadowed.
func (c *Config) Lookup(method, path string) *Route {
	var wildcard *Route
	for i := range c.Routes {
		r := &c.Routes[i]
		if r.Path != path {
			continue
		}
		if r.Method == method {
			return r
		}
		if r.Method == "" {
			wildcard = r
		}
	}
	return wildcard
}

// TrustedProxyNets parses TrustedProxies. Validate has already rejected
// malformed entries, so anything unparseable here is skipped rather than
// failing a request.
func (c *Config) TrustedProxyNets() []*net.IPNet {
	out := make([]*net.IPNet, 0, len(c.TrustedProxies))
	for _, p := range c.TrustedProxies {
		if _, n, err := net.ParseCIDR(p); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// ClientIP resolves the caller's address, honouring X-Forwarded-For ONLY when
// the immediate peer is a trusted proxy.
//
// This is a correctness constraint, not hardening. Believing a forwarded
// header from an arbitrary caller would let anyone claim any source address
// and walk straight through every IP allowlist the routes declare — the
// allowlist would look enforced and be worthless.
func ClientIP(r *http.Request, trusted []*net.IPNet) string {
	peer := peerIP(r.RemoteAddr)
	if len(trusted) == 0 || !ipIn(peer, trusted) {
		return peer
	}
	fwd := r.Header.Get("X-Forwarded-For")
	if fwd == "" {
		return peer
	}
	// Left-most entry is the original client; the rest are proxies.
	if i := strings.IndexByte(fwd, ','); i >= 0 {
		fwd = fwd[:i]
	}
	return strings.TrimSpace(fwd)
}

func peerIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func ipIn(ip string, nets []*net.IPNet) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}

// Allowed reports whether ip satisfies the route's allowlist. An empty
// allowlist allows everything.
func (r *Route) Allowed(ip string) bool {
	if len(r.AllowCIDRs) == 0 {
		return true
	}
	nets := make([]*net.IPNet, 0, len(r.AllowCIDRs))
	for _, c := range r.AllowCIDRs {
		if _, n, err := net.ParseCIDR(c); err == nil {
			nets = append(nets, n)
		}
	}
	return ipIn(ip, nets)
}
