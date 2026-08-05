package config_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aaron-au/shift/gateway/internal/config"
)

func valid() *config.Config {
	return &config.Config{Version: 1, Routes: []config.Route{
		{Path: "/hook", Flow: "orders", Group: "prod"},
	}}
}

func TestValidateAcceptsAWorkableConfig(t *testing.T) {
	if err := valid().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// The hub validates first and is the authority; this is the second line,
// because the blast radius of a bad config here is "the internet cannot reach
// us".
func TestValidateRejectsUnserveableConfigs(t *testing.T) {
	cases := map[string]*config.Config{
		"no routes":      {Version: 1},
		"relative path":  {Version: 1, Routes: []config.Route{{Path: "hook", Flow: "f", Group: "g"}}},
		"no flow":        {Version: 1, Routes: []config.Route{{Path: "/h", Group: "g"}}},
		"no group":       {Version: 1, Routes: []config.Route{{Path: "/h", Flow: "f"}}},
		"lowercase verb": {Version: 1, Routes: []config.Route{{Path: "/h", Flow: "f", Group: "g", Method: "post"}}},
		"bad cidr":       {Version: 1, Routes: []config.Route{{Path: "/h", Flow: "f", Group: "g", AllowCIDRs: []string{"10.0.0.0"}}}},
		"negative cap":   {Version: 1, Routes: []config.Route{{Path: "/h", Flow: "f", Group: "g", MaxBodyBytes: -1}}},
		"bad proxy cidr": {Version: 1, Routes: []config.Route{{Path: "/h", Flow: "f", Group: "g"}}, TrustedProxies: []string{"nope"}},
		"duplicate route": {Version: 1, Routes: []config.Route{
			{Path: "/h", Flow: "a", Group: "g"}, {Path: "/h", Flow: "b", Group: "g"},
		}},
	}
	for name, c := range cases {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
}

// Believing a forwarded header from an arbitrary caller would let anyone claim
// any source address and walk through every allowlist — the allowlist would
// look enforced and be worthless.
func TestClientIPIgnoresForwardedHeadersFromUntrustedPeers(t *testing.T) {
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.9:1234"
	r.Header.Set("X-Forwarded-For", "10.0.0.5")

	if got := config.ClientIP(r, nil); got != "203.0.113.9" {
		t.Fatalf("ClientIP = %q, want the real peer when no proxy is trusted", got)
	}
	trusted := (&config.Config{TrustedProxies: []string{"192.0.2.0/24"}}).TrustedProxyNets()
	if got := config.ClientIP(r, trusted); got != "203.0.113.9" {
		t.Fatalf("ClientIP = %q, want the real peer — 203.0.113.9 is not a trusted proxy", got)
	}
}

// From a trusted proxy the left-most entry is the original client, which is
// how the gateway runs behind an F5/ALB (ADR-0038 §6 upstream-tls).
func TestClientIPHonoursForwardedHeadersFromTrustedProxies(t *testing.T) {
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	r.RemoteAddr = "192.0.2.7:443"
	r.Header.Set("X-Forwarded-For", "10.0.0.5, 192.0.2.7")
	trusted := (&config.Config{TrustedProxies: []string{"192.0.2.0/24"}}).TrustedProxyNets()

	if got := config.ClientIP(r, trusted); got != "10.0.0.5" {
		t.Fatalf("ClientIP = %q, want the left-most forwarded entry", got)
	}
}

func TestAllowedIsPermissiveOnlyWhenNoAllowlistIsSet(t *testing.T) {
	open := config.Route{}
	if !open.Allowed("203.0.113.9") {
		t.Error("an empty allowlist must allow everything")
	}
	restricted := config.Route{AllowCIDRs: []string{"10.0.0.0/8"}}
	if restricted.Allowed("203.0.113.9") {
		t.Error("an address outside the allowlist was allowed")
	}
	if !restricted.Allowed("10.1.2.3") {
		t.Error("an address inside the allowlist was blocked")
	}
	if restricted.Allowed("not-an-ip") {
		t.Error("an unparseable address was allowed")
	}
}
