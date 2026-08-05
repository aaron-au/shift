package config_test

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/aaron-au/shift/gateway/internal/config"
)

// The roster is the hub's assertion of what each runner IS (ADR-0041 §3), so a
// roster the gateway cannot read unambiguously must be refused whole rather
// than partially applied. Every case here would otherwise resolve a runner's
// placement by scan order or by accident.
func TestRosterValidationRejectsAnAmbiguousRoster(t *testing.T) {
	for _, tc := range []struct {
		name    string
		runners []config.Runner
		want    string
	}{{
		name:    "empty runner id",
		runners: []config.Runner{{ID: ""}},
		want:    "empty runner id",
	}, {
		name:    "whitespace-only runner id",
		runners: []config.Runner{{ID: "   "}},
		want:    "empty runner id",
	}, {
		// Two entries for one runner means two answers to "what is this
		// runner", and the winner would depend on scan order.
		name: "duplicate runner",
		runners: []config.Runner{
			{ID: "rnr-1", Labels: map[string]string{"environment": "development"}},
			{ID: "rnr-1", Labels: map[string]string{"environment": "production"}},
		},
		want: "duplicate roster entry",
	}, {
		// An empty label key can never be matched by a validated selector, so
		// it is silently dead weight — and dead weight in a placement rule
		// reads as placement that was applied.
		name:    "empty label key",
		runners: []config.Runner{{ID: "rnr-1", Labels: map[string]string{"": "production"}}},
		want:    "empty label key",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := &config.Config{Version: 1,
				Routes:  []config.Route{{Path: "/x", Flow: "f"}},
				Runners: tc.runners,
			}
			err := c.Validate()
			if err == nil {
				t.Fatalf("accepted a roster with %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestRosterValidationAcceptsAWellFormedRoster(t *testing.T) {
	c := &config.Config{Version: 1,
		Routes: []config.Route{{Path: "/x", Flow: "f"}},
		Runners: []config.Runner{
			{ID: "rnr-1", Labels: map[string]string{"environment": "production"}},
			{ID: "rnr-2"}, // no labels is legitimate: a runner with no placement
		},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("rejected a valid roster: %v", err)
	}
}

// LabelsFor must distinguish "vouched for, with no labels" from "not vouched
// for at all". The caller refuses the second, and label-less satisfies every
// empty selector — so collapsing the two would hand an unknown runner exactly
// the traffic nobody thought to restrict.
func TestLabelsForSeparatesUnknownFromUnlabelled(t *testing.T) {
	c := &config.Config{Runners: []config.Runner{
		{ID: "rnr-labelled", Labels: map[string]string{"environment": "production"}},
		{ID: "rnr-bare"},
	}}

	if labels, ok := c.LabelsFor("rnr-labelled"); !ok || labels["environment"] != "production" {
		t.Errorf("LabelsFor(rnr-labelled) = %v, %v; want the roster's labels", labels, ok)
	}
	if labels, ok := c.LabelsFor("rnr-bare"); !ok || len(labels) != 0 {
		t.Errorf("LabelsFor(rnr-bare) = %v, %v; want empty labels and KNOWN", labels, ok)
	}
	if _, ok := c.LabelsFor("rnr-ghost"); ok {
		t.Error("LabelsFor reported an unrostered runner as known")
	}
	// An empty id is what an unauthenticated connection resolves to, so it must
	// never match a roster entry — including one the hub never sent.
	if _, ok := c.LabelsFor(""); ok {
		t.Error("LabelsFor treated an empty (unproven) identity as known")
	}
}

// ClientIP is what per-route IP allowlists are enforced against, so where it
// reads the address from is a security decision, not a convenience.
func TestClientIPTrustsForwardedHeadersOnlyFromATrustedProxy(t *testing.T) {
	_, trustedNet, err := net.ParseCIDR("10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	trusted := []*net.IPNet{trustedNet}

	fromInternet := &http.Request{RemoteAddr: "203.0.113.9:5555", Header: http.Header{}}
	fromInternet.Header.Set("X-Forwarded-For", "10.1.2.3")
	if got := config.ClientIP(fromInternet, trusted); got != "203.0.113.9" {
		t.Errorf("ClientIP = %q, want the peer address — a spoofable header defeats every allowlist", got)
	}

	fromProxy := &http.Request{RemoteAddr: "10.4.5.6:5555", Header: http.Header{}}
	fromProxy.Header.Set("X-Forwarded-For", "198.51.100.7, 10.4.5.6")
	if got := config.ClientIP(fromProxy, trusted); got != "198.51.100.7" {
		t.Errorf("ClientIP = %q, want the first forwarded address from a trusted proxy", got)
	}

	// A trusted proxy that forwarded nothing is still just a peer.
	quiet := &http.Request{RemoteAddr: "10.4.5.6:5555", Header: http.Header{}}
	if got := config.ClientIP(quiet, trusted); got != "10.4.5.6" {
		t.Errorf("ClientIP = %q, want the peer when no header was forwarded", got)
	}

	// A malformed RemoteAddr must not panic or yield a partial address.
	bare := &http.Request{RemoteAddr: "not-an-address", Header: http.Header{}}
	if got := config.ClientIP(bare, trusted); got != "not-an-address" {
		t.Errorf("ClientIP = %q for a malformed RemoteAddr", got)
	}
}

// A TLS-terminating gateway with no proxy in front sees the caller directly.
func TestClientIPWithoutTrustedProxies(t *testing.T) {
	r := &http.Request{RemoteAddr: "198.51.100.20:443", Header: http.Header{}, TLS: &tls.ConnectionState{}}
	r.Header.Set("X-Forwarded-For", "10.9.9.9") // present, and correctly ignored
	if got := config.ClientIP(r, nil); got != "198.51.100.20" {
		t.Errorf("ClientIP = %q, want the peer address", got)
	}
}
