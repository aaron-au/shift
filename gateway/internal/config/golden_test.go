package config_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/aaron-au/shift/gateway/internal/config"
)

// The hub builds this document from its own mirror of these types
// (hub/internal/store.GatewayConfig), because the two live in separate modules
// and the gateway's has no dependencies — an auditable property of the one
// component that may sit in a DMZ.
//
// Duplicated wire types that silently drift are how a hub ends up pushing
// configuration a gateway ignores half of: a renamed field simply vanishes on
// unmarshal, no error anywhere, and the route quietly stops being protected by
// whatever it described. So both sides parse ONE fixture —
// testdata/gateway-config.golden.json at the repo root — and assert the same
// values. Its twin is TestTheBuiltConfigMatchesTheGatewaysModel in
// hub/internal/store.
func TestTheGoldenConfigParsesIntoTheGatewaysModel(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "gateway-config.golden.json"))
	if err != nil {
		t.Fatalf("golden fixture: %v", err)
	}
	var c config.Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	// Unknown fields are an ERROR here. Tolerating them is exactly how drift
	// hides: the hub adds a field, the gateway ignores it, and nobody learns
	// until the policy it carried turns out not to be applied.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		t.Fatalf("the hub's document does not fit the gateway's model: %v", err)
	}

	if c.Version != 7 {
		t.Fatalf("version = %d, want 7", c.Version)
	}
	if len(c.Routes) != 2 {
		t.Fatalf("routes = %d, want 2", len(c.Routes))
	}

	r := c.Routes[0]
	if r.Path != "/orders" || r.Method != http.MethodPost || r.Flow != "orders-in" {
		t.Fatalf("route 0 = %+v", r)
	}
	if r.Selector["environment"] != "production" || r.Selector["workload"] != "api" {
		t.Fatalf("selector = %v", r.Selector)
	}
	if r.AuthPrincipal != "acme-erp" || len(r.AuthTokenSHA256) != 64 {
		t.Fatalf("caller credential did not survive: principal=%q hash=%q", r.AuthPrincipal, r.AuthTokenSHA256)
	}
	if len(r.AllowCIDRs) != 1 || r.AllowCIDRs[0] != "203.0.113.0/24" {
		t.Fatalf("allow_cidrs = %v", r.AllowCIDRs)
	}
	if _, ok := r.RequireHeaders["X-Acme-Signature"]; !ok {
		t.Fatalf("require_headers = %v", r.RequireHeaders)
	}
	if r.MaxBodyBytes != 1048576 {
		t.Fatalf("max_body_bytes = %d", r.MaxBodyBytes)
	}

	// A route with no method and no credential still parses — that shape is
	// how a deliberately open path is expressed.
	if c.Routes[1].Method != "" || c.Routes[1].AuthTokenSHA256 != "" {
		t.Fatalf("route 1 = %+v, want an unauthenticated any-method route", c.Routes[1])
	}

	if len(c.Runners) != 2 {
		t.Fatalf("roster = %d runners, want 2", len(c.Runners))
	}
	labels, known := c.LabelsFor("11111111-1111-1111-1111-111111111111")
	if !known || labels["workload"] != "api" {
		t.Fatalf("roster labels = %v known=%v", labels, known)
	}
	// A runner with no labels is still ON the roster — known, and label-less.
	// The distinction matters: unknown must be refused, label-less must not.
	if labels, known := c.LabelsFor("22222222-2222-2222-2222-222222222222"); !known || len(labels) != 0 {
		t.Fatalf("label-less runner = %v known=%v", labels, known)
	}
	if _, known := c.LabelsFor("33333333-3333-3333-3333-333333333333"); known {
		t.Fatal("a runner absent from the roster was reported as known")
	}

	if len(c.TrustedProxies) != 1 || c.TrustedProxies[0] != "10.0.0.0/8" {
		t.Fatalf("trusted_proxies = %v", c.TrustedProxies)
	}

	// And the gateway must actually accept it — a document that parses but
	// fails validation would be rejected at push time, which is worse.
	if err := c.Validate(); err != nil {
		t.Fatalf("the hub's document is not servable: %v", err)
	}
}
