package ingress

import (
	"testing"

	"github.com/aaron-au/shift/gateway/internal/config"
)

func routeAt(path, flow string) config.Route {
	return config.Route{Path: path, Method: "POST", Flow: flow, AuthPrincipal: "acme-erp"}
}

// The hub raises a monotonic generation on every change, so an OLDER
// configuration arriving after a newer one is a push that lost a race — a
// retry overtaken by its own successor, or two hub instances pushing under HA.
//
// Applying it would roll the gateway back onto a stale ROSTER: a runner the
// hub has since revoked served again, one it has since vouched for refused —
// and no error anywhere, because both pushes succeeded.
func TestAnOlderConfigurationIsRefused(t *testing.T) {
	h := New(nil, nil)

	newer := &config.Config{Version: 7, Routes: []config.Route{routeAt("/new", "f")}}
	if err := h.SetConfig(newer); err != nil {
		t.Fatal(err)
	}

	older := &config.Config{Version: 6, Routes: []config.Route{routeAt("/old", "f")}}
	if err := h.SetConfig(older); err == nil {
		t.Fatal("a configuration older than the active one was applied")
	}
	if h.ConfigVersion() != 7 {
		t.Fatalf("active version = %d, want the newer 7 still in place", h.ConfigVersion())
	}
	if h.Config().Lookup("POST", "/new") == nil {
		t.Fatal("the newer configuration was replaced by the one that was refused")
	}
}

// A REPEAT of the current generation is a normal retry, not a regression: the
// hub re-derives configuration from state on every pass and re-pushes after a
// failure. Refusing it would leave a gateway stuck whenever an ack was lost.
func TestTheSameGenerationIsAcceptedAgain(t *testing.T) {
	h := New(nil, nil)
	if err := h.SetConfig(&config.Config{Version: 4, Routes: []config.Route{routeAt("/a", "f")}}); err != nil {
		t.Fatal(err)
	}
	if err := h.SetConfig(&config.Config{Version: 4, Routes: []config.Route{routeAt("/b", "f")}}); err != nil {
		t.Fatalf("a re-push of the current generation was refused: %v", err)
	}
	if h.Config().Lookup("POST", "/b") == nil {
		t.Fatal("the re-pushed configuration was not applied")
	}
}

// The FIRST push has nothing to compare against and must always land,
// whatever generation the hub has reached — a gateway adopted into a
// long-running deployment starts at version 0 with no configuration at all.
func TestTheFirstConfigurationAlwaysApplies(t *testing.T) {
	h := New(nil, nil)
	if err := h.SetConfig(&config.Config{Version: 99, Routes: []config.Route{routeAt("/a", "f")}}); err != nil {
		t.Fatal(err)
	}
	if h.ConfigVersion() != 99 {
		t.Fatalf("active version = %d, want 99", h.ConfigVersion())
	}
}
