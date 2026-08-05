package main

import "testing"

// loopbackOnly decides whether gatewayd may start WITHOUT a control-listener
// credential. Getting it wrong in the permissive direction ships an
// unauthenticated /poll on a public interface, so the bare-port and
// all-interfaces forms matter more than the obvious loopback ones — those are
// exactly what a container image ships with.
func TestLoopbackOnly(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8444": true,
		"localhost:8444": true,
		"[::1]:8444":     true,

		":8444":          false, // all interfaces — the container default
		"0.0.0.0:8444":   false,
		"[::]:8444":      false,
		"10.0.0.5:8444":  false,
		"gateway-0:8444": false, // a name we cannot resolve to loopback here
		"garbage":        false, // unparseable: assume the worst
		"":               false,
	}
	for addr, want := range cases {
		if got := loopbackOnly(addr); got != want {
			t.Errorf("loopbackOnly(%q) = %v, want %v", addr, got, want)
		}
	}
}
