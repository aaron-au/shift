package ratelimit

import "testing"

func TestAllowBurstThenReject(t *testing.T) {
	l := New(map[string]Cfg{"t": {RPS: 1, Burst: 2}})
	// Burst of 2 immediate allows, then rejected (refill is ~1/s).
	for i := range 2 {
		if !l.Allow("t", "k") {
			t.Fatalf("burst allow %d should succeed", i)
		}
	}
	if l.Allow("t", "k") {
		t.Fatal("3rd immediate request should be rejected")
	}
	if got := l.Rejected("t"); got != 1 {
		t.Fatalf("rejected = %d, want 1", got)
	}
	// A different key has its own bucket.
	if !l.Allow("t", "other") {
		t.Fatal("distinct key should have its own budget")
	}
}

func TestDisabledClassAlwaysAllows(t *testing.T) {
	l := New(map[string]Cfg{"off": {RPS: 0, Burst: 5}})
	for range 100 {
		if !l.Allow("off", "k") {
			t.Fatal("rps<=0 class must never reject")
		}
	}
	// Unknown class also allows.
	if !l.Allow("nope", "k") {
		t.Fatal("unknown class must allow")
	}
	if l.Rejected("off") != 0 {
		t.Fatal("disabled class should record no rejects")
	}
}

func TestNilLimiterAllows(t *testing.T) {
	var l *Limiter
	if !l.Allow("t", "k") {
		t.Fatal("nil limiter must allow")
	}
	if l.Rejected("t") != 0 || l.Classes() != nil {
		t.Fatal("nil limiter accessors must be zero-valued")
	}
}
