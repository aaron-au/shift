package ratelimit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

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

func TestClassesEnumeration(t *testing.T) {
	l := New(map[string]Cfg{"a": {RPS: 1, Burst: 1}, "b": {RPS: 0}})
	got := l.Classes()
	if len(got) != 2 {
		t.Fatalf("Classes() len = %d, want 2 (%v)", len(got), got)
	}
	seen := map[string]bool{}
	for _, c := range got {
		seen[c] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("Classes() = %v, want both a and b", got)
	}
}

// TestRefillOverTime verifies tokens are replenished as time passes. The
// package exposes no clock seam for token refill — golang.org/x/time/rate
// uses its own internal time.Now, and the injectable `now` func only drives
// bucket.seen/sweep cutoff — so this uses a short real wait. A high RPS keeps
// the wait tiny and the assertion robust.
func TestRefillOverTime(t *testing.T) {
	// 100 rps => one token every ~10ms; burst 1 => a single immediate token.
	l := New(map[string]Cfg{"t": {RPS: 100, Burst: 1}})
	if !l.Allow("t", "k") {
		t.Fatal("first request should consume the initial token")
	}
	if l.Allow("t", "k") {
		t.Fatal("second immediate request should be rejected (bucket empty)")
	}
	// Wait well past one refill interval, then it must allow again.
	deadline := time.Now().Add(2 * time.Second)
	for !l.Allow("t", "k") { // loops until refilled
		if time.Now().After(deadline) {
			t.Fatal("token was never refilled within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentAllowIsRaceClean(t *testing.T) {
	l := New(map[string]Cfg{"t": {RPS: 50, Burst: 10}})
	const goroutines = 32
	const perG = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			// Mix shared and per-goroutine keys to exercise bucket
			// creation, contention on one bucket, and per-key isolation.
			for i := range perG {
				l.Allow("t", "shared")
				if i%3 == 0 {
					l.Allow("t", "key-"+string(rune('a'+g%26)))
				}
				_ = l.Rejected("t")
				_ = l.Classes()
			}
		}(g)
	}
	wg.Wait()
	// Shared key with burst 10 and heavy hammering must have rejected some.
	if l.Rejected("t") == 0 {
		t.Fatal("expected some rejects under concurrent overload")
	}
}

func TestClientIP(t *testing.T) {
	// Well-formed host:port -> host only.
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	r.RemoteAddr = "192.0.2.7:54321"
	if got := ClientIP(r); got != "192.0.2.7" {
		t.Fatalf("ClientIP = %q, want 192.0.2.7", got)
	}
	// IPv6 host:port.
	r.RemoteAddr = "[2001:db8::1]:443"
	if got := ClientIP(r); got != "2001:db8::1" {
		t.Fatalf("ClientIP(ipv6) = %q, want 2001:db8::1", got)
	}
	// Malformed (no port) -> returned as-is (SplitHostPort error path).
	r.RemoteAddr = "noport"
	if got := ClientIP(r); got != "noport" {
		t.Fatalf("ClientIP(malformed) = %q, want noport", got)
	}
}

func TestReject(t *testing.T) {
	w := httptest.NewRecorder()
	Reject(w)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want text/plain; charset=utf-8", ct)
	}
	if body := w.Body.String(); body != `{"error":"rate limited"}`+"\n" {
		t.Fatalf("body = %q", body)
	}
}

// TestLimiterStop verifies Stop ends the sweeper without panicking and that a
// nil limiter's Stop is a no-op (constructing many limiters in tests must not
// leak the sweeper goroutine — M6 review hardening).
func TestLimiterStop(t *testing.T) {
	var nilL *Limiter
	nilL.Stop() // must not panic

	l := New(map[string]Cfg{"x": {RPS: 1, Burst: 1}})
	if !l.Allow("x", "k") { // first token available
		t.Fatal("first request should be allowed")
	}
	l.Stop()               // ends the sweeper; Allow still works (buckets untouched)
	if l.Allow("x", "k") { // burst 1 exhausted
		t.Fatal("second immediate request should be rejected")
	}
}
