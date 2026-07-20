// Package ratelimit is a per-process, per-key token-bucket limiter for the
// hub control API (M6c, ADR-0021). Limits are per-replica by design — the
// goal is overload/abuse protection, not global quota accounting (that is
// billing's job). A class with RPS <= 0 is disabled (the default), so
// loopback/dev and self-hosted single-user stay frictionless.
package ratelimit

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Cfg is one class's budget. RPS <= 0 disables the class.
type Cfg struct {
	RPS   float64
	Burst int
}

type bucket struct {
	lim  *rate.Limiter
	seen time.Time
}

// Limiter holds per-class configs and per-(class,key) token buckets.
type Limiter struct {
	mu       sync.Mutex
	classes  map[string]Cfg
	buckets  map[string]*bucket
	rejected map[string]int64 // per-class reject counter (observability)
	now      func() time.Time
	stop     chan struct{} // closed by Stop to end the sweeper
}

// New builds a limiter from per-class configs and starts an idle-bucket
// sweeper (buckets unused for 10m are dropped so the key space can't grow
// unbounded). Returns nil-safe: a nil *Limiter allows everything.
func New(classes map[string]Cfg) *Limiter {
	l := &Limiter{
		classes:  classes,
		buckets:  map[string]*bucket{},
		rejected: map[string]int64{},
		now:      time.Now,
		stop:     make(chan struct{}),
	}
	go l.sweep()
	return l
}

// Stop ends the sweeper goroutine. Nil-safe; call once at process shutdown.
// Constructing a Limiter without Stopping it leaks the sweeper goroutine +
// ticker (harmless for a process-lifetime singleton, a footgun anywhere else).
func (l *Limiter) Stop() {
	if l == nil {
		return
	}
	close(l.stop)
}

// Allow reports whether one request in class for key may proceed. A nil
// limiter, an unknown class, or a class with RPS <= 0 always allows.
func (l *Limiter) Allow(class, key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	cfg, ok := l.classes[class]
	if !ok || cfg.RPS <= 0 {
		return true
	}
	k := class + "\x00" + key
	b := l.buckets[k]
	if b == nil {
		b = &bucket{lim: rate.NewLimiter(rate.Limit(cfg.RPS), max(cfg.Burst, 1))}
		l.buckets[k] = b
	}
	b.seen = l.now()
	if !b.lim.Allow() {
		l.rejected[class]++
		return false
	}
	return true
}

// Rejected returns the cumulative reject count for a class (for metrics).
func (l *Limiter) Rejected(class string) int64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rejected[class]
}

// Classes lists configured class names (for metric enumeration).
func (l *Limiter) Classes() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.classes))
	for c := range l.classes {
		out = append(out, c)
	}
	return out
}

func (l *Limiter) sweep() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			cut := l.now().Add(-10 * time.Minute)
			l.mu.Lock()
			for k, b := range l.buckets {
				if b.seen.Before(cut) {
					delete(l.buckets, k)
				}
			}
			l.mu.Unlock()
		}
	}
}

// ClientIP extracts the rate-limit key for an unauthenticated request: the
// direct peer address (host only). Proxy-header trust is deliberately not
// honored by default — a client must not be able to spoof its key
// (ADR-0021 open question).
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Reject writes a 429 with a Retry-After hint.
func Reject(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
}
