// Package scheduler drives cron schedules on every hub replica. The
// database owns correctness (store.FireDue's advisory lock + SKIP
// LOCKED + idempotency keys); this loop is just the heartbeat that
// makes passes happen, plus the periodic lease sweep that surfaces
// runner crashes without waiting for claim traffic (ADR-0012).
//
// robfig/cron is used strictly as an expression parser — its runner
// machinery is not. All usage is isolated behind ParseCron/nextAfter so
// the dependency is trivially replaceable.
package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
	cron "github.com/robfig/cron/v3"
)

var parser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ParseCron validates a standard 5-field cron expression (plus
// @hourly-style descriptors). Used by the API at schedule creation.
func ParseCron(expr string) error {
	_, err := parser.Parse(expr)
	if err != nil {
		return fmt.Errorf("scheduler: invalid cron %q: %w", expr, err)
	}
	return nil
}

// NextAfter computes the first fire time strictly after t, in UTC
// (schedules are UTC-only in M4b — no DST ambiguity).
func NextAfter(expr string, t time.Time) (time.Time, error) {
	sched, err := parser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("scheduler: invalid cron %q: %w", expr, err)
	}
	return sched.Next(t.UTC()), nil
}

// Options tune the loop.
type Options struct {
	// Interval is the poll period (default 5s).
	Interval time.Duration
	// Batch caps schedules fired per pass (default 50); a full batch
	// triggers an immediate follow-up pass to drain bursts.
	Batch int
}

// Scheduler runs the firing loop.
type Scheduler struct {
	st   *store.Store
	opts Options

	mu   sync.Mutex
	last Status
}

// Status is the dashboard snapshot of the loop.
type Status struct {
	LastPass  time.Time `json:"last_pass,omitzero"`
	LastFired int       `json:"last_fired"`
	LastError string    `json:"last_error,omitempty"`
}

// New builds a scheduler.
func New(st *store.Store, opts Options) *Scheduler {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Batch <= 0 {
		opts.Batch = 50
	}
	return &Scheduler{st: st, opts: opts}
}

// Status snapshots the last pass.
func (s *Scheduler) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// Run ticks until ctx ends. Every replica runs this; the store's
// advisory lock elects a de-facto worker per pass. Errors are recorded
// and retried next tick — never fatal.
func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.pass(ctx)
		}
	}
}

func (s *Scheduler) pass(ctx context.Context) {
	var errMsg string
	if err := s.st.ReapExpired(ctx); err != nil && ctx.Err() == nil {
		errMsg = err.Error()
		log.Printf("scheduler: lease sweep: %v", err)
	}
	total := 0
	for {
		fired, err := s.st.FireDue(ctx, NextAfter, s.opts.Batch)
		total += fired
		if err != nil {
			if ctx.Err() == nil {
				errMsg = err.Error()
				log.Printf("scheduler: fire pass: %v", err)
			}
			break
		}
		if fired < s.opts.Batch {
			break // drained
		}
	}
	s.mu.Lock()
	s.last = Status{LastPass: time.Now(), LastFired: total, LastError: errMsg}
	s.mu.Unlock()
}
