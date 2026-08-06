package store

import (
	"context"
	"log/slog"
	"time"
)

// SweepStatus prunes caller-facing execution status on an interval (ADR-0042
// §3c).
//
// It is deliberately unsynchronised across replicas: unlike the scheduler,
// which must elect exactly one worker per tick because firing a flow twice is
// a duplicated side effect (ADR-0012), this only DELETES rows that are already
// past their grace or their TTL. Two replicas doing that concurrently is
// wasted work, not a correctness problem, and the advisory-lock machinery
// would cost more than it saves.
//
// It runs until ctx ends.
func (s *Store) SweepStatus(ctx context.Context, interval, grace time.Duration, log *slog.Logger) {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// A fresh bound per pass, and NOT ctx: a sweep that outlives
			// shutdown would hold a connection open past the point the process
			// is trying to exit, and a pruning pass has nothing worth waiting
			// for.
			pass, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			n, err := s.SweepExecutionStatus(pass, grace)
			cancel()
			switch {
			case err != nil:
				// Nothing is lost by a failed pass — the rows are still there
				// and the next tick tries again — so this is a warning rather
				// than anything louder.
				log.Warn("execution status sweep failed", "error", err)
			case n > 0:
				log.Info("execution status swept", "rows", n)
			}
		}
	}
}
