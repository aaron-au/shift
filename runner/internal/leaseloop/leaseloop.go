// Package leaseloop is the runner's hub intake (M3b, ADR-0008): a loop
// that leases tasks from the hub queue and submits them to the same task
// service the local HTTP intake uses. Claiming is capacity-gated
// (ADR-0005): the loop only leases when the memory governor has headroom
// for another task, so work queues at the hub — where any runner can take
// it — never inside a busy runner.
package leaseloop

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
	"github.com/aaron-au/shift/runner/internal/hubclient"
	"github.com/aaron-au/shift/runner/internal/service"
)

// Options configure the loop.
type Options struct {
	// Client is the registered hub client.
	Client *hubclient.Client
	// Service executes the leased flows.
	Service *service.Service
	// LeaseWait is the long-poll window per lease request (default 20s).
	LeaseWait time.Duration
	// HeadroomPoll is the re-check interval while the runner is at
	// capacity (default 250ms).
	HeadroomPoll time.Duration
	// TaskPoll is the local completion poll interval (default 100ms).
	TaskPoll time.Duration
}

// Loop leases and executes hub tasks until its context ends.
type Loop struct {
	opts Options

	wg     sync.WaitGroup
	active atomic.Int64
	leased atomic.Int64
	errs   atomic.Int64
}

// New builds a loop.
func New(opts Options) *Loop {
	if opts.LeaseWait <= 0 {
		opts.LeaseWait = 20 * time.Second
	}
	if opts.HeadroomPoll <= 0 {
		opts.HeadroomPoll = 250 * time.Millisecond
	}
	if opts.TaskPoll <= 0 {
		opts.TaskPoll = 100 * time.Millisecond
	}
	return &Loop{opts: opts}
}

// Status is the intake's dashboard snapshot.
type Status struct {
	Active      int64 `json:"active"`
	TotalLeased int64 `json:"total_leased"`
	Errors      int64 `json:"errors"`
}

// Status snapshots the loop.
func (l *Loop) Status() Status {
	return Status{Active: l.active.Load(), TotalLeased: l.leased.Load(), Errors: l.errs.Load()}
}

// Run leases until ctx ends, then waits for in-flight tasks to report.
func (l *Loop) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if !l.headroom() {
			sleep(ctx, l.opts.HeadroomPoll)
			continue
		}
		task, ttl, err := l.opts.Client.Lease(ctx, l.opts.LeaseWait)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			l.errs.Add(1)
			log.Printf("leaseloop: lease: %v (retrying in %s)", err, backoff)
			sleep(ctx, backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		if task == nil {
			continue // empty long-poll window
		}
		l.leased.Add(1)
		l.active.Add(1)
		l.wg.Go(func() {
			defer l.active.Add(-1)
			l.execute(ctx, task, ttl)
		})
	}
	l.wg.Wait()
}

// headroom reports whether the governor can admit another task without
// waiting — leasing beyond that would strand hub work behind this runner.
func (l *Loop) headroom() bool {
	st := l.opts.Service.Status()
	return st.Governor.Used+st.TaskCost <= st.Governor.Budget
}

// execute runs one leased task: submit to the service, heartbeat while it
// runs, then report the terminal state to the hub.
func (l *Loop) execute(ctx context.Context, t *hubclient.LeasedTask, ttl time.Duration) {
	doc, err := flowdoc.Parse(t.Document)
	if err == nil {
		// Step idempotency (ADR-0002): the sink sees a key that is stable
		// across re-dispatched attempts of the same task, so at-least-once
		// delivery cannot double side effects on idempotent receivers.
		key := t.IdempotencyKey
		if key == "" {
			key = t.ID
		}
		doc, err = doc.WithSinkConfig(map[string]any{"idempotency_key": key})
	}
	if err != nil {
		l.report(t.ID, func(ctx context.Context) error {
			return l.opts.Client.Fail(ctx, t.ID, "invalid flow document: "+err.Error())
		})
		return
	}

	localID, err := l.opts.Service.Submit(doc, false)
	if err != nil {
		l.report(t.ID, func(ctx context.Context) error {
			return l.opts.Client.Fail(ctx, t.ID, err.Error())
		})
		return
	}

	hb := time.NewTicker(max(ttl/3, 500*time.Millisecond))
	defer hb.Stop()
	poll := time.NewTicker(l.opts.TaskPoll)
	defer poll.Stop()
	leaseHeld := true
	done := ctx.Done()

	for {
		select {
		case <-hb.C:
			if !leaseHeld {
				continue
			}
			hctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := l.opts.Client.Heartbeat(hctx, t.ID)
			cancel()
			if errors.Is(err, hubclient.ErrLeaseLost) {
				// The hub re-dispatched (we were presumed dead). Keep the
				// local task running to completion — idempotency keys make
				// the duplicate side-effect-free — but stop reporting.
				leaseHeld = false
				l.errs.Add(1)
				log.Printf("leaseloop: task %s: lease lost mid-run", t.ID)
			} else if err != nil {
				l.errs.Add(1)
				log.Printf("leaseloop: task %s: heartbeat: %v", t.ID, err)
			}
		case <-poll.C:
			lt, ok := l.opts.Service.Task(localID)
			if !ok {
				l.report(t.ID, func(ctx context.Context) error {
					return l.opts.Client.Fail(ctx, t.ID, "task evicted from runner store")
				})
				return
			}
			switch lt.State {
			case "completed":
				if leaseHeld {
					res := hubclient.Result{
						RecordsIn:     lt.RecordsIn,
						RecordsOut:    lt.RecordsOut,
						SinkConfirmed: lt.SinkConfirmed,
						RunnerTaskID:  localID,
					}
					for _, op := range lt.Ops {
						res.Ops = append(res.Ops, hubclient.OpStat(op))
					}
					l.report(t.ID, func(ctx context.Context) error {
						return l.opts.Client.Complete(ctx, t.ID, res)
					})
				}
				return
			case "failed":
				if leaseHeld {
					l.report(t.ID, func(ctx context.Context) error {
						return l.opts.Client.Fail(ctx, t.ID, lt.Error)
					})
				}
				return
			default:
				// waiting/running: keep heartbeating.
			}
		case <-done:
			// Drain: the service finishes the task; heartbeats and reports
			// run on background contexts, so just stop selecting on ctx.
			done = nil
		}
	}
}

// report delivers a terminal state with retries — losing the race to a
// re-dispatch (ErrLeaseLost) is expected and final.
func (l *Loop) report(taskID string, fn func(context.Context) error) {
	for attempt := range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := fn(ctx)
		cancel()
		if err == nil || errors.Is(err, hubclient.ErrLeaseLost) {
			if errors.Is(err, hubclient.ErrLeaseLost) {
				log.Printf("leaseloop: task %s: result discarded, lease was re-dispatched", taskID)
			}
			return
		}
		l.errs.Add(1)
		log.Printf("leaseloop: task %s: report attempt %d: %v", taskID, attempt+1, err)
		time.Sleep(time.Second << attempt)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
