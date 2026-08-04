package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/aaron-au/shift/engine/record"
)

// Fan-out execution for flow model v3 (ADR-0029): a single upstream stream is
// duplicated (tee) or partitioned (router) across N downstream branches, each
// running its own operator chain into its own sink, concurrently and under
// bounded memory.
//
// Concurrency model. One driver (the calling goroutine) pulls the upstream
// once per batch — never double-pulls, never re-executes the source. Each
// branch runs in its own goroutine, consuming batches from a bounded channel
// (flow control: a full channel blocks the driver on that branch, so the tee
// runs at the pace of its slowest branch without unbounded buffering —
// ADR-0005 applied within a task). Batches are reference-counted and pooled.
//
// Batch ownership (the correctness core). Upstream reuses its batch across
// Next calls, so the driver snapshots each pulled batch into a pool-owned
// batch before handing it on — that one copy is unavoidable and lets several
// batches be in flight at once. A branch marked Shared reads that snapshot
// directly (multiple shared branches read the same immutable batch
// concurrently — race-free, no copy). A branch NOT marked Shared performs
// copy-on-write: it copies the snapshot into its own batch and releases the
// shared one immediately, so its operators may mutate freely. Shared is opt-in
// and the caller sets it only where the branch provably never mutates (a
// tee straight to a sink) or already owns its batch exclusively (every router
// branch); the default (false ⇒ COW) is always safe.
//
// Spill above the memory watermark (ADR-0029) is a later addition; today the
// bounded channels provide backpressure by blocking, which is correct and
// memory-bounded (depth × branches batches in flight).

// Branch is one downstream consumer of a fan-out node.
type Branch struct {
	// Build constructs this branch's operator chain on top of the provided
	// source (typically New(src, name).Filter(...).Project(...)). The returned
	// pipeline is Run into Sink.
	Build func(src Source) *Pipeline
	Sink  Sink
	// Name labels the branch's sink stage in its Report and any OpError.
	Name string
	// Shared lets this branch read the tee snapshot without a copy. Set it
	// ONLY when the branch never mutates the batch (no operators — a tee
	// straight to a sink) or receives an exclusively-owned batch (router
	// branches). Leave false (the default) for any branch with operators; it
	// then copy-on-writes and may mutate safely.
	Shared bool
}

// FanoutReport holds each branch's run report, indexed as the branches were
// passed in.
type FanoutReport struct {
	Branches []Report
	// Stopped reports that a branch reached a @stop terminal, ending the whole
	// topology deliberately and successfully (ADR-0031 §3); StopStep names it.
	// These ride the report rather than the error because a stop resolves the
	// run's error to nil — without them the caller would see a clean success
	// and could not tell that a stop is what produced it.
	Stopped  bool
	StopStep string
}

// FanoutOptions tune fan-out execution.
type FanoutOptions struct {
	// QueueDepth bounds the per-branch in-flight batch channel (flow control).
	// <=0 uses a small default.
	QueueDepth int
}

const defaultQueueDepth = 4

// RunTee duplicates upstream to every branch: each record is delivered to
// every branch. It returns when all branches finish (upstream drained and
// each sink closed) or when the first error tears the topology down.
func RunTee(ctx context.Context, upstream Source, branches []Branch, opts FanoutOptions) (FanoutReport, error) {
	e := newFanoutExec(ctx, branches, opts)
	e.start()
	derr := e.driveTee(upstream)
	return e.finish(upstream, derr)
}

// RunRouter partitions upstream across branches: match maps each record to a
// single branch index (or <0 to drop it). Each branch receives an exclusively
// owned batch of its matched records, so its operators may mutate freely
// regardless of the Shared flag.
func RunRouter(ctx context.Context, upstream Source, branches []Branch, match func(record.Value) int, opts FanoutOptions) (FanoutReport, error) {
	e := newFanoutExec(ctx, branches, opts)
	e.start()
	derr := e.driveRouter(upstream, match)
	return e.finish(upstream, derr)
}

type fanoutExec struct {
	// parent is the context handed to the executor; ctx is the one derived
	// from it that the executor cancels on first error or on @stop. Keeping
	// both is what lets Canonical tell a caller-initiated abort (parent done)
	// from this executor's own teardown (only ctx done) — ADR-0031 §1.
	parent   context.Context
	ctx      context.Context
	cancel   context.CancelFunc
	branches []Branch
	queues   []chan *sharedBatch
	pool     *batchPool
	reports  []Report
	errs     []error
	wg       sync.WaitGroup
}

func newFanoutExec(ctx context.Context, branches []Branch, opts FanoutOptions) *fanoutExec {
	depth := opts.QueueDepth
	if depth <= 0 {
		depth = defaultQueueDepth
	}
	cctx, cancel := context.WithCancel(ctx)
	e := &fanoutExec{
		parent:   ctx,
		ctx:      cctx,
		cancel:   cancel,
		branches: branches,
		queues:   make([]chan *sharedBatch, len(branches)),
		pool:     &batchPool{},
		reports:  make([]Report, len(branches)),
		errs:     make([]error, len(branches)),
	}
	for i := range e.queues {
		e.queues[i] = make(chan *sharedBatch, depth)
	}
	return e
}

// start launches one goroutine per branch, each running its pipeline over a
// branchSource that reads from that branch's queue.
func (e *fanoutExec) start() {
	for i := range e.branches {
		bs := &branchSource{ch: e.queues[i], shared: e.branches[i].Shared}
		p := e.branches[i].Build(bs)
		e.wg.Add(1)
		go func(i int, p *Pipeline) {
			defer e.wg.Done()
			rep, err := p.Run(e.ctx, e.branches[i].Sink, e.branches[i].Name)
			e.reports[i] = rep
			if err != nil {
				e.errs[i] = err
				e.cancel() // a failed branch tears the whole topology down
			}
		}(i, p)
	}
}

// driveTee pulls upstream and broadcasts a shared snapshot of each batch to
// every branch queue.
func (e *fanoutExec) driveTee(upstream Source) error {
	for {
		ub, err := upstream.Next(e.ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		snap := e.pool.get()
		for _, r := range ub.Records() {
			snap.Append(record.CopyValue(snap, r))
		}
		sb := &sharedBatch{batch: snap, pool: e.pool}
		//nolint:gosec // G115: branch count is a flow node count (tiny), never near int32.
		sb.refs.Store(int32(len(e.branches)))
		for i := range e.queues {
			select {
			case e.queues[i] <- sb:
			case <-e.ctx.Done():
				// A branch failed; branches i..end will never receive this
				// snapshot, so drop their refs to let it return to the pool.
				//nolint:gosec // G115: queue count is a flow node count (tiny).
				sb.releaseN(int32(len(e.queues) - i))
				return e.ctx.Err()
			}
		}
	}
}

// driveRouter pulls upstream and sends each branch an exclusively-owned batch
// of the records that matched it.
func (e *fanoutExec) driveRouter(upstream Source, match func(record.Value) int) error {
	for {
		ub, err := upstream.Next(e.ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var obs []*record.Batch // per-branch, lazily allocated
		for _, r := range ub.Records() {
			bi := match(r)
			if bi < 0 || bi >= len(e.branches) {
				continue // dropped: no matching route and no default
			}
			if obs == nil {
				obs = make([]*record.Batch, len(e.branches))
			}
			if obs[bi] == nil {
				obs[bi] = e.pool.get()
			}
			obs[bi].Append(record.CopyValue(obs[bi], r))
		}
		for bi, ob := range obs {
			if ob == nil {
				continue
			}
			sb := &sharedBatch{batch: ob, pool: e.pool}
			sb.refs.Store(1)
			select {
			case e.queues[bi] <- sb:
			case <-e.ctx.Done():
				sb.release()
				return e.ctx.Err()
			}
		}
	}
}

// finish closes the queues (branches drain then see EOF), waits for all
// branches, closes the upstream, drains any leftover queued batches, and
// resolves the run's error: a real branch/upstream error wins over the
// context cancellation it triggers.
func (e *fanoutExec) finish(upstream Source, derr error) (FanoutReport, error) {
	for i := range e.queues {
		close(e.queues[i])
	}
	e.wg.Wait()
	e.cancel()
	e.drainQueues()
	if cerr := upstream.Close(); cerr != nil && derr == nil {
		derr = cerr
	}

	rep := FanoutReport{Branches: e.reports}
	// A branch that failed cancels the shared ctx, which surfaces as
	// context.Canceled in its innocent siblings (and in the driver), and a
	// branch reaching @stop does the same thing deliberately. Canonical
	// (ADR-0031 §1) resolves the one result worth reporting: a stop is a
	// success, a real error beats the teardown noise it caused, and a
	// cancellation is reported only when the PARENT context is what was
	// cancelled — i.e. the caller aborted rather than this executor tearing
	// itself down. e.parent is the context handed to the executor; e.ctx is
	// the derived one it cancels itself.
	errs := make([]error, 0, len(e.errs)+1)
	errs = append(errs, e.errs...)
	errs = append(errs, derr)
	out := Classify(e.parent, errs...)
	rep.Stopped, rep.StopStep = out.Stopped, out.StopStep
	return rep, out.Err
}

// drainQueues releases any snapshots still queued for a branch that stopped
// reading (after an error), returning their batches to the pool. Safe because
// finish has closed every queue and joined every branch, so no goroutine is
// still sending or receiving.
func (e *fanoutExec) drainQueues() {
	for _, q := range e.queues {
		for sb := range q {
			sb.release()
		}
	}
}

// sharedBatch is a pooled record batch shared by reference count across the
// branches that received it. The last release returns it to the pool.
type sharedBatch struct {
	batch *record.Batch
	refs  atomic.Int32
	pool  *batchPool
}

func (s *sharedBatch) release() { s.releaseN(1) }

func (s *sharedBatch) releaseN(n int32) {
	if s.refs.Add(-n) == 0 {
		s.pool.put(s.batch)
	}
}

// batchPool recycles record batches across fan-out snapshots to keep steady
// state allocation-free.
type batchPool struct{ p sync.Pool }

func (bp *batchPool) get() *record.Batch {
	if v := bp.p.Get(); v != nil {
		b := v.(*record.Batch)
		b.Reset()
		return b
	}
	return record.NewBatch()
}

func (bp *batchPool) put(b *record.Batch) { bp.p.Put(b) }

// branchSource feeds one branch's pipeline from its bounded queue. A shared
// branch returns the queued snapshot directly (holding it until the next pull
// releases it); a copy-on-write branch copies into its own reusable batch and
// releases the snapshot at once, so its operators may mutate.
type branchSource struct {
	ch     <-chan *sharedBatch
	shared bool
	held   *sharedBatch  // shared mode: the batch currently lent downstream
	owned  *record.Batch // cow mode: reusable destination
}

func (s *branchSource) Next(ctx context.Context) (*record.Batch, error) {
	s.releaseHeld()
	select {
	case sb, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		if s.shared {
			s.held = sb
			return sb.batch, nil
		}
		if s.owned == nil {
			s.owned = record.NewBatch()
		}
		s.owned.Reset()
		for _, r := range sb.batch.Records() {
			s.owned.Append(record.CopyValue(s.owned, r))
		}
		sb.release()
		return s.owned, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *branchSource) releaseHeld() {
	if s.held != nil {
		s.held.release()
		s.held = nil
	}
}

func (s *branchSource) Close() error {
	s.releaseHeld()
	return nil
}
