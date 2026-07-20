// Package connpool manages connector subprocess lifecycles for the runner:
// one live process per connector name, launched on first use, reused across
// tasks, health-checked on reuse (crashed connectors are relaunched), and
// reaped after idling (ADR-0007 §lifecycle; ADR-0008).
package connpool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/aaron-au/shift/sdk/host"
)

// nameRE restricts connector names to safe path components.
var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

// Options tune the pool.
type Options struct {
	// Dir holds operator-provisioned connector binaries named
	// shift-connector-<name> (local trust: whoever populates Dir vouches
	// for its contents — the M2/M3a dev workflow).
	Dir string
	// Locate resolves a connector to a VERIFIED executable path when Dir
	// doesn't have it (wired to connstore.Ensure when a hub registry is
	// configured). Optional.
	Locate func(ctx context.Context, name string) (string, error)
	// RequireSigned disables the Dir fallback entirely: every binary
	// must come through Locate (SHIFT_REQUIRE_SIGNED).
	RequireSigned bool
	// IdleTTL reaps processes unused this long (default 5m).
	IdleTTL time.Duration
	// ReapEvery is the sweep interval (default 30s).
	ReapEvery time.Duration
}

// Pool is safe for concurrent use.
type Pool struct {
	opts Options

	mu      sync.Mutex
	entries map[string]*entry
	stopped bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Launches counts process spawns (observability + tests).
	launches int64
}

type entry struct {
	proc     *host.Process
	refs     int
	lastUsed time.Time
}

// New creates a pool and starts its reaper.
func New(opts Options) *Pool {
	if opts.IdleTTL <= 0 {
		opts.IdleTTL = 5 * time.Minute
	}
	if opts.ReapEvery <= 0 {
		opts.ReapEvery = 30 * time.Second
	}
	p := &Pool{opts: opts, entries: map[string]*entry{}, stopCh: make(chan struct{})}
	p.wg.Add(1)
	go p.reaper()
	return p
}

// Get returns a live process for the named connector, spawning or
// relaunching as needed. Callers must Put when done with it for this task.
func (p *Pool) Get(ctx context.Context, name string) (*host.Process, error) {
	if !nameRE.MatchString(name) {
		return nil, fmt.Errorf("connpool: invalid connector name %q", name)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil, errors.New("connpool: pool is closed")
	}

	if e, ok := p.entries[name]; ok {
		// Reuse if the process still answers; otherwise relaunch.
		hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := e.proc.Health(hctx)
		cancel()
		if err == nil {
			e.refs++
			e.lastUsed = time.Now()
			return e.proc, nil
		}
		_ = e.proc.Close()
		delete(p.entries, name)
	}

	binary, err := p.locate(ctx, name)
	if err != nil {
		return nil, err
	}
	proc, err := host.Launch(ctx, binary, host.LaunchOptions{})
	if err != nil {
		return nil, fmt.Errorf("connpool: launch %s: %w", name, err)
	}
	p.launches++
	p.entries[name] = &entry{proc: proc, refs: 1, lastUsed: time.Now()}
	return proc, nil
}

// locate finds the connector binary: operator Dir first (unless signed
// artifacts are required), then the registry locator. Verification of
// registry artifacts happens inside Locate (connstore) — a path
// returned here is cleared to execute.
func (p *Pool) locate(ctx context.Context, name string) (string, error) {
	if !p.opts.RequireSigned && p.opts.Dir != "" {
		binary := filepath.Join(p.opts.Dir, "shift-connector-"+name)
		if _, err := os.Stat(binary); err == nil {
			return binary, nil
		}
	}
	if p.opts.Locate != nil {
		path, err := p.opts.Locate(ctx, name)
		if err != nil {
			return "", fmt.Errorf("connpool: connector %q: %w", name, err)
		}
		return path, nil
	}
	if p.opts.RequireSigned {
		return "", fmt.Errorf("connpool: connector %q: signed artifacts required but no registry locator configured", name)
	}
	return "", fmt.Errorf("connpool: connector %q not installed (%s)", name,
		filepath.Join(p.opts.Dir, "shift-connector-"+name))
}

// Put releases a task's use of the named connector.
func (p *Pool) Put(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[name]; ok && e.refs > 0 {
		e.refs--
		e.lastUsed = time.Now()
	}
}

// Launches reports total process spawns.
func (p *Pool) Launches() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.launches
}

// Status describes one pooled connector for the API.
type Status struct {
	Name     string    `json:"name"`
	Version  string    `json:"version"`
	InUse    int       `json:"in_use"`
	LastUsed time.Time `json:"last_used"`
}

// Snapshot lists pooled connectors.
func (p *Pool) Snapshot() []Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Status, 0, len(p.entries))
	for name, e := range p.entries {
		out = append(out, Status{
			Name:     name,
			Version:  e.proc.Info().Version,
			InUse:    e.refs,
			LastUsed: e.lastUsed,
		})
	}
	return out
}

func (p *Pool) reaper() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.opts.ReapEvery)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.reapIdle()
		}
	}
}

func (p *Pool) reapIdle() {
	p.mu.Lock()
	var victims []*host.Process
	for name, e := range p.entries {
		if e.refs == 0 && time.Since(e.lastUsed) > p.opts.IdleTTL {
			victims = append(victims, e.proc)
			delete(p.entries, name)
		}
	}
	p.mu.Unlock()
	for _, v := range victims {
		_ = v.Close() // Close is slow-path (grace period); do it unlocked
	}
}

// Close stops the reaper and shuts every pooled connector down.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	close(p.stopCh)
	procs := make([]*host.Process, 0, len(p.entries))
	for _, e := range p.entries {
		procs = append(procs, e.proc)
	}
	p.entries = map[string]*entry{}
	p.mu.Unlock()

	p.wg.Wait()
	var firstErr error
	for _, proc := range procs {
		if err := proc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
