// Package webhook is the runner's in-memory registry of direct-execution
// hooks (ADR-0016): name → flow document + optional per-hook token. In
// stage 1 hooks are registered on the runner directly; a later stage syncs
// them from the hub. The payload a hook carries stays entirely in the data
// plane — it never reaches the hub.
package webhook

import "sync"

// Hook is one registered webhook: an inbound POST to /hooks/{Name} runs Doc
// with the request body as the flow's @webhook source. TokenHash, when set,
// is the hex SHA-256 of the per-hook credential the caller must present
// (never the plaintext) — so hub-synced and locally-registered hooks share
// one verification path.
type Hook struct {
	Name      string
	Doc       []byte // raw, validated flow document
	TokenHash string // hex sha256 of the token ("" = open)
}

// Registry is a concurrency-safe map of hooks by name.
type Registry struct {
	mu    sync.RWMutex
	hooks map[string]Hook
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{hooks: map[string]Hook{}}
}

// Put registers or replaces a hook.
func (r *Registry) Put(h Hook) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks[h.Name] = h
}

// Get returns the hook and whether it exists.
func (r *Registry) Get(name string) (Hook, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.hooks[name]
	return h, ok
}

// Delete removes a hook; reports whether it existed.
func (r *Registry) Delete(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.hooks[name]
	delete(r.hooks, name)
	return ok
}

// Replace swaps the entire hook set atomically — used by the hub sync loop,
// where the hub is authoritative for an attached runner.
func (r *Registry) Replace(hooks []Hook) {
	m := make(map[string]Hook, len(hooks))
	for _, h := range hooks {
		m[h.Name] = h
	}
	r.mu.Lock()
	r.hooks = m
	r.mu.Unlock()
}

// Names returns the registered hook names (unordered).
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.hooks))
	for n := range r.hooks {
		out = append(out, n)
	}
	return out
}
