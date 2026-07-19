// Package task holds the runner's task model and the in-memory result
// ring. The ring is a dashboard convenience, not durable state — durable
// task truth belongs to the hub (ADR-0002/0008).
package task

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// State is a task's lifecycle phase.
type State string

// Task states. Waiting means admission is blocked on resource capacity
// (ADR-0005) — the only queueing the runner does.
const (
	StateWaiting   State = "waiting"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

// OpStat mirrors engine per-operator stats in API form. StepID is the flow
// step id (v2 graph) or the synthesized linear id (source, op0…, sink).
type OpStat struct {
	Name       string  `json:"name"`
	StepID     string  `json:"step_id,omitempty"`
	RecordsIn  int64   `json:"records_in"`
	RecordsOut int64   `json:"records_out"`
	Seconds    float64 `json:"seconds"`
}

// Task is one flow execution.
type Task struct {
	ID        string     `json:"id"`
	Flow      string     `json:"flow"`
	Benchmark bool       `json:"benchmark,omitempty"`
	State     State      `json:"state"`
	Error     string     `json:"error,omitempty"`
	Submitted time.Time  `json:"submitted"`
	Started   *time.Time `json:"started,omitempty"`
	Finished  *time.Time `json:"finished,omitempty"`

	RecordsIn     int64    `json:"records_in"`
	RecordsOut    int64    `json:"records_out"`
	SinkConfirmed int64    `json:"sink_confirmed,omitempty"`
	Ops           []OpStat `json:"ops,omitempty"`

	// Error handling (v2 flows). When a step fails and its flow declares an
	// onFailure handler, the handler runs and these record it. The task
	// still ends failed (data never reached the real sink).
	Handled      bool   `json:"handled,omitempty"`
	HandlerStep  string `json:"handler_step,omitempty"`
	HandlerError string `json:"handler_error,omitempty"`
}

// NewID returns a 16-byte random hex task id.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b[:])
}

// Totals are monotonic counters across the runner's lifetime.
type Totals struct {
	Submitted int64 `json:"submitted"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Waiting   int64 `json:"waiting"`
	Running   int64 `json:"running"`
	RecordsIn int64 `json:"records_in"`
}

// Store is a bounded ring of recent tasks plus lifetime totals. Safe for
// concurrent use.
type Store struct {
	mu     sync.RWMutex
	byID   map[string]*Task
	order  []string // newest last; bounded to cap
	limit  int
	totals Totals
}

// NewStore keeps the most recent limit tasks (default 500).
func NewStore(limit int) *Store {
	if limit <= 0 {
		limit = 500
	}
	return &Store{byID: map[string]*Task{}, limit: limit}
}

// Add registers a new task (state Waiting).
func (s *Store) Add(t *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[t.ID] = t
	s.order = append(s.order, t.ID)
	s.totals.Submitted++
	s.totals.Waiting++
	if len(s.order) > s.limit {
		evict := s.order[0]
		s.order = s.order[1:]
		delete(s.byID, evict)
	}
}

// Update mutates a task under the store lock, keeping state counters
// consistent.
func (s *Store) Update(id string, fn func(*Task)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.byID[id]
	if !ok {
		return
	}
	before := t.State
	fn(t)
	if before == t.State {
		return
	}
	switch before {
	case StateWaiting:
		s.totals.Waiting--
	case StateRunning:
		s.totals.Running--
	default:
	}
	switch t.State {
	case StateRunning:
		s.totals.Running++
	case StateCompleted:
		s.totals.Completed++
		s.totals.RecordsIn += t.RecordsIn
	case StateFailed:
		s.totals.Failed++
	default:
	}
}

// Get returns a copy of the task.
func (s *Store) Get(id string) (Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.byID[id]
	if !ok {
		return Task{}, false
	}
	return *t, true
}

// Recent returns up to n tasks, newest first.
func (s *Store) Recent(n int) []Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n <= 0 || n > len(s.order) {
		n = len(s.order)
	}
	out := make([]Task, 0, n)
	for i := len(s.order) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, *s.byID[s.order[i]])
	}
	return out
}

// Totals returns lifetime counters.
func (s *Store) Totals() Totals {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totals
}
