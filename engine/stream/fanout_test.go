package stream

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// captureSink records the "s" field of every record it receives. It can add a
// per-write delay (to exercise slow-branch backpressure under -race) and fail
// after a given number of records.
type captureSink struct {
	mu       sync.Mutex
	got      []string
	delay    time.Duration
	failAt   int // -1 = never
	failWith error
}

func newCapture() *captureSink { return &captureSink{failAt: -1} }

func (c *captureSink) Write(_ context.Context, b *record.Batch) error {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range b.Records() {
		if c.failAt >= 0 && len(c.got) >= c.failAt {
			return c.failWith
		}
		s, _ := r.Field("s")
		c.got = append(c.got, s.String())
	}
	return nil
}

func (c *captureSink) Close() error { return nil }

func (c *captureSink) sorted() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.got...)
	sort.Strings(out)
	return out
}

func upstreamOf(recs ...string) Source {
	return ndjson.NewReader(strings.NewReader(strings.Join(recs, "\n")+"\n"), ndjson.ReaderOptions{BatchRecords: 2})
}

// identity branch: no operators, straight to sink (may be Shared).
func passthrough(sink Sink, name string, shared bool) Branch {
	return Branch{
		Build:  func(src Source) *Pipeline { return New(src, name) },
		Sink:   sink,
		Name:   name,
		Shared: shared,
	}
}

func TestTeeDuplicatesToAllBranches(t *testing.T) {
	a, b := newCapture(), newCapture()
	rep, err := RunTee(context.Background(),
		upstreamOf(`{"s":"x"}`, `{"s":"y"}`, `{"s":"z"}`),
		[]Branch{passthrough(a, "A", true), passthrough(b, "B", true)},
		FanoutOptions{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"x", "y", "z"}
	for name, sink := range map[string]*captureSink{"A": a, "B": b} {
		if got := sink.sorted(); !equalStrings(got, want) {
			t.Fatalf("branch %s got %v, want %v", name, got, want)
		}
	}
	if len(rep.Branches) != 2 {
		t.Fatalf("reports = %d, want 2", len(rep.Branches))
	}
}

// A copy-on-write (mutating) branch must not corrupt a shared read-only
// sibling that reads the same snapshot.
func TestTeeCOWIsolatesMutation(t *testing.T) {
	readOnly := newCapture()
	mutated := newCapture()
	branches := []Branch{
		// read-only: shares the snapshot.
		passthrough(readOnly, "ro", true),
		// mutating: a project that renames $.s → $.s (rebuild) then drops it,
		// setting a different field. Shared=false ⇒ COW.
		{
			Name: "mut",
			Sink: mutated,
			Build: func(src Source) *Pipeline {
				return New(src, "mut").Project(
					ProjectField{Out: "s", From: record.MustParsePath("$.other")},
				)
			},
			Shared: false,
		},
	}
	if _, err := RunTee(context.Background(),
		upstreamOf(`{"s":"keep","other":"MUT"}`),
		branches, FanoutOptions{}); err != nil {
		t.Fatalf("run: %v", err)
	}
	// The read-only branch still sees the original $.s == "keep".
	if got := readOnly.sorted(); !equalStrings(got, []string{"keep"}) {
		t.Fatalf("read-only branch corrupted: got %v, want [keep]", got)
	}
	// The mutating branch rebuilt $.s from $.other.
	if got := mutated.sorted(); !equalStrings(got, []string{"MUT"}) {
		t.Fatalf("mutating branch: got %v, want [MUT]", got)
	}
}

// A slow branch throttles the tee (bounded queue) but neither branch loses
// records. Run under -race in the gate. Depth 1 forces the driver to block.
func TestTeeSlowBranchBackpressure(t *testing.T) {
	fast := newCapture()
	slow := &captureSink{failAt: -1, delay: 2 * time.Millisecond}
	recs := make([]string, 0, 20)
	for i := range 20 {
		recs = append(recs, `{"s":"`+string(rune('a'+i))+`"}`)
	}
	if _, err := RunTee(context.Background(), upstreamOf(recs...),
		[]Branch{passthrough(fast, "fast", true), passthrough(slow, "slow", true)},
		FanoutOptions{QueueDepth: 1}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(fast.sorted()) != 20 || len(slow.sorted()) != 20 {
		t.Fatalf("record counts fast=%d slow=%d, want 20/20", len(fast.sorted()), len(slow.sorted()))
	}
}

// A branch error tears the whole topology down and surfaces from RunTee.
func TestTeeBranchErrorPropagates(t *testing.T) {
	boom := errors.New("sink boom")
	ok := newCapture()
	bad := &captureSink{failAt: 1, failWith: boom}
	_, err := RunTee(context.Background(),
		upstreamOf(`{"s":"1"}`, `{"s":"2"}`, `{"s":"3"}`, `{"s":"4"}`),
		[]Branch{passthrough(ok, "ok", true), passthrough(bad, "bad", true)},
		FanoutOptions{QueueDepth: 1})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

type erroringSource struct {
	err error
}

func (e *erroringSource) Next(context.Context) (*record.Batch, error) { return nil, e.err }
func (e *erroringSource) Close() error                                { return nil }

func TestTeeUpstreamErrorPropagates(t *testing.T) {
	boom := errors.New("upstream boom")
	a, b := newCapture(), newCapture()
	_, err := RunTee(context.Background(), &erroringSource{err: boom},
		[]Branch{passthrough(a, "A", true), passthrough(b, "B", true)},
		FanoutOptions{})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want upstream boom", err)
	}
}

// Router partitions records to a single branch each; counts sum to the input
// minus dropped records.
func TestRouterPartitions(t *testing.T) {
	evens, odds, dropped := newCapture(), newCapture(), newCapture()
	// branch 0 = "e*", branch 1 = "o*", else drop.
	match := func(v record.Value) int {
		s, _ := v.Field("s")
		switch {
		case strings.HasPrefix(s.String(), "e"):
			return 0
		case strings.HasPrefix(s.String(), "o"):
			return 1
		default:
			return -1
		}
	}
	branches := []Branch{
		passthrough(evens, "e", true),
		passthrough(odds, "o", true),
		passthrough(dropped, "x", true),
	}
	if _, err := RunRouter(context.Background(),
		upstreamOf(`{"s":"e1"}`, `{"s":"o1"}`, `{"s":"e2"}`, `{"s":"drop"}`, `{"s":"o2"}`),
		branches, match, FanoutOptions{}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := evens.sorted(); !equalStrings(got, []string{"e1", "e2"}) {
		t.Fatalf("evens got %v", got)
	}
	if got := odds.sorted(); !equalStrings(got, []string{"o1", "o2"}) {
		t.Fatalf("odds got %v", got)
	}
	if got := dropped.sorted(); len(got) != 0 {
		t.Fatalf("drop branch got %v, want none", got)
	}
}

// A router branch owns its batch exclusively, so a mutating op is safe even
// with Shared=true (no sibling reads it).
func TestRouterBranchMutatesExclusive(t *testing.T) {
	out := newCapture()
	branches := []Branch{
		{
			Name: "m",
			Sink: out,
			Build: func(src Source) *Pipeline {
				return New(src, "m").Project(ProjectField{Out: "s", From: record.MustParsePath("$.v")})
			},
			Shared: true, // exclusive batch ⇒ safe to mutate without COW
		},
	}
	match := func(record.Value) int { return 0 }
	if _, err := RunRouter(context.Background(),
		upstreamOf(`{"s":"orig","v":"NEW"}`),
		branches, match, FanoutOptions{}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := out.sorted(); !equalStrings(got, []string{"NEW"}) {
		t.Fatalf("router mutate got %v, want [NEW]", got)
	}
}

func equalStrings(a, b []string) bool { return slices.Equal(a, b) }
