package webhook

import (
	"sort"
	"sync"
	"testing"
)

// TestNewRegistryEmpty: a fresh registry has no hooks and Get misses cleanly.
func TestNewRegistryEmpty(t *testing.T) {
	r := NewRegistry()
	if got := r.Names(); len(got) != 0 {
		t.Fatalf("new registry Names() = %v, want empty", got)
	}
	if _, ok := r.Get("nope"); ok {
		t.Fatalf("Get on empty registry reported ok=true")
	}
}

// TestPutGet: Put registers a hook, Get returns it by value, and a second Put
// with the same name replaces (not duplicates) the prior entry.
func TestPutGet(t *testing.T) {
	r := NewRegistry()
	h := Hook{Name: "ingest", Doc: []byte(`{"name":"f"}`), TokenHash: "abc123"}
	r.Put(h)

	got, ok := r.Get("ingest")
	if !ok {
		t.Fatal("Get after Put reported ok=false")
	}
	if got.Name != h.Name || got.TokenHash != h.TokenHash || string(got.Doc) != string(h.Doc) {
		t.Fatalf("Get returned %+v, want %+v", got, h)
	}

	// Replace-by-name: same name, new contents.
	r.Put(Hook{Name: "ingest", Doc: []byte(`{"name":"g"}`), TokenHash: ""})
	got, ok = r.Get("ingest")
	if !ok {
		t.Fatal("Get after replacing Put reported ok=false")
	}
	if string(got.Doc) != `{"name":"g"}` || got.TokenHash != "" {
		t.Fatalf("replace did not take effect: %+v", got)
	}
	if names := r.Names(); len(names) != 1 {
		t.Fatalf("replace-by-name grew registry to %v, want 1", names)
	}
}

// TestGetMiss: Get on an unknown name returns the zero Hook and ok=false.
func TestGetMiss(t *testing.T) {
	r := NewRegistry()
	r.Put(Hook{Name: "known"})
	got, ok := r.Get("unknown")
	if ok {
		t.Fatalf("Get(unknown) reported ok=true (%+v)", got)
	}
	if got.Name != "" {
		t.Fatalf("Get miss returned non-zero Hook: %+v", got)
	}
}

// TestDelete: Delete reports true for an existing hook and false for a missing
// one, and the deleted hook is no longer retrievable.
func TestDelete(t *testing.T) {
	r := NewRegistry()
	r.Put(Hook{Name: "temp"})

	if !r.Delete("temp") {
		t.Fatal("Delete(existing) reported false")
	}
	if _, ok := r.Get("temp"); ok {
		t.Fatal("hook still present after Delete")
	}
	if r.Delete("temp") {
		t.Fatal("Delete(already-gone) reported true")
	}
	if r.Delete("never-existed") {
		t.Fatal("Delete(unknown) reported true")
	}
}

// TestReplace: Replace swaps the entire hook set atomically, dropping prior
// entries and de-duplicating by name (last write wins).
func TestReplace(t *testing.T) {
	r := NewRegistry()
	r.Put(Hook{Name: "old1"})
	r.Put(Hook{Name: "old2"})

	r.Replace([]Hook{
		{Name: "new1", TokenHash: "h1"},
		{Name: "new2", TokenHash: "h2"},
		{Name: "new2", TokenHash: "override"}, // duplicate name, last wins
	})

	if _, ok := r.Get("old1"); ok {
		t.Error("old1 survived Replace")
	}
	if _, ok := r.Get("old2"); ok {
		t.Error("old2 survived Replace")
	}
	if h, ok := r.Get("new1"); !ok || h.TokenHash != "h1" {
		t.Errorf("new1 = %+v, ok=%v", h, ok)
	}
	if h, ok := r.Get("new2"); !ok || h.TokenHash != "override" {
		t.Errorf("new2 dedup last-wins failed: %+v, ok=%v", h, ok)
	}

	names := r.Names()
	sort.Strings(names)
	if len(names) != 2 || names[0] != "new1" || names[1] != "new2" {
		t.Fatalf("Names after Replace = %v, want [new1 new2]", names)
	}
}

// TestReplaceEmpty: Replace with an empty slice clears the registry.
func TestReplaceEmpty(t *testing.T) {
	r := NewRegistry()
	r.Put(Hook{Name: "a"})
	r.Put(Hook{Name: "b"})
	r.Replace(nil)
	if names := r.Names(); len(names) != 0 {
		t.Fatalf("Replace(nil) left %v, want empty", names)
	}
}

// TestNames: Names returns exactly the registered names (order-independent).
func TestNames(t *testing.T) {
	r := NewRegistry()
	want := []string{"alpha", "beta", "gamma"}
	for _, n := range want {
		r.Put(Hook{Name: n})
	}
	got := r.Names()
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("Names() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Names() = %v, want %v", got, want)
		}
	}
}

// TestConcurrentAccess exercises every method concurrently so the RWMutex
// discipline is validated under -race. It asserts no panic/corruption; exact
// final contents are non-deterministic by design.
func TestConcurrentAccess(t *testing.T) {
	r := NewRegistry()
	const workers = 16
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			name := string(rune('a' + id%8))
			for i := 0; i < iters; i++ {
				switch i % 6 {
				case 0:
					r.Put(Hook{Name: name, Doc: []byte("d"), TokenHash: "t"})
				case 1:
					r.Get(name)
				case 2:
					r.Delete(name)
				case 3:
					r.Names()
				case 4:
					r.Replace([]Hook{{Name: name}, {Name: "shared"}})
				case 5:
					r.Get("shared")
				}
			}
		}(w)
	}
	wg.Wait()

	// Registry must still be usable and internally consistent after the storm.
	r.Replace(nil)
	r.Put(Hook{Name: "final"})
	if _, ok := r.Get("final"); !ok {
		t.Fatal("registry unusable after concurrent access")
	}
	if names := r.Names(); len(names) != 1 || names[0] != "final" {
		t.Fatalf("post-storm Names() = %v, want [final]", names)
	}
}
