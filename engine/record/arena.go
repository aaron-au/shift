package record

// Chunked allocators backing a Batch. Chunks are never grown in place, so
// slices handed out remain valid until Reset. Reset keeps the largest chunk
// and drops the rest, so a warmed batch stops allocating on the steady state.

const (
	minByteChunk = 64 << 10 // 64 KiB
	maxByteChunk = 4 << 20  // 4 MiB cap per chunk; more chunks, not bigger ones
	minSlabLen   = 512      // elements
	maxSlabLen   = 64 << 10
)

// byteArena hands out stable []byte copies.
type byteArena struct {
	chunks [][]byte // chunks[len-1] is current; all have fixed cap
	used   int64    // total bytes handed out since Reset
}

func (a *byteArena) copyIn(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	c := a.room(len(b))
	start := len(c)
	c = append(c, b...)
	a.chunks[len(a.chunks)-1] = c
	a.used += int64(len(b))
	return c[start:len(c):len(c)]
}

// copyInString copies a Go string into the arena without an intermediate
// []byte conversion (append accepts strings directly, so this is alloc-free).
func (a *byteArena) copyInString(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	c := a.room(len(s))
	start := len(c)
	c = append(c, s...)
	a.chunks[len(a.chunks)-1] = c
	a.used += int64(len(s))
	return c[start:len(c):len(c)]
}

// room returns the current chunk, guaranteed to have capacity for n more
// bytes (opening a new chunk if needed).
func (a *byteArena) room(n int) []byte {
	cur := len(a.chunks) - 1
	if cur < 0 || cap(a.chunks[cur])-len(a.chunks[cur]) < n {
		size := minByteChunk
		if cur >= 0 {
			size = min(cap(a.chunks[cur])*2, maxByteChunk)
		}
		size = max(size, n)
		a.chunks = append(a.chunks, make([]byte, 0, size))
		cur = len(a.chunks) - 1
	}
	return a.chunks[cur]
}

func (a *byteArena) reset() {
	if len(a.chunks) == 0 {
		a.used = 0
		return
	}
	// Keep only the largest chunk to converge on zero steady-state allocs.
	largest := 0
	for i, c := range a.chunks {
		if cap(c) > cap(a.chunks[largest]) {
			largest = i
		}
	}
	keep := a.chunks[largest][:0]
	a.chunks = append(a.chunks[:0], keep)
	a.used = 0
}

// memSize reports bytes currently reserved by the arena.
func (a *byteArena) memSize() int64 {
	var n int64
	for _, c := range a.chunks {
		n += int64(cap(c))
	}
	return n
}

// valSlab hands out stable []Value slices.
type valSlab struct {
	chunks [][]Value
	used   int64
}

func (s *valSlab) alloc(n int) []Value {
	if n == 0 {
		return nil
	}
	cur := -1
	if l := len(s.chunks); l > 0 {
		cur = l - 1
	}
	if cur < 0 || cap(s.chunks[cur])-len(s.chunks[cur]) < n {
		size := minSlabLen
		if cur >= 0 {
			size = min(cap(s.chunks[cur])*2, maxSlabLen)
		}
		size = max(size, n)
		s.chunks = append(s.chunks, make([]Value, 0, size))
		cur = len(s.chunks) - 1
	}
	c := s.chunks[cur]
	start := len(c)
	s.chunks[cur] = c[:start+n] // extend len, keep chunk capacity
	s.used += int64(n)
	return c[start : start+n : start+n] // capped view: callers cannot append into slab space
}

func (s *valSlab) reset() {
	if len(s.chunks) == 0 {
		s.used = 0
		return
	}
	largest := 0
	for i, c := range s.chunks {
		if cap(c) > cap(s.chunks[largest]) {
			largest = i
		}
	}
	keep := s.chunks[largest][:0]
	clear(s.chunks[largest][:cap(s.chunks[largest])]) // drop references for GC
	s.chunks = append(s.chunks[:0], keep)
	s.used = 0
}

func (s *valSlab) memSize() int64 {
	var n int64
	for _, c := range s.chunks {
		n += int64(cap(c)) * int64(valueSize)
	}
	return n
}

// keySlab hands out stable [][]byte slices (map field names).
type keySlab struct {
	chunks [][][]byte
	used   int64
}

func (s *keySlab) alloc(n int) [][]byte {
	if n == 0 {
		return nil
	}
	cur := -1
	if l := len(s.chunks); l > 0 {
		cur = l - 1
	}
	if cur < 0 || cap(s.chunks[cur])-len(s.chunks[cur]) < n {
		size := minSlabLen
		if cur >= 0 {
			size = min(cap(s.chunks[cur])*2, maxSlabLen)
		}
		size = max(size, n)
		s.chunks = append(s.chunks, make([][]byte, 0, size))
		cur = len(s.chunks) - 1
	}
	c := s.chunks[cur]
	start := len(c)
	s.chunks[cur] = c[:start+n]
	s.used += int64(n)
	return c[start : start+n : start+n]
}

func (s *keySlab) reset() {
	if len(s.chunks) == 0 {
		s.used = 0
		return
	}
	largest := 0
	for i, c := range s.chunks {
		if cap(c) > cap(s.chunks[largest]) {
			largest = i
		}
	}
	keep := s.chunks[largest][:0]
	clear(s.chunks[largest][:cap(s.chunks[largest])])
	s.chunks = append(s.chunks[:0], keep)
	s.used = 0
}

func (s *keySlab) memSize() int64 {
	var n int64
	for _, c := range s.chunks {
		n += int64(cap(c)) * 24 // slice header
	}
	return n
}

const valueSize = 88 // approximate unsafe.Sizeof(Value{}); kept as a constant to avoid unsafe
