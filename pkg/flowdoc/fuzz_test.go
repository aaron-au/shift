package flowdoc

import "testing"

// FuzzParse asserts Parse never panics on arbitrary input — flow documents
// are untrusted API/builder input, so the parser + validator must reject
// malformed input with an error, never crash or hang.
//
// The property is deliberately "no panic", not a marshal→re-parse
// idempotency round trip: an earlier round-trip variant produced a single
// unreproducible failure that ~17M subsequent execs + a 3000× determinism
// probe could not recreate, and a fuzz target that might flake poisons CI.
// (Parse's accept/reject is deterministic — verified; the map ranges in
// validation only affect which error message is returned, not the outcome.)
func FuzzParse(f *testing.F) {
	f.Add([]byte(`{"name":"x","source":{"connector":"gen","action":"gen"},"sink":{"connector":"gen","action":"discard"}}`))
	f.Add([]byte(`{"name":"g","start":"s","steps":[{"id":"s","type":"source","connector":"gen","action":"gen","onSuccess":"o"},{"id":"o","type":"sink","connector":"gen","action":"discard"}]}`))
	f.Add([]byte(`{"name":"f","source":{"connector":"gen","action":"gen"},"ops":[{"type":"filter","path":"$.a","op":"eq","value":1}],"sink":{"connector":"gen","action":"discard"}}`))
	f.Add([]byte(`{"name":"g","start":"a","steps":[{"id":"a","type":"source","connector":"gen","action":"gen","onSuccess":"a"}]}`)) // self-loop
	f.Add([]byte(`{}`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Parse(data) // must not panic; a returned error is a valid outcome
	})
}
