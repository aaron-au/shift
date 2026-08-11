package schema

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TC-018. Compile inlines each $ref by recompiling its target at every
// reference SITE, and the cycle guard only rejects a $ref already on the
// CURRENT stack. A diamond — each level referencing the next twice — is not a
// cycle, so nothing stops it, and the compiled form doubles per level: n levels
// cost 2^n nodes from a schema whose text grows linearly.
//
// This is the same class as TC-022: structure that is cheap to write and
// expensive to expand. ADR-0042 §4 leans on Compile being cheap, and today the
// only thing keeping this out of reach is that schema text is authored by an
// authenticated user — the weakest of the guarantees here, since the hub is
// still open-access until RBAC (issue #16).
func diamondSchema(levels int) string {
	var b strings.Builder
	b.WriteString(`{"$defs":{`)
	for i := range levels {
		if i > 0 {
			b.WriteString(",")
		}
		// Each level references the next TWICE. Two references to one target is
		// ordinary schema reuse, not an abuse — which is why nothing rejects it.
		fmt.Fprintf(&b, `"L%d":{"type":"object","properties":{`, i)
		if i == levels-1 {
			b.WriteString(`"leaf":{"type":"string"}`)
		} else {
			fmt.Fprintf(&b, `"a":{"$ref":"#/$defs/L%d"},"b":{"$ref":"#/$defs/L%d"}`, i+1, i+1)
		}
		b.WriteString(`}}`)
	}
	b.WriteString(`},"$ref":"#/$defs/L0"}`)
	return b.String()
}

func TestSchemaCompilationIsBounded(t *testing.T) {
	// 40 levels is ~1 KB of text and 2^40 nodes if expansion is unbounded.
	const levels = 40
	src := diamondSchema(levels)
	t.Logf("schema text is %d bytes for %d levels", len(src), levels)

	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	before := ms.TotalAlloc

	done := make(chan error, 1)
	go func() {
		_, err := Compile([]byte(src))
		done <- err
	}()

	select {
	case err := <-done:
		runtime.ReadMemStats(&ms)
		t.Logf("Compile returned %v, allocating %d MiB", err, (ms.TotalAlloc-before)>>20)
		// Either outcome is acceptable — compiling cheaply, or refusing — but
		// it must not be "spend unbounded time and memory trying".
		if grew := ms.TotalAlloc - before; grew > 64<<20 {
			t.Fatalf("compiling %d bytes of schema allocated %d MiB: $ref expansion is unbounded",
				len(src), grew>>20)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("Compile did not finish within 20s for %d bytes of schema: "+
			"each $ref is recompiled per reference site, so a diamond of $defs expands 2^n",
			len(src))
	}
}

// TestIndirectRecursionIsStillRefusedUnderMemoisation is the guard on the fix
// itself. Memoising $ref targets is only sound if a target still being compiled
// can never be served from the cache — otherwise A -> B -> A would resolve to a
// half-built node and compile "successfully" into something with no bounded
// form. The memo is written only AFTER a successful compile, and this pins it.
func TestIndirectRecursionIsStillRefusedUnderMemoisation(t *testing.T) {
	src := `{"$ref":"#/$defs/A","$defs":{` +
		`"A":{"type":"object","properties":{"b":{"$ref":"#/$defs/B"}}},` +
		`"B":{"type":"object","properties":{"a":{"$ref":"#/$defs/A"}}}}}`
	_, err := Compile([]byte(src))
	if err == nil {
		t.Fatal("indirect recursion A->B->A compiled; a recursive schema has no bounded compiled form")
	}
	if !strings.Contains(err.Error(), "recursive") {
		t.Fatalf("indirect recursion refused as %q, not as recursion", err)
	}
}

// TestOrdinarySchemaReuseStillCompiles guards the other side: referencing one
// definition from several places is normal, and a bound that refused it would
// refuse most real schemas.
func TestOrdinarySchemaReuseStillCompiles(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"$defs":{"addr":{"type":"object","properties":{"city":{"type":"string"}}}},`)
	b.WriteString(`"type":"object","properties":{`)
	for i := range 200 {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `"a%d":{"$ref":"#/$defs/addr"}`, i)
	}
	b.WriteString(`}}`)

	if _, err := Compile([]byte(b.String())); err != nil {
		t.Fatalf("200 references to one definition were refused: %v", err)
	}
}
