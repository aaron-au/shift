package starlarkop

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// TC-017 (starlark half). Scripts are authored by an authenticated user, so
// this is not an anonymous-attacker surface like the gateway's. It is fuzzed
// anyway for two reasons ADR-0052 states outright: the sandbox bounds blast
// radius rather than authority, and the hub is still open-access until RBAC
// (issue #16) — so "authenticated" is a weaker guarantee here than it sounds.
//
// The properties are the ones the ADR promises, not merely "does not crash":
//
//   - Compile TERMINATES. Top-level code runs during Compile, so a script that
//     loops at the top level would otherwise hang the compile, not the run.
//   - Errors carry NO PAYLOAD. Script error text travels to the hub in an
//     execution report, and starlark's own backtraces quote the values that
//     caused the failure (ADR-0052 §8). cleanError strips them; that must hold
//     for every input, not just the ones someone thought to write a case for.

// FuzzCompile feeds arbitrary script text to the compiler.
func FuzzCompile(f *testing.F) {
	f.Add("def transform(rec):\n  return rec\n")
	f.Add("")
	f.Add("def transform(rec):\n  return None\n")
	f.Add("transform = 1\n")
	f.Add("def transform(a, b):\n  return a\n")
	f.Add("load('x', 'y')\n")                                                 // load() must be refused: no supply chain at all
	f.Add("def f():\n  return f()\ndef transform(r):\n  return f()\n")        // recursion is off
	f.Add("for i in range(100000000): pass\ndef transform(r):\n  return r\n") // top-level loop: fuel must stop it
	f.Add("x = 1\n" + strings.Repeat("x = x + 1\n", 5000))
	f.Add("def transform(rec):\n  return {'a': " + strings.Repeat("[", 200) + strings.Repeat("]", 200) + "}\n")

	f.Fuzz(func(t *testing.T, script string) {
		done := make(chan struct{})
		var err error
		go func() {
			defer close(done)
			_, err = Compile(Options{Script: script, StepID: "s1", Allowed: yes()})
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			// Not a flake guard: top-level code executes during Compile, so a
			// script that never terminates would hang a runner at plan build.
			t.Fatalf("Compile did not terminate within 30s: fuel does not bound top-level execution")
		}
		if err != nil {
			// Only the backtrace check applies here. A compile error naming an
			// identifier from the script is legitimate diagnosis, not a leak:
			// the script is authored configuration that the hub already stores
			// in the flow document, whereas a RECORD's contents are payload it
			// must never see. The fuzzer found that distinction by producing a
			// 64-character identifier and tripping a check that had conflated
			// the two.
			assertNoBacktrace(t, err.Error())
		}
	})
}

// FuzzRun compiles a fixed, well-formed script and runs it over arbitrary
// record content — the other half, where the untrusted thing is the DATA.
func FuzzRun(f *testing.F) {
	f.Add("a", "1")
	f.Add("a", "x")
	f.Add("", "")
	f.Add(strings.Repeat("k", 1000), "1")
	f.Add("a", strings.Repeat("v", 10000))
	f.Add("a\x00b", "\xff\xfe")

	p, err := Compile(Options{
		// Touches the record, returns a derived one: enough to exercise
		// adaptation in both directions without depending on any one shape.
		Script:  "def transform(rec):\n  return {'k': str(rec), 'n': len(str(rec))}\n",
		StepID:  "s1",
		Allowed: yes(),
	})
	if err != nil {
		f.Fatalf("seed script must compile: %v", err)
	}

	f.Fuzz(func(t *testing.T, key, value string) {
		src := record.NewBatch()
		bld := src.Builder()
		bld.BeginMap()
		bld.Key([]byte(key))
		bld.StringLiteral(value)
		bld.EndMap()
		rec := bld.Finish()

		dst := record.NewBatch()
		_, _, err := p.Run(context.Background(), dst, rec)
		if err != nil {
			assertNoPayloadInError(t, err.Error(), key+"\n"+value)
		}
	})
}

// assertNoBacktrace holds the specific half of ADR-0052 §8 that applies to any
// starlark error: a backtrace quotes the values that caused the failure, and
// its presence means cleanError stopped working.
func assertNoBacktrace(t *testing.T, msg string) {
	t.Helper()
	if strings.Contains(msg, "Traceback") || strings.Contains(msg, "in transform\n") {
		t.Fatalf("error text carries a starlark backtrace, which quotes values: %q", msg)
	}
}

// assertNoPayloadInError additionally holds that a RECORD's contents never
// reach the error text. It applies to Run, not to Compile — see FuzzCompile.
func assertNoPayloadInError(t *testing.T, msg, input string) {
	t.Helper()
	assertNoBacktrace(t, msg)
	// A long run of the input appearing verbatim is the general form of the
	// same leak. Short fragments are unavoidable (a name in a syntax error is
	// legitimate diagnosis), so this looks for a substantial quotation.
	for _, line := range strings.Split(input, "\n") {
		s := strings.TrimSpace(line)
		if len(s) >= 64 && strings.Contains(msg, s) {
			t.Fatalf("error text quotes %d bytes of the input verbatim: %q", len(s), msg)
		}
	}
}
