package starlarkop

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// The tests in this file are the ones that must not regress quietly. Each maps
// to a numbered clause of ADR-0052; a failure here is a security property gone,
// not a feature broken.

// §9 — off by default. The sandbox bounds the blast radius; it does not answer
// who is allowed. While the hub is open-access, a flow author is anyone who can
// reach the studio.
func TestCodeStepsAreRefusedUnlessTheDeploymentOptsIn(t *testing.T) {
	no := false
	_, err := Compile(Options{Script: "def transform(rec): return rec", StepID: "s1", Allowed: &no})
	if !errors.Is(err, ErrNotAllowed) {
		t.Fatalf("error = %v, want ErrNotAllowed", err)
	}
	// The refusal has to name the setting, or an operator cannot act on it.
	if !strings.Contains(err.Error(), AllowEnv) {
		t.Errorf("refusal %q does not name %s", err, AllowEnv)
	}

	// And the env var is what decides it when the caller does not.
	t.Setenv(AllowEnv, "")
	if Allowed() {
		t.Error("code steps allowed with the variable unset")
	}
	t.Setenv(AllowEnv, "1")
	if !Allowed() {
		t.Error("code steps refused with the variable set to 1")
	}
	t.Setenv(AllowEnv, "no")
	if Allowed() {
		t.Error("code steps allowed with the variable set to no")
	}
}

// §6 — no I/O, and therefore no supply chain. Nothing to import means nothing
// to pin, vendor, audit or revoke.
func TestAScriptCannotReachAnythingOutsideItself(t *testing.T) {
	forbidden := []struct {
		name   string
		script string
	}{
		{"load", `load("other.star", "f")` + "\ndef transform(rec): return rec"},
		{"open", `def transform(rec): return {"x": open("/etc/passwd")}`},
		{"import", `import os` + "\ndef transform(rec): return rec"},
		{"exec", `def transform(rec): return {"x": exec("1")}`},
		{"eval", `def transform(rec): return {"x": eval("1")}`},
		{"time", `def transform(rec): return {"x": time.now()}`},
		{"os", `def transform(rec): return {"x": os.getenv("PATH")}`},
		{"random", `def transform(rec): return {"x": random.random()}`},
		{"http", `def transform(rec): return {"x": http.get("http://example.com")}`},
	}
	for _, c := range forbidden {
		t.Run(c.name, func(t *testing.T) {
			prog, err := Compile(Options{Script: c.script, StepID: "s1", Allowed: yes()})
			if err != nil {
				return // rejected at compile time, which is even better
			}
			_, _, err = runOne(t, prog, func(b *record.Builder) {
				b.KeyLiteral("x")
				b.Int(1)
			})
			if err == nil {
				t.Errorf("%s was available to a script", c.name)
			}
		})
	}
}

// §4 — fuel, not wall-clock. Deterministic: a script either always fits its
// budget or never does, so this cannot flake on a loaded machine.
func TestAnUnboundedLoopIsStoppedByFuel(t *testing.T) {
	prog, err := Compile(Options{
		Script: `
def transform(rec):
    n = 0
    for i in range(100000000):
        n = n + i
    return {"n": n}
`,
		StepID: "s1", Fuel: 10_000, Allowed: yes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	})
	if err == nil {
		t.Fatal("an unbounded loop ran to completion")
	}
	// The message must be actionable rather than a bare interpreter error.
	for _, want := range []string{"budget", "execution steps"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// Recursion is off because fuel cannot save a blown host stack: the interpreter
// enforces it at CALL time, not at parse time, so this asserts the run.
func TestRecursionIsRefused(t *testing.T) {
	prog, err := Compile(Options{
		Script: `
def f(n):
    return 1 if n <= 0 else f(n - 1)
def transform(rec):
    return {"n": f(100000)}
`,
		StepID: "s1", Allowed: yes(),
	})
	if err != nil {
		return // rejected at compile time would be better still
	}
	_, _, err = runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	})
	if err == nil {
		t.Fatal("a recursive script ran")
	}
	if !strings.Contains(err.Error(), "recursi") {
		t.Errorf("error = %v, want it to name the recursion", err)
	}
}

// §4 — output is bounded. Fuel bounds the WORK a script does; this bounds what
// it can hand back, which is a separate budget an accidental loop can exhaust.
func TestAnUnboundedResultIsRefused(t *testing.T) {
	prog, err := Compile(Options{
		Script: `
def transform(rec):
    out = {}
    for i in range(500):
        out[str(i)] = i
    return out
`,
		StepID: "s1", MaxOutputFields: 100, Allowed: yes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	})
	if err == nil || !strings.Contains(err.Error(), "limit is 100") {
		t.Fatalf("error = %v, want a field-count limit", err)
	}

	deep, err := Compile(Options{
		Script: `
def transform(rec):
    v = {"leaf": 1}
    for i in range(50):
        v = {"n": v}
    return v
`,
		StepID: "s1", MaxOutputDepth: 8, Allowed: yes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = runOne(t, deep, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	}); err == nil || !strings.Contains(err.Error(), "nests deeper") {
		t.Fatalf("error = %v, want a depth limit", err)
	}
}

// §5 — determinism. At-least-once delivery (ADR-0002) means a transform that
// varies between attempts produces a different result for the same idempotency
// key, so this is a correctness property, not tidiness.
func TestTheSameRecordAlwaysProducesTheSameOutput(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    fields = []
    for k in rec.keys():
        fields.append(k)
    return {"joined": ",".join(fields), "n": len(fields)}
`)
	var first string
	for range 20 {
		got, _, err := runOne(t, prog, func(b *record.Builder) {
			b.KeyLiteral("a")
			b.Int(1)
			b.KeyLiteral("b")
			b.Int(2)
			b.KeyLiteral("c")
			b.Int(3)
		})
		if err != nil {
			t.Fatal(err)
		}
		if first == "" {
			first = got["joined"]
			continue
		}
		if got["joined"] != first {
			t.Fatalf("iteration order varies between runs: %q then %q", first, got["joined"])
		}
	}
	// And it is the record's own field order, which the record model preserves.
	if first != "string:a,b,c" {
		t.Errorf("joined = %s, want the record's field order", first)
	}
}

// §1/§3 — no state may cross records, or results depend on batch boundaries
// and a retry of a partial batch produces different output.
func TestStateCannotLeakBetweenRecords(t *testing.T) {
	prog, err := Compile(Options{
		Script: `
seen = []
def transform(rec):
    seen.append(rec.id)
    return {"count": len(seen)}
`,
		StepID: "s1", Allowed: yes(),
	})
	if err != nil {
		return // rejected at compile time is fine
	}
	for i := range 3 {
		got, _, runErr := runOne(t, prog, func(b *record.Builder) {
			b.KeyLiteral("id")
			b.Int(int64(i))
		})
		if runErr != nil {
			continue // the frozen global rejects the append, which is the point
		}
		if got["count"] != "int:1" {
			t.Fatalf("record %d saw count %s — state leaked between records", i, got["count"])
		}
	}
}

// §8 — a script error must not carry payload to the hub. The backtrace quotes
// the offending value, and that text travels in an execution report.
func TestScriptErrorsDoNotCarryPayload(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    fail("boom: " + rec.secret_ish)
`)
	_, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("secret_ish")
		b.StringLiteral("4111111111111111")
	})
	if err == nil {
		t.Fatal("expected the script to fail")
	}
	// The script CHOSE to put the value in its message, and we cannot stop
	// that — but the interpreter's own backtrace, which quotes locals and
	// source lines the author did not choose to expose, must not be appended.
	if strings.Contains(err.Error(), "Traceback") || strings.Contains(err.Error(), "flow.star:") {
		t.Errorf("error carries an interpreter backtrace: %q", err)
	}
	if strings.Count(err.Error(), "\n") > 0 {
		t.Errorf("error is multi-line, so it is carrying more than a message: %q", err)
	}
}

// §7 — a script cannot read its own config, so a flow author cannot use one to
// exfiltrate a resolved secret through the payload plane.
func TestAScriptHasNoAccessToConfigOrSecrets(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    names = []
    for n in dir(rec):
        names.append(n)
    return {"names": ",".join(names)}
`)
	got, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"config", "secret", "env"} {
		if strings.Contains(got["names"], forbidden) {
			t.Errorf("a script can see %q: %s", forbidden, got["names"])
		}
	}
}

// print() would be an unbounded, unredacted channel from payload into the
// runner's logs.
func TestPrintGoesNowhere(t *testing.T) {
	prog := compile(t, `
def transform(rec):
    print("card " + rec.pan)
    return {"ok": True}
`)
	if _, _, err := runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("pan")
		b.StringLiteral("4111111111111111")
	}); err != nil {
		t.Fatalf("print should be a no-op, not an error: %v", err)
	}
}

func TestAScriptMustDefineTheEntryPoint(t *testing.T) {
	cases := map[string]string{
		"missing":   `x = 1`,
		"not a fn":  `transform = 1`,
		"wrong ary": `def transform(a, b): return a`,
	}
	for name, script := range cases {
		if _, err := Compile(Options{Script: script, StepID: "s1", Allowed: yes()}); err == nil {
			t.Errorf("%s: compiled without a usable %s", name, EntryPoint)
		}
	}
	if _, err := Compile(Options{Script: "   ", StepID: "s1", Allowed: yes()}); err == nil {
		t.Error("an empty script compiled")
	}
	big := strings.Repeat("# padding\n", DefaultMaxScriptBytes/10+10)
	if _, err := Compile(Options{Script: big, StepID: "s1", Allowed: yes()}); err == nil {
		t.Error("an oversized script compiled")
	}
}

func TestContextCancellationIsHonoured(t *testing.T) {
	prog := compile(t, `def transform(rec): return {"ok": True}`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := prog.Run(ctx, record.NewBatch(), record.Null()); !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
}

func TestOnlyARecordOrNoneMayBeReturned(t *testing.T) {
	for _, script := range []string{
		`def transform(rec): return 1`,
		`def transform(rec): return "x"`,
		`def transform(rec): return [1, 2]`,
	} {
		prog := compile(t, script)
		_, _, err := runOne(t, prog, func(b *record.Builder) {
			b.KeyLiteral("x")
			b.Int(1)
		})
		if err == nil {
			t.Errorf("%q returned a non-record and was accepted", script)
		}
	}
}

// TestADeadlineAbandonsAScriptFuelCannotStop covers the gap fuel leaves: fuel
// counts STEPS, and a single step can allocate a great deal, so a script can
// sit far inside its fuel budget while consuming the machine. Measured before
// this existed: 190 MiB allocated inside a 1000-step budget.
//
// The deadline does not bound memory — nothing in-process can (see isolate.go)
// — but it stops the runner waiting on such a script forever.
func TestADeadlineAbandonsAScriptFuelCannotStop(t *testing.T) {
	prog, err := Compile(Options{
		Script: `
def transform(rec):
    n = 0
    for i in range(100000000):
        n = n + i
    return {"n": n}
`,
		// Fuel high enough not to fire, deadline low enough to.
		StepID: "s1", Fuel: 1 << 62, Deadline: 50 * time.Millisecond, Allowed: yes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, _, err = runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("x")
		b.Int(1)
	})
	if err == nil {
		t.Fatal("a script past its deadline was not abandoned")
	}
	if !strings.Contains(err.Error(), "did not finish") {
		t.Errorf("error = %v, want it to name the deadline", err)
	}
	// The caller must be released promptly rather than waiting on the script.
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("caller blocked for %s after the deadline", elapsed)
	}
}

// TestDeadlineErrorsCarryNoPayload — the abandonment message reaches the hub.
func TestDeadlineErrorsCarryNoPayload(t *testing.T) {
	prog, err := Compile(Options{
		Script: `
def transform(rec):
    n = 0
    for i in range(100000000):
        n = n + rec.pan
    return {"n": n}
`,
		StepID: "s1", Fuel: 1 << 62, Deadline: 30 * time.Millisecond, Allowed: yes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = runOne(t, prog, func(b *record.Builder) {
		b.KeyLiteral("pan")
		b.Int(4111111111111111)
	})
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	if strings.Contains(err.Error(), "4111111111111111") {
		t.Errorf("deadline error carries payload: %q", err)
	}
}
