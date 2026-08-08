package sdktest_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/sdktest"
)

// The gate itself, tested the only way a gate can honestly be tested: by
// running it against changes that MUST fail, and confirming they do. A
// compatibility check nobody has watched refuse anything is worse than none —
// it converts an unverified promise into a verified-looking one.

const hostPort = `{"type":"object",
  "properties":{"host":{"type":"string"},"port":{"type":"integer"}},
  "required":["host"]}`

const hostOnly = `{"type":"object",
  "properties":{"host":{"type":"string"}},"required":["host"]}`

// connector builds a fixture. Action factories are nil because BuildDescriptor
// reads only the keys — nothing here ever serves a stream.
func connector(version, compatClass, schema string) sdk.Connector {
	c := sdk.Connector{
		Name: "demo", Version: version, Compat: compatClass,
		Sources: map[string]func() sdk.SourceAction{"get": nil},
		Sinks:   map[string]func() sdk.SinkAction{"put": nil},
	}
	if schema != "" {
		c.Schemas = map[string][]byte{"get": []byte(schema)}
	}
	return c
}

// baseline records c as the last-released surface, using CheckSurface's own
// writer so the fixture cannot drift from the thing under test.
func baseline(t *testing.T, c sdk.Connector) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "surface.json")
	t.Setenv(sdktest.UpdateEnv, "1")
	if out := check(t, c, path); out == "" {
		t.Fatalf("recording a new baseline said nothing; it must be loud enough not to read as a pass")
	}
	t.Setenv(sdktest.UpdateEnv, "")
	return path
}

// check runs CheckSurface against a recorder and returns what it complained
// about — "" meaning it passed.
func check(t *testing.T, c sdk.Connector, golden string) string {
	t.Helper()
	rec := &recorder{name: t.Name()}
	rec.run(func() { sdktest.CheckSurface(rec, c, golden) })
	return rec.out.String()
}

// A declared class weaker than the change is the exact failure §8 exists to
// stop: every downstream feature — §5's currency notices, §9's bulk-upgrade
// report — renders that class as if somebody had checked it.
func TestABreakingChangeDeclaredCompatibleFailsTheBuild(t *testing.T) {
	golden := baseline(t, connector("1.0.0", "compatible", hostPort))

	// A config field removed, declared compatible.
	out := check(t, connector("2.0.0", "compatible", hostOnly), golden)
	if !strings.Contains(out, `declares Compat "compatible"`) || !strings.Contains(out, `is "breaking"`) {
		t.Fatalf("a breaking change declared compatible did not fail:\n%s", out)
	}
	if !strings.Contains(out, `config field "port" removed`) {
		t.Fatalf("the failure does not name what changed:\n%s", out)
	}
	// The way out has to be in the message, or the next person guesses.
	if !strings.Contains(out, sdktest.UpdateEnv) {
		t.Fatalf("the failure does not say how to re-record:\n%s", out)
	}
}

// Declaring something stronger than required is always allowed. A publisher
// who knows a "compatible" change will surprise people has a real reason to
// say breaking, and the gate has no business arguing.
func TestDeclaringAStrongerClassIsAccepted(t *testing.T) {
	golden := baseline(t, connector("1.0.0", "compatible", hostPort))

	next := connector("2.0.0", "breaking", hostPort)
	next.Sources["list"] = nil // purely additive

	if out := check(t, next, golden); out != "" {
		t.Fatalf("over-declaring was refused:\n%s", out)
	}
}

// A pin that resolves to a different shape is a pin that means nothing
// (ADR-0047 §1). Shipping a changed surface under the same version is exactly
// that.
func TestAChangedSurfaceMustMoveTheVersion(t *testing.T) {
	golden := baseline(t, connector("1.0.0", "compatible", hostPort))

	next := connector("1.0.0", "compatible", hostPort)
	next.Sources["list"] = nil

	out := check(t, next, golden)
	if !strings.Contains(out, "without moving its version") {
		t.Fatalf("an unversioned surface change was accepted:\n%s", out)
	}
}

// §5 and §9 both render breaking hops loudly. A class claimed over a release
// that changed nothing visible spends that attention for nothing, and an
// operator who learns to ignore the warning has lost the only one that counts.
func TestAClassClaimedOverNothingIsRefused(t *testing.T) {
	golden := baseline(t, connector("1.0.0", "compatible", hostPort))

	out := check(t, connector("2.0.0", "breaking", hostPort), golden)
	if !strings.Contains(out, "surface is unchanged") {
		t.Fatalf("a breaking claim over an identical surface was accepted:\n%s", out)
	}
	// The same release declared honestly passes.
	if out := check(t, connector("2.0.0", "compatible", hostPort), golden); out != "" {
		t.Fatalf("an unchanged surface was refused:\n%s", out)
	}
}

// Undeclared is not a pass. §6 shows "undeclared" separately from
// "compatible" for the same reason: it does not mean the change is safe, it
// means nobody said.
func TestAnUndeclaredClassDoesNotCoverEvenACompatibleChange(t *testing.T) {
	golden := baseline(t, connector("1.0.0", "", hostPort))

	next := connector("2.0.0", "", hostPort)
	next.Sources["list"] = nil

	out := check(t, next, golden)
	if !strings.Contains(out, "(undeclared)") {
		t.Fatalf("an undeclared class was waved through:\n%s", out)
	}
}

// Update mode is the escape hatch, and it must genuinely re-record —
// otherwise the only way past a legitimate change is hand-editing the golden,
// which is how goldens end up wrong.
func TestUpdateModeReRecordsTheSurface(t *testing.T) {
	golden := baseline(t, connector("1.0.0", "compatible", hostPort))
	next := connector("2.0.0", "breaking", hostOnly)

	t.Setenv(sdktest.UpdateEnv, "1")
	if out := check(t, next, golden); !strings.Contains(out, "updated") {
		t.Fatalf("update mode did not report a re-record:\n%s", out)
	}

	// Recorded, so the same build now passes WITHOUT the flag.
	t.Setenv(sdktest.UpdateEnv, "")
	if out := check(t, next, golden); out != "" {
		t.Fatalf("the re-recorded surface does not match itself:\n%s", out)
	}
}

// A class describes a hop somebody already took. Rewriting it after the
// release has shipped changes what §5 and §9 tell an operator about a version
// they have already installed — the notice moves, the release does not.
//
// This rule is why the golden records the class alongside the shape. Without
// it, re-recording a legitimate breaking release would leave the connector
// still declaring "breaking" against a now-matching surface, and the very
// next CI run would fail every release the day after it shipped.
func TestAReleasedClassCannotBeRewrittenAfterTheFact(t *testing.T) {
	golden := baseline(t, connector("2.0.0", "breaking", hostPort))

	// Same version, same shape, quietly downgraded claim.
	out := check(t, connector("2.0.0", "compatible", hostPort), golden)
	if !strings.Contains(out, "it was released as") {
		t.Fatalf("a rewritten class was accepted:\n%s", out)
	}

	// The release re-checked unchanged — the normal CI run after a release —
	// passes. This is the case that would fail without the recorded class.
	if out := check(t, connector("2.0.0", "breaking", hostPort), golden); out != "" {
		t.Fatalf("re-checking a just-released version failed:\n%s", out)
	}
}

// recorder captures what CheckSurface reports instead of failing the
// enclosing test, which is what makes the refusing direction testable at all.
type recorder struct {
	name   string
	out    strings.Builder
	failed bool
}

// bail unwinds a Fatalf the way testing.T's runtime.Goexit does, without
// taking the real test with it.
type bail struct{}

func (r *recorder) run(fn func()) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(bail); !ok {
				panic(p)
			}
		}
	}()
	fn()
}

func (r *recorder) Helper()      {}
func (r *recorder) Name() string { return r.name }
func (r *recorder) Failed() bool { return r.failed }

func (r *recorder) Logf(format string, args ...any) {
	r.out.WriteString(fmt.Sprintf(format, args...) + "\n")
}

func (r *recorder) Errorf(format string, args ...any) {
	r.failed = true
	r.Logf(format, args...)
}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failed = true
	r.Logf(format, args...)
	panic(bail{})
}
