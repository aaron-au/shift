package sdktest

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/compat"
)

// The compatibility gate (ADR-0047 §8).
//
// ADR-0047 builds real machinery on the compatibility class a version
// declares: §5's currency notices fold it across a span, and §9's bulk
// upgrade shows an operator what crossing three releases will cost before
// they move forty flows. Every one of those is only as good as the
// declaration underneath it — and a class chosen by a human at publish time,
// about a diff they wrote weeks earlier, is exactly the promise that rots
// first and quietly.
//
// CheckSurface makes it a build failure instead. Each connector records the
// action surface it last shipped as a golden file; the test compares the
// current surface against it and fails if the connector declares a class the
// diff cannot support. "Discipline on our side" becomes CI.

// UpdateEnv is the environment variable that rewrites golden surfaces instead
// of failing on them.
//
// Regenerating is a deliberate act with a visible diff, not a flag somebody
// leaves on. The recorded surface is the only evidence of what was released;
// a gate that refreshed it automatically would agree with every change and
// catch none.
const UpdateEnv = "SHIFT_UPDATE_SURFACE"

// T is the slice of *testing.T that CheckSurface uses.
//
// It is an interface rather than the concrete type for one reason: a gate is
// only trustworthy if somebody has watched it REFUSE something, and a check
// that takes *testing.T can only ever be tested in the passing direction —
// its failures take the enclosing test down with them. `testing.TB` cannot
// serve here because it is sealed against outside implementations.
type T interface {
	Helper()
	Name() string
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Failed() bool
}

// goldenSurface is what gets checked into the repo next to each connector: a
// released version's FULL claim — its identity, the class it was released
// under, and the shape it shipped.
//
// Compat is recorded rather than derived, and that is load-bearing. Once a
// release is recorded, its declaration belongs to it: the connector still
// says `Compat: "breaking"` in code, and on the next CI run the surface now
// matches the golden. Without the recorded class, that run would read as "a
// breaking class claimed over no change" and fail every legitimate release
// the day after it shipped.
type goldenSurface struct {
	// Note is first so anybody opening the file learns what it is before
	// they learn what is in it.
	Note       string          `json:"_note"`
	Name       string          `json:"name"`
	Version    string          `json:"version"`
	Compat     string          `json:"compat,omitempty"`
	Descriptor json.RawMessage `json:"descriptor"`
}

const goldenNote = "Recorded connector action surface — the LAST RELEASED shape. " +
	"The compatibility gate (ADR-0047 §8) diffs the current build against this and refuses a " +
	"declared compat class the diff cannot support. Regenerate deliberately with SHIFT_UPDATE_SURFACE=1 " +
	"and review the diff; it is the only record of what was shipped."

// CheckSurface is the gate. Call it from a connector's own test:
//
//	func TestSurfaceStaysCompatible(t *testing.T) {
//	    sdktest.CheckSurface(t, Connector(), "testdata/surface.json")
//	}
//
// It enforces four things, and each exists because of a specific way a
// connector release goes wrong:
//
//  1. The declared class covers the computed one. A publisher may always
//     declare something STRONGER than the diff requires — knowing a
//     "compatible" change will surprise people is a real reason to say
//     breaking — but never weaker.
//  2. A changed surface moves the version. Shipping a different shape under
//     the version somebody already pinned is precisely the silent change
//     ADR-0047 §1 removes; a pin that resolves to a different surface makes
//     pinning meaningless.
//  3. An unchanged surface may not claim a class. "breaking" on a release
//     that changed nothing visible burns the signal — §5 and §9 both surface
//     breaking hops loudly, and an operator who learns to ignore them has
//     lost the only warning that matters.
//  4. A released version's class is not rewritten. A class describes a hop
//     somebody already took; editing it afterwards changes what §5 and §9
//     say about a version already installed, without changing the version.
//
// What it CANNOT see is behaviour: no static check knows that an HTTP source
// started following redirects. That is the honest boundary, and it is why
// `behaviour-change` stays a human judgement the gate accepts without proof.
func CheckSurface(t T, c sdk.Connector, goldenPath string) {
	t.Helper()

	current := sdk.BuildDescriptor(c)
	canonical, err := sdk.CanonicalDescriptor(current)
	if err != nil {
		t.Fatalf("surface: canonical descriptor: %v", err)
	}

	raw, err := os.ReadFile(goldenPath) //nolint:gosec // a test-owned path from the caller's own package
	if os.IsNotExist(err) {
		// First run for a new connector: record what it is, and say so
		// loudly enough that nobody mistakes it for a passing gate.
		writeGolden(t, goldenPath, c, canonical)
		t.Fatalf("surface: recorded a new baseline at %s — review and commit it; "+
			"there was nothing to compare this build against", goldenPath)
	}
	if err != nil {
		t.Fatalf("surface: read %s: %v", goldenPath, err)
	}

	var golden goldenSurface
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("surface: %s is not a recorded surface: %v", goldenPath, err)
	}
	var previous sdk.Descriptor
	if err := json.Unmarshal(golden.Descriptor, &previous); err != nil {
		t.Fatalf("surface: %s holds an unreadable descriptor: %v", goldenPath, err)
	}

	report := compat.Compare(previous, current)
	declared := compat.Class(c.Compat)

	if os.Getenv(UpdateEnv) != "" {
		writeGolden(t, goldenPath, c, canonical)
		t.Logf("surface: %s updated (%s)", goldenPath, report.String())
		return
	}

	// Same version as the golden means this IS the recorded release, being
	// re-checked. Nothing may have moved since it was recorded — not the
	// shape, and not the claim made about it.
	if c.Version == golden.Version {
		if report.Changed() {
			t.Errorf("surface: %s %s changed its public surface without moving its version.\n"+
				"A pin that resolves to a different shape is a pin that means nothing (ADR-0047 §1).\n%s",
				c.Name, c.Version, report.String())
		}
		if c.Compat != golden.Compat {
			t.Errorf("surface: %s %s now declares Compat %q, but it was released as %q.\n"+
				"A class describes the hop somebody already took; rewriting it after the fact changes "+
				"what §5 and §9 tell an operator about a release they have already installed.",
				c.Name, c.Version, orNone(c.Compat), orNone(golden.Compat))
		}
		return
	}

	// A new version. This is where the declaration is actually made.
	if !report.Changed() {
		// Nothing visible moved. A class here is a claim with no evidence,
		// and §5/§9 render breaking hops loudly enough that a false one costs
		// real attention.
		if declared == compat.Breaking || declared == compat.Behaviour {
			t.Errorf("surface: %s %s declares Compat %q but its public surface is unchanged since %s.\n"+
				"Either the change is invisible to this gate (behaviour — say so in the release notes, "+
				"not in a class the tooling renders as a hard hop), or the declaration is stale.",
				c.Name, c.Version, c.Compat, golden.Version)
		}
	} else if !compat.AtLeast(declared, report.Class) {
		t.Errorf("surface: %s %s declares Compat %q, but the change against %s is %q.\n"+
			"Declare at least %q, or narrow the change.\n%s",
			c.Name, c.Version, orNone(c.Compat), golden.Version, report.Class, report.Class, report.String())
	}
	if t.Failed() {
		t.Logf("If this change is intended and correctly declared, re-record the surface:\n"+
			"    %s=1 go test ./... -run %s", UpdateEnv, t.Name())
	}
}

func orNone(s string) string {
	if s == "" {
		return "(undeclared)"
	}
	return s
}

func writeGolden(t T, path string, c sdk.Connector, canonical []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("surface: %v", err)
	}
	out, err := json.MarshalIndent(goldenSurface{
		Note: goldenNote, Name: c.Name, Version: c.Version, Compat: c.Compat, Descriptor: canonical,
	}, "", "  ")
	if err != nil {
		t.Fatalf("surface: %v", err)
	}
	if err := os.WriteFile(path, append(out, '\n'), 0o600); err != nil {
		t.Fatalf("surface: write %s: %v", path, err)
	}
}
