package shiftlog_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The gateway does not import pkg/shiftlog — its go.mod has zero dependencies,
// and that is an auditable security property of the one component that may sit
// in a DMZ (ADR-0046 §2). The contract between the three binaries is therefore
// the OUTPUT SCHEMA, and this is what enforces it: nothing else would notice
// the two drifting apart until an operator's filter silently stopped matching.

// repoFile reads a file relative to the repository root.
func repoFile(tb testing.TB, rel string) string {
	tb.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", rel)) // #nosec G304 -- repo-relative test fixture
	if err != nil {
		tb.Fatalf("reading %s: %v", rel, err)
	}
	return string(raw)
}

// TestGatewayEmitsTheSameSchema pins the fields every record must carry.
func TestGatewayEmitsTheSameSchema(t *testing.T) {
	src := repoFile(t, "gateway/cmd/gatewayd/main.go")
	for _, want := range []struct{ frag, why string }{
		{`slog.String("component", "gateway")`, "every record must name its component"},
		{`slog.String("version", buildVersion)`, "every record must carry the build version"},
		{`slog.NewJSONHandler(os.Stdout`, "logs go to stdout, in JSON when not a terminal"},
		{`slog.NewTextHandler(os.Stdout`, "and in text when they are"},
		{`stdlog.SetOutput(logBridge{log})`, "the stdlib logger must not write prose into a JSON stream"},
		{`SHIFT_LOG_FORMAT`, "the format override is platform-wide"},
		{`SHIFT_LOG_LEVEL`, "the level knob is platform-wide"},
	} {
		if !strings.Contains(src, want.frag) {
			t.Errorf("gatewayd no longer contains %q — %s", want.frag, want.why)
		}
	}
}

// The other two go through Setup, so asserting they call it is enough; what
// Setup produces is covered by the tests in this package.
func TestHubAndRunnerUseSetup(t *testing.T) {
	for _, f := range []string{"hub/cmd/hubd/main.go", "runner/cmd/runnerd/main.go"} {
		src := repoFile(t, f)
		if !strings.Contains(src, "shiftlog.Setup(") {
			t.Errorf("%s does not configure logging through shiftlog.Setup", f)
		}
		if !strings.Contains(src, "SHIFT_LOG_FORMAT") {
			t.Errorf("%s does not honour SHIFT_LOG_FORMAT", f)
		}
	}
}

// Nothing may log to stderr as a matter of course (ADR-0046 §1): the operator
// decides where stdout goes, and a component that splits its output across two
// streams makes that decision meaningless. Fatal exit messages are the one
// exception, and they live in this package.
func TestNoBinaryLogsToStderr(t *testing.T) {
	handler := regexp.MustCompile(`slog\.New(JSON|Text)Handler\(os\.Stderr`)
	for _, f := range []string{
		"hub/cmd/hubd/main.go",
		"runner/cmd/runnerd/main.go",
		"gateway/cmd/gatewayd/main.go",
	} {
		if handler.MatchString(repoFile(t, f)) {
			t.Errorf("%s installs a log handler on stderr; logs go to stdout", f)
		}
	}
}

// The gateway's zero-dependency go.mod is the reason its logging setup is
// duplicated rather than shared. If that stops being true, the duplication has
// no justification and should be deleted in favour of pkg/shiftlog.
func TestGatewayModuleStaysDependencyFree(t *testing.T) {
	mod := repoFile(t, "gateway/go.mod")
	if strings.Contains(mod, "require") {
		t.Errorf("gateway/go.mod has grown dependencies:\n%s\n"+
			"ADR-0046 §2 duplicates the logging setup to preserve exactly this; "+
			"if the property is gone, import pkg/shiftlog instead of keeping the copy", mod)
	}
}

// Every record a long-running binary emits should be filterable the same way,
// which means an event name — including the per-request access line, which is
// otherwise the one high-volume record you cannot select on.
func TestTheHubAccessLineCarriesAnEvent(t *testing.T) {
	src := repoFile(t, "hub/internal/api/api.go")
	if !strings.Contains(src, `slog.String("event", "hub.request")`) {
		t.Error("the hub access line has no event name; a request record cannot be filtered like the others")
	}
	// Platform-wide key names (ADR-0046 §3), not per-call-site spellings.
	for _, k := range []string{`slog.String("request", rid)`, `slog.Int64("duration_ms"`} {
		if !strings.Contains(src, k) {
			t.Errorf("the access line does not use the platform key: %s", k)
		}
	}
}

// A process dying must not look like every other record. log.Fatalf through
// the stdlib bridge emits at INFO, so the binaries use shiftlog.Fatalf, which
// is ERROR plus a stderr copy for when the logger itself is what failed.
func TestBinariesDoNotUseStdlibFatal(t *testing.T) {
	for _, f := range []string{"hub/cmd/hubd/main.go", "runner/cmd/runnerd/main.go"} {
		src := repoFile(t, f)
		if strings.Contains(src, "log.Fatal") && !strings.Contains(src, "shiftlog.Fatal") {
			t.Errorf("%s uses the stdlib log.Fatal — a fatal would be emitted at INFO", f)
		}
		for _, bad := range []string{"\tlog.Fatalf(", "\tlog.Fatal(", " log.Fatalf(", " log.Fatal("} {
			if strings.Contains(src, bad) {
				t.Errorf("%s still calls the stdlib %q; use shiftlog.Fatalf", f, strings.TrimSpace(bad))
			}
		}
	}
}
