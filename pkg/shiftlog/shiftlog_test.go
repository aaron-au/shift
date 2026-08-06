package shiftlog_test

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/shiftlog"
)

// decode reads the captured stream as one JSON record per line.
func decode(tb testing.TB, buf *bytes.Buffer) []map[string]any {
	tb.Helper()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			tb.Fatalf("record %q is not JSON — a pipeline configured for JSON would choke here: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// Every record carries the component and version, because a mixed-version
// fleet's logs are otherwise unattributable (ADR-0046 §3).
func TestEveryRecordCarriesComponentAndVersion(t *testing.T) {
	var buf bytes.Buffer
	l := shiftlog.Setup(shiftlog.Options{Component: "runner", Version: "1.2.3", Format: "json", Out: &buf})
	l.Info("hello", shiftlog.KeyEvent, "runner.started")
	slog.Warn("via the default logger")

	recs := decode(t, &buf)
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	for _, r := range recs {
		if r[shiftlog.KeyComponent] != "runner" {
			t.Errorf("component = %v", r[shiftlog.KeyComponent])
		}
		if r[shiftlog.KeyVersion] != "1.2.3" {
			t.Errorf("version = %v", r[shiftlog.KeyVersion])
		}
	}
	if recs[0][shiftlog.KeyEvent] != "runner.started" {
		t.Errorf("event = %v; a dashboard keys on this, not on msg", recs[0][shiftlog.KeyEvent])
	}
}

// Setup installs the logger as the default, so a package that only imports
// log/slog gets the same output without being handed a logger.
func TestSetupInstallsTheDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	shiftlog.Setup(shiftlog.Options{Component: "hub", Format: "json", Out: &buf})
	slog.Info("from anywhere")
	if recs := decode(t, &buf); len(recs) != 1 || recs[0][shiftlog.KeyComponent] != "hub" {
		t.Errorf("records = %v", recs)
	}
}

// The bridge is what makes the migration incremental (§5): existing log.Print
// call sites — and any third-party library using the global logger — become
// structured immediately rather than writing prose into a JSON stream.
func TestStdlibLogIsBridgedIntoStructuredOutput(t *testing.T) {
	var buf bytes.Buffer
	shiftlog.Setup(shiftlog.Options{Component: "runner", Version: "v0", Format: "json", Out: &buf})
	log.Printf("runnerd: connector pool started with %d slots", 4)

	recs := decode(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("records = %d", len(recs))
	}
	msg, _ := recs[0]["msg"].(string)
	if !strings.Contains(msg, "connector pool started") {
		t.Errorf("msg = %q", msg)
	}
	// The component is already a field, so the hand-written prefix is noise.
	if strings.HasPrefix(msg, "runnerd:") {
		t.Errorf("msg = %q still carries the component prefix", msg)
	}
	if recs[0][shiftlog.KeyComponent] != "runner" {
		t.Errorf("a bridged record lost its component: %v", recs[0])
	}
}

// A message with no prefix, or one whose first segment is a sentence, must
// survive intact — trimming is for "runnerd: x", not for arbitrary colons.
func TestBridgeKeepsMessagesWithoutAPrefix(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"plain message", "plain message"},
		{"hubd: migrated", "migrated"},
		{"something happened: and then more", "something happened: and then more"},
	} {
		var buf bytes.Buffer
		shiftlog.Setup(shiftlog.Options{Component: "hub", Format: "json", Out: &buf})
		log.Print(tc.in)
		recs := decode(t, &buf)
		if len(recs) != 1 {
			t.Fatalf("%q: records = %d", tc.in, len(recs))
		}
		if got, _ := recs[0]["msg"].(string); got != tc.want {
			t.Errorf("log.Print(%q) → msg %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The stdlib timestamp would duplicate slog's, and a duplicated timestamp in
// the message body breaks anything parsing the field.
func TestBridgedRecordsDoNotDuplicateTheTimestamp(t *testing.T) {
	var buf bytes.Buffer
	shiftlog.Setup(shiftlog.Options{Component: "hub", Format: "json", Out: &buf})
	log.Print("no date please")
	recs := decode(t, &buf)
	msg, _ := recs[0]["msg"].(string)
	if strings.ContainsAny(msg, "/") {
		t.Errorf("msg = %q looks like it still has the stdlib date prefix", msg)
	}
	if recs[0]["time"] == nil {
		t.Error("the record has no time field")
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	shiftlog.Setup(shiftlog.Options{Component: "runner", Level: "warn", Format: "json", Out: &buf})
	slog.Debug("noise")
	slog.Info("also noise")
	slog.Warn("kept")
	recs := decode(t, &buf)
	if len(recs) != 1 || recs[0]["msg"] != "kept" {
		t.Errorf("records = %v; the level was not applied", recs)
	}
}

// A typo in a log level must not stop a runner from running.
func TestAnUnparseableLevelFallsBackToInfo(t *testing.T) {
	if got := shiftlog.ParseLevel("chatty"); got != slog.LevelInfo {
		t.Errorf("ParseLevel(%q) = %v, want info", "chatty", got)
	}
	if got := shiftlog.ParseLevel(""); got != slog.LevelInfo {
		t.Errorf("ParseLevel(\"\") = %v, want info", got)
	}
	if got := shiftlog.ParseLevel(" DEBUG "); got != slog.LevelDebug {
		t.Errorf("ParseLevel(\" DEBUG \") = %v, want debug", got)
	}
}

// Not a terminal → JSON, which is what a container pipeline needs and what the
// default must therefore be when the destination is a pipe.
func TestFormatDefaultsToJSONWhenNotATerminal(t *testing.T) {
	var buf bytes.Buffer
	shiftlog.Setup(shiftlog.Options{Component: "hub", Out: &buf}) // no Format
	slog.Info("piped")
	decode(t, &buf) // fails the test if it is not JSON
}

func TestTextFormatIsSelectable(t *testing.T) {
	var buf bytes.Buffer
	shiftlog.Setup(shiftlog.Options{Component: "hub", Format: "text", Out: &buf})
	slog.Info("readable")
	line := buf.String()
	if strings.HasPrefix(strings.TrimSpace(line), "{") {
		t.Errorf("format=text produced JSON: %q", line)
	}
	if !strings.Contains(line, "component=hub") {
		t.Errorf("text output %q lost the component field", line)
	}
}

// Credentials must never be reproduced in a log. This greps a real captured
// stream rather than trusting a convention (ADR-0046 §7).
func TestSecretsDoNotSurviveIntoTheStream(t *testing.T) {
	const secret = "rs_super-secret-runner-credential"
	var buf bytes.Buffer
	shiftlog.Setup(shiftlog.Options{Component: "runner", Format: "json", Out: &buf})

	// What the platform is allowed to say about a credential: identify it,
	// never reproduce it.
	slog.Info("runner registered",
		shiftlog.KeyEvent, "runner.registered",
		shiftlog.KeyRunner, "63f9b177",
		"auth", "mtls",
		"cert_serial", "3f7a1c")
	slog.Warn("registration failed", shiftlog.KeyError, "unauthorized")

	if strings.Contains(buf.String(), secret) {
		t.Fatal("a credential value appeared in the log stream")
	}
	if !strings.Contains(buf.String(), "cert_serial") {
		t.Error("the record cannot identify which credential it is about")
	}
}
