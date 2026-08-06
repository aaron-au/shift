package schema_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/schema"
)

// Conformance against the official JSON-Schema-Test-Suite (ADR-0042 §4c-i).
//
// The rule this enforces: a keyword is enabled only if it passes the
// specification's own corpus for that keyword. Nothing here is written by us,
// which is the point — hand-written tests check the cases the author thought
// of, and the cases an author does not think of are exactly where a validator
// quietly disagrees with the spec.
//
// Groups whose schema this subset refuses to compile are SKIPPED and counted:
// refusing to compile is the designed behaviour for an unsupported keyword, so
// treating it as a failure would be measuring the wrong thing. The skip count
// is reported, and the assertion floor below stops the whole thing passing
// vacuously if a change ever made everything refuse to compile.
//
// Vendored under testdata/suite from
// github.com/json-schema-org/JSON-Schema-Test-Suite (tests/draft2020-12), MIT.

type suiteGroup struct {
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
	Tests       []struct {
		Description string          `json:"description"`
		Data        json.RawMessage `json:"data"`
		Valid       bool            `json:"valid"`
	} `json:"tests"`
}

// minAssertions guards against a VACUOUS pass. If a regression made Compile
// refuse everything, every group would skip and this test would report success
// while checking nothing.
//
// It is a collapse detector, not a coverage target: the vendored corpus
// currently yields 498 assertions, and the floor sits just under that. Raising
// the supported keyword set should raise this number; a drop means schemas that
// used to compile no longer do.
const minAssertions = 480

func TestJSONSchemaConformanceSuite(t *testing.T) {
	files, err := filepath.Glob("testdata/suite/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no vendored suite files found (%v); run scripts/fetch-schema-suite.sh", err)
	}
	sort.Strings(files)

	var totalRun, totalSkipped int
	skippedReasons := map[string]int{}

	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".json")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(file) // #nosec G304 -- vendored test fixture
			if err != nil {
				t.Fatal(err)
			}
			var groups []suiteGroup
			if err := json.Unmarshal(raw, &groups); err != nil {
				t.Fatal(err)
			}

			var run, skipped int
			for _, g := range groups {
				s, err := schema.Compile(g.Schema)
				if err != nil {
					// Not a failure: this subset refuses what it cannot
					// enforce, and that refusal is itself tested elsewhere.
					skipped++
					skippedReasons[reasonOf(err)]++
					continue
				}
				for _, tc := range g.Tests {
					doc, err := parseJSON(compact(tc.Data))
					if err != nil {
						t.Fatalf("%s / %s: parsing instance %s: %v",
							g.Description, tc.Description, tc.Data, err)
					}
					got := s.Valid(doc)
					run++
					if got != tc.Valid {
						t.Errorf("%s\n  case:     %s\n  schema:   %s\n  instance: %s\n  got valid=%v, want %v",
							g.Description, tc.Description, g.Schema, tc.Data, got, tc.Valid)
					}
				}
			}
			t.Logf("%d assertions run, %d group(s) skipped as unsupported", run, skipped)
			totalRun += run
			totalSkipped += skipped
		})
	}

	t.Logf("TOTAL: %d assertions run, %d groups skipped", totalRun, totalSkipped)
	for _, r := range sortedReasons(skippedReasons) {
		t.Logf("  skipped: %-52s ×%d", r, skippedReasons[r])
	}
	if totalRun < minAssertions {
		t.Errorf("only %d conformance assertions ran, want at least %d — "+
			"a change that made Compile refuse everything would otherwise pass this test silently",
			totalRun, minAssertions)
	}
}

// reasonOf condenses a compile error to its cause, so the skip summary is
// readable rather than a wall of distinct messages.
func reasonOf(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "unsupported keyword"):
		if i := strings.Index(s, "unsupported keyword(s) "); i >= 0 {
			rest := s[i+len("unsupported keyword(s) "):]
			if j := strings.Index(rest, " —"); j > 0 {
				rest = rest[:j]
			}
			if j := strings.Index(rest, " ("); j > 0 {
				rest = rest[:j]
			}
			return "unsupported keyword " + rest
		}
		return "unsupported keyword"
	case strings.Contains(s, "not local"):
		return "remote $ref"
	case strings.Contains(s, "recursive"):
		return "recursive $ref"
	case strings.Contains(s, "no such member"), strings.Contains(s, "cannot descend"):
		return "$ref the subset cannot resolve"
	case strings.Contains(s, "additionalProperties must be true or false"):
		return "additionalProperties in schema form"
	case strings.Contains(s, "const/enum members must be scalars"):
		return "const/enum with a non-scalar member"
	case strings.Contains(s, "unsupported format"):
		return "unsupported format"
	default:
		return s
	}
}

func sortedReasons(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// compact renders an instance on ONE line, which is what the line reader
// consumes. json.RawMessage from the suite is already compact, but a
// multi-line fixture would otherwise be split into several records.
func compact(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return string(raw)
	}
	return buf.String()
}
