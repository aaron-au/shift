package api

import (
	"regexp"
	"strings"
	"testing"
)

// The studio is a round-trip editor: it reads a stored flow document, lets
// somebody move a node, and writes the whole document back. Anything its
// serialiser forgets is not an editing bug — it is silent DATA LOSS on
// redeploy, and it looks like the flow simply changed behaviour.
//
// That has real teeth per field:
//
//   - connection (ADR-0034): the step falls back to inline config it does not
//     have, so it loses its host and credentials.
//   - version (ADR-0047 §1): a published flow goes back to "newest at
//     dispatch", which is exactly what pinning removed.
//   - input / ack (ADR-0042): an endpoint loses its request verification, so
//     the studio becomes the thing that made it unsafe.
//
// This guards the serialiser at the source level because there is no JS
// runtime in the test suite; it is a coarse net, but the failure it catches is
// the one that has already happened once.
func TestBuilderSerialiserCarriesEveryStoredField(t *testing.T) {
	body := funcBody(t, string(uiHTML), "cleanStep")
	for _, field := range []string{"connection", "version", "input", "ack", "config", "mock", "testInput"} {
		if !strings.Contains(body, "out."+field) {
			t.Errorf("cleanStep drops %q: a flow opened in the builder and redeployed would lose it silently", field)
		}
	}
}

// The connector pin is RECORDED by publishing, never typed. A free-text
// version box would let an author claim an artifact nobody checked exists, and
// the registry is the only thing that knows which builds are real.
func TestThePinnedVersionIsNotAnInputField(t *testing.T) {
	body := funcBody(t, string(uiHTML), "versionRow")
	if strings.Contains(body, "<input") {
		t.Error("versionRow renders an input: a pin is recorded by publishing, not typed")
	}
	if !strings.Contains(body, "unpinStep") {
		t.Error("versionRow offers no way to unpin, so a step can never be moved to a newer build")
	}
}

// funcBody returns the source of one top-level `function name(` declaration,
// up to the matching closing brace at column zero.
func funcBody(t *testing.T, src, name string) string {
	t.Helper()
	start := regexp.MustCompile(`(?m)^function ` + regexp.QuoteMeta(name) + `\(`).FindStringIndex(src)
	if start == nil {
		t.Fatalf("ui.html has no function %s — if it was renamed, this guard needs renaming with it", name)
	}
	rest := src[start[0]:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("could not find the end of %s", name)
	}
	return rest[:end]
}
