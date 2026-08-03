package api

import (
	"regexp"
	"strings"
	"testing"
)

// handlerAttr matches an inline event-handler attribute (onclick="…") and
// captures its value.
var handlerAttr = regexp.MustCompile(`on[a-z]+="([^"]*)"`)

// interpolation matches one ${…} template substitution.
var interpolation = regexp.MustCompile(`\$\{([^{}]*)\}`)

// TestInlineHandlersUseEscJS guards the studio's most dangerous sink class.
//
// A value interpolated into a JS string literal inside an HTML attribute —
// onclick="fn('${x}')" — must go through escJS, not esc. esc() is inert there:
// the HTML parser decodes character references in an attribute value BEFORE
// the JS parser compiles the handler, so esc()'s &#39; becomes a live quote
// that closes the string literal (stored XSS). This test fails if any handler
// interpolates a value another way, which is how the bug got in.
func TestInlineHandlersUseEscJS(t *testing.T) {
	ui := string(uiHTML)
	checked := 0
	for _, m := range handlerAttr.FindAllStringSubmatch(ui, -1) {
		for _, sub := range interpolation.FindAllStringSubmatch(m[1], -1) {
			expr := strings.TrimSpace(sub[1])
			checked++
			if strings.HasPrefix(expr, "escJS(") {
				continue
			}
			// A loop index or a literal-valued expression carries no user
			// data; anything else must be escaped for the JS-string sink.
			if expr == "i" {
				continue
			}
			t.Errorf("inline handler interpolates %q without escJS:\n  %s", expr, m[0])
		}
	}
	if checked == 0 {
		t.Fatal("no inline-handler interpolations found — the guard is not testing anything")
	}
	t.Logf("checked %d inline-handler interpolations", checked)
}

// TestEscJSDefeatsQuoteBreakout pins the escaper's contract by asserting the
// two properties that matter, at the source level: it escapes the quote with a
// BACKSLASH (which survives entity decoding) rather than as an entity, and it
// composes with esc so the HTML layer is still escaped.
func TestEscJSDefeatsQuoteBreakout(t *testing.T) {
	ui := string(uiHTML)
	if !strings.Contains(ui, `const escJS =`) && !strings.Contains(ui, `const escJS=`) {
		t.Fatal("escJS is not defined in ui.html")
	}
	// The backslash-escape of the single quote is the whole point.
	if !strings.Contains(ui, `"'":"\\'"`) {
		t.Error("escJS must backslash-escape the single quote; an entity escape is decoded back into a quote before JS parses")
	}
	// escJS must feed its result through esc(), or the value would still be
	// raw in the HTML layer (markup injection instead of JS injection).
	if !regexp.MustCompile(`escJS\s*=\s*s\s*=>\s*esc\(`).MatchString(ui) {
		t.Error("escJS must wrap esc() so the HTML layer stays escaped")
	}
}
