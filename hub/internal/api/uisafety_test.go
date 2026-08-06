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

// TestStudioSurfacesDesignNotices guards the wiring of the design-time review
// (ADR-0042 §7) in a studio that has no build step and therefore no compiler.
//
// The point of the review is that a developer learns their route is
// asynchronous, or unverified, at design time rather than from a caller. Every
// piece of that is a string in this file: if one is renamed or dropped, the
// notices silently stop appearing and nothing else fails.
func TestStudioSurfacesDesignNotices(t *testing.T) {
	ui := string(uiHTML)
	for _, want := range []struct{ frag, why string }{
		{`'/api/v1/flows/review'`, "the live canvas review call"},
		{`/review?version=`, "the pre-publish review of a specific version"},
		{`function confirmPublish`, "publish is the last cheap moment to change your mind"},
		{`if (!await confirmPublish(name, v)) return;`, "publish must actually go through the confirmation"},
		{`res.notices`, "the deploy response's notices"},
		{`function showNotices`, "rendering the notices"},
		{`function noticesByStep`, "badging the node a notice is about"},
		{`class="b-notices"`, "the panel the notices render into"},
		// The builder is not the only way to author a webhook's request
		// contract, so a round trip through it must not delete one. A studio
		// that silently dropped an input schema would be the thing that made
		// an endpoint unsafe (ADR-0042 §4, §3d).
		{`if (s.input !== undefined) out.input = s.input;`, "cleanStep must preserve an input schema"},
		{`if (s.ack) out.ack = s.ack;`, "cleanStep must preserve the acknowledgement mode"},
		{`function webhookPickers`, "editing ack + input on a @webhook source"},
	} {
		if !strings.Contains(ui, want.frag) {
			t.Errorf("ui.html no longer contains %q — %s", want.frag, want.why)
		}
	}
	// A notice is server-authored text rendered with innerHTML; it must go
	// through the HTML escaper like every other dynamic string in the studio.
	for _, field := range []string{"esc(n.title || n.code)", "esc(n.detail)", "esc(n.step)"} {
		if !strings.Contains(ui, field) {
			t.Errorf("notice rendering must escape its fields; %q is missing", field)
		}
	}
}
