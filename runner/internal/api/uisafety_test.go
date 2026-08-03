package api

import (
	"regexp"
	"strings"
	"testing"
)

var handlerAttr = regexp.MustCompile(`on[a-z]+="([^"]*)"`)
var interpolation = regexp.MustCompile(`\$\{([^{}]*)\}`)

// TestInlineHandlersUseEscJS mirrors the hub's guard: a value interpolated
// into a JS string literal inside an HTML attribute must go through escJS.
// esc() is inert there — the HTML parser decodes character references before
// the JS parser sees the handler, turning &#39; back into a live quote.
//
// This dashboard renders task errors, which can carry connector- and
// engine-derived text, so an unescaped sink here is payload-driven XSS.
func TestInlineHandlersUseEscJS(t *testing.T) {
	checked := 0
	for _, m := range handlerAttr.FindAllStringSubmatch(string(uiHTML), -1) {
		for _, sub := range interpolation.FindAllStringSubmatch(m[1], -1) {
			expr := strings.TrimSpace(sub[1])
			checked++
			if strings.HasPrefix(expr, "escJS(") {
				continue
			}
			t.Errorf("inline handler interpolates %q without escJS:\n  %s", expr, m[0])
		}
	}
	if checked == 0 {
		t.Fatal("no inline-handler interpolations found — the guard is not testing anything")
	}
}

// TestTaskFieldsAreEscaped: the task table renders hub-supplied and
// engine-supplied strings. t.error in particular may quote connector output,
// so every one of these must pass through esc().
func TestTaskFieldsAreEscaped(t *testing.T) {
	ui := string(uiHTML)
	for _, raw := range []string{
		"${t.flow}", "${t.error}", "${t.state}", "${c.name}", "${c.version}", "${o.name}",
	} {
		if strings.Contains(ui, raw) {
			t.Errorf("%s is interpolated unescaped; wrap it in esc()", raw)
		}
	}
	for _, want := range []string{"${esc(t.flow)}", "${esc(t.error)}", "${esc(c.name)}"} {
		if !strings.Contains(ui, want) {
			t.Errorf("expected %s in the dashboard", want)
		}
	}
}
