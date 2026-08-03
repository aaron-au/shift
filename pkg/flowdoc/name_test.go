package flowdoc

import (
	"strings"
	"testing"
)

// TestFlowNameCharset: a flow name is both a URL path segment in the hub's
// control API and a value rendered into the studio's DOM. Names carrying
// markup, quotes, or path syntax are rejected at validation so neither sink
// has to be the only line of defense.
func TestFlowNameCharset(t *testing.T) {
	doc := func(name string) []byte {
		return []byte(`{"name":` + quote(name) + `,"source":{"connector":"http","action":"get"},` +
			`"sink":{"connector":"http","action":"post"}}`)
	}

	for _, ok := range []string{"orders", "Orders Sync", "orders-v2", "a.b_c-d", "X1"} {
		if _, err := Parse(doc(ok)); err != nil {
			t.Errorf("valid name %q rejected: %v", ok, err)
		}
	}

	bad := map[string]string{
		"quote breakout":  `x');alert(1);//`,
		"markup":          `<img src=x onerror=alert(1)>`,
		"double quote":    `a"b`,
		"backtick":        "a`b",
		"path traversal":  `../../keys/rotate?`,
		"slash":           `a/b`,
		"leading space":   ` orders`,
		"leading dash":    `-orders`,
		"newline":         "a\nb",
		"too long":        strings.Repeat("a", 129),
		"backslash":       `a\b`,
		"percent encoded": `a%2Fb`,
	}
	for what, name := range bad {
		t.Run(what, func(t *testing.T) {
			if _, err := Parse(doc(name)); err == nil {
				t.Fatalf("name %q accepted, want rejection", name)
			}
		})
	}
}

// quote produces a JSON string literal for the given value.
func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
