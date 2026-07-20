package api

import "testing"

// TestCSVSafe pins spreadsheet formula-injection neutralization on audit CSV
// export: a leading =,+,-,@ is prefixed with a quote; other values pass through.
func TestCSVSafe(t *testing.T) {
	risky := []string{"=cmd()", "+1", "-2", "@ref"}
	for _, s := range risky {
		if got := csvSafe(s); got != "'"+s {
			t.Errorf("csvSafe(%q) = %q, want %q", s, got, "'"+s)
		}
	}
	for _, s := range []string{"", "user:a@b.com", "secret.put", "normal"} {
		if got := csvSafe(s); got != s {
			t.Errorf("csvSafe(%q) = %q, want unchanged", s, got)
		}
	}
}
