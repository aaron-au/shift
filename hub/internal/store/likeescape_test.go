package store

import "testing"

// TestLikeEscape pins LIKE-metacharacter escaping used by the audit family
// filter: %, _, and \ become literals so a caller-supplied prefix matches
// precisely (paired with an ESCAPE '\' clause).
func TestLikeEscape(t *testing.T) {
	cases := map[string]string{
		"secret.": "secret.",
		"a_b.":    `a\_b.`,
		"a%b":     `a\%b`,
		`a\b`:     `a\\b`,
		"a_%\\.":  `a\_\%\\.`,
	}
	for in, want := range cases {
		if got := likeEscape(in); got != want {
			t.Errorf("likeEscape(%q) = %q, want %q", in, got, want)
		}
	}
}
