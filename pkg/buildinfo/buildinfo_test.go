package buildinfo

import (
	"strings"
	"testing"
)

func TestStringContainsVersion(t *testing.T) {
	if got := String(); !strings.HasPrefix(got, Version) {
		t.Errorf("String() = %q, want prefix %q", got, Version)
	}
}
