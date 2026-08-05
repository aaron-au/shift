package config

import (
	"errors"
	"sort"
	"strings"
)

// Selector names the runners eligible to serve a route: a set of labels a
// runner must carry (ADR-0038 §5, ADR-0030 placement).
//
// A runner matches when its own labels are a SUPERSET of the selector — the
// selector says what must be true, never what must be absent. A single group
// string could not express "any production API runner", which is the shape
// real fleets have; two labels can.
//
// The empty selector matches every runner. That is deliberate for a
// single-group deployment, where demanding ceremony to say "any runner" would
// be noise — but it means an empty selector on a route in a MIXED fleet will
// happily pick a non-production runner, so the hub is expected to be explicit.
type Selector map[string]string

// Matches reports whether a runner carrying labels satisfies s.
func (s Selector) Matches(labels map[string]string) bool {
	for k, want := range s {
		if labels[k] != want {
			return false
		}
	}
	return true
}

// Validate rejects a selector the gateway cannot evaluate. Empty keys are the
// real hazard: `{"": "production"}` silently matches nothing forever, and a
// route that can never be served is worse than one that fails to load.
func (s Selector) Validate() error {
	for k := range s {
		if strings.TrimSpace(k) == "" {
			return errors.New("selector: empty label key")
		}
	}
	return nil
}

// String renders a selector deterministically (sorted) for logs and for the
// health endpoint. Map iteration order would otherwise make identical
// configurations look different between two log lines.
func (s Selector) String() string {
	if len(s) == 0 {
		return "*"
	}
	parts := make([]string, 0, len(s))
	for k, v := range s {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
