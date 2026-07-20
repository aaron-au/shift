// Package connpolicy is the hub's per-deployment connector capability
// policy: which connectors a hub will let flows use, list, and resolve.
//
// Shared/cloud hubs disable and HIDE dangerous connectors (filesystem,
// disk, process) so tenant flows cannot reach the host — and cannot even
// see that those connectors exist. Self-hosted hubs run with no policy
// (allow all). The policy is deployment-wide config, not per-tenant (a
// later refinement); it is name-based today — a connector declaring its
// capabilities in a signed manifest is a larger change deferred until the
// name list stops being enough.
package connpolicy

import "strings"

// Policy decides whether a connector name is permitted on this hub.
type Policy struct {
	allow map[string]bool // when non-empty, an allowlist (nothing else permitted)
	deny  map[string]bool // always blocked, applied after the allowlist
}

// New builds a policy. An allowlist, when non-empty, permits only its
// members; the denylist is always subtracted. Empty/empty (the zero
// deployment) permits everything.
func New(allow, deny []string) *Policy {
	return &Policy{allow: toSet(allow), deny: toSet(deny)}
}

// Parse builds a policy from comma-separated config strings (hub flags).
func Parse(allowCSV, denyCSV string) *Policy {
	return New(splitCSV(allowCSV), splitCSV(denyCSV))
}

// Allowed reports whether name may be used, listed, or resolved. A nil
// policy allows everything (self-hosted default).
func (p *Policy) Allowed(name string) bool {
	if p == nil {
		return true
	}
	if len(p.allow) > 0 && !p.allow[name] {
		return false
	}
	return !p.deny[name]
}

// Restricted reports whether the policy blocks anything at all (used to
// skip work and to describe the deployment).
func (p *Policy) Restricted() bool {
	return p != nil && (len(p.allow) > 0 || len(p.deny) > 0)
}

func toSet(names []string) map[string]bool {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]bool, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			m[n] = true
		}
	}
	return m
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.Split(s, ",")
}
