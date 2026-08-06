package config

import (
	"fmt"
	"strings"
)

// Runner is one entry in the hub-pushed roster: which runner exists, and what
// it IS (ADR-0041 §3).
//
// The labels live here rather than in the runner's poll body, and that is the
// point. A runner proves its identity with a client certificate and never
// states its own placement, so it cannot promote itself into
// `environment: production` by claiming to be there. Placement is a fact the
// control plane owns — which is what ADR-0038 §5 always said and did not,
// until now, actually enforce.
//
// Labels are deliberately NOT in the certificate either: labels are mutable
// (a runner is retiered, a workload class is added) and identity is not, so
// encoding them would make every placement change a certificate reissue.
type Runner struct {
	ID     string            `json:"id"`
	Labels map[string]string `json:"labels,omitempty"`
}

// LabelsFor returns the hub-asserted labels for a runner id, and whether the
// roster knows it at all.
//
// An unknown runner is reported as unknown rather than as label-less, and the
// caller must refuse it. Treating it as label-less would silently satisfy
// every EMPTY selector — so a runner the hub has never vouched for would
// receive exactly the traffic that nobody bothered to restrict.
func (c *Config) LabelsFor(runnerID string) (map[string]string, bool) {
	if runnerID == "" {
		return nil, false
	}
	for i := range c.Runners {
		if c.Runners[i].ID == runnerID {
			return c.Runners[i].Labels, true
		}
	}
	return nil, false
}

// validateRunners rejects a roster the gateway cannot use.
func (c *Config) validateRunners() error {
	seen := make(map[string]bool, len(c.Runners))
	for i := range c.Runners {
		r := &c.Runners[i]
		if strings.TrimSpace(r.ID) == "" {
			return fmt.Errorf("config: roster entry %d: empty runner id", i)
		}
		if seen[r.ID] {
			// Two entries for one runner means two answers to "what is this
			// runner", and the one that wins would depend on scan order.
			return fmt.Errorf("config: duplicate roster entry for runner %q", r.ID)
		}
		seen[r.ID] = true
		for k := range r.Labels {
			if strings.TrimSpace(k) == "" {
				return fmt.Errorf("config: runner %q: empty label key", r.ID)
			}
		}
	}
	return nil
}
