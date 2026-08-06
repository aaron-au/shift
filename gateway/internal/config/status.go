package config

import (
	"fmt"
	"strings"
)

// The status sub-path (ADR-0042 §3).
//
// A flow's async status lives under the developer's OWN route —
// `GET /orders/_status/{id}` — rather than at a global `/_shift/tasks/{id}`.
// That is not only extra entropy. It makes authorisation structural: a caller
// with access to /orders has no path on which to try a /payroll id, and the
// read inherits the route's entire policy (token, allowlist, rate limit,
// principal) instead of needing a parallel one.

// StatusSegment is the reserved path segment. A route may not contain it.
const StatusSegment = "_status"

// StatusPath returns the status URL path for a route and task id.
func StatusPath(routePath, id string) string {
	return strings.TrimSuffix(routePath, "/") + "/" + StatusSegment + "/" + id
}

// StatusRequest reports whether path is a status read under some configured
// route, returning that route and the task id.
//
// It is deliberately GET-only and deliberately independent of the route's own
// method: a route's method constrains how work is TRIGGERED, not how its
// status is read, and a POST-only route whose status could not be read would
// be a strange thing to ship.
func (c *Config) StatusRequest(method, path string) (*Route, string) {
	if method != "GET" {
		return nil, ""
	}
	i := strings.LastIndex(path, "/"+StatusSegment+"/")
	if i < 0 {
		return nil, ""
	}
	routePath, id := path[:i], path[i+len("/"+StatusSegment+"/"):]
	if routePath == "" {
		routePath = "/"
	}
	if id == "" || strings.Contains(id, "/") {
		return nil, ""
	}
	// Any method's route may own the status path — see above — so this looks
	// the route up by PATH rather than through Lookup, which is method-aware.
	for i := range c.Routes {
		if c.Routes[i].Path == routePath {
			return &c.Routes[i], id
		}
	}
	return nil, ""
}

// validateStatusPaths rejects configurations where a route would shadow a
// status sub-path.
//
// Two shapes are refused. A route containing the reserved segment at all, and
// a route that sits exactly where another route's status reads land — the
// second is the subtle one: `/orders` and `/orders/_status` can both look
// reasonable in a config file, and one of them silently swallows the other's
// status.
func (c *Config) validateStatusPaths() error {
	paths := make(map[string]bool, len(c.Routes))
	for i := range c.Routes {
		paths[c.Routes[i].Path] = true
	}
	for i := range c.Routes {
		p := c.Routes[i].Path
		for seg := range strings.SplitSeq(strings.Trim(p, "/"), "/") {
			if seg == StatusSegment {
				return fmt.Errorf("config: route %q uses the reserved %q segment, "+
					"which is where async status reads land", p, StatusSegment)
			}
		}
		// Would this route's status path be swallowed by another route?
		if paths[strings.TrimSuffix(p, "/")+"/"+StatusSegment] {
			return fmt.Errorf("config: route %q shadows the status path of route %q",
				strings.TrimSuffix(p, "/")+"/"+StatusSegment, p)
		}
	}
	return nil
}
