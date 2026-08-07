package flowdoc

import "fmt"

// Connector version pinning (ADR-0047 §1).
//
// A flow that does not say which connector build it runs takes whatever is
// newest at the moment a task dispatches — so publishing a connector changes
// behaviour for every flow using it, on the next run, against live data.
// Nobody chose that; it is what "resolve latest" means once a registry holds
// more than one version.
//
// The fix is not new machinery, it is moving the existing resolution earlier:
// a DRAFT still means "newest" (a draft has no promise to keep), and PUBLISHING
// records the answer. From then on the flow runs the builds it was published
// against, and upgrading is a republish with a visible diff.
//
// This lives in flowdoc because both ends need the same traversal: the hub
// rewrites documents at publish, and the retention/locate queries read the
// pins back out. Two implementations would drift, and the drift would be
// invisible — a step the reader missed is a version nothing holds a reference
// to, which is a version GC is free to delete out from under a live flow.

// Pin is one connector step's pinned build.
type Pin struct {
	StepID    string // "source" / "sink" in the linear form
	Connector string
	Version   string // empty when the step is unpinned (a draft)
}

// ConnectorPins reports every registry-connector step in the document, in a
// stable order. Built-ins are excluded: they are compiled into the runner, so
// there is no artifact to pin and nothing for retention to count.
func (d *Document) ConnectorPins() []Pin {
	var out []Pin
	add := func(id string, ep Endpoint) {
		if ep.Connector == "" || IsBuiltinConnector(ep.Connector) {
			return
		}
		out = append(out, Pin{StepID: id, Connector: ep.Connector, Version: ep.Version})
	}
	if len(d.Steps) > 0 {
		for i := range d.Steps {
			s := &d.Steps[i]
			if isConnectorType(s.Type) {
				add(s.ID, s.Endpoint())
			}
		}
	} else {
		add("source", d.Source)
		add("sink", d.Sink)
	}
	return out
}

// PinConnectors fills in every unpinned connector step, in place, by asking
// resolve for the version to record.
//
// Already-pinned steps are LEFT ALONE. That is what makes republishing an
// unchanged document a no-op rather than a silent upgrade: moving a flow
// forward is an edit somebody makes, not something publish does to them. It
// also means a rollback — republishing an older version — keeps the builds
// that version was published against.
//
// resolve returning an error aborts the whole document: a half-pinned flow
// would run a mix of recorded and newest builds, which is the ambiguity this
// exists to remove. Returning an empty version is not an error — it means the
// registry has nothing to pin, which is legitimate.
func (d *Document) PinConnectors(resolve func(connector string) (string, error)) error {
	pin := func(ep *Endpoint) error {
		if ep.Connector == "" || IsBuiltinConnector(ep.Connector) || ep.Version != "" {
			return nil
		}
		version, err := resolve(ep.Connector)
		if err != nil {
			return err
		}
		if version == "" {
			// Nothing to pin — the registry has no artifact for this
			// connector, which is legitimate in a deployment that provisions
			// binaries locally. It stays unpinned and the `connector-pin`
			// review check reports it, rather than the hub deciding that a
			// registry is mandatory.
			return nil
		}
		if !ConnectorVersionPattern.MatchString(version) {
			return fmt.Errorf("connector %q: resolved version %q is not a usable version", ep.Connector, version)
		}
		ep.Version = version
		return nil
	}
	if len(d.Steps) > 0 {
		for i := range d.Steps {
			s := &d.Steps[i]
			if !isConnectorType(s.Type) {
				continue
			}
			ep := s.Endpoint()
			if err := pin(&ep); err != nil {
				return err
			}
			s.Version = ep.Version
		}
		return nil
	}
	if err := pin(&d.Source); err != nil {
		return err
	}
	return pin(&d.Sink)
}
