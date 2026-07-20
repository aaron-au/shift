package flowdoc

import (
	"fmt"
	"sort"
)

// Connectors returns the sorted, unique connector names the document
// references — source and sink in the linear form; every connector step
// (including error handlers) in the graph form. The hub uses this to apply
// its per-deployment connector capability policy at deploy time.
func (d *Document) Connectors() []string {
	seen := map[string]bool{}
	add := func(n string) {
		if n != "" {
			seen[n] = true
		}
	}
	if len(d.Steps) > 0 {
		for i := range d.Steps {
			if isConnectorType(d.Steps[i].Type) {
				add(d.Steps[i].Connector)
			}
		}
	} else {
		add(d.Source.Connector)
		add(d.Sink.Connector)
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Plan is the normalized, validated execution plan a document lowers to.
// Both authoring forms (linear and graph) produce one; the hub validates
// through it at deploy time and the runner compiles from it.
type Plan struct {
	// Main is the happy path in order: Main[0] is the source, the last
	// element is the terminal sink, and everything between is a transform.
	Main []*Step
	// Catch maps each main step id to the error handler that fires when
	// that step errors — its own onFailure, or the nearest preceding main
	// step's onFailure (try/catch scoping). A missing/nil entry means the
	// failure is unhandled and the task fails as it does today.
	Catch map[string]*Step
}

// HandlerFor returns the error handler for a failing main step id, or nil
// when the failure is unhandled.
func (p *Plan) HandlerFor(stepID string) *Step { return p.Catch[stepID] }

// Plan lowers the document to its execution plan. Linear-form documents
// are synthesized directly (Parse already validated them); graph-form
// documents are validated as the plan is built.
func (d *Document) Plan() (*Plan, error) {
	if len(d.Steps) > 0 {
		return d.buildPlan()
	}
	return d.linearPlan(), nil
}

// linearPlan synthesizes the step graph for the linear sugar form:
// source → op0 → … → sink, chained by the happy path, no handlers. The
// synthesized ids (source, op0…, sink) become the per-step telemetry keys.
func (d *Document) linearPlan() *Plan {
	main := make([]*Step, 0, len(d.Ops)+2)

	src := &Step{ID: "source", Connector: d.Source.Connector, Action: d.Source.Action, Config: d.Source.Config}
	src.Type = "source"
	main = append(main, src)

	for i := range d.Ops {
		main = append(main, &Step{ID: fmt.Sprintf("op%d", i), Op: d.Ops[i]})
	}

	sink := &Step{ID: "sink", Connector: d.Sink.Connector, Action: d.Sink.Action, Config: d.Sink.Config}
	sink.Type = "sink"
	main = append(main, sink)

	return &Plan{Main: main, Catch: map[string]*Step{}}
}

// buildPlan validates and lowers the graph form.
func (d *Document) buildPlan() (*Plan, error) {
	// Index + duplicate detection + per-step field validation.
	byID := make(map[string]*Step, len(d.Steps))
	for i := range d.Steps {
		s := &d.Steps[i]
		if s.ID == "" {
			return nil, fmt.Errorf("flow: step %d: id is required", i)
		}
		if _, dup := byID[s.ID]; dup {
			return nil, fmt.Errorf("flow: duplicate step id %q", s.ID)
		}
		byID[s.ID] = s
		if err := s.validate(); err != nil {
			return nil, fmt.Errorf("flow: step %q: %w", s.ID, err)
		}
	}

	// Entry: explicit start, else the sole source step.
	entry := d.Start
	if entry == "" {
		var srcs []string
		for i := range d.Steps {
			if d.Steps[i].Type == "source" {
				srcs = append(srcs, d.Steps[i].ID)
			}
		}
		if len(srcs) != 1 {
			return nil, fmt.Errorf("flow: set start, or provide exactly one source step (found %d)", len(srcs))
		}
		entry = srcs[0]
	}
	start, ok := byID[entry]
	if !ok {
		return nil, fmt.Errorf("flow: start step %q not found", entry)
	}
	if start.Type != "source" {
		return nil, fmt.Errorf("flow: start step %q must be a source", entry)
	}

	// Walk the happy path from the entry. It must be linear (each step one
	// happy edge), acyclic, source-only-at-entry, and terminate at a sink.
	var main []*Step
	onMain := map[string]bool{}
	cur := start
	for {
		if onMain[cur.ID] {
			return nil, fmt.Errorf("flow: cycle in happy path at step %q", cur.ID)
		}
		onMain[cur.ID] = true
		if len(main) > 0 && cur.Type == "source" {
			return nil, fmt.Errorf("flow: step %q: only the entry step may be a source", cur.ID)
		}
		main = append(main, cur)

		next, has := cur.happyEdge()
		if cur.Type == "sink" {
			if has {
				return nil, fmt.Errorf("flow: sink step %q must not have a happy-path edge", cur.ID)
			}
			break // terminal
		}
		if !has {
			return nil, fmt.Errorf("flow: step %q needs an onSuccess/onComplete edge (only a sink terminates the flow)", cur.ID)
		}
		nxt, ok := byID[next]
		if !ok {
			return nil, fmt.Errorf("flow: step %q: edge to unknown step %q", cur.ID, next)
		}
		cur = nxt
	}

	// Handlers: onFailure targets, resolved in main order so each step's
	// effective handler is its own onFailure or the nearest preceding one.
	catch := make(map[string]*Step, len(main))
	handlerIDs := map[string]bool{}
	var current *Step
	for _, s := range main {
		if s.OnFailure != "" {
			h, ok := byID[s.OnFailure]
			if !ok {
				return nil, fmt.Errorf("flow: step %q: onFailure to unknown step %q", s.ID, s.OnFailure)
			}
			if h.Type != "sink" {
				return nil, fmt.Errorf("flow: onFailure handler %q must be a sink step", h.ID)
			}
			if onMain[h.ID] {
				return nil, fmt.Errorf("flow: onFailure handler %q must not be on the main path", h.ID)
			}
			if _, hasHappy := h.happyEdge(); hasHappy || h.OnFailure != "" {
				return nil, fmt.Errorf("flow: handler step %q must not have outgoing edges", h.ID)
			}
			handlerIDs[h.ID] = true
			current = h
		}
		catch[s.ID] = current
	}

	// No orphans: every step is either on the main path or a handler.
	for i := range d.Steps {
		id := d.Steps[i].ID
		if !onMain[id] && !handlerIDs[id] {
			return nil, fmt.Errorf("flow: step %q is unreachable", id)
		}
	}

	return &Plan{Main: main, Catch: catch}, nil
}

// validate checks one step's own fields (edges are checked while building
// the plan, which has the whole step set for reference).
func (s *Step) validate() error {
	if s.OnSuccess != "" && s.OnComplete != "" {
		return fmt.Errorf("step has both onSuccess and onComplete; use one")
	}
	switch {
	case isReservedType(s.Type):
		return fmt.Errorf("step type %q is not yet supported", s.Type)
	case isConnectorType(s.Type):
		if s.Connector == "" || s.Action == "" {
			return fmt.Errorf("%s step needs connector and action", s.Type)
		}
		return nil
	case isTransformType(s.Type):
		return s.Op.validate()
	default:
		return fmt.Errorf("unknown step type %q", s.Type)
	}
}
