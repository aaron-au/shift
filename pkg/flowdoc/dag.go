package flowdoc

import (
	"errors"
	"fmt"
	"slices"

	"github.com/aaron-au/shift/engine/record"
)

// This file lowers the v3 flow model (ADR-0029) — fan-out (tee/router) and
// fan-in (merge) — to a validated DAG Plan. The hub validates through it
// without touching payload; the multi-path engine (a later change) compiles
// execution segments from Plan.Nodes/Data. Linear/v2 documents never reach
// here; they lower via buildPlan's happy-path walk.

// validateStructural checks a fan-out / fan-in node's own fields. Cross-step
// concerns (edge resolvability, acyclicity, roles, reachability) are checked
// in buildDAGPlan, which has the whole step set.
func (s *Step) validateStructural() error {
	switch s.Type {
	case "tee":
		if len(s.Branches) < 2 {
			return errors.New("tee needs at least 2 branches")
		}
		if s.OnSuccess != "" || s.OnComplete != "" {
			return errors.New("tee fans out via branches; it must not also have onSuccess/onComplete")
		}
		if len(s.Routes) > 0 || len(s.Inputs) > 0 || s.Mode != "" || s.On != nil {
			return errors.New("tee takes only branches")
		}
	case "router":
		targets := len(s.Routes)
		if s.Default != "" {
			targets++
		}
		if targets < 2 {
			return errors.New("router needs at least 2 targets (routes plus an optional default)")
		}
		if s.OnSuccess != "" || s.OnComplete != "" {
			return errors.New("router fans out via routes/default; it must not also have onSuccess/onComplete")
		}
		if len(s.Branches) > 0 || len(s.Inputs) > 0 || s.Mode != "" || s.On != nil {
			return errors.New("router takes only routes and default")
		}
		for i, r := range s.Routes {
			if r.To == "" {
				return fmt.Errorf("route %d needs a target (to)", i)
			}
			if err := validatePredicate(r.Path, r.Cmp, r.Value); err != nil {
				return fmt.Errorf("route %d: %w", i, err)
			}
		}
	case "merge":
		if len(s.Inputs) < 2 {
			return errors.New("merge needs at least 2 inputs")
		}
		if len(s.Branches) > 0 || len(s.Routes) > 0 {
			return errors.New("merge fans in via inputs; it must not have branches/routes")
		}
		switch s.Mode {
		case MergeConcat:
			if s.On != nil || s.JoinType != "" || s.Build != "" || s.As != "" {
				return errors.New("merge concat takes no join key (drop on/joinType/build/as)")
			}
		case MergeJoin:
			if len(s.Inputs) != 2 {
				return errors.New("merge join requires exactly 2 inputs (chain merges for more)")
			}
			if s.On == nil || s.On.Left == "" || s.On.Right == "" {
				return errors.New("merge join needs on.left and on.right (the linked element)")
			}
			if _, err := record.ParsePath(s.On.Left); err != nil {
				return fmt.Errorf("merge join on.left: %w", err)
			}
			if _, err := record.ParsePath(s.On.Right); err != nil {
				return fmt.Errorf("merge join on.right: %w", err)
			}
			switch s.JoinType {
			case JoinInner, JoinLeft:
			default:
				return fmt.Errorf("merge join type %q must be %q or %q", s.JoinType, JoinInner, JoinLeft)
			}
			if s.Build == "" {
				return errors.New("merge join needs build (which input is the build/right side)")
			}
			if s.Build != s.Inputs[0] && s.Build != s.Inputs[1] {
				return fmt.Errorf("merge join build %q must name one of its inputs", s.Build)
			}
			if s.As == "" {
				return errors.New("merge join needs an 'as' field to nest the matched record under")
			}
		default:
			return fmt.Errorf("merge mode %q must be %q or %q", s.Mode, MergeConcat, MergeJoin)
		}
	default:
		return fmt.Errorf("unknown structural step type %q", s.Type)
	}
	return nil
}

// dataSuccessors returns the happy-path data targets a node forwards records
// to: a tee's branches, a router's routes+default, or the single happy edge
// of an ordinary step (with branches sugar for an implicit tee). A sink and a
// dangling non-sink both return none — the caller's role check distinguishes
// them.
func (s *Step) dataSuccessors() ([]string, error) {
	_, hasHappy := s.happyEdge()
	switch s.Type {
	case "tee":
		return slices.Clone(s.Branches), nil
	case "router":
		outs := make([]string, 0, len(s.Routes)+1)
		for _, r := range s.Routes {
			outs = append(outs, r.To)
		}
		if s.Default != "" {
			outs = append(outs, s.Default)
		}
		return outs, nil
	default:
		// Ordinary source / transform / merge / connector-sink step. Branches
		// on such a step is sugar for an implicit tee.
		if len(s.Branches) > 0 {
			if hasHappy {
				return nil, errors.New("step has both branches and onSuccess/onComplete; use one")
			}
			if len(s.Branches) < 2 {
				return nil, errors.New("branches is fan-out (≥2 targets); use onSuccess for a single successor")
			}
			return slices.Clone(s.Branches), nil
		}
		if s.Type == "sink" {
			if hasHappy {
				return nil, errors.New("sink must not have a happy-path edge")
			}
			return nil, nil
		}
		if h, ok := s.happyEdge(); ok {
			return []string{h}, nil
		}
		return nil, nil // terminal non-sink → role check rejects it
	}
}

// buildDAGPlan validates and lowers a v3 (fan-out / fan-in) document to a DAG
// Plan. byID indexes every step; each step has already passed its own-field
// validation (validate → validateStructural) in buildPlan.
func (d *Document) buildDAGPlan(byID map[string]*Step) (*Plan, error) {
	// onFailure handlers first — they sit off the data path (ADR-0013 rules,
	// per-node in the DAG: no positional inheritance).
	handlerIDs := map[string]bool{}
	for i := range d.Steps {
		s := &d.Steps[i]
		if s.OnFailure == "" {
			continue
		}
		h, ok := byID[s.OnFailure]
		if !ok {
			return nil, fmt.Errorf("flow: step %q: onFailure to unknown step %q", s.ID, s.OnFailure)
		}
		if h.Type != "sink" {
			return nil, fmt.Errorf("flow: onFailure handler %q must be a sink step", h.ID)
		}
		if _, hasHappy := h.happyEdge(); hasHappy || h.OnFailure != "" || len(h.Branches) > 0 {
			return nil, fmt.Errorf("flow: handler step %q must not have outgoing edges", h.ID)
		}
		handlerIDs[h.ID] = true
	}

	// Data nodes = every step that is not an error handler.
	nodes := make(map[string]*Step, len(d.Steps))
	for i := range d.Steps {
		s := &d.Steps[i]
		if !handlerIDs[s.ID] {
			nodes[s.ID] = s
		}
	}

	// Forward data adjacency + in-degree, built in document order (so error
	// messages are deterministic).
	data := make(map[string][]string, len(nodes))
	preds := make(map[string]int, len(nodes))
	for i := range d.Steps {
		s := &d.Steps[i]
		if handlerIDs[s.ID] {
			continue
		}
		succ, err := s.dataSuccessors()
		if err != nil {
			return nil, fmt.Errorf("flow: step %q: %w", s.ID, err)
		}
		for _, t := range succ {
			if _, ok := nodes[t]; !ok {
				if handlerIDs[t] {
					return nil, fmt.Errorf("flow: step %q: data edge to error-handler %q (handlers are onFailure targets, not data targets)", s.ID, t)
				}
				return nil, fmt.Errorf("flow: step %q: edge to unknown step %q", s.ID, t)
			}
			if nodes[t].Type == "source" {
				return nil, fmt.Errorf("flow: step %q: edge into source %q (sources have no inputs)", s.ID, t)
			}
			preds[t]++
		}
		data[s.ID] = succ
	}

	// Sources: source-typed nodes with no incoming edge. Need ≥1.
	var sources []string
	for i := range d.Steps {
		s := &d.Steps[i]
		if handlerIDs[s.ID] || s.Type != "source" {
			continue
		}
		if preds[s.ID] > 0 { // unreachable given the edge-into-source check, kept as a guard
			return nil, fmt.Errorf("flow: source %q must not have incoming edges", s.ID)
		}
		sources = append(sources, s.ID)
	}
	if len(sources) == 0 {
		return nil, errors.New("flow: a graph needs at least one source step")
	}

	// Merge fan-in wiring: declared inputs must actually flow to the merge,
	// and every producer that flows to it must be a declared input (so the
	// merge's arity and per-input roles are exact).
	for i := range d.Steps {
		s := &d.Steps[i]
		if s.Type != "merge" || handlerIDs[s.ID] {
			continue
		}
		declared := make(map[string]bool, len(s.Inputs))
		for _, in := range s.Inputs {
			if _, ok := nodes[in]; !ok {
				return nil, fmt.Errorf("flow: merge %q: unknown input %q", s.ID, in)
			}
			if declared[in] {
				return nil, fmt.Errorf("flow: merge %q: duplicate input %q", s.ID, in)
			}
			declared[in] = true
			if !slices.Contains(data[in], s.ID) {
				return nil, fmt.Errorf("flow: merge %q: input %q does not flow to it (set %q's onSuccess to %q)", s.ID, in, in, s.ID)
			}
		}
		for j := range d.Steps {
			p := &d.Steps[j]
			if handlerIDs[p.ID] {
				continue
			}
			if slices.Contains(data[p.ID], s.ID) && !declared[p.ID] {
				return nil, fmt.Errorf("flow: merge %q: step %q flows into it but is not a declared input", s.ID, p.ID)
			}
		}
	}

	// Roles + terminals.
	var sinks []string
	for i := range d.Steps {
		s := &d.Steps[i]
		if handlerIDs[s.ID] {
			continue
		}
		outs := data[s.ID]
		switch s.Type {
		case "tee", "router":
			if len(outs) < 2 {
				return nil, fmt.Errorf("flow: %s %q needs at least 2 outgoing branches", s.Type, s.ID)
			}
		case "sink":
			sinks = append(sinks, s.ID)
		default: // source, transform, merge
			if len(outs) == 0 {
				return nil, fmt.Errorf("flow: step %q needs an onSuccess/onComplete edge (only a sink terminates the flow)", s.ID)
			}
		}
	}
	if len(sinks) == 0 {
		return nil, errors.New("flow: a graph needs at least one sink")
	}

	if err := ensureAcyclic(d, handlerIDs, data); err != nil {
		return nil, err
	}

	// Reachability from the sources over data edges.
	reached := make(map[string]bool, len(nodes))
	var walk func(id string)
	walk = func(id string) {
		if reached[id] {
			return
		}
		reached[id] = true
		for _, t := range data[id] {
			walk(t)
		}
	}
	for _, s := range sources {
		walk(s)
	}
	for i := range d.Steps {
		id := d.Steps[i].ID
		if handlerIDs[id] {
			continue
		}
		if !reached[id] {
			return nil, fmt.Errorf("flow: step %q is unreachable", id)
		}
	}

	// Per-node error handlers (own onFailure only).
	catch := make(map[string]*Step, len(d.Steps))
	for i := range d.Steps {
		s := &d.Steps[i]
		if handlerIDs[s.ID] || s.OnFailure == "" {
			continue
		}
		catch[s.ID] = byID[s.OnFailure]
	}

	return &Plan{
		Multi:   true,
		Nodes:   nodes,
		Data:    data,
		Sources: sources,
		Sinks:   sinks,
		Catch:   catch,
	}, nil
}

// ensureAcyclic rejects a cycle in the data graph (onFailure edges are
// excluded — handlers are terminal). Standard three-colour DFS; walks in
// document order for a deterministic first-cycle report.
func ensureAcyclic(d *Document, handlerIDs map[string]bool, data map[string][]string) error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(data))
	var visit func(id string) error
	visit = func(id string) error {
		color[id] = gray
		for _, t := range data[id] {
			switch color[t] {
			case gray:
				return fmt.Errorf("flow: cycle in data path through step %q", t)
			case white:
				if err := visit(t); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}
	for i := range d.Steps {
		id := d.Steps[i].ID
		if handlerIDs[id] {
			continue
		}
		if color[id] == white {
			if err := visit(id); err != nil {
				return err
			}
		}
	}
	return nil
}
