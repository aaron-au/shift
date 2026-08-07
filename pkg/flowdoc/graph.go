package flowdoc

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
)

// stepIDPattern constrains step ids to an identifier charset. Step ids are
// referenced in edge targets and rendered into the studio builder's DOM; a
// tight charset keeps them safe as identifiers (no quotes/angle-brackets/pipe
// that a UI sink or an edge-delimiter split could misparse) — defense in depth
// alongside the builder's output escaping.
var stepIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// NamePattern constrains flow names for the same reason as stepIDPattern, with
// one addition: a flow name is also a URL PATH SEGMENT in the hub's control API
// (/api/v1/flows/{name}/...). Allowing quotes or slashes would let a name break
// out of a UI sink or, unencoded by a client, address a different endpoint
// entirely. Spaces are permitted (names are human-facing) but the charset stops
// at anything with meaning in markup or a path.
var NamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$`)

// ConnectionNamePattern constrains a Connection's name (ADR-0034). Connection
// names are addressed in the hub's control API and rendered on studio nodes, so
// they take the same tight charset as a secret name — no spaces, since a
// connection name is an identifier the author types rather than prose.
var ConnectionNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ConnectorVersionPattern constrains a pinned connector version (ADR-0047 §1).
//
// The registry treats a version as an opaque label, not semver, so this only
// bounds the character set — but it bounds it tightly, because the string
// reaches a path component in the runner's artifact cache and a query
// parameter on the way there.
var ConnectorVersionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$`)

// Connectors returns the sorted, unique connector names the document
// references — source and sink in the linear form; every connector step
// (including error handlers) in the graph form. The hub uses this to apply
// its per-deployment connector capability policy at deploy time.
func (d *Document) Connectors() []string {
	seen := map[string]bool{}
	add := func(n string) {
		if n != "" && !IsBuiltinConnector(n) { // built-ins aren't registry connectors
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

// Connections returns the sorted, unique Connection names the document
// references (ADR-0034) — both authoring forms. The hub uses this to validate
// at deploy time that every referenced connection exists, and to know which
// flows depend on a connection before it is edited or deleted.
func (d *Document) Connections() []string {
	seen := map[string]bool{}
	add := func(n string) {
		if n != "" {
			seen[n] = true
		}
	}
	if len(d.Steps) > 0 {
		for i := range d.Steps {
			if isConnectorType(d.Steps[i].Type) {
				add(d.Steps[i].Connection)
			}
		}
	} else {
		add(d.Source.Connection)
		add(d.Sink.Connection)
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
	// It is nil for a multi-path (v3) DAG document — Multi reports which.
	Main []*Step
	// Catch maps each step id to the error handler that fires when that step
	// errors. In the linear/v2 form this is its own onFailure or the nearest
	// preceding main step's onFailure (try/catch scoping); in the v3 DAG form
	// it is the node's own onFailure only (no positional inheritance across a
	// graph). A missing/nil entry means the failure is unhandled.
	Catch map[string]*Step

	// DAG form (flow model v3, ADR-0029). Populated only when the document
	// uses fan-out (tee/router) or fan-in (merge); Main is nil in that case.
	// The hub validates through these fields (it never touches payload) and
	// the multi-path engine compiles execution segments from them. Linear/v2
	// documents leave these zero-valued and are executed via Main.
	Multi   bool                // true ⇒ DAG form (Main is nil, Nodes/Data hold the graph)
	Nodes   map[string]*Step    // every data node by id (onFailure handlers live in Catch)
	Data    map[string][]string // happy data edges: node id → ordered successor ids
	Sources []string            // ≥1 data roots (source / config-driven-source steps)
	Sinks   []string            // ≥1 terminals (nodes with no data successor)
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
		if !stepIDPattern.MatchString(s.ID) {
			return nil, fmt.Errorf("flow: step %d: id %q must match %s (letters, digits, . _ -)", i, s.ID, stepIDPattern)
		}
		if _, dup := byID[s.ID]; dup {
			return nil, fmt.Errorf("flow: duplicate step id %q", s.ID)
		}
		byID[s.ID] = s
		if err := s.validate(); err != nil {
			return nil, fmt.Errorf("flow: step %q: %w", s.ID, err)
		}
	}

	// v3: any fan-out/fan-in node routes the whole document through the DAG
	// planner (a superset of the linear/v2 walk below).
	for i := range d.Steps {
		if d.Steps[i].usesDAG() {
			return d.buildDAGPlan(byID)
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

// GraphNode is one node in a document's rendering graph.
type GraphNode struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Connector string `json:"connector,omitempty"`
	Action    string `json:"action,omitempty"`
	Role      string `json:"role"` // "main" or "handler"
}

// GraphEdge is a typed outcome edge for rendering (kind: success, complete,
// or failure).
type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Kind string `json:"kind"`
}

// GraphView is a render-oriented projection of the document: nodes plus
// typed edges, with the happy path ordered in Main for left-to-right
// layout. It is data-free (studio reads it; the hub never touches payload).
type GraphView struct {
	Start string      `json:"start"`
	Main  []string    `json:"main"` // ordered main step ids
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

// GraphView validates the document and returns its rendering graph.
func (d *Document) GraphView() (*GraphView, error) {
	plan, err := d.Plan()
	if err != nil {
		return nil, err
	}
	onMain := map[string]bool{}
	g := &GraphView{}
	if plan.Multi {
		// DAG form (v3): no single ordered happy path. Data nodes render as
		// "main"; the layout roots are the sources (the builder positions
		// nodes from Document.Layout anyway).
		for id := range plan.Nodes {
			onMain[id] = true
		}
		if len(plan.Sources) > 0 {
			g.Start = plan.Sources[0]
			g.Main = slices.Clone(plan.Sources)
		}
	} else {
		g.Start = plan.Main[0].ID
		for _, s := range plan.Main {
			onMain[s.ID] = true
			g.Main = append(g.Main, s.ID)
		}
	}

	node := func(s *Step) GraphNode {
		role := "handler"
		if onMain[s.ID] {
			role = "main"
		}
		return GraphNode{ID: s.ID, Type: s.Type, Connector: s.Connector, Action: s.Action, Role: role}
	}

	if len(d.Steps) > 0 {
		for i := range d.Steps {
			s := &d.Steps[i]
			g.Nodes = append(g.Nodes, node(s))
			if s.OnSuccess != "" {
				g.Edges = append(g.Edges, GraphEdge{s.ID, s.OnSuccess, "success"})
			}
			if s.OnComplete != "" {
				g.Edges = append(g.Edges, GraphEdge{s.ID, s.OnComplete, "complete"})
			}
			if s.OnFailure != "" {
				g.Edges = append(g.Edges, GraphEdge{s.ID, s.OnFailure, "failure"})
			}
			// v3 fan-out edges (ADR-0029): a tee/sugar node's branches and a
			// router's routes+default carry the record stream, not an outcome.
			switch s.Type {
			case "router":
				for _, r := range s.Routes {
					g.Edges = append(g.Edges, GraphEdge{s.ID, r.To, "route"})
				}
				if s.Default != "" {
					g.Edges = append(g.Edges, GraphEdge{s.ID, s.Default, "route"})
				}
			default:
				for _, b := range s.Branches { // tee node or branches-sugar step
					g.Edges = append(g.Edges, GraphEdge{s.ID, b, "branch"})
				}
			}
		}
		return g, nil
	}

	// Linear form: synthesized nodes chained by complete edges.
	for i, s := range plan.Main {
		g.Nodes = append(g.Nodes, node(s))
		if i+1 < len(plan.Main) {
			g.Edges = append(g.Edges, GraphEdge{s.ID, plan.Main[i+1].ID, "complete"})
		}
	}
	return g, nil
}

// validate checks one step's own fields (edges are checked while building
// the plan, which has the whole step set for reference).
func (s *Step) validate() error {
	if s.OnSuccess != "" && s.OnComplete != "" {
		return errors.New("step has both onSuccess and onComplete; use one")
	}
	switch {
	case isReservedType(s.Type):
		return fmt.Errorf("step type %q is not yet supported", s.Type)
	case isStructuralType(s.Type):
		return s.validateStructural()
	case isConnectorType(s.Type):
		if IsBuiltinConnector(s.Connector) {
			// Built-ins need no action and are role-locked: @webhook is a source
			// (request body), @discard is a sink (drop the stream).
			switch s.Connector {
			case WebhookSource:
				if s.Type != "source" {
					return fmt.Errorf("built-in connector %q is only valid as a source", s.Connector)
				}
			case DiscardSink, ResponseSink, StopSink:
				if s.Type != "sink" {
					return fmt.Errorf("built-in connector %q is only valid as a sink", s.Connector)
				}
			default:
				return fmt.Errorf("unknown built-in connector %q", s.Connector)
			}
			// Built-ins talk to no external system, so a connection is
			// meaningless on them — reject rather than ignore, or an author
			// gets silence where they expected configuration.
			if s.Connection != "" {
				return fmt.Errorf("built-in connector %q takes no connection", s.Connector)
			}
			// Nor a version: a built-in is compiled into the runner, so there
			// is no artifact to pin and no registry to pin it from
			// (ADR-0047 §1).
			if s.Version != "" {
				return fmt.Errorf("built-in connector %q takes no version", s.Connector)
			}
			return validateTestOnly(s.ID, s.Endpoint(), s.Type)
		}
		if s.Connector == "" || s.Action == "" {
			return fmt.Errorf("%s step needs connector and action", s.Type)
		}
		if s.Connection != "" && !ConnectionNamePattern.MatchString(s.Connection) {
			return fmt.Errorf("connection %q must match %s (letters, digits, . _ -)", s.Connection, ConnectionNamePattern)
		}
		if s.Version != "" && !ConnectorVersionPattern.MatchString(s.Version) {
			return fmt.Errorf("version %q must match %s", s.Version, ConnectorVersionPattern)
		}
		return validateTestOnly(s.ID, s.Endpoint(), s.Type)
	case isTransformType(s.Type):
		return s.Op.validate()
	default:
		return fmt.Errorf("unknown step type %q", s.Type)
	}
}
