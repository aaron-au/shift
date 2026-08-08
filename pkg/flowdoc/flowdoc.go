// Package flowdoc defines the flow document — the declarative JSON that
// describes an integration. It is shared by the hub (which validates and
// stores documents; it never touches payload data) and the runner (which
// compiles them onto engine pipelines, see runner/internal/flow).
// Documents are deliberately plain data (developer- and AI-friendly:
// no DSL, no code), validated eagerly at deploy/submit time.
package flowdoc

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/aaron-au/shift/engine/record"
)

// Document is one executable flow definition. Two authoring forms lower
// to the same execution plan (see graph.go, Document.Plan):
//
//   - linear form (v1 sugar): Source + Ops + Sink — the ergonomic,
//     AI-friendly shape for a straight-through pipeline. Kept unchanged.
//   - graph form (v2): Steps + Start — nodes with typed outcome edges
//     (onSuccess / onComplete happy path, onFailure error handler). The
//     two forms are mutually exclusive within one document.
type Document struct {
	Name string `json:"name"`

	// Delivery is the flow's at-least-once vs at-most-once intent for
	// hub-queued triggers (schedule, API execute). "" / "at_least_once"
	// (default): a runner that dies mid-task has the task re-dispatched
	// (idempotency keys dedupe side effects). "at_most_once": the flow is
	// non-idempotent, so a lost runner fails the task terminally rather than
	// risk a double side effect — it caps max_attempts at 1 and cannot be
	// overridden by a trigger requesting retries. The engine ignores this
	// field; it is a control-plane dispatch policy only. (See ADR-0002.)
	Delivery string `json:"delivery,omitempty"`

	// Linear form.
	Source Endpoint `json:"source,omitzero"`
	Ops    []Op     `json:"ops,omitempty"`
	Sink   Endpoint `json:"sink,omitzero"`

	// Graph form.
	Steps []Step `json:"steps,omitempty"`
	Start string `json:"start,omitempty"` // entry step id ("" = the sole source step)

	// Layout is presentational only: the studio builder's saved node
	// positions, keyed by step id (ADR-0019). It is ignored by Validate,
	// Plan/buildPlan, and the engine — a stale or missing key just means a
	// node falls back to auto-layout. Round-trips in the stored JSONB
	// document; never affects execution.
	Layout map[string]Point `json:"layout,omitempty"`
}

// Point is a node's saved canvas position (presentational, ADR-0019).
type Point struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Step is one node in the flow graph (v2). Every node — connector or
// transform — is a step. A Step embeds Op, so the transform types reuse
// the exact same fields and validation as the linear form and compile
// through the same applyOp path on the runner.
//
// Type namespace:
//   - connector: source | sink   (Connector/Action/Config apply)
//   - transform: filter | project | coerce | flatten | aggregate (Op fields)
//   - reserved:  starlark | python | subflow — parsed but rejected until
//     built (custom code: ADR-0017; sub-flows: later).
type Step struct {
	ID string `json:"id"`
	Op        // promotes Type + the transform option fields

	// Connector steps (source|sink).
	Connector string `json:"connector,omitempty"`
	Action    string `json:"action,omitempty"`
	// Version pins the connector build this step runs (ADR-0047 §1).
	//
	// Empty in a DRAFT means "newest", which is what a draft should mean —
	// it has no promise to keep. PUBLISHING fills it in, and from then on the
	// flow runs the builds it was published against. Without that, publishing
	// a new connector version silently changes behaviour for every flow using
	// it, on the next task, against live data.
	Version string `json:"version,omitempty"`
	// Connection names a reusable Connection supplying this step's
	// connection-level config — host, credentials, TLS (ADR-0034). Optional
	// and additive: a step with inline Config and no Connection compiles
	// exactly as it did before connections existed, so every stored flow
	// stays valid.
	//
	// The runner merges the named connection ahead of Config and resolves
	// its {"$secret":...} refs, so the hub still stores only references.
	Connection string          `json:"connection,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`

	// Mock and TestInput are the test-only diversions (ADR-0048 §5): options on
	// the REAL step, never substitutes for it. Inert in a deployed execution,
	// so neither has to be removed before publishing — the flow that runs in
	// production is the same document that was tested.
	Mock      *Mock      `json:"mock,omitempty"`
	TestInput *TestInput `json:"testInput,omitempty"`

	// Input verifies the inbound request before the flow accepts it
	// (ADR-0042). Valid only on a @webhook source step; see input.go.
	Input *Input `json:"input,omitempty"`

	// Ack decides what an ASYNCHRONOUS caller gets back (ADR-0042 §3d).
	// Valid only on a @webhook source step; see input.go.
	Ack string `json:"ack,omitempty"`

	// Outcome edges (step ids). A non-terminal step has exactly one happy
	// edge (OnSuccess XOR OnComplete); the two are structurally identical
	// (both name the next step on the happy path) — the distinction is
	// authoring intent. OnFailure names an error-handler step.
	OnSuccess  string `json:"onSuccess,omitempty"`
	OnComplete string `json:"onComplete,omitempty"`
	OnFailure  string `json:"onFailure,omitempty"`

	// Structural fan-out / fan-in fields (flow model v3, ADR-0029). Each is
	// valid only on its matching node kind and is validated in dag.go; a
	// document that uses any of them lowers to the DAG plan (Plan.Multi),
	// not the linear/v2 Main chain.
	//
	//   tee    → Branches (≥2): every record to every branch.
	//   router → Routes (ordered, first-match) + optional Default.
	//   merge  → Inputs (≥2 upstream ids) + Mode (concat|join); join adds
	//            On (the linked element), JoinType, Build, As.
	//
	// Branches may also ride a plain transform/source step as sugar for an
	// implicit tee (send this stream to N places without a separate node).
	Branches []string `json:"branches,omitempty"`
	Routes   []Route  `json:"routes,omitempty"`
	Default  string   `json:"default,omitempty"`
	Inputs   []string `json:"inputs,omitempty"`
	Mode     string   `json:"mode,omitempty"`
	On       *JoinOn  `json:"on,omitempty"`
	JoinType string   `json:"joinType,omitempty"`
	Build    string   `json:"build,omitempty"`
	As       string   `json:"as,omitempty"`
}

// Route is one ordered branch of a router node: a filter predicate (reusing
// the linear filter grammar — Path/Cmp/Value) plus the step it sends the
// first-matching records to. Records are evaluated against routes in order
// and delivered to the first match only (a partition, not a broadcast).
type Route struct {
	Path  string          `json:"path"`
	Cmp   string          `json:"op"`
	Value json.RawMessage `json:"value,omitempty"`
	To    string          `json:"to"`
}

// JoinOn names the linked element of a merge/join: the left (probe) and
// right (build) record paths whose equality defines a match.
type JoinOn struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

// Merge modes (ADR-0029). concat is a streaming, keyless union; join is a
// keyed relational join on the linked element.
const (
	MergeConcat = "concat"
	MergeJoin   = "join"
)

// Join types (ADR-0029). inner drops unmatched probe rows; left keeps them
// with a null match under the As field.
const (
	JoinInner = "inner"
	JoinLeft  = "left"
)

// Endpoint views a connector step (source|sink) as an Endpoint, so the
// runner can bind it exactly like a linear-form endpoint.
func (s *Step) Endpoint() Endpoint {
	return Endpoint{
		Connector: s.Connector, Action: s.Action, Version: s.Version,
		Connection: s.Connection, Config: s.Config, Input: s.Input, Ack: s.Ack,
		Mock: s.Mock, TestInput: s.TestInput,
	}
}

// WebhookSource is the reserved built-in source that binds an inbound
// request body (a webhook / direct execution, ADR-0016) as the flow's
// source instead of a connector subprocess. Its action names the body
// format (e.g. "ndjson"). It is valid only on a source step; it is not a
// registry connector, so it is exempt from the capability policy and from
// signed-artifact resolution.
const WebhookSource = "@webhook"

// DiscardSink is the reserved built-in sink: a connector-free, side-effect-free
// terminal that reads the stream and drops it. It lets a flow whose purpose is
// a single source-side action (e.g. an SFTP mkdir/delete that emits only a
// status record) terminate validly without a real sink connector. Valid only on
// a sink step; needs no action; exempt from capability policy + signing.
const DiscardSink = "@discard"

// ResponseSink is the reserved built-in sink that returns the flow's output to
// the caller — the requestor of a synchronous direct execution (ADR-0016 data
// plane, runner-side: the payload never touches the hub). It is connector-free
// and side-effect-free (it serializes the terminal stream to the response body,
// bounded), distinct from @discard (which drops). Valid only on a sink step;
// needs no action; exempt from capability policy + signing. (ADR-0024 Phase 2.)
const ResponseSink = "@response"

// StopSink is the reserved built-in terminal that ends a flow early and
// deliberately, as a SUCCESS (ADR-0031 §3). Routing a record into it — the
// usual shape is one arm of a router, "if this condition holds, stop" — ends
// the whole execution: the task is `completed` with a stopped marker, not
// failed and not retried.
//
// It is distinct from @discard, which drops records and lets the stream run
// to its natural end. @stop terminates. Connector-free and side-effect-free;
// valid only on a sink step; needs no action; exempt from capability policy
// and signing.
const StopSink = "@stop"

// isBuiltinSink reports whether name is a built-in that may terminate a flow
// (the side-effect-free terminals). @webhook is a source, not a sink.
func isBuiltinSink(name string) bool {
	return name == DiscardSink || name == ResponseSink || name == StopSink
}

// IsBuiltinConnector reports whether name is a reserved built-in (the
// runner binds it directly) rather than a registry connector.
func IsBuiltinConnector(name string) bool {
	return len(name) > 0 && name[0] == '@'
}

// happyEdge returns the step's single happy-path successor id (OnSuccess
// or OnComplete) and whether one is set.
func (s *Step) happyEdge() (string, bool) {
	if s.OnSuccess != "" {
		return s.OnSuccess, true
	}
	if s.OnComplete != "" {
		return s.OnComplete, true
	}
	return "", false
}

func isConnectorType(t string) bool { return t == "source" || t == "sink" }

func isTransformType(t string) bool {
	switch t {
	case "filter", "project", "coerce", "flatten", "aggregate", "map":
		return true
	}
	// A probe occupies a transform position — it sits between steps and passes
	// everything through — so it compiles down the same path (ADR-0048 §5).
	return t == ProbeType
}

// isStructuralType reports whether t is a v3 fan-out / fan-in node (ADR-0029).
// These are not connectors and not record transforms; they route the stream.
func isStructuralType(t string) bool {
	switch t {
	case "tee", "router", "merge":
		return true
	}
	return false
}

// usesDAG reports whether the step engages the v3 multi-path model — either
// it is a structural node, or it carries branch/route/input fan-out fields on
// an ordinary step (the sugar forms). Any such step forces the whole document
// through the DAG plan.
func (s *Step) usesDAG() bool {
	return isStructuralType(s.Type) || len(s.Branches) > 0 || len(s.Routes) > 0 || len(s.Inputs) > 0
}

func isReservedType(t string) bool {
	switch t {
	case "starlark", "python", "subflow":
		return true
	}
	return false
}

// Endpoint names a connector action plus its opaque config document, and
// optionally the reusable Connection supplying its connection-level settings
// (ADR-0034).
type Endpoint struct {
	Connector string `json:"connector"`
	Action    string `json:"action"`
	// Version pins the connector build (ADR-0047 §1); see Step.Version.
	Version    string          `json:"version,omitempty"`
	Connection string          `json:"connection,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`

	// Mock and TestInput are the test-only diversions (ADR-0048 §5). They are
	// options on the REAL step, never substitutes for it: the connector and its
	// config stay in the document, so a deployed flow is complete whether or
	// not either is set, and neither ever needs removing before shipping.
	Mock      *Mock      `json:"mock,omitempty"`
	TestInput *TestInput `json:"testInput,omitempty"`

	// Input verifies the inbound request before the flow accepts it
	// (ADR-0042). Valid only on a @webhook source; see input.go.
	Input *Input `json:"input,omitempty"`

	// Ack decides what an ASYNCHRONOUS caller gets back: a task id and a
	// status URL (AckStatus, the default) or nothing to poll (AckNone,
	// fire-and-forget). Valid only on a @webhook source; see input.go.
	Ack string `json:"ack,omitempty"`
}

// Op is one transform step. Type selects which of the option blocks apply.
type Op struct {
	Type string `json:"type"` // filter | project | coerce | flatten | aggregate

	// filter
	Path  string          `json:"path,omitempty"`
	Cmp   string          `json:"op,omitempty"` // eq | ne | gt | gte | lt | lte | exists
	Value json.RawMessage `json:"value,omitempty"`

	// project
	Fields []ProjectField `json:"fields,omitempty"`

	// coerce
	Rules []CoerceRule `json:"rules,omitempty"`

	// flatten
	Sep string `json:"sep,omitempty"`

	// aggregate
	Key  string `json:"key,omitempty"`
	Aggs []Agg  `json:"aggs,omitempty"`

	// map (declarative mapper, ADR-0027)
	Maps []MapField `json:"maps,omitempty"`
}

// MapField is one output assignment of a map (mapper) op: a value written at a
// dotted output path (nested maps for a multi-segment path), sourced from a
// path, a constant, or a concat expression, with an optional default (when the
// source is missing/null) and an optional inline coercion. Exactly one of
// From / Const / Concat applies. Concat elements are literals except those
// beginning with "$", which are source paths.
type MapField struct {
	Out     string          `json:"out"`
	From    string          `json:"from,omitempty"`
	Const   json.RawMessage `json:"const,omitempty"`
	Concat  []string        `json:"concat,omitempty"`
	Default json.RawMessage `json:"default,omitempty"`
	To      string          `json:"to,omitempty"`
}

// MapOutSegments splits a mapper output path ("customer.name", "$.a.b") into
// its segments, stripping any leading "$"/"$." root marker.
func MapOutSegments(out string) []string {
	out = strings.TrimPrefix(out, "$.")
	out = strings.TrimPrefix(out, "$")
	if out == "" {
		return nil
	}
	return strings.Split(out, ".")
}

// ProjectField mirrors stream.ProjectField in document form.
type ProjectField struct {
	Out  string `json:"out,omitempty"`
	Path string `json:"path"`
}

// CoerceRule converts a top-level field to a kind (int|float|bool|string).
type CoerceRule struct {
	Field string `json:"field"`
	To    string `json:"to"`
}

// Agg is one aggregate output column.
type Agg struct {
	Op   string `json:"op"` // count | sum | min | max
	Path string `json:"path,omitempty"`
	Out  string `json:"out"`
}

// CoerceKinds are the legal CoerceRule.To names.
var CoerceKinds = map[string]bool{"int": true, "float": true, "bool": true, "string": true}

// Parse decodes and validates a flow document.
func Parse(data []byte) (*Document, error) {
	var d Document
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("flow: invalid JSON: %w", err)
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// Validate checks the document without touching connectors. It routes to
// the graph validator when the document is in v2 (Steps) form, and keeps
// the linear-form checks otherwise.
// Delivery modes and the default attempt ceiling (ADR-0002).
const (
	DeliveryAtLeastOnce = "at_least_once"
	DeliveryAtMostOnce  = "at_most_once"
	DefaultMaxAttempts  = 3
)

// MaxAttempts is the attempt ceiling implied by the flow's delivery intent.
// at_most_once caps at 1 (a lost runner fails the task, never retries);
// everything else uses the default. This is a hard cap the hub applies at
// enqueue — a trigger cannot raise a non-idempotent flow's attempts.
func (d *Document) MaxAttempts() int {
	if d.Delivery == DeliveryAtMostOnce {
		return 1
	}
	return DefaultMaxAttempts
}

// DeliveryFromDoc extracts the delivery mode from a stored (already-validated)
// flow document without a full parse, for the hub's enqueue-time attempt
// resolution. An unreadable/absent field yields "" (at-least-once default).
func DeliveryFromDoc(raw []byte) string {
	var d struct {
		Delivery string `json:"delivery"`
	}
	_ = json.Unmarshal(raw, &d)
	return d.Delivery
}

func (d *Document) Validate() error {
	if d.Name == "" {
		return errors.New("flow: name is required")
	}
	if !NamePattern.MatchString(d.Name) {
		return fmt.Errorf("flow: name %q must match %s (letters, digits, space, . _ -)", d.Name, NamePattern)
	}
	switch d.Delivery {
	case "", DeliveryAtLeastOnce, DeliveryAtMostOnce:
	default:
		return fmt.Errorf("flow: delivery %q must be %q or %q", d.Delivery, DeliveryAtLeastOnce, DeliveryAtMostOnce)
	}
	if err := d.validateInputs(); err != nil {
		return err
	}
	if err := d.validateAck(); err != nil {
		return err
	}
	if len(d.Steps) > 0 {
		if d.Source.Connector != "" || len(d.Ops) > 0 || d.Sink.Connector != "" {
			return errors.New("flow: use either the linear form (source/ops/sink) or the graph form (steps), not both")
		}
		_, err := d.buildPlan()
		return err
	}
	for label, ep := range map[string]Endpoint{"source": d.Source, "sink": d.Sink} {
		// Built-ins (@webhook source, @discard sink) need no action.
		if ep.Connector == "" || (ep.Action == "" && !IsBuiltinConnector(ep.Connector)) {
			return fmt.Errorf("flow: %s needs connector and action", label)
		}
		if ep.Version != "" {
			// A built-in is compiled into the runner: no artifact to pin
			// (ADR-0047 §1).
			if IsBuiltinConnector(ep.Connector) {
				return fmt.Errorf("flow: %s: built-in connector %q takes no version", label, ep.Connector)
			}
			if !ConnectorVersionPattern.MatchString(ep.Version) {
				return fmt.Errorf("flow: %s: version %q must match %s", label, ep.Version, ConnectorVersionPattern)
			}
		}
		if ep.Connection == "" {
			continue
		}
		// Same rules as the graph form (ADR-0034): built-ins talk to no
		// external system, so a connection on one is meaningless.
		if IsBuiltinConnector(ep.Connector) {
			return fmt.Errorf("flow: %s: built-in connector %q takes no connection", label, ep.Connector)
		}
		if !ConnectionNamePattern.MatchString(ep.Connection) {
			return fmt.Errorf("flow: %s: connection %q must match %s (letters, digits, . _ -)",
				label, ep.Connection, ConnectionNamePattern)
		}
	}
	if IsBuiltinConnector(d.Sink.Connector) && !isBuiltinSink(d.Sink.Connector) {
		return fmt.Errorf("flow: built-in connector %q cannot be a sink", d.Sink.Connector)
	}
	if IsBuiltinConnector(d.Source.Connector) && d.Source.Connector != WebhookSource {
		return fmt.Errorf("flow: unknown built-in source %q", d.Source.Connector)
	}
	for label, ep := range map[string]Endpoint{"source": d.Source, "sink": d.Sink} {
		if err := validateTestOnly(label, ep, label); err != nil {
			return fmt.Errorf("flow: %w", err)
		}
	}
	for i, op := range d.Ops {
		if err := op.validate(); err != nil {
			return fmt.Errorf("flow: op %d: %w", i, err)
		}
	}
	return nil
}

// validatePredicate checks a filter-grammar predicate (path/op/value). It is
// shared by the filter op and by router routes (ADR-0029) so both accept
// exactly the same expression grammar.
func validatePredicate(path, cmp string, value json.RawMessage) error {
	if _, err := record.ParsePath(path); err != nil {
		return err
	}
	switch cmp {
	case "eq", "ne", "gt", "gte", "lt", "lte":
		if len(value) == 0 {
			return fmt.Errorf("filter %s needs a value", cmp)
		}
		if _, err := ScalarValue(value); err != nil {
			return err
		}
	case "contains", "startsWith", "endsWith":
		if len(value) == 0 {
			return fmt.Errorf("filter %s needs a value", cmp)
		}
		v, err := ScalarValue(value)
		if err != nil {
			return err
		}
		if v.Kind() != record.KindString {
			return fmt.Errorf("filter %s needs a string value", cmp)
		}
	case "exists":
	default:
		return fmt.Errorf("unknown filter op %q", cmp)
	}
	return nil
}

func (o *Op) validate() error {
	switch o.Type {
	case "filter":
		return validatePredicate(o.Path, o.Cmp, o.Value)
	case "project":
		if len(o.Fields) == 0 {
			return errors.New("project needs fields")
		}
		for _, f := range o.Fields {
			p, err := record.ParsePath(f.Path)
			if err != nil {
				return err
			}
			if f.Out == "" && p.LeafName() == "" {
				return fmt.Errorf("project field %s needs an out name", f.Path)
			}
		}
	case "coerce":
		if len(o.Rules) == 0 {
			return errors.New("coerce needs rules")
		}
		for _, r := range o.Rules {
			if !CoerceKinds[r.To] {
				return fmt.Errorf("unknown coerce kind %q", r.To)
			}
			if r.Field == "" {
				return errors.New("coerce rule needs field")
			}
		}
	case "flatten":
		if o.Sep == "" {
			return errors.New("flatten needs sep")
		}
	case "aggregate":
		if _, err := record.ParsePath(o.Key); err != nil {
			return err
		}
		if len(o.Aggs) == 0 {
			return errors.New("aggregate needs aggs")
		}
		for _, a := range o.Aggs {
			switch a.Op {
			case "count":
				// count ignores Path, but the compiler still parses it when
				// set (flow.go) via the panicking MustParsePath — so a malformed
				// path here must be rejected at validation, not reach the runner.
				if a.Path != "" {
					if _, err := record.ParsePath(a.Path); err != nil {
						return err
					}
				}
			case "sum", "min", "max":
				if _, err := record.ParsePath(a.Path); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown agg op %q", a.Op)
			}
			if a.Out == "" {
				return errors.New("agg needs out name")
			}
		}
	case "map":
		return validateMap(o.Maps)
	case ProbeType:
		// A probe has no operands: it taps whatever passes through it. The
		// optional label is presentational (ADR-0048 §5).
	default:
		return fmt.Errorf("unknown op type %q", o.Type)
	}
	return nil
}

// validateMap checks a mapper op's field list: each field has a valid output
// path, exactly one source, parseable paths/values, a known coerce kind, and no
// output path that collides with another (one a prefix of the other).
func validateMap(fields []MapField) error {
	if len(fields) == 0 {
		return errors.New("map needs fields")
	}
	var seen [][]string
	for i := range fields {
		m := &fields[i]
		segs := MapOutSegments(m.Out)
		if len(segs) == 0 {
			return fmt.Errorf("map field %d needs an out path", i)
		}
		for _, s := range segs {
			if s == "" {
				return fmt.Errorf("map field %d: empty path segment in %q", i, m.Out)
			}
		}
		srcs := 0
		if m.From != "" {
			srcs++
		}
		if len(m.Const) > 0 {
			srcs++
		}
		if len(m.Concat) > 0 {
			srcs++
		}
		if srcs != 1 {
			return fmt.Errorf("map field %q needs exactly one of from/const/concat", m.Out)
		}
		if m.From != "" {
			if _, err := record.ParsePath(m.From); err != nil {
				return err
			}
		}
		for _, part := range m.Concat {
			if strings.HasPrefix(part, "$") {
				if _, err := record.ParsePath(part); err != nil {
					return err
				}
			}
		}
		if len(m.Const) > 0 {
			if _, err := ScalarValue(m.Const); err != nil {
				return err
			}
		}
		if len(m.Default) > 0 {
			if _, err := ScalarValue(m.Default); err != nil {
				return err
			}
		}
		if m.To != "" && !CoerceKinds[m.To] {
			return fmt.Errorf("map field %q: unknown coerce kind %q", m.Out, m.To)
		}
		for _, prev := range seen {
			if prefixConflict(prev, segs) {
				return fmt.Errorf("map: output path %q collides with another field", m.Out)
			}
		}
		seen = append(seen, segs)
	}
	return nil
}

// prefixConflict reports whether two output paths collide: one is a prefix of
// the other (equal paths included), which would put a leaf where a branch must
// go (or vice versa).
func prefixConflict(a, b []string) bool {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ScalarValue converts a JSON scalar into a record scalar for comparison.
func ScalarValue(raw json.RawMessage) (record.Value, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return record.Value{}, fmt.Errorf("bad filter value: %w", err)
	}
	switch x := v.(type) {
	case nil:
		return record.Null(), nil
	case bool:
		return record.Bool(x), nil
	case float64:
		if x == float64(int64(x)) {
			return record.Int(int64(x)), nil
		}
		return record.Float(x), nil
	case string:
		return record.UnsafeString([]byte(x)), nil // retained: backed by the caller's compiled closure
	default:
		return record.Value{}, fmt.Errorf("filter value must be a scalar, got %T", v)
	}
}

// IdempotencyKeyField is the config key the runner injects the task's
// idempotency key under, and the one a sink must honour (ADR-0002).
const IdempotencyKeyField = "idempotency_key"

// WithSinkConfig returns a copy of the document whose sink config has the
// given extra fields merged in (used by the runner to inject the task
// idempotency key before execution).
//
// Under fan-out the injected `idempotency_key` is DERIVED per sink —
// `<task_key>:<stepID>` (ADR-0029 §5). See derivedKeys for why.
func (d *Document) WithSinkConfig(extra map[string]any) (*Document, error) {
	out := *d
	// Graph form (canonical, ADR-0013): the executing sink config lives in a
	// Step, not d.Sink. Merge into EVERY sink step (a flow may have a happy-path
	// sink and a dead-letter sink; both carry side effects). Writing only to
	// d.Sink here — as the linear branch does — would silently drop the
	// idempotency key for graph flows, breaking at-least-once (the primary flow
	// model produces graph docs).
	if len(d.Steps) > 0 {
		steps := make([]Step, len(d.Steps))
		copy(steps, d.Steps)
		derive := d.derivedKeys()
		for i := range steps {
			if steps[i].Type != "sink" {
				continue
			}
			perStep := extra
			if derive {
				if key, ok := extra[IdempotencyKeyField].(string); ok && key != "" {
					perStep = maps.Clone(extra)
					perStep[IdempotencyKeyField] = key + ":" + steps[i].ID
				}
			}
			merged, err := mergeRawConfig(steps[i].Config, perStep)
			if err != nil {
				return nil, fmt.Errorf("flow: step %q sink config: %w", steps[i].ID, err)
			}
			steps[i].Config = merged
		}
		out.Steps = steps
		return &out, nil
	}
	// Linear (sugar) form.
	merged, err := mergeRawConfig(d.Sink.Config, extra)
	if err != nil {
		return nil, fmt.Errorf("flow: sink config: %w", err)
	}
	out.Sink.Config = merged
	return &out, nil
}

// derivedKeys reports whether this document's sinks need per-step idempotency
// keys (ADR-0029 §5).
//
// The hub injects ONE key per task, stable across re-dispatch. That is exactly
// right for a single sink, and wrong the moment two side-effecting sinks write
// the SAME target: the second write dedupes against the first and one write is
// silently lost. Nothing fails, nothing is logged — the record is simply not
// there. ADR-0029 §5 names this case and answers it: seed each sink's key with
// its own step id, which is distinct by construction and stable in the plan,
// so a re-dispatched task replays each sink under its own key.
//
// Two sinks writing DIFFERENT targets dedupe against different stores, so a
// shared key is harmless there — which is the common case, and why this went
// unnoticed. Deriving anyway costs nothing and removes the need to reason
// about whether two configs point at the same system, which the flow document
// cannot know.
//
// Built-in terminals do not count toward the threshold. @discard, @stop and
// @response have no side effect and no store to dedupe against, so a flow with
// one real sink and a @discard branch is still a one-key flow. Letting a
// side-effect-free terminal change a real sink's key would be a behaviour
// change for no safety gain — and key changes are not free (see below).
//
// A SINGLE-sink flow deliberately keeps the bare task key. Two reasons: it is
// what ADR-0029 §5 specifies ("no branch, no derivation"), and changing the
// key a sink sees is itself a hazard — a task in flight across an upgrade
// would retry under a different key than its first attempt and double-write
// against a receiver that had already deduped it. Confining the change to
// multi-sink documents keeps that window to the flows that were already
// broken, where a doubled write replaces a silently lost one.
func (d *Document) derivedKeys() bool {
	sinks := 0
	for i := range d.Steps {
		s := &d.Steps[i]
		if s.Type == "sink" && !IsBuiltinConnector(s.Connector) {
			sinks++
			if sinks > 1 {
				return true
			}
		}
	}
	return false
}

// mergeRawConfig unmarshals a JSON object config (may be empty), overlays
// extra, and re-marshals. extra wins on key collision.
func mergeRawConfig(cfg json.RawMessage, extra map[string]any) (json.RawMessage, error) {
	merged := map[string]any{}
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &merged); err != nil {
			return nil, err
		}
	}
	maps.Copy(merged, extra)
	return json.Marshal(merged)
}
