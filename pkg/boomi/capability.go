package boomi

import "sort"

// Support is how completely SHIFT can express a Boomi shape today. The four
// values are ADR-0032 §5's status vocabulary.
type Support string

const (
	// Mapped: a faithful SHIFT construct exists and the semantics carry over.
	Mapped Support = "mapped"
	// Divergent: a construct exists but the semantics differ in a way the
	// author must know about. Counted separately from Mapped precisely so a
	// coverage number can never hide a behavior change.
	Divergent Support = "mapped-with-divergence"
	// NeedsManual: no construct yet, but one is designed and the gap is a
	// build task. Blocker names the feature.
	NeedsManual Support = "needs-manual"
	// Unsupported: no construct and none planned in a form that would
	// translate; the author must re-author this step.
	Unsupported Support = "unsupported"
)

// Importable reports whether a shape with this status lands in a runnable flow
// without human intervention.
func (s Support) Importable() bool { return s == Mapped || s == Divergent }

// Capability is the verdict for one Boomi shape type.
type Capability struct {
	// Shape is the Boomi `shapetype` attribute.
	Shape string
	// Support is how well SHIFT expresses it today.
	Support Support
	// Construct is the SHIFT construct it lowers onto ("" when none).
	Construct string
	// Note explains the mapping, or why there isn't one.
	Note string
	// Blocker names the unbuilt SHIFT feature this shape waits on, so the
	// report can rank remaining work by how many real shapes each feature
	// unblocks. Empty for mapped shapes.
	Blocker string
}

// capabilities encodes ADR-0032 §3's shape-mapping table.
//
// It is data, not code, for two reasons: the report ranks blockers by summing
// real shape counts against it, and every entry is a claim that must be
// re-checked as SHIFT gains features — a table makes both a diff away.
var capabilities = map[string]Capability{
	// --- mapped: built and faithful ------------------------------------
	"start": {
		Shape: "start", Support: Mapped, Construct: "source",
		Note: "connector start becomes a connector source; a no-data or listener start becomes @webhook (ADR-0024/0031)",
	},
	"connectoraction": {
		Shape: "connectoraction", Support: Mapped, Construct: "connector node",
		Note: "source or sink follows the action's declared direction (ADR-0024); side-effecting sinks honor the idempotency key",
	},
	"decision": {
		Shape: "decision", Support: Mapped, Construct: "router (2-way)",
		Note: "predicate to true/false edges",
	},
	"route": {
		Shape: "route", Support: Mapped, Construct: "router (n-way)",
		Note: "value-based fan-out",
	},
	"map": {
		Shape: "map", Support: Mapped, Construct: "map transform",
		Note: "field pairs lower onto the declarative mapper; map FUNCTIONS are assessed separately (see the functions section)",
	},
	"returndocuments": {
		Shape: "returndocuments", Support: Mapped, Construct: "@response sink",
		Note: "synchronous reply terminal",
	},
	"note": {
		Shape: "note", Support: Mapped, Construct: "(none — annotation)",
		Note: "canvas annotation, carries no behavior",
	},

	// --- divergent: expressible, but the semantics differ ---------------
	"branch": {
		Shape: "branch", Support: Divergent, Construct: "tee",
		Note: "DIVERGENCE: Boomi runs branches SEQUENTIALLY (branch 1 to completion, then branch 2); SHIFT tee runs them CONCURRENTLY. Safe when branches are independent; a branch that depends on an earlier branch's side effect must be re-authored as a chain (ADR-0032 §4)",
	},
	"catcherrors": {
		Shape: "catcherrors", Support: Divergent, Construct: "onFailure edge",
		Note:    "try/catch maps to the error edge; the canonical error record and forced-stop semantics are ADR-0031 and not yet built, so handler bodies may need review",
		Blocker: "flow error model (ADR-0031)",
	},

	// --- needs-manual: designed, not built ------------------------------
	"documentproperties": {
		Shape: "documentproperties", Support: NeedsManual, Construct: "flow variables",
		Note:    "sets document/process properties read by later shapes; SHIFT has no flow-variable primitive yet",
		Blocker: "flow variables",
	},
	"message": {
		Shape: "message", Support: NeedsManual, Construct: "set-value / template transform",
		Note:    "builds a document from a template with parameters, which are usually process properties or execution metadata; needs the template transform and, for parameters, flow variables",
		Blocker: "flow variables",
	},
	"stop": {
		Shape: "stop", Support: NeedsManual, Construct: "@stop terminal",
		Note:    "deliberate early end AS SUCCESS; the terminal is designed but unbuilt",
		Blocker: "@stop terminal (ADR-0031)",
	},
	"processcall": {
		Shape: "processcall", Support: NeedsManual, Construct: "subflow step",
		Note:    "calls another process; the subflow step type is reserved in flowdoc but unbuilt (ADR-0017)",
		Blocker: "subflow step",
	},
	"processroute": {
		Shape: "processroute", Support: NeedsManual, Construct: "subflow step (dynamic)",
		Note:    "dynamically routes to one of several processes; needs subflow plus dynamic selection",
		Blocker: "subflow step",
	},
	"doccacheload": {
		Shape: "doccacheload", Support: NeedsManual, Construct: "document cache",
		Note:    "stores documents for later lookup within a run; SHIFT has no cache primitive (a keyed join covers some uses)",
		Blocker: "document cache",
	},
	"doccacheretrieve": {
		Shape: "doccacheretrieve", Support: NeedsManual, Construct: "document cache / lookup",
		Note:    "retrieves cached documents by key; some uses lower onto a merge join, others need a real cache",
		Blocker: "document cache",
	},
	"doccacheremove": {
		Shape: "doccacheremove", Support: NeedsManual, Construct: "document cache",
		Note:    "evicts from the cache",
		Blocker: "document cache",
	},
	"notify": {
		Shape: "notify", Support: NeedsManual, Construct: "log / notify sink",
		Note:    "writes to the Boomi process log; needs a logging sink to land somewhere honest rather than being dropped",
		Blocker: "log sink",
	},
	"exception": {
		Shape: "exception", Support: NeedsManual, Construct: "fail terminal",
		Note:    "raises a process error deliberately; needs the ADR-0031 canonical error to be raisable by an author",
		Blocker: "flow error model (ADR-0031)",
	},
	"flowcontrol": {
		Shape: "flowcontrol", Support: NeedsManual, Construct: "(parallelism / batching control)",
		Note:    "controls threading and batching of the document set; SHIFT governs concurrency by resource signals (ADR-0005), so most uses have no equivalent and should simply be dropped — but that is an author decision, not ours",
		Blocker: "review (usually droppable)",
	},

	// --- unsupported: re-author required --------------------------------
	"dataprocess": {
		Shape: "dataprocess", Support: Unsupported, Construct: "",
		Note:    "custom Groovy/JavaScript, or split/combine of the document set; scripting awaits the starlark/python steps (ADR-0017), and split/combine sometimes lowers onto flatten/aggregate — inspect each one",
		Blocker: "custom code steps (ADR-0017)",
	},
	"businessrules": {
		Shape: "businessrules", Support: Unsupported, Construct: "",
		Note:    "Boomi's rules engine has no SHIFT equivalent; simple rule sets can be re-authored as filter/router chains",
		Blocker: "manual re-author",
	},
	"cleanse": {
		Shape: "cleanse", Support: Unsupported, Construct: "",
		Note:    "field-level validation/cleansing; simple rules re-author onto filter/coerce, the rest is manual",
		Blocker: "manual re-author",
	},
}

// Lookup returns the capability for a Boomi shape type. An unknown shape is
// reported as unsupported rather than ignored — a shape this analyzer has
// never seen is exactly the thing a migration estimate must not omit.
func Lookup(shapeType string) Capability {
	if c, ok := capabilities[shapeType]; ok {
		return c
	}
	return Capability{
		Shape: shapeType, Support: Unsupported,
		Note:    "shape type not yet assessed by this analyzer",
		Blocker: "assessment",
	}
}

// Capabilities returns the whole table, shape-sorted (for docs and tests).
func Capabilities() []Capability {
	out := make([]Capability, 0, len(capabilities))
	for _, c := range capabilities {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Shape < out[j].Shape })
	return out
}
