package flowdoc

import (
	"sort"
	"strings"
	"sync"
)

// Design-time review: the advisory half of flow validation (ADR-0042 §7).
//
// Validate answers "will this run". Review answers "will this do what you
// think" — a flow with no @response is perfectly valid and deploys happily,
// and the developer who assumed their caller would get a result finds out in
// production. Notices carry that class of fact to the studio at deploy and
// publish, where it is still cheap to change your mind.
//
// A notice NEVER blocks. The moment an advisory can fail a deploy it stops
// being advice and becomes validation that lives in the wrong file, so the
// separation is structural: Review has no error return and no caller may
// derive one from it.
//
// Checks are REGISTERED rather than listed, so adding or retiring one is a
// file, not an edit to a switch that every future check has to thread through.
// A deployment can register its own — a cloud hub that forbids anonymous
// webhooks can say so at design time in its own build, without that policy
// leaking into the shared model.

// Severity ranks a notice. Deliberately two values: a third invites the
// question "does error block?", and the answer must stay no.
type Severity string

const (
	// SeverityInfo states a consequence the author probably intended.
	SeverityInfo Severity = "info"

	// SeverityWarn states a consequence the author probably did not intend.
	SeverityWarn Severity = "warn"
)

// Notice is one advisory fact about a document.
type Notice struct {
	// Code is stable and machine-readable, namespaced by its check
	// ("async-response", "async-response.no-status"). The studio keys
	// dismissals and badges off it, so it outlives any wording change.
	Code string `json:"code"`

	Severity Severity `json:"severity"`

	// Title is the one-line form, shown on a badge or in a list.
	Title string `json:"title"`

	// Detail is what the author needs in order to decide, in their terms:
	// what will happen, and what to do instead if that is not what they want.
	Detail string `json:"detail,omitempty"`

	// Step names the node to badge, when a notice has one. Empty means the
	// notice is about the flow as a whole.
	Step string `json:"step,omitempty"`

	// Docs points at the ADR or page that explains the behaviour.
	Docs string `json:"docs,omitempty"`
}

// Check is one registered reviewer.
type Check struct {
	// Code is the check's identity and the namespace its notices live in.
	Code string

	// Summary is one line describing what the check looks for. It is served
	// alongside the checks so the studio can explain a notice's provenance
	// without a second source of truth.
	Summary string

	// Fn inspects a VALIDATED document and returns any notices. It must not
	// mutate the document and must be safe to call concurrently — the hub
	// reviews one document per request, on whatever goroutine that request
	// landed on.
	Fn func(*Document) []Notice
}

var (
	registryMu sync.RWMutex
	registry   []Check
	frozen     bool
)

// RegisterCheck adds a check. Call it from init() or from process startup,
// before serving.
//
// It panics on a duplicate code or on registration after the first Review:
// both are programming errors, and a checkset that changes underneath a
// running hub would mean two identical documents reviewed differently
// depending on when they arrived.
func RegisterCheck(c Check) {
	if c.Code == "" || c.Fn == nil {
		panic("flowdoc: RegisterCheck needs a code and a function")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if frozen {
		panic("flowdoc: RegisterCheck after the first Review: register from init() or startup")
	}
	for _, existing := range registry {
		if existing.Code == c.Code {
			panic("flowdoc: duplicate review check " + c.Code)
		}
	}
	registry = append(registry, c)
}

// Checks returns the registered checks, ordered by code.
func Checks() []Check {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := append([]Check(nil), registry...)
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

// Review runs every check over a document and returns its notices, ordered
// warnings-first and then by code — the order they should be read in, decided
// once here rather than in each of the two clients that render them.
//
// A nil document reviews to nothing rather than panicking: Review is called on
// the deploy path next to a parse, and an advisory pass must never be the
// thing that takes the hub down.
func Review(d *Document) []Notice {
	if d == nil {
		return nil
	}
	registryMu.Lock()
	frozen = true
	checks := append([]Check(nil), registry...)
	registryMu.Unlock()

	var out []Notice
	for _, c := range checks {
		for _, n := range c.Fn(d) {
			if n.Code == "" {
				n.Code = c.Code
			} else if !strings.HasPrefix(n.Code, c.Code) {
				// A notice that escapes its check's namespace breaks the one
				// thing codes are for: tracing a message back to the rule that
				// produced it. Re-root it rather than serving a lie.
				n.Code = c.Code + "." + n.Code
			}
			if n.Severity == "" {
				n.Severity = SeverityInfo
			}
			out = append(out, n)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Severity != out[j].Severity {
			return out[i].Severity == SeverityWarn
		}
		return out[i].Code < out[j].Code
	})
	return out
}

// ReviewRaw parses and reviews a document, ignoring a parse failure: an
// unparseable document has already been refused by validation, and Review is
// never the component that reports that.
func ReviewRaw(raw []byte) []Notice {
	d, err := Parse(raw)
	if err != nil {
		return nil
	}
	return Review(d)
}
