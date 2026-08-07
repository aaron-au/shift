package api

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Connector end-of-life (ADR-0047 §7).
//
// Retention cannot touch a version a live flow pins — that is what makes GC
// safe — which leaves one gap: a build that is genuinely poisoned and still in
// use. EOL is the deliberate act that closes it, and it is reserved for
// SECURITY. The routine upgrade path is §4 and §5.
//
// The shape is announced, escalating, and then loud:
//
//  1. an administrator sets a deadline, with a reason and where to go instead
//  2. every flow pinning that version carries a notice, sharpening as the date
//     approaches, on both the deploy and publish responses
//  3. at the deadline the version stops resolving and pinned tasks FAIL
//
// They fail; they are not silently upgraded. Swapping a connector underneath
// live customer data without anyone testing it is precisely the risk ADR-0047
// exists to remove, and doing it automatically on a timer would be the same
// mistake with a clock attached.

// maxEOLReason bounds the free text. It is read to a customer, not parsed.
const maxEOLReason = 2 << 10

// setConnectorEOL: POST /api/v1/connectors/{name}/versions/{version}/eol
//
// Body {"eol_at":RFC3339,"reason":…,"target":…}. Omitting eol_at clears a
// scheduled end-of-life instead — declaring one is a human act on a live
// system, and humans mistype version numbers.
func (a *api) setConnectorEOL(w http.ResponseWriter, r *http.Request) {
	name, version := r.PathValue("name"), r.PathValue("version")
	var req struct {
		EOLAt  string `json:"eol_at"`
		Reason string `json:"reason"`
		Target string `json:"target"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.EOLAt == "" {
		if err := a.st.ClearConnectorEOL(r.Context(), name, version); err != nil {
			writeLookupErr(w, err)
			return
		}
		_ = a.st.Audit(r.Context(), actor(r), "connector.eol-clear", name, map[string]any{"version": version})
		slog.Warn("scheduled connector end-of-life withdrawn",
			"event", "hub.connector.eol_cleared", "connector", name, "version", version)
		writeJSON(w, http.StatusOK, map[string]any{"name": name, "version": version, "eol_at": nil})
		return
	}
	deadline, err := time.Parse(time.RFC3339, req.EOLAt)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("eol_at must be RFC3339: %w", err))
		return
	}
	if len(req.Reason) > maxEOLReason {
		writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("reason exceeds %d bytes", maxEOLReason))
		return
	}
	// A reason is required, not optional. This is a security action whose
	// whole output is a message somebody reads under pressure, and "it was end
	// of lifed" answers nothing.
	if req.Reason == "" {
		writeErr(w, http.StatusUnprocessableEntity,
			errors.New("reason is required: it is what a customer is told when their flow stops"))
		return
	}
	if err := a.st.SetConnectorEOL(r.Context(), name, version, deadline, req.Reason, req.Target); err != nil {
		writeLookupErr(w, err)
		return
	}
	refs, _ := a.st.ConnectorReferences(r.Context(), name, version)
	_ = a.st.Audit(r.Context(), actor(r), "connector.eol", name, map[string]any{
		"version": version, "eol_at": deadline.UTC().Format(time.RFC3339),
		"reason": req.Reason, "target": req.Target, "flows": len(refs),
	})
	// Loud in the log as well as the response: this is the one registry action
	// that will stop working flows, and it should be findable afterwards
	// without reading the audit table.
	slog.Warn("connector version scheduled for end of life",
		"event", "hub.connector.eol_set", "connector", name, "version", version,
		"eol_at", deadline.UTC().Format(time.RFC3339), "flows_affected", len(refs))

	writeJSON(w, http.StatusOK, map[string]any{
		"name": name, "version": version,
		"eol_at": deadline.UTC().Format(time.RFC3339),
		"reason": req.Reason, "target": req.Target,
		// Named, not counted: the next step is telling these people.
		"flows": refs,
	})
}

// listEOLs: GET /api/v1/connectors/eol — every scheduled end-of-life with the
// flows it will take down, soonest first.
func (a *api) listEOLs(w http.ResponseWriter, r *http.Request) {
	out, err := a.st.ScheduledEOLs(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"eol": out})
}

// eolNotices reports the scheduled end-of-life on every pinned version.
//
// Severity is always warn, and the WORDING escalates rather than the level.
// flowdoc has two severities on purpose ("a third invites the question of what
// the middle one means"), so urgency belongs in the sentence somebody reads,
// not in a level nobody can rank.
func (a *api) eolNotices(r *http.Request, doc *flowdoc.Document) []flowdoc.Notice {
	if doc == nil {
		return nil
	}
	var out []flowdoc.Notice
	seen := map[string]bool{}
	for _, p := range doc.ConnectorPins() {
		key := p.Connector + "@" + p.Version
		if p.Version == "" || seen[key] {
			continue
		}
		seen[key] = true
		e, err := a.st.EOLFor(r.Context(), p.Connector, p.Version)
		if err != nil || e == nil {
			continue
		}
		out = append(out, eolNotice(p, e, time.Now()))
	}
	return out
}

// eolNotice writes the sentence for one pinned, dying version.
func eolNotice(p flowdoc.Pin, e *store.EOLVersion, now time.Time) flowdoc.Notice {
	target := "a supported version"
	if e.Target != "" {
		target = e.Connector + " " + e.Target
	}
	var title, urgency string
	switch days := int(e.EOLAt.Sub(now).Hours() / 24); {
	case e.Passed:
		title = fmt.Sprintf("%s %s has reached end of life", e.Connector, e.Version)
		urgency = fmt.Sprintf("This flow CANNOT RUN: the version stopped resolving on %s, and every task fails at its first step.",
			e.EOLAt.UTC().Format("2 Jan 2006"))
	case days <= 7:
		title = fmt.Sprintf("%s %s stops working in %d day(s)", e.Connector, e.Version, max(days, 0))
		urgency = fmt.Sprintf("On %s this flow STOPS RUNNING — every task will fail at its first step.",
			e.EOLAt.UTC().Format("2 Jan 2006"))
	default:
		title = fmt.Sprintf("%s %s is scheduled for end of life", e.Connector, e.Version)
		urgency = fmt.Sprintf("On %s (%d days) this flow stops running.",
			e.EOLAt.UTC().Format("2 Jan 2006"), days)
	}
	detail := fmt.Sprintf("%s Reason: %s. Republish the flow against %s — it will not be upgraded for you, "+
		"because swapping a connector underneath live data without anyone testing it is the risk this exists to avoid.",
		urgency, e.Reason, target)
	return flowdoc.Notice{
		Code:     "connector-eol.scheduled",
		Severity: flowdoc.SeverityWarn,
		Title:    title,
		Detail:   detail,
		Step:     p.StepID,
		Docs:     "docs/adr/0047-connector-versioning-and-retention.md",
	}
}

// deadPins reports pins whose end-of-life has already passed.
//
// Publishing one produces a flow that cannot run — every task fails at its
// first step — so publish refuses and says where to go instead. This refusal
// applies to a ROLLBACK too, unlike the currency gate (§4): rolling back to a
// version whose connector no longer resolves does not give anybody a working
// flow, so blocking it costs nothing and telling them now beats a task failure
// in an hour.
func (a *api) deadPins(r *http.Request, doc *flowdoc.Document) []string {
	if doc == nil {
		return nil
	}
	var dead []string
	for _, p := range doc.ConnectorPins() {
		if p.Version == "" {
			continue
		}
		e, err := a.st.EOLFor(r.Context(), p.Connector, p.Version)
		if err != nil || e == nil || !e.Passed {
			continue
		}
		msg := fmt.Sprintf("step %q pins %s %s, which reached end of life on %s (%s)",
			p.StepID, p.Connector, p.Version, e.EOLAt.UTC().Format("2 Jan 2006"), e.Reason)
		if e.Target != "" {
			msg += " — move it to " + e.Target
		}
		dead = append(dead, msg)
	}
	return dead
}

// deadPinsFor loads a stored version and reports its dead pins.
func (a *api) deadPinsFor(r *http.Request, name string, version int) []string {
	_, raw, err := a.st.GetFlow(r.Context(), name, version)
	if err != nil {
		return nil // publish reports a missing flow or version itself
	}
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		return nil // an unparseable stored document is publish's problem
	}
	return a.deadPins(r, doc)
}
