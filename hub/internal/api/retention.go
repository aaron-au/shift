package api

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/aaron-au/shift/hub/internal/store"
)

// Connector retention (ADR-0047 §2/§3).
//
// Two verbs that are easy to conflate and must not be: yank changes what gets
// SELECTED for new pins and takes effect at once; collection DELETES bytes and
// only ever touches builds no live flow runs.

// connectorReferences: GET /api/v1/connectors/{name}/versions/{version}/references
//
// The question behind "is this safe to yank / EOL / delete?", answered by name
// rather than by count — an operator needs to know WHICH flows, because the
// next step is telling someone.
func (a *api) connectorReferences(w http.ResponseWriter, r *http.Request) {
	name, version := r.PathValue("name"), r.PathValue("version")
	if !a.opts.ConnectorPolicy.Allowed(name) {
		writeErr(w, http.StatusNotFound, store.ErrNotFound) // hidden by policy
		return
	}
	refs, err := a.st.ConnectorReferences(r.Context(), name, version)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name": name, "version": version, "references": refs,
	})
}

// collectConnectors: POST /api/v1/connectors/collect
//
// Without ?apply=1 this REPORTS and deletes nothing. That default is the point:
// the publisher's private key is not held server-side (ADR-0011), so a deleted
// artifact cannot be regenerated from anything the hub has. A destructive
// default would make one mistyped call unrecoverable.
func (a *api) collectConnectors(w http.ResponseWriter, r *http.Request) {
	apply := r.URL.Query().Get("apply") == "1"
	if !apply {
		refs, err := a.st.CollectableConnectorVersions(r.Context())
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"applied": false, "versions": refs})
		return
	}
	collected, err := a.st.CollectConnectorVersions(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	names := make([]string, 0, len(collected))
	for _, c := range collected {
		names = append(names, c.String())
	}
	// Audited by content, not by count: "collected 4 versions" is not an
	// answer to "which build went missing?" six months later.
	_ = a.st.Audit(r.Context(), actor(r), "connector.collect", "",
		map[string]any{"versions": names})
	if len(collected) > 0 {
		slog.Warn("collected unreferenced connector artifacts",
			"event", "hub.connector.collected", "versions", len(collected))
	}
	writeJSON(w, http.StatusOK, map[string]any{"applied": true, "versions": collected})
}

// yankReferences is what a yank response carries: the published flows that
// still run the version being withdrawn.
//
// Yanking does not stop them — that is what makes yank safe (ADR-0047 §3) —
// but the person yanking almost always believes it does, and finding out from
// this response beats finding out from a support ticket.
func (a *api) yankReferences(r *http.Request, name, version string) []store.FlowRef {
	refs, err := a.st.ConnectorReferences(r.Context(), name, version)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		slog.Error("reading connector references for a yank",
			"event", "hub.connector.references_failed", "connector", name, "error", err.Error())
		return nil
	}
	if len(refs) > 0 {
		slog.Warn("a yanked connector version is still pinned by published flows",
			"event", "hub.connector.yank_referenced",
			"connector", name, "version", version, "flows", len(refs))
	}
	return refs
}
