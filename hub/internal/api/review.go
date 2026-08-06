package api

import (
	"io"
	"net/http"
	"strconv"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Design-time review, served (ADR-0042 §7).
//
// Notices ride on the deploy and publish responses, so a developer is told what
// their flow will DO at the two moments they are deciding to ship it, and
// reviewFlow answers the same question for an unsaved canvas.
//
// The hub adds no judgement of its own here: it parses, calls flowdoc.Review
// and serialises. A rule that only the hub knows would be a rule the runner
// and the CLI silently disagree with.

// reviewFlow reviews a document that has not been stored (POST /api/v1/flows/review).
// The studio calls it as the canvas changes, so a warning appears while the
// author is still editing rather than after they hit deploy.
func (a *api) reviewFlow(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	doc, err := flowdoc.Parse(raw)
	if err != nil {
		// Validation still reports validation. Review never becomes a second,
		// softer place to learn a document is broken.
		writeErrCode(w, http.StatusUnprocessableEntity, "flow_invalid", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"notices": flowdoc.Review(doc)})
}

// reviewStoredFlow reviews a stored version (GET /api/v1/flows/{name}/review,
// ?version=N for a specific one). It is what the studio calls when it opens a
// flow it did not just author.
func (a *api) reviewStoredFlow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version, _ := strconv.Atoi(r.URL.Query().Get("version"))
	if version <= 0 {
		// Default to the LATEST version, not the published one: reviewing a
		// draft is how you decide whether to publish it, and defaulting to
		// "published" would make the endpoint useless on exactly the flow the
		// developer is looking at.
		f, err := a.st.FlowByName(r.Context(), name)
		if err != nil {
			writeLookupErr(w, err)
			return
		}
		version = f.LatestVersion
	}
	_, raw, err := a.st.GetFlow(r.Context(), name, version)
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"notices": flowdoc.ReviewRaw(raw)})
}

// listReviewChecks serves the registered checks (GET /api/v1/review-checks) so
// the studio can explain where a notice came from, and so a deployment that
// registers its own check does not need a studio change to describe it.
func (a *api) listReviewChecks(w http.ResponseWriter, _ *http.Request) {
	cs := flowdoc.Checks()
	out := make([]map[string]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, map[string]string{"code": c.Code, "summary": c.Summary})
	}
	writeJSON(w, http.StatusOK, map[string]any{"checks": out})
}

// noticesFor reviews a stored version for a response that is primarily about
// something else (deploy, publish). Best-effort by construction: a document
// that has just been stored parses, and if it somehow does not, the deploy is
// still what happened and is still what gets reported.
func (a *api) noticesFor(r *http.Request, name string, version int) []flowdoc.Notice {
	_, raw, err := a.st.GetFlow(r.Context(), name, version)
	if err != nil {
		return nil
	}
	return flowdoc.ReviewRaw(raw)
}
