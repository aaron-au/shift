package api

import (
	"encoding/csv"
	"net/http"
	"strconv"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// listAudit serves GET /api/v1/audit (M6b): account-scoped audit rows,
// newest-first, keyset-paginated by descending id. Filters: action (exact,
// or a trailing-dot family prefix like "secret."), actor, entity, before
// (id cursor), limit. `?format=csv` streams a CSV export.
func (a *api) listAudit(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.AuditFilter{
		Action: q.Get("action"),
		Actor:  q.Get("actor"),
		Entity: q.Get("entity"),
	}
	if v := q.Get("before"); v != "" {
		f.BeforeID, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := q.Get("limit"); v != "" {
		f.Limit, _ = strconv.Atoi(v)
	}
	entries, err := a.st.ListAudit(r.Context(), f)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if q.Get("format") == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", `attachment; filename="audit.csv"`)
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"id", "at", "actor", "action", "entity", "detail"})
		for _, e := range entries {
			_ = cw.Write([]string{
				strconv.FormatInt(e.ID, 10), e.At.UTC().Format(time.RFC3339),
				e.Actor, e.Action, e.Entity, string(e.Detail),
			})
		}
		cw.Flush()
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"audit": entries})
}
