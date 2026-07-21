package api

import (
	"encoding/csv"
	"net/http"
	"strconv"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// defaultUsageWindow is the range served when the caller gives no since/until.
const defaultUsageWindow = 30 * 24 * time.Hour

// usageReport serves GET /api/v1/usage (M6d): the account-scoped usage rollup
// (totals + per-flow + daily series) over [since, until). Both bounds are
// optional RFC3339; default is the last 30 days. Metadata only — counts and
// seconds, never payload. The hub is task control, not the billing platform;
// this is operational visibility over the metering substrate.
func (a *api) usageReport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	until := time.Now().UTC()
	if v := q.Get("until"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		until = t
	}
	since := until.Add(-defaultUsageWindow)
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		since = t
	}
	rep, err := a.st.Usage(r.Context(), since, until)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// usageEventsExport serves GET /api/v1/usage/events (M6d): the cursor-based
// incremental pull the external billing platform ingests. `?since_id=` is the
// last id already consumed (exclusive); rows come back in id order, bounded by
// `?limit=` (<=1000). `next` is the cursor for the following page (0 when the
// page was not full — caller is caught up). `?format=csv` streams instead.
func (a *api) usageEventsExport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var sinceID int64
	if v := q.Get("since_id"); v != "" {
		sinceID, _ = strconv.ParseInt(v, 10, 64)
	}
	limit := 1000
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	events, err := a.st.UsageEventsSince(r.Context(), sinceID, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// next cursor: the last id when the page filled, else 0 (caught up).
	var next int64
	if len(events) == limit && len(events) > 0 {
		next = events[len(events)-1].ID
	}

	if q.Get("format") == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", `attachment; filename="usage.csv"`)
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"id", "at", "source", "flow_name", "outcome", "records_in", "records_out", "exec_seconds"})
		for _, e := range events {
			_ = cw.Write([]string{
				strconv.FormatInt(e.ID, 10), e.At.UTC().Format(time.RFC3339),
				csvSafe(e.Source), csvSafe(e.FlowName), csvSafe(e.Outcome),
				strconv.FormatInt(e.RecordsIn, 10), strconv.FormatInt(e.RecordsOut, 10),
				strconv.FormatFloat(e.ExecSeconds, 'f', 3, 64),
			})
		}
		cw.Flush()
		return
	}
	if events == nil {
		events = []store.UsageEvent{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events, "next": next})
}
