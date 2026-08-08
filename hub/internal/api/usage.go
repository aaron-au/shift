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

// headerParam renders the RFC 4180 `header` media-type parameter.
func headerParam(present bool) string {
	if present {
		return "present"
	}
	return "absent"
}

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
		// RFC 4180 makes the header row optional and signals its presence in the
		// MEDIA TYPE, not in the body — without the parameter a strict consumer
		// cannot tell whether row 1 is data. So say which one we sent.
		//
		// `?header=absent` exists because this endpoint is PAGINATED: a consumer
		// looping `curl >> usage.csv` over the cursor would otherwise get a
		// header row interleaved at every page boundary. Each page is a valid
		// standalone document either way; this makes the obvious ingest pattern
		// correct too. Column order is identical in both modes — it is the
		// positional contract whether or not the names are sent with it.
		header := q.Get("header") != "absent"
		w.Header().Set("Content-Type", "text/csv; charset=utf-8; header="+headerParam(header))
		w.Header().Set("Content-Disposition", `attachment; filename="usage.csv"`)
		// A headless page still needs the cursor the JSON body carries. `id` is
		// column 0 so it is derivable, but a consumer should not have to parse
		// the payload to page: 0 means caught up, same as JSON's `next`.
		w.Header().Set("X-Shift-Next-Cursor", strconv.FormatInt(next, 10))
		cw := csv.NewWriter(w)
		if header {
			// "test" is a column, not a filter: the billing platform must be able
			// to exclude those rows itself (ADR-0048 §4), and dropping them here
			// would also hide the usage §4 exists to make visible. It is appended
			// LAST deliberately — a new name is ignored by name-based consumers
			// and leaves indices 0..7 untouched for positional ones. Inserting a
			// column mid-list is the shape that breaks a reader silently.
			_ = cw.Write([]string{"id", "at", "source", "flow_name", "outcome", "records_in", "records_out", "exec_seconds", "test"})
		}
		for _, e := range events {
			_ = cw.Write([]string{
				strconv.FormatInt(e.ID, 10), e.At.UTC().Format(time.RFC3339),
				csvSafe(e.Source), csvSafe(e.FlowName), csvSafe(e.Outcome),
				strconv.FormatInt(e.RecordsIn, 10), strconv.FormatInt(e.RecordsOut, 10),
				strconv.FormatFloat(e.ExecSeconds, 'f', 3, 64),
				strconv.FormatBool(e.Test),
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
