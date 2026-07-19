package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aaron-au/shift/hub/internal/scheduler"
	"github.com/aaron-au/shift/hub/internal/store"
)

// putSchedule creates/replaces the flow's cron schedule.
func (a *api) putSchedule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req struct {
		Cron        string `json:"cron"`
		Enabled     *bool  `json:"enabled"`
		MaxAttempts int    `json:"max_attempts"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := scheduler.ParseCron(req.Cron); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	// A schedule on a never-published flow would only accumulate
	// last_error noise — reject it up front.
	f, _, err := a.st.GetFlow(r.Context(), name, 0)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if errors.Is(err, store.ErrNotPublished) {
		writeErr(w, http.StatusConflict, fmt.Errorf("flow %q has no published version to schedule", name))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	next, err := scheduler.NextAfter(req.Cron, time.Now())
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	sc, err := a.st.UpsertSchedule(r.Context(), f.Name, req.Cron, enabled, req.MaxAttempts, next)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "schedule.put", name,
		map[string]any{"cron": req.Cron, "enabled": enabled})
	writeJSON(w, http.StatusCreated, map[string]any{"schedule": sc})
}

func (a *api) getSchedule(w http.ResponseWriter, r *http.Request) {
	sc, err := a.st.GetSchedule(r.Context(), r.PathValue("name"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": sc})
}

func (a *api) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	err := a.st.DeleteSchedule(r.Context(), name)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "schedule.delete", name, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) listSchedules(w http.ResponseWriter, r *http.Request) {
	scs, err := a.st.Schedules(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": scs})
}
