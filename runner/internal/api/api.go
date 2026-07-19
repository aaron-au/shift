// Package api exposes the runner's HTTP surface: the task/benchmark API
// and the embedded dashboard. Local intake per ADR-0008 — unauthenticated
// today because it binds loopback by default; hub-issued identity arrives
// with M4 and MUST land before any non-local bind ships.
package api

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/aaron-au/shift/runner/internal/flow"
	"github.com/aaron-au/shift/runner/internal/service"
)

//go:embed ui.html
var uiHTML []byte

// Handler builds the runner's HTTP mux. hubStatus is optional: when the
// hub lease intake is running it supplies the intake snapshot for
// /api/status (nil = local-only runner).
func Handler(svc *service.Service, runnerName, version string, started time.Time, hubStatus func() any) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		body := map[string]any{
			"name":     runnerName,
			"version":  version,
			"uptime_s": time.Since(started).Seconds(),
			"status":   svc.Status(),
		}
		if hubStatus != nil {
			body["hub"] = hubStatus()
		}
		writeJSON(w, http.StatusOK, body)
	})

	mux.HandleFunc("POST /api/flows/execute", func(w http.ResponseWriter, r *http.Request) {
		doc, err := decodeFlow(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		id, err := svc.Submit(doc, false)
		if err != nil {
			writeErr(w, http.StatusUnprocessableEntity, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"task_id": id})
	})

	mux.HandleFunc("GET /api/tasks", func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = n
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"tasks": svc.Tasks(limit)})
	})

	mux.HandleFunc("GET /api/tasks/{id}", func(w http.ResponseWriter, r *http.Request) {
		t, ok := svc.Task(r.PathValue("id"))
		if !ok {
			writeErr(w, http.StatusNotFound, fmt.Errorf("unknown task"))
			return
		}
		writeJSON(w, http.StatusOK, t)
	})

	mux.HandleFunc("POST /api/benchmark", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Records int64 `json:"records"`
			Streams int   `json:"streams"`
		}
		if r.ContentLength > 0 {
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
		}
		// Run asynchronously; the dashboard polls status for completion.
		go func() { _, _ = svc.RunBenchmark(req.Records, req.Streams) }()
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "benchmark started"})
	})

	mux.HandleFunc("GET /api/benchmark", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"history": svc.BenchHistory()})
	})

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(uiHTML)
	})

	return mux
}

func decodeFlow(r *http.Request) (*flow.Document, error) {
	defer func() { _ = r.Body.Close() }()
	var raw json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 4<<20)).Decode(&raw); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	return flow.Parse(raw)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
