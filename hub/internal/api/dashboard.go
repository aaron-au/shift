package api

import (
	_ "embed"
	"net/http"

	"github.com/aaron-au/shift/hub/internal/scheduler"
)

// The dashboard is a single dependency-free page (runner pattern): it
// polls the JSON API with the viewer's credential. The page itself is
// static and safe to serve unauthenticated; every data endpoint stays
// behind auth.
//
//go:embed ui.html
var uiHTML []byte

func (a *api) dashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(uiHTML)
}

// stats feeds the dashboard's overview strip.
func (a *api) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.st.Stats(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := map[string]any{
		"stats":      st,
		"oidc":       a.opts.OIDC != nil,
		"oidc_login": a.opts.OIDCFlow != nil,
	}
	if a.opts.SchedStatus != nil {
		out["scheduler"] = a.opts.SchedStatus()
	} else {
		out["scheduler"] = scheduler.Status{}
	}
	writeJSON(w, http.StatusOK, out)
}
