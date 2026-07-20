package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/aaron-au/shift/hub/internal/store"
)

// hashToken returns the hex SHA-256 of a webhook token (the form stored and
// synced; the runner hashes the presented token the same way to verify).
func hashToken(tok string) string {
	if tok == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// putWebhook creates/replaces a hub-authored webhook (admin). The token is
// accepted in plaintext and stored only as a hash.
func (a *api) putWebhook(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FlowName string `json:"flow_name"`
		Token    string `json:"token"`
		Enabled  *bool  `json:"enabled"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.FlowName == "" {
		writeErr(w, http.StatusUnprocessableEntity, errors.New("flow_name is required"))
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	err := a.st.UpsertWebhook(r.Context(), store.Webhook{
		Name: r.PathValue("name"), FlowName: req.FlowName,
		TokenHash: hashToken(req.Token), Enabled: enabled,
	})
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusUnprocessableEntity, errors.New("unknown flow "+req.FlowName))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "webhook.upsert", r.PathValue("name"), map[string]any{"flow": req.FlowName, "protected": req.Token != ""})
	writeJSON(w, http.StatusOK, map[string]any{"name": r.PathValue("name"), "protected": req.Token != ""})
}

func (a *api) listWebhooks(w http.ResponseWriter, r *http.Request) {
	hooks, err := a.st.Webhooks(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// Present metadata only — a "protected" flag, never the token hash.
	out := make([]map[string]any, 0, len(hooks))
	for _, h := range hooks {
		out = append(out, map[string]any{
			"name": h.Name, "flow_name": h.FlowName, "enabled": h.Enabled,
			"protected": h.TokenHash != "", "updated_at": h.Updated,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"webhooks": out})
}

func (a *api) deleteWebhook(w http.ResponseWriter, r *http.Request) {
	err := a.st.DeleteWebhook(r.Context(), r.PathValue("name"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "webhook.delete", r.PathValue("name"), nil)
	w.WriteHeader(http.StatusNoContent)
}

// syncWebhooks returns the runnable webhook configs for the calling runner's
// account (runner realm): enabled hooks on published flows, with the token
// hash and the flow document. Metadata + document only — no payload.
func (a *api) syncWebhooks(w http.ResponseWriter, r *http.Request) {
	cfgs, err := a.st.EnabledWebhookConfigs(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"webhooks": cfgs})
}
